#!/usr/bin/env python3
"""Checks for event-daemon.py: consolidation window, and the rclone commands built.

Run: python3 test_event_daemon.py   (asserts only, no framework, no network)
"""
import importlib.util
import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("event_daemon", os.path.join(HERE, "event-daemon.py"))
ed = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ed)

DAY = "Uploads/Node A/2026-08-13"
FILE = DAY + "/Source A_01_Node_20260813122116.mp4"
FILE_RENAMED = DAY + "/Source A_01_Node_20260813122115.mp4"
FILE_RENAMED2 = DAY + "/Source A_01_Node_20260813122114.mp4"


class FakeRunner:
    def __init__(self):
        self.calls = []

    def run(self, argv):
        self.calls.append(argv)
        return True


class FakeClock:
    def __init__(self):
        self.t = 1_000_000.0

    def __call__(self):
        return self.t

    def tick(self, seconds):
        self.t += seconds


def local_tree(files):
    root = tempfile.mkdtemp()
    for rel, size in files.items():
        p = os.path.join(root, rel)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "wb") as fh:
            fh.write(b"x" * size)
    return root


def daemon(files, clock=None):
    clock = clock or FakeClock()
    d = ed.Daemon(local_tree(files), "backup:", FakeRunner(), clock=clock)
    return d, clock


# ---- consolidation window -------------------------------------------------

def test_upload_is_held_until_the_window_goes_quiet():
    d, clock = daemon({FILE: 100})
    d.on_upload("/" + FILE)
    assert d.ready_jobs() == [], "released before the window closed"
    clock.tick(1.0)
    assert d.ready_jobs() == [], "released after 1s, window is 2s"
    clock.tick(1.1)
    assert d.ready_jobs() == [ed.Job(FILE, ())], d.ready_jobs()


def test_rename_after_upload_collapses_into_one_job():
    d, clock = daemon({FILE_RENAMED: 100})
    d.on_upload("/" + FILE)
    clock.tick(0.5)
    d.on_rename("/" + FILE, "/" + FILE_RENAMED)
    clock.tick(2.1)
    assert d.ready_jobs() == [ed.Job(FILE_RENAMED, (FILE,))], d.ready_jobs()


def test_rename_before_upload_collapses_into_one_job():
    """The client renames mid-transfer, so the rename can land first."""
    d, clock = daemon({FILE_RENAMED: 100})
    d.on_rename("/" + FILE, "/" + FILE_RENAMED)
    clock.tick(0.3)
    d.on_upload("/" + FILE)
    clock.tick(2.1)
    assert d.ready_jobs() == [ed.Job(FILE_RENAMED, (FILE,))], d.ready_jobs()


def test_chained_renames_keep_every_old_name_for_deletion():
    d, clock = daemon({FILE_RENAMED2: 100})
    d.on_upload("/" + FILE)
    d.on_rename("/" + FILE, "/" + FILE_RENAMED)
    clock.tick(0.4)
    d.on_rename("/" + FILE_RENAMED, "/" + FILE_RENAMED2)
    clock.tick(2.1)
    assert d.ready_jobs() == [ed.Job(FILE_RENAMED2, (FILE, FILE_RENAMED))], d.ready_jobs()


def test_each_event_restarts_the_quiet_window():
    d, clock = daemon({FILE_RENAMED: 100})
    d.on_upload("/" + FILE)
    clock.tick(1.5)
    d.on_rename("/" + FILE, "/" + FILE_RENAMED)
    clock.tick(1.0)                       # 2.5s since the upload, 1s since the rename
    assert d.ready_jobs() == [], "released while events were still arriving"
    clock.tick(1.2)
    assert d.ready_jobs() == [ed.Job(FILE_RENAMED, (FILE,))], d.ready_jobs()


def test_a_rename_with_no_upload_event_is_never_released():
    """No upload event means no proof the transfer finished."""
    d, clock = daemon({FILE_RENAMED: 100})
    d.on_rename("/" + FILE, "/" + FILE_RENAMED)
    for _ in range(6):
        clock.tick(40)
        assert d.ready_jobs() == [], "released without an upload event"
    assert d.pending_files() == [FILE], d.pending_files()


def test_a_stale_entry_is_dropped_before_the_sweep_could_reach_it():
    """The sweep only considers files older than 5 min, so the daemon lets go
    at 4:50 -- early enough that the two never both act on the same file."""
    assert ed.MAX_AGE == 290, ed.MAX_AGE
    assert ed.MAX_AGE < 300, "must give up before the sweep's --min-age 5m"
    d, clock = daemon({FILE_RENAMED: 100})
    d.on_rename("/" + FILE, "/" + FILE_RENAMED)
    clock.tick(289)
    assert d.pending_files() == [FILE], "let go too early"
    d.ready_jobs()
    clock.tick(2)
    assert d.ready_jobs() == [], d.ready_jobs()
    assert d.pending_files() == [], "stale entry kept past the handover point"


def test_two_files_are_tracked_independently():
    other = DAY + "/Source B_06_Node_20260813122200.jpg"
    d, clock = daemon({FILE_RENAMED: 100, other: 10})
    d.on_upload("/" + FILE)
    d.on_rename("/" + FILE, "/" + FILE_RENAMED)
    clock.tick(1.5)
    d.on_upload("/" + other)               # keeps its own timer
    clock.tick(0.8)
    assert d.ready_jobs() == [ed.Job(FILE_RENAMED, (FILE,))], d.ready_jobs()
    clock.tick(1.5)
    assert d.ready_jobs() == [ed.Job(other, ())], d.ready_jobs()


def test_a_repeated_upload_event_just_extends_the_window():
    d, clock = daemon({FILE: 100})
    d.on_upload("/" + FILE)
    clock.tick(1.0)
    d.on_upload("/" + FILE)
    clock.tick(1.5)
    assert d.ready_jobs() == [], "second event did not restart the window"
    clock.tick(0.6)
    assert d.ready_jobs() == [ed.Job(FILE, ())], d.ready_jobs()


def test_released_entries_leave_the_buffer():
    d, clock = daemon({FILE: 100})
    d.on_upload("/" + FILE)
    clock.tick(2.1)
    assert len(d.ready_jobs()) == 1
    assert d.ready_jobs() == [], "same entry released twice"
    assert d.pending_files() == [], d.pending_files()


# ---- what the jobs run ----------------------------------------------------

def test_plain_upload_job_copies_just_that_file():
    d, _ = daemon({FILE: 100})
    d.handle(ed.Job(FILE, ()))
    argv = d.runner.calls[0]
    assert argv[0] == "copy", argv
    assert argv[1] == os.path.join(d.src, DAY), argv
    assert argv[2] == "backup:/" + DAY, argv
    assert "--size-only" in argv, argv
    includes = [argv[i + 1] for i, a in enumerate(argv) if a == "--include"]
    assert includes == ["/" + os.path.basename(FILE)], includes
    assert "--max-delete" not in argv, "a plain upload must never delete"


def test_renamed_job_syncs_the_final_name_and_clears_the_old_ones():
    d, _ = daemon({FILE_RENAMED: 100})
    d.handle(ed.Job(FILE_RENAMED, (FILE,)))
    argv = d.runner.calls[0]
    assert argv[0] == "sync", argv
    assert argv[1] == os.path.join(d.src, DAY), argv
    includes = [argv[i + 1] for i, a in enumerate(argv) if a == "--include"]
    assert sorted(includes) == sorted(["/" + os.path.basename(FILE_RENAMED),
                                       "/" + os.path.basename(FILE)]), includes
    assert argv[argv.index("--max-delete") + 1] == "1", argv


def test_max_delete_tracks_the_number_of_old_names():
    d, _ = daemon({FILE_RENAMED2: 100})
    d.handle(ed.Job(FILE_RENAMED2, (FILE, FILE_RENAMED)))
    argv = d.runner.calls[0]
    assert argv[argv.index("--max-delete") + 1] == "2", argv


def test_rename_across_directories_falls_back_to_delete_plus_copy():
    old = "Uploads/Node A/2026-08-12/Source A_01_Node_20260812235959.mp4"
    new = "Uploads/Node A/2026-08-13/Source A_01_Node_20260813000000.mp4"
    d, _ = daemon({new: 100})
    d.handle(ed.Job(new, (old,)))
    assert [c[0] for c in d.runner.calls] == ["deletefile", "copyto"], d.runner.calls
    assert d.runner.calls[0][1] == "backup:/" + old, d.runner.calls
    assert d.runner.calls[1][2] == "backup:/" + new, d.runner.calls


def test_a_job_whose_file_vanished_locally_is_left_to_the_sweep():
    """A sync here would delete the backup's copy instead of restoring it."""
    d, _ = daemon({})
    d.handle(ed.Job(FILE_RENAMED, (FILE,)))
    assert d.runner.calls == [], d.runner.calls


def test_filter_specials_are_escaped():
    d, _ = daemon({})
    assert d.include_pattern("Cam [1]_2026*.mp4") == "/Cam \\[1\\]_2026\\*.mp4"


def test_paths_outside_the_source_tree_are_rejected():
    d, _ = daemon({FILE: 10})
    for bad in ("/../../etc/passwd", "/Uploads/../../etc/passwd", ""):
        try:
            d.on_upload(bad)
        except ValueError:
            continue
        raise AssertionError("accepted a path outside the tree: %r" % bad)


def test_a_failing_rclone_call_does_not_kill_the_consumer():
    d, _ = daemon({FILE: 100})

    def boom(argv):
        d.runner.calls.append(argv)
        raise RuntimeError("rclone exploded")

    d.runner.run = boom
    d.handle(ed.Job(FILE, ()))
    d.handle(ed.Job(FILE, ()))
    assert len(d.runner.calls) == 2, d.runner.calls


def main():
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    failed = 0
    for t in tests:
        try:
            t()
            print("PASS", t.__name__)
        except Exception as exc:                      # noqa: BLE001 - report and continue
            failed += 1
            print("FAIL", t.__name__, "->", type(exc).__name__, exc)
    print("%d/%d passed" % (len(tests) - failed, len(tests)))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
