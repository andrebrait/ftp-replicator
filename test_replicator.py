#!/usr/bin/env python3
"""Checks for replicator.py: consolidation window, sweep decisions, reporting.

Run: python3 test_replicator.py   (asserts only, no framework, no network)
"""
import importlib.util
import json
import re
import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("replicator", os.path.join(HERE, "replicator.py"))
ed = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ed)

DAY = "Uploads/Node A/2026-08-13"
FILE = DAY + "/Source A_01_Node_20260813122116.mp4"
FILE_RENAMED = DAY + "/Source A_01_Node_20260813122115.mp4"
FILE_RENAMED2 = DAY + "/Source A_01_Node_20260813122114.mp4"


class FakeRunner:
    def __init__(self):
        self.calls = []
        self.remote = {}

    def stat(self, rel):
        return self.remote.get(rel)

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


# ---- log output --------------------------------------------------------

def emitted(fn):
    """Run fn with stdout captured, return the lines it logged."""
    import io, contextlib
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        fn()
    return [l for l in buf.getvalue().splitlines() if l.strip()]


def test_json_is_the_default_shape_and_carries_fields_not_just_a_sentence():
    line = emitted(lambda: ed.log("queued", event="release", path=FILE, depth=3))[0]
    rec = json.loads(line)
    assert rec["component"] == "replicator", rec
    assert rec["msg"] == "queued", rec
    assert rec["event"] == "release", rec
    assert rec["path"] == FILE, rec
    assert rec["depth"] == 3, rec
    assert rec["level"] == "info", rec
    # ISO-8601 UTC, same instant format the text mode prints
    assert re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$", rec["time"]), rec


def test_a_file_name_with_quotes_and_backslashes_stays_parseable():
    nasty = 'Uploads/od"d\\name_20260813122116.mp4'
    rec = json.loads(emitted(lambda: ed.log("queued", path=nasty))[0])
    assert rec["path"] == nasty, rec


def test_text_mode_prints_the_bracketed_line():
    ed.LOG_JSON = False
    try:
        line = emitted(lambda: ed.log("queued", event="release", path=FILE, depth=3))[0]
    finally:
        ed.LOG_JSON = True
    assert re.match(r"^\[\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z\] replicator: queued", line), line
    assert FILE in line, line          # the fields still have to be readable
    assert "depth=3" in line, line


def test_level_is_carried_through():
    rec = json.loads(emitted(lambda: ed.log("rclone failed", level="error", rc=3))[0])
    assert rec["level"] == "error" and rec["rc"] == 3, rec


# ---- a rename that arrives after its entry was already released ----------

class StatRunner(FakeRunner):
    """Knows what is on the backup, so the late-rename path can consult it."""

    def __init__(self, remote=None):
        FakeRunner.__init__(self)
        self.remote = dict(remote or {})

    def stat(self, rel):
        return self.remote.get(rel)


def test_a_late_rename_moves_the_bytes_already_on_the_backup():
    """Upload released, copied under the old name, and only then the rename
    lands. The bytes are already there, so rename them in place."""
    r = StatRunner(remote={FILE: 100})
    d = ed.Daemon(local_tree({FILE_RENAMED: 100}), "backup:", r, clock=FakeClock())
    clock = d.clock
    d.on_rename("/" + FILE, "/" + FILE_RENAMED)      # no upload event: it went earlier
    clock.tick(ed.MAX_AGE + 1)
    jobs = d.ready_jobs()
    assert jobs == [ed.Job(FILE_RENAMED, (FILE,), "move")], jobs
    d.handle(jobs[0])
    assert r.calls == [["moveto", "backup:/" + FILE, "backup:/" + FILE_RENAMED]], r.calls


def test_a_late_rename_with_nothing_on_the_backup_is_left_to_the_sweep():
    r = StatRunner()
    d = ed.Daemon(local_tree({FILE_RENAMED: 100}), "backup:", r, clock=FakeClock())
    d.on_rename("/" + FILE, "/" + FILE_RENAMED)
    d.clock.tick(ed.MAX_AGE + 1)
    assert d.ready_jobs() == [], "pushed a job with no proof the bytes are there"
    assert d.pending_files() == [], "kept the entry forever"


def test_a_late_rename_whose_backup_copy_is_the_wrong_size_is_left_to_the_sweep():
    """A short copy on the backup must not be renamed into place as if good."""
    r = StatRunner(remote={FILE: 63})
    d = ed.Daemon(local_tree({FILE_RENAMED: 100}), "backup:", r, clock=FakeClock())
    d.on_rename("/" + FILE, "/" + FILE_RENAMED)
    d.clock.tick(ed.MAX_AGE + 1)
    assert d.ready_jobs() == [], "renamed a short file into place"


def test_a_late_rename_whose_file_is_gone_locally_is_left_to_the_sweep():
    r = StatRunner(remote={FILE: 100})
    d = ed.Daemon(local_tree({}), "backup:", r, clock=FakeClock())
    d.on_rename("/" + FILE, "/" + FILE_RENAMED)
    d.clock.tick(ed.MAX_AGE + 1)
    assert d.ready_jobs() == [], d.ready_jobs()


# ---- the sweep -----------------------------------------------------------

class FakeSweepRunner:
    """rclone stand-in for the sweep: scripted check output and stat answers."""

    def __init__(self, missing=(), differ=(), remote=None, fail=()):
        self.missing, self.differ = list(missing), list(differ)
        self.remote = dict(remote or {})
        self.fail = set(fail)
        self.calls = []
        self.check_rc = 0

    def check(self, src, dest, min_age):
        return self.check_rc, list(self.missing), list(self.differ)

    def stat(self, rel):
        return self.remote.get(rel)

    def run(self, argv):
        self.calls.append(argv)
        return argv[-1] not in self.fail


def sweep_for(files, runner):
    d = ed.Daemon(local_tree(files), "backup:", runner, clock=FakeClock())
    return ed.Sweep(d, runner, mailer=FakeMailer())


class FakeMailer:
    def __init__(self):
        self.sent = []

    def send(self, subject, body, attachment=None, filename=None):
        self.sent.append({"subject": subject, "body": body,
                          "attachment": attachment, "filename": filename})
        return True


def test_a_candidate_the_stat_finds_is_an_artefact_not_a_copy():
    """The listing lies sometimes; the stat is what decides."""
    r = FakeSweepRunner(missing=[FILE], remote={FILE: 100})
    s = sweep_for({FILE: 100}, r)
    report = s.run_once()
    assert r.calls == [], r.calls
    assert report.copied == [] and report.artefacts == 1, report


def test_a_candidate_the_stat_cannot_find_is_copied():
    r = FakeSweepRunner(missing=[FILE])
    s = sweep_for({FILE: 100}, r)
    report = s.run_once()
    assert [c[0] for c in r.calls] == ["copyto"], r.calls
    assert report.copied == [FILE] and report.artefacts == 0, report


def test_a_candidate_with_the_wrong_size_on_the_backup_is_copied():
    r = FakeSweepRunner(differ=[FILE], remote={FILE: 63})
    s = sweep_for({FILE: 100}, r)
    assert s.run_once().copied == [FILE]


def test_a_candidate_that_vanished_locally_is_left_alone():
    r = FakeSweepRunner(missing=[FILE])
    s = sweep_for({}, r)
    report = s.run_once()
    assert r.calls == [] and report.copied == [], report


def test_a_failed_copy_is_not_reported_as_copied():
    r = FakeSweepRunner(missing=[FILE], fail={"backup:/" + FILE})
    s = sweep_for({FILE: 100}, r)
    report = s.run_once()
    assert report.copied == [] and report.failed == [FILE], report


def test_a_quiet_sweep_sends_no_mail():
    r = FakeSweepRunner()
    s = sweep_for({FILE: 100}, r)
    s.run_once()
    assert s.mailer.sent == [], s.mailer.sent


def test_a_sweep_that_copied_something_mails_one_report_with_the_zip():
    r = FakeSweepRunner(missing=[FILE])
    s = sweep_for({FILE: 100}, r)
    s.run_once()
    assert len(s.mailer.sent) == 1, s.mailer.sent
    mail = s.mailer.sent[0]
    assert "1 file" in mail["subject"], mail["subject"]
    assert mail["filename"].endswith(".zip"), mail
    names, body = zip_contents(mail["attachment"])
    assert names == ["replication-report.csv"], names
    assert FILE in body and "TOTAL (1 files),100" in body, body


def zip_contents(blob):
    import io, zipfile
    with zipfile.ZipFile(io.BytesIO(blob)) as z:
        name = z.namelist()[0]
        return z.namelist(), z.read(name).decode()


# ---- failure alerting ----------------------------------------------------

def failing_sweep():
    r = FakeSweepRunner()
    r.check_rc = 7
    s = sweep_for({}, r)
    return s


def test_one_failed_sweep_does_not_alert():
    s = failing_sweep()
    s.run_once()
    assert s.mailer.sent == [], "alerted on a single failure"


def test_alert_fires_only_after_the_threshold_and_then_is_throttled():
    s = failing_sweep()
    for _ in range(ed.FAIL_THRESHOLD):
        s.run_once()
    assert len(s.mailer.sent) == 1, s.mailer.sent
    assert "FAILED" in s.mailer.sent[0]["subject"], s.mailer.sent[0]
    s.run_once()
    assert len(s.mailer.sent) == 1, "alerted again inside the quiet gap"
    s.last_alert -= ed.ALERT_MIN_GAP + 1
    s.run_once()
    assert len(s.mailer.sent) == 2, "never alerted again after the gap"


def test_a_good_sweep_clears_the_failure_streak():
    s = failing_sweep()
    s.run_once()
    s.run_once()
    s.runner.check_rc = 0
    s.run_once()
    assert s.fail_count == 0, s.fail_count
    s.runner.check_rc = 7
    s.run_once()
    assert s.mailer.sent == [], "streak was not reset by the good sweep"


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
