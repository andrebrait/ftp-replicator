#!/usr/bin/env python3
"""Replicate files to a backup SFTPGo instance, in one process.

Two paths, both rclone, sharing this process so they share one logger and one
place to configure:

  - the event path: SFTPGo POSTs every upload and rename to the HTTP endpoints
    below, events are consolidated per file, and the file is pushed within
    seconds of its transfer closing (see the Daemon docstring);
  - the sweep: every SYNC_INTERVAL, asks rclone which files the backup lacks,
    confirms each candidate with a direct stat, copies the real misses and
    mails a report. That report is the signal that the event path dropped one.

Endpoints (POST, JSON body):
    /upload   {"path": "/<dir>/<file>"}
    /rename   {"path": "<old>", "target": "<new>"}
    /health   GET -> buffer, queue and sweep state

Everything logs one JSON object per line (LOG_FORMAT=text for the plain form).
This used to be a shell script driving msmtp; assembling JSON log lines in sh
meant hand-escaping file names that contain spaces and quotes, and the mail
needed msmtp and zip installed at boot. Python does all three itself.
"""
import collections
import csv
import io
import http.server
import json
import os
import smtplib
import subprocess
import sys
import threading
import time
import zipfile
from email.message import EmailMessage

SRC = os.environ.get("SYNC_SRC", "/data/source")
DEST = os.environ.get("SYNC_DEST", "backup:")
PORT = int(os.environ.get("SYNC_DAEMON_PORT", "8787"))
RCLONE = os.environ.get("SYNC_RCLONE", "rclone")
TIMEOUT = int(os.environ.get("SYNC_RCLONE_TIMEOUT", "900"))
QUIET = float(os.environ.get("SYNC_QUIET_PERIOD", "2"))
FLUSH_INTERVAL = float(os.environ.get("SYNC_FLUSH_INTERVAL", "1"))
MAX_AGE = float(os.environ.get("SYNC_MAX_ENTRY_AGE", "290"))
COMPONENT = os.environ.get("LOG_COMPONENT", "replicator")
LOG_JSON = os.environ.get("LOG_FORMAT", "json").lower() != "text"

Job = collections.namedtuple("Job", "final olds mode")
Job.__new__.__defaults__ = ("sync",)   # "sync" copies; "move" renames on the backup


def log(msg, level="info", **fields):
    """One line per event. JSON by default so it can be filtered alongside
    SFTPGo's own output; LOG_FORMAT=text gives the bracketed human form.

    The fields are the point: `event`, `path`, `target`, `rc`, `elapsed_ms` and
    friends are what you filter on. `msg` is only there to be read."""
    ts = time.strftime("%FT%TZ", time.gmtime())
    if LOG_JSON:
        record = {"time": ts, "level": level, "component": COMPONENT, "msg": msg}
        record.update(fields)
        print(json.dumps(record, sort_keys=False), flush=True)
        return
    extra = " ".join("%s=%s" % (k, v) for k, v in fields.items())
    print("[%s] %s: %s%s" % (ts, COMPONENT, msg, " " + extra if extra else ""), flush=True)


class RcloneRunner:
    def check(self, src, dest, min_age):
        """Which files does the backup lack, or hold at a different size?
        rclone exits 1 when it finds differences, which is an ordinary result."""
        missing, differ = "/tmp/missing.txt", "/tmp/differ.txt"
        argv = [RCLONE, "check", src, dest, "--one-way", "--min-age", min_age,
                "--size-only", "--missing-on-dst", missing, "--differ", differ,
                "--checkers", "8", "--log-level", "NOTICE"]
        out = subprocess.run(argv, capture_output=True, text=True, timeout=TIMEOUT)
        rc = 0 if out.returncode in (0, 1) else out.returncode
        if rc != 0:
            log("rclone check failed", level="error", event="rclone", rc=out.returncode,
                stderr=out.stderr.strip()[-300:])
            return rc, [], []
        return rc, read_lines(missing), read_lines(differ)

    def stat(self, rel):
        """Size on the backup, or None. A stat resolves one path, so unlike a
        listing it cannot silently skip an entry."""
        out = subprocess.run([RCLONE, "lsjson", "--stat", "%s/%s" % (DEST.rstrip("/"), rel)],
                             capture_output=True, text=True, timeout=120)
        if out.returncode != 0:
            return None
        try:
            record = json.loads(out.stdout)
        except ValueError:
            return None
        return record.get("Size") if record else None

    def run(self, argv):
        started = time.time()
        out = subprocess.run([RCLONE] + argv, capture_output=True, text=True, timeout=TIMEOUT)
        elapsed = time.time() - started
        if out.returncode != 0:
            log("rclone failed", level="error", event="rclone", rc=out.returncode,
                elapsed_ms=round(elapsed * 1000), argv=argv,
                stderr=out.stderr.strip()[-300:])
            return False
        log("rclone ok", event="rclone", rc=0, elapsed_ms=round(elapsed * 1000), argv=argv)
        return True


def read_lines(path):
    try:
        with open(path) as fh:
            return [l.strip() for l in fh if l.strip()]
    except OSError:
        return []


class Entry:
    """Everything heard about one file inside the consolidation window."""

    def __init__(self, name, now):
        self.final = name          # where the file is expected to be, so far
        self.olds = []             # names it has been known by, oldest first
        self.uploaded = False      # an upload event arrived: transfer finished
        self.first_seen = now
        self.last_event = now

    def names(self):
        return [self.final] + self.olds


class Daemon:
    def __init__(self, src=SRC, dest=DEST, runner=None, clock=None):
        self.src = src
        self.dest = dest.rstrip("/")
        self.runner = runner or RcloneRunner()
        self.clock = clock or time.time
        self.quiet = QUIET
        self.max_age = MAX_AGE
        self._entries = {}         # keyed by the name the file was first seen as
        self._index = {}           # every name it has had -> that key
        self._lock = threading.Lock()
        self._queue = []
        self._cv = threading.Condition()

    # ---- paths -------------------------------------------------------------

    def rel(self, vpath):
        """SFTPGo virtual path -> path relative to the source root."""
        rel = os.path.normpath((vpath or "").lstrip("/"))
        if not rel or rel == "." or rel.startswith("..") or os.path.isabs(rel):
            raise ValueError("path outside the source tree: %r" % vpath)
        return rel

    def remote(self, rel):
        return "%s/%s" % (self.dest, rel)

    def include_pattern(self, name):
        """Anchor a literal file name as an rclone filter (globs are escaped)."""
        out = []
        for ch in name:
            if ch in "*?[]{}\\":
                out.append("\\")
            out.append(ch)
        return "/" + "".join(out)

    # ---- consolidation buffer ---------------------------------------------

    def pending_files(self):
        with self._lock:
            return sorted(self._entries)

    def _entry_for(self, name, now):
        """The entry this name belongs to, creating one if it is new."""
        key = self._index.get(name)
        if key is not None:
            return key, self._entries[key]
        entry = Entry(name, now)
        self._entries[name] = entry
        self._index[name] = name
        return name, entry

    def on_upload(self, vpath):
        rel = self.rel(vpath)
        now = self.clock()
        with self._lock:
            key, entry = self._entry_for(rel, now)
            entry.uploaded = True
            entry.last_event = now
            log("upload event", event="upload", path=rel, expecting=entry.final)

    def on_rename(self, vpath, target):
        old = self.rel(vpath)
        new = self.rel(target)
        now = self.clock()
        with self._lock:
            key, entry = self._entry_for(old, now)
            if entry.final != new:
                entry.olds.append(entry.final)
                entry.final = new
            self._index[new] = key
            entry.last_event = now
            log("rename event", event="rename", path=old, target=new,
                upload_seen=entry.uploaded)

    def ready_jobs(self):
        """Entries that hold an upload event and have gone quiet. Stale ones
        (no upload event, ever) are dropped here too."""
        now = self.clock()
        jobs, stale = [], []
        with self._lock:
            for key, entry in list(self._entries.items()):
                if now - entry.first_seen > self.max_age:
                    stale.append((key, entry))
                    continue
                if not entry.uploaded or now - entry.last_event < self.quiet:
                    continue
                jobs.append(Job(entry.final, tuple(entry.olds)))
                self._forget(key, entry)
        # Stale means no upload event ever arrived. Usually that is a rename
        # whose upload was released before it landed: the bytes are already on
        # the backup under the old name, and all that is missing is the rename.
        # Confirm that by size and it is a server-side move, no transfer. The
        # stats go outside the lock; they are network calls.
        for key, entry in stale:
            jobs.extend(self._salvage(key, entry))
        return jobs

    def _salvage(self, key, entry):
        local = os.path.join(self.src, entry.final)
        try:
            size = os.path.getsize(local)
        except OSError:
            size = None
        source = None
        if size is not None:
            for old in entry.olds:
                if self.runner.stat(old) == size:
                    source = old
                    break
        with self._lock:
            self._forget(key, entry)
        if source is None:
            log("discarded with no upload event, the sweep takes it from here",
                level="warn", event="discard", path=key, age_s=round(self.max_age))
            return []
        log("late rename, the bytes are already on the backup under the old name",
            event="late_rename", path=source, target=entry.final, size=size)
        return [Job(entry.final, (source,), "move")]

    def _forget(self, key, entry):
        self._entries.pop(key, None)
        for name in entry.names():
            if self._index.get(name) == key:
                del self._index[name]

    # ---- queue -------------------------------------------------------------

    def pending_jobs(self):
        with self._cv:
            return list(self._queue)

    def push(self, job):
        with self._cv:
            if job in self._queue:
                log("already queued", event="queue_skip", path=job.final)
                return
            self._queue.append(job)
            self._cv.notify()
        log("queued", event="queued", path=job.final, replacing=list(job.olds),
            depth=len(self._queue))

    def pop(self, timeout=None):
        with self._cv:
            if not self._queue:
                self._cv.wait(timeout)
            return self._queue.pop(0) if self._queue else None

    # ---- running the jobs --------------------------------------------------

    def handle(self, job):
        try:
            self._run(job)
        except Exception as exc:                      # noqa: BLE001 - the sweep is the net
            log("job failed", level="error", event="job_error", path=job.final,
                error="%s: %s" % (type(exc).__name__, exc))

    def _run(self, job):
        local = os.path.join(self.src, job.final)
        if job.mode == "move":
            self.runner.run(["moveto", self.remote(job.olds[0]), self.remote(job.final)])
            return
        if not os.path.exists(local):
            log("gone locally before we pushed it, leaving it to the sweep",
                level="warn", event="skip_missing", path=job.final)
            return
        day = os.path.dirname(job.final)
        cross_dir = [o for o in job.olds if os.path.dirname(o) != day]
        if cross_dir:
            # Filters are anchored to one sync root; a midnight rollover is not.
            for old in job.olds:
                self.runner.run(["deletefile", self.remote(old)])
            self.runner.run(["copyto", local, self.remote(job.final)])
            return
        argv = ["sync" if job.olds else "copy",
                os.path.join(self.src, day), self.remote(day), "--size-only"]
        for name in [job.final] + list(job.olds):
            argv += ["--include", self.include_pattern(os.path.basename(name))]
        if job.olds:
            # The old names are gone locally, so sync deletes them from the
            # backup -- along with any partial copy left under one of them.
            argv += ["--max-delete", str(len(job.olds))]
        self.runner.run(argv)

    # ---- threads -----------------------------------------------------------

    def flush_loop(self):
        while True:
            time.sleep(FLUSH_INTERVAL)
            try:
                for job in self.ready_jobs():
                    self.push(job)
            except Exception as exc:                  # noqa: BLE001 - keep flushing
                log("flush error", level="error", event="flush_error",
                    error="%s: %s" % (type(exc).__name__, exc))

    def consume(self):
        while True:
            job = self.pop(timeout=5)
            if job is not None:
                self.handle(job)




# ---------------------------------------------------------------- the sweep

FAIL_THRESHOLD = int(os.environ.get("SYNC_FAIL_THRESHOLD", "3"))
ALERT_MIN_GAP = float(os.environ.get("SYNC_ALERT_MIN_GAP", "3600"))
SUBJECT_PREFIX = os.environ.get("MAIL_SUBJECT_PREFIX", "[replicator]")
INTERVAL = float(os.environ.get("SYNC_INTERVAL", "300"))
MIN_AGE = os.environ.get("MIN_AGE", "5m")


class Report:
    def __init__(self):
        self.copied = []
        self.failed = []
        self.artefacts = 0
        self.elapsed = 0.0

    def __repr__(self):
        return "Report(copied=%r, failed=%r, artefacts=%d)" % (
            self.copied, self.failed, self.artefacts)

    def csv_bytes(self, src):
        out = io.StringIO()
        writer = csv.writer(out, lineterminator="\r\n")
        writer.writerow(["path", "copied size (bytes)", "info", "error"])
        total = 0
        for rel in self.copied:
            try:
                size = os.path.getsize(os.path.join(src, rel))
            except OSError:
                size = 0
            total += size
            writer.writerow([rel, size, "copied", ""])
        writer.writerow(["TOTAL (%d files)" % len(self.copied), total, "", ""])
        return out.getvalue().encode(), total

    def zip_bytes(self, src):
        csv_data, total = self.csv_bytes(src)
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
            z.writestr("replication-report.csv", csv_data)
        return buf.getvalue(), total


class Sweep:
    """Reconciliation. Owns the failure streak and the alert throttle."""

    def __init__(self, daemon, runner, mailer, interval=INTERVAL, min_age=MIN_AGE):
        self.daemon = daemon
        self.runner = runner
        self.mailer = mailer
        self.interval = interval
        self.min_age = min_age
        self.fail_count = 0
        self.last_alert = 0.0
        self.last_report = None

    def run_once(self):
        started = time.time()
        report = Report()
        log("sweep start", event="sweep_start")
        rc, missing, differ = self.runner.check(self.daemon.src, self.daemon.dest, self.min_age)
        if rc != 0:
            report.elapsed = time.time() - started
            self._on_failure(rc, report)
            return report

        self.fail_count = 0
        for rel in sorted(set(missing) | set(differ)):
            local = os.path.join(self.daemon.src, rel)
            try:
                local_size = os.path.getsize(local)
            except OSError:
                log("candidate vanished locally, leaving it", level="warn",
                    event="candidate_gone", path=rel)
                continue
            # The listing is not trusted on its own: it intermittently omits a
            # freshly written file, and re-copying one that is already there is
            # both wasted transfer and a false alarm in your inbox.
            remote_size = self.runner.stat(rel)
            if remote_size == local_size:
                report.artefacts += 1
                log("listing called it missing, a direct stat found it",
                    event="listing_artefact", path=rel, size=remote_size)
                continue
            if self.runner.run(["copyto", local, self.daemon.remote(rel)]):
                report.copied.append(rel)
                log("copied", event="copied", path=rel,
                    backup_size=remote_size, source_size=local_size)
            else:
                report.failed.append(rel)
                log("copy failed", level="error", event="copy_failed", path=rel)

        report.elapsed = time.time() - started
        self.last_report = report
        self._report(report)
        return report

    def _report(self, report):
        if not report.copied:
            log("sweep ok, nothing new to copy", event="sweep_done", files=0,
                elapsed_s=round(report.elapsed, 1), listing_artefacts=report.artefacts,
                failed=len(report.failed))
            return
        blob, total = report.zip_bytes(self.daemon.src)
        body = ("Replication sweep OK.\n"
                "Copied: %d file(s), %d bytes\n"
                "Source: %s\nTarget: %s\nElapsed: %.0fs\n\n"
                "These files were NOT delivered by the event path, which is the normal\n"
                "one -- the sweep only copies what it left behind, and each copy was\n"
                "confirmed missing by a direct stat, not just by a directory listing.\n"
                "To see what happened to one of them, filter the log for its name.\n"
                % (len(report.copied), total, self.daemon.src, self.daemon.dest, report.elapsed))
        self.mailer.send("%s %d file(s) copied" % (SUBJECT_PREFIX, len(report.copied)),
                         body, attachment=blob, filename="replication-report.zip")
        log("copied files the event path missed, report emailed", event="sweep_done",
            files=len(report.copied), bytes=total, elapsed_s=round(report.elapsed, 1),
            listing_artefacts=report.artefacts, failed=len(report.failed))

    def _on_failure(self, rc, report):
        self.fail_count += 1
        log("sweep failed", level="error", event="sweep_failed", rc=rc,
            elapsed_s=round(report.elapsed, 1), streak=self.fail_count)
        now = time.time()
        if self.fail_count < FAIL_THRESHOLD or now - self.last_alert < ALERT_MIN_GAP:
            return
        self.mailer.send("%s FAILED (rc=%s, %d in a row)" % (SUBJECT_PREFIX, rc, self.fail_count),
                         "Replication FAILED.\nrclone exit code: %s\n"
                         "Consecutive failed sweeps: %d\nSource: %s\nTarget: %s\n"
                         % (rc, self.fail_count, self.daemon.src, self.daemon.dest))
        self.last_alert = now
        log("failure alert emailed", event="alert_sent")

    def loop(self):
        while True:
            try:
                self.run_once()
            except Exception as exc:                  # noqa: BLE001 - never kill the loop
                log("sweep crashed", level="error", event="sweep_error",
                    error="%s: %s" % (type(exc).__name__, exc))
            log("next sweep scheduled", event="sweep_wait", interval_s=round(self.interval))
            time.sleep(self.interval)


# ---------------------------------------------------------------- mail

class Mailer:
    """SMTP over STARTTLS. Replaces msmtp, so nothing has to be apk-added."""

    def __init__(self):
        self.host = os.environ.get("SMTP_HOST")
        self.port = int(os.environ.get("SMTP_PORT", "587"))
        self.user = os.environ.get("SMTP_USER")
        self.password = os.environ.get("SMTP_PASS")
        self.sender = os.environ.get("MAIL_FROM") or self.user
        self.to = os.environ.get("MAIL_TO")

    def send(self, subject, body, attachment=None, filename=None):
        if not (self.host and self.to):
            log("no SMTP configured, not sending", level="warn", event="mail_skipped",
                subject=subject)
            return False
        msg = EmailMessage()
        msg["From"] = self.sender
        msg["To"] = self.to
        msg["Subject"] = subject
        msg.set_content(body)
        if attachment:
            msg.add_attachment(attachment, maintype="application", subtype="zip",
                               filename=filename or "report.zip")
        try:
            with smtplib.SMTP(self.host, self.port, timeout=60) as smtp:
                smtp.starttls()
                if self.user:
                    smtp.login(self.user, self.password or "")
                smtp.send_message(msg)
        except Exception as exc:                      # noqa: BLE001 - mail is not the job
            log("sending mail failed", level="error", event="mail_error",
                subject=subject, error="%s: %s" % (type(exc).__name__, exc))
            return False
        log("mail sent", event="mail_sent", subject=subject)
        return True


def make_handler(daemon, sweep=None):
    class Handler(http.server.BaseHTTPRequestHandler):
        def _reply(self, code, body=""):
            payload = body.encode()
            self.send_response(code)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self):
            if self.path.startswith("/health"):
                last = getattr(sweep, "last_report", None)
                self._reply(200, "buffered=%d queued=%d last_sweep_copied=%s\n" %
                            (len(daemon.pending_files()), len(daemon.pending_jobs()),
                             len(last.copied) if last else "none"))
            else:
                self._reply(404)

        def do_POST(self):
            length = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(length).decode("utf-8", "replace")
            try:
                body = json.loads(raw) if raw else {}
            except ValueError:
                log("bad request body", level="warn", event="bad_request",
                    endpoint=self.path, body=raw[:200])
                return self._reply(400, "bad json\n")
            try:
                if self.path.startswith("/upload"):
                    daemon.on_upload(body.get("path"))
                elif self.path.startswith("/rename"):
                    daemon.on_rename(body.get("path"), body.get("target"))
                else:
                    return self._reply(404)
            except ValueError as exc:
                log("rejected", level="warn", event="rejected",
                    endpoint=self.path, error=str(exc))
                return self._reply(400, "bad path\n")
            except Exception as exc:                  # noqa: BLE001 - never 500 at SFTPGo
                log("error handling request", level="error", event="handler_error",
                    endpoint=self.path, error="%s: %s" % (type(exc).__name__, exc))
            self._reply(200, "ok\n")

        def log_message(self, format, *args):
            pass                                      # access lines add nothing here

    return Handler




def main():
    daemon = Daemon()
    sweep = Sweep(daemon, daemon.runner, Mailer())
    threading.Thread(target=daemon.flush_loop, daemon=True).start()
    threading.Thread(target=daemon.consume, daemon=True).start()
    threading.Thread(target=sweep.loop, daemon=True).start()
    server = http.server.ThreadingHTTPServer(("0.0.0.0", PORT), make_handler(daemon, sweep))
    log("replicator starting", event="start", port=PORT, src=daemon.src, dest=daemon.dest,
        quiet_s=daemon.quiet, max_entry_age_s=round(daemon.max_age),
        interval_s=round(sweep.interval), min_age=sweep.min_age)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
