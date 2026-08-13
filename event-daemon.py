#!/usr/bin/env python3
"""Push files to the backup as SFTPGo reports them, using rclone.

SFTPGo fires an HTTP action per upload and per rename; this daemon turns those
into rclone calls. It is deliberately disposable: no persistence, no retries, no
email. Anything it drops (crash, restart, power cut, failed rclone call) is
picked up by the reconciliation sweep in replicator.sh, and that sweep's email
is the signal that something here missed one -- so every event, decision and
rclone invocation is logged, greppable by file name.

Endpoints (POST, JSON body):
    /upload   {"path": "/<dir>/<file>"}
    /rename   {"path": "<old>", "target": "<new>"}
    /health   GET -> buffer and queue depth

Events land in a consolidation buffer first, keyed by the file they concern (a
rename joins the entry its source name belongs to, so a chain A->B->C stays one
entry). An entry is released to the rclone queue once it holds an upload event
AND nothing has arrived for it in QUIET seconds:

  - the upload event is what proves the transfer finished. The client renames a
    file a second or two after uploading it, sometimes while the transfer is
    still open, so a rename on its own says nothing about completeness;
  - the rename events are what say where the file ended up. The last target
    wins; every earlier name is deleted from the backup, in case a partial copy
    of one got there.

An entry that never gets its upload event is dropped after MAX_AGE without
being pushed: something is wrong with it, and guessing is worse than letting
the sweep decide. MAX_AGE is 4:50, just inside the sweep's `--min-age 5m`, so
the daemon has let go by the time the sweep first considers the file and the
two never act on it at once.
"""
import collections
import http.server
import json
import os
import subprocess
import sys
import threading
import time

SRC = os.environ.get("SYNC_SRC", "/data/source")
DEST = os.environ.get("SYNC_DEST", "backup:")
PORT = int(os.environ.get("SYNC_DAEMON_PORT", "8787"))
RCLONE = os.environ.get("SYNC_RCLONE", "rclone")
TIMEOUT = int(os.environ.get("SYNC_RCLONE_TIMEOUT", "900"))
QUIET = float(os.environ.get("SYNC_QUIET_PERIOD", "2"))
FLUSH_INTERVAL = float(os.environ.get("SYNC_FLUSH_INTERVAL", "1"))
MAX_AGE = float(os.environ.get("SYNC_MAX_ENTRY_AGE", "290"))
COMPONENT = "daemon"
LOG_JSON = os.environ.get("LOG_FORMAT", "json").lower() != "text"

Job = collections.namedtuple("Job", "final olds")


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
        for key, entry in stale:
            log("discarded with no upload event, the sweep takes it from here",
                level="warn", event="discard", path=key, age_s=round(self.max_age))
            with self._lock:
                self._forget(key, entry)
        return jobs

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


def make_handler(daemon):
    class Handler(http.server.BaseHTTPRequestHandler):
        def _reply(self, code, body=""):
            payload = body.encode()
            self.send_response(code)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self):
            if self.path.startswith("/health"):
                self._reply(200, "buffered=%d queued=%d\n" %
                            (len(daemon.pending_files()), len(daemon.pending_jobs())))
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
    threading.Thread(target=daemon.flush_loop, daemon=True).start()
    threading.Thread(target=daemon.consume, daemon=True).start()
    server = http.server.ThreadingHTTPServer(("0.0.0.0", PORT), make_handler(daemon))
    log("listening", event="start", port=PORT, src=daemon.src, dest=daemon.dest,
        quiet_s=daemon.quiet, max_entry_age_s=round(daemon.max_age))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
