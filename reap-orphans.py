#!/usr/bin/env python3
"""Delete pre-rename orphan duplicates on the backup SFTP remote.

The uploading client writes "<cam>_<YYYYmmddHHMMSS>.mp4" and then renames it a second
or two later.  SFTPGo pushes the file to the backup asynchronously on the upload
event; when the rename event wins that race, "backup_rename" fails (the remote
file does not exist yet), the failure action pushes the final name, and the
in-flight upload copy then lands the pre-rename name as a duplicate.

A file on the backup is deleted only when ALL of these hold:
  - it has no counterpart under the same name in the local source tree,
  - a sibling with the same name prefix + extension and a timestamp within
    WINDOW seconds exists BOTH locally and on the backup,
  - that sibling has the same size on both sides.
Anything else (e.g. files the source's own retention already pruned) is kept.
"""
import os
import re
import subprocess
import sys
from datetime import datetime, timedelta, timezone

SRC = os.environ.get("REAP_SRC", "/data/source")
REMOTE = os.environ.get("REAP_REMOTE", "backup:/")
RCLONE = os.environ.get("REAP_RCLONE", "rclone").split()
WINDOW = 2  # ponytail: observed renames are -1s (a few cross a minute boundary); widen if that changes
DAYS = int(os.environ.get("REAP_DAYS", "2"))  # ponytail: leftovers appear within seconds; 0 scans every day dir
NAME_RE = re.compile(r"^(.+)_(\d{14})(\.\w+)$")
LSL_RE = re.compile(r"\s*(\d+) \S+ \S+ (.+)$")


def log(msg):
    print("[%s] reap: %s" % (datetime.now(timezone.utc).strftime("%FT%TZ"), msg))


def remote_sizes(day):
    out = subprocess.run(RCLONE + ["lsl", "%s/%s/" % (REMOTE, day)],
                         capture_output=True, text=True, timeout=300)
    if out.returncode != 0:
        raise RuntimeError("rclone lsl %s failed: %s" % (day, out.stderr.strip()[-200:]))
    sizes = {}
    for line in out.stdout.splitlines():
        m = LSL_RE.match(line)
        if m:
            sizes[m.group(2)] = int(m.group(1))
    return sizes


def siblings(name):
    m = NAME_RE.match(name)
    if not m:
        return
    prefix, ts, ext = m.groups()
    stamp = datetime.strptime(ts, "%Y%m%d%H%M%S")
    for delta in range(-WINDOW, WINDOW + 1):
        if delta:
            yield "%s_%s%s" % (prefix, (stamp + timedelta(seconds=delta)).strftime("%Y%m%d%H%M%S"), ext)


def main():
    dry = "--dry-run" in sys.argv
    deleted = kept = 0
    days = sorted(os.listdir(SRC))
    for day in days[-DAYS:] if DAYS else days:
        local = {e.name: e.stat().st_size for e in os.scandir(os.path.join(SRC, day)) if e.is_file()}
        remote = remote_sizes(day)
        if remote and not local:
            # Nothing can be deleted in this state (every rule needs a local
            # twin), but it means REAP_SRC/REAP_REMOTE are not both pointing at
            # the level that holds the date directories.
            log("WARNING: %d file(s) on the backup under %s and none locally -- check "
                "REAP_SRC=%s and REAP_REMOTE=%s" % (len(remote), day, SRC, REMOTE))
        for name in sorted(set(remote) - set(local)):
            twin = next((s for s in siblings(name)
                         if s in local and remote.get(s) == local[s]), None)
            if not twin:
                kept += 1
                continue
            path = "%s/%s/%s" % (REMOTE, day, name)
            log("%sorphan %s/%s (%d bytes, twin %s)" %
                ("DRY-RUN " if dry else "", day, name, remote[name], twin))
            if not dry:
                subprocess.run(RCLONE + ["deletefile", path], check=True, timeout=300,
                               capture_output=True, text=True)
            deleted += 1
    log("%d orphan(s), %d remote-only file(s) kept" % (deleted, kept))


if __name__ == "__main__":
    main()
