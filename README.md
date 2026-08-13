# ftp-replicator

Replicates recorder footage from one SFTPGo instance to another, driven by SFTPGo's event
actions, with rclone doing all of the copying.

Extracted from a working deployment. Everything site-specific (paths, hosts, credentials)
is an environment variable, so replicating it is a matter of filling in `.env`.

## The setup

```
   recorder (FTP client)
        |  uploads <camera>_<YYYYmmddHHMMSS>.mp4, then renames it ~1-2s later
        v
   SFTPGo  "source"            event actions (HTTP)        this repo
   - FTP in                ----------------------------->  event-daemon.py
   - data retention                                          |  rclone
        |                                                    v
        |                                              SFTPGo  "backup"
        |                                              - SFTP in
        `--- replicator.sh sweep (rclone, every 5 min) ---> (same target)
```

Two independent paths write to the backup, and only these two:

* **event-daemon.py** — the normal path. SFTPGo POSTs every upload and rename to it; it
  pushes the clip within seconds.
* **replicator.sh** — the safety net, every `SYNC_INTERVAL`. Copies whatever the daemon
  missed (crash, restart, failed call, backup offline) and emails a report when it finds
  anything. **An email therefore means the event path dropped one** — that is the signal;
  the daemon itself never mails.

The source instance keeps a short local retention (SFTPGo's own data-retention rule); the
backup keeps everything. So the backup is deliberately **not** a mirror, and nothing here
ever deletes on the backup except `reap-orphans.py` (below) and the scoped `sync` of a
renamed clip's old names.

## Why it is built this way

Two failure modes drove the design. Both are easy to re-introduce if you simplify it.

**1. The recorder renames a clip while the transfer is still open.** Observed repeatedly:
the rename event arrives ~130 ms *before* the transfer closes. So a rename event on its
own says nothing about whether the file is complete, and any scheme that copies on rename
can publish a short file. Only the *upload* event proves completion — SFTPGo fires it
after the transfer closes.

That is why the daemon buffers: events for one clip accumulate, keyed by the file (a
rename joins the entry its *source* name belongs to, so `A→B→C` stays one entry), and the
entry is released only when it holds an upload event **and** has been quiet for
`SYNC_QUIET_PERIOD`. The last rename target wins; every earlier name is deleted from the
backup, in case a partial copy of one got there.

An entry that never receives an upload event is dropped at `SYNC_MAX_ENTRY_AGE` (4:50),
deliberately just inside the sweep's `--min-age 5m`, so the daemon has let go before the
sweep first considers that file and the two never act on it at once.

**2. A recursive listing over a large tree intermittently omits a freshly written file.**
This made an earlier sweep re-upload files that were present and complete the whole time
(confirmed server-side: the receiver logged a `Remove` of the existing target before the
replacement was renamed in — with a genuinely absent target there is no `Remove`). So the
sweep never trusts a listing alone: it collects candidates with `rclone check`, then
confirms each one with a direct `rclone lsjson --stat` before copying. Listing artefacts
are logged and skipped; only real misses are copied and mailed.

## Components

| File | Role |
| --- | --- |
| `event-daemon.py` | HTTP daemon: consolidation buffer + single-consumer rclone queue |
| `replicator.sh` | Reconciliation sweep, mail reporting, supervises the daemon |
| `reap-orphans.py` | Deletes pre-rename duplicates on the backup (see below) |
| `test_event_daemon.py` | `python3 test_event_daemon.py` — asserts only, no framework, no network |

`reap-orphans.py` exists for clips the event path did not settle: it removes a backup file
only when it has no counterpart locally, a sibling within ±2 s exists on **both** sides,
and that sibling has the same size on both. It never touches footage the local retention
merely aged out.

## Configuration

All via environment (`.env`, see `.env.example`).

| Variable | Default | Meaning |
| --- | --- | --- |
| `RCLONE_CONFIG_BACKUP_TYPE` … | — | rclone remote named `backup:`, defined entirely by env vars |
| `RCLONE_CONFIG_BACKUP_KNOWN_HOSTS_FILE` | — | path to a known_hosts file; without it rclone does no host-key validation |
| `SYNC_SRC` | `/data/source` | local root, read-only mount of the source instance's data dir |
| `SYNC_DEST` | `backup:` | rclone remote |
| `SYNC_DAEMON_PORT` | `8787` | daemon listen port; publish it on loopback only |
| `SYNC_QUIET_PERIOD` | `2` | seconds of silence before an entry is released |
| `SYNC_FLUSH_INTERVAL` | `1` | how often the buffer is checked |
| `SYNC_MAX_ENTRY_AGE` | `290` | give-up point; **must stay below the sweep's `--min-age`** |
| `SYNC_RCLONE_TIMEOUT` | `900` | per-invocation timeout |
| `SYNC_INTERVAL` | `300` | seconds between sweeps |
| `MIN_AGE` | `5m` | sweep ignores files younger than this |
| `REAP_SRC` / `REAP_REMOTE` | `/data/source`, `backup:/` | must point at the directory that directly contains the day directories |
| `SMTP_*`, `MAIL_FROM`, `MAIL_TO` | — | msmtp settings for the sweep's reports |

## Wiring the source SFTPGo

Two event rules on the source instance, both filtered to the uploading user. No virtual
folder is involved — the backup is reached only by rclone.

* rule on the `upload` filesystem event → HTTP action:
  `POST http://127.0.0.1:8787/upload`, body `{"path":"{{.VirtualPath}}"}`
* rule on the `rename` filesystem event → HTTP action:
  `POST http://127.0.0.1:8787/rename`,
  body `{"path":"{{.VirtualPath}}","target":"{{.VirtualTargetPath}}"}`

Both with header `Content-Type: application/json`. Action type `1` is HTTP; the exact
schema for any field is in the bundled `/usr/share/sftpgo/openapi/openapi.yaml`.

Gotchas found the hard way:

* SFTPGo (2.7.x) has **no `dumpdata` CLI**. Without admin credentials, changes go through
  the sqlite provider DB directly.
* It polls for rule changes roughly every 10 minutes and keys on
  `events_rules.updated_at`. After editing an **action**, bump the parent **rule's**
  `updated_at` or nothing reloads.
* Until that reload happens the old actions keep running from cache. If an old action
  wrote into a virtual folder you just deleted, the path resolves to a **real** directory
  and it starts filling your source tree. Change the rules first, drop the folder second.

## Deploying

Any runtime that gives you rclone + python3, a read-only mount of the source data
directory, and a port SFTPGo can reach. `examples/compose.yaml` is one way, not a
requirement; `replicator.sh` is the entrypoint and starts the daemon itself (and
restarts it if it dies).

## Operating it

```sh
# is it alive, and is anything stuck?
wget -qO- http://127.0.0.1:8787/health          # buffered=<n> queued=<n>

# what happened to one clip? (after a sweep email)
docker logs <container> 2>&1 | grep '<file name>'

# do the two sides actually agree?
rclone check "$SYNC_SRC" backup:/ --one-way --min-age 5m --size-only
```

Every event, decision and rclone invocation is logged with the file name in it, which is
what makes a sweep email actionable rather than alarming.
