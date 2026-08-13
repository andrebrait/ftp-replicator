#!/bin/sh
# replicator: pushes files to the backup as the source instance reports them
# (event-daemon.py), and reconciles whatever that missed on an interval. Email
# notifications are sent directly from this container (msmtp).
#
#   - After every sweep that copies files: emails a zipped CSV report
#     (SFTPGo-retention-report style) listing each copied file + size.
#   - On failure: emails an alert with the rclone exit code and log tail, but
#     only after FAIL_THRESHOLD consecutive failed sweeps, and at most once per
#     ALERT_MIN_GAP seconds.
#   - Sweeps never overlap (sequential loop + a lock guard), and rclone's
#     per-file + periodic progress is streamed to `docker logs`.
#
# All config/secrets come from the environment (compose env_file: .env):
#   SMTP_HOST SMTP_PORT SMTP_USER SMTP_PASS MAIL_FROM MAIL_TO
#   RCLONE_CONFIG_BACKUP_* (rclone reads these directly; password pre-obscured)
#   SYNC_INTERVAL (default 300) MIN_AGE (default 5m)
#
# State (consecutive-failure count, last-alert time) lives in shell variables:
# it resets if the container restarts — acceptable, a restart is itself a signal.
set -u

# Site-specific values all come from the environment; these defaults only match
# the example deployment. SYNC_DEST may be given with or without a trailing slash.
SRC="${SYNC_SRC:-/data/source}"
DST="${SYNC_DEST:-backup:}"
DST="${DST%/}/"
INTERVAL="${SYNC_INTERVAL:-300}"
MIN_AGE="${MIN_AGE:-5m}"
FAIL_THRESHOLD=3
SUBJECT_PREFIX="${MAIL_SUBJECT_PREFIX:-[replicator]}"
ALERT_MIN_GAP=3600
LOCK=/tmp/sweep.lock

# ---- one-time: install mailer + archiver, write msmtp config ----
if ! command -v msmtp >/dev/null 2>&1 || ! command -v zip >/dev/null 2>&1 \
   || ! command -v python3 >/dev/null 2>&1; then
  apk add --no-cache msmtp ca-certificates zip python3 >/dev/null 2>&1 || {
    echo "FATAL: apk add failed (need network for msmtp/zip/python3)"; exit 1; }
fi
MSMTPRC=/tmp/msmtprc
umask 077
cat > "$MSMTPRC" <<CFG
defaults
tls on
tls_starttls on
tls_trust_file /etc/ssl/certs/ca-certificates.crt
auth on
account smtp
host ${SMTP_HOST}
port ${SMTP_PORT}
from ${SMTP_USER}
user ${SMTP_USER}
password ${SMTP_PASS}
account default : smtp
CFG

cleanup() { rmdir "$LOCK" 2>/dev/null; }
trap cleanup EXIT INT TERM

now_utc() { date -u +%FT%TZ; }

# send_mail SUBJECT BODY [ATTACHMENT_PATH]
send_mail() {
  _subj="$1"; _body="$2"; _att="${3:-}"
  _bnd="mime_$(date +%s)_$$"
  {
    printf 'From: %s\r\n' "${MAIL_FROM:-$SMTP_USER}"
    printf 'To: %s\r\n' "$MAIL_TO"
    printf 'Subject: %s\r\n' "$_subj"
    printf 'MIME-Version: 1.0\r\n'
    if [ -n "$_att" ] && [ -f "$_att" ]; then
      printf 'Content-Type: multipart/mixed; boundary="%s"\r\n\r\n' "$_bnd"
      printf -- '--%s\r\n' "$_bnd"
      printf 'Content-Type: text/plain; charset=utf-8\r\n\r\n%s\r\n' "$_body"
      printf -- '--%s\r\n' "$_bnd"
      printf 'Content-Type: application/zip; name="%s"\r\n' "$(basename "$_att")"
      printf 'Content-Transfer-Encoding: base64\r\n'
      printf 'Content-Disposition: attachment; filename="%s"\r\n\r\n' "$(basename "$_att")"
      base64 "$_att"
      printf '\r\n--%s--\r\n' "$_bnd"
    else
      printf 'Content-Type: text/plain; charset=utf-8\r\n\r\n%s\r\n' "$_body"
    fi
  } | msmtp -C "$MSMTPRC" -t 2>>/tmp/msmtp.err
}

# ---- event daemon ------------------------------------------------------
# Primary delivery path: SFTPGo posts every upload/rename to it and it pushes
# the file straight away. It keeps no state and is not restarted by anything
# else, so the loop below re-launches it if it died; whatever it missed in the
# meantime this sweep picks up.
ensure_daemon() {
  if [ -n "${daemon_pid:-}" ] && kill -0 "$daemon_pid" 2>/dev/null; then
    return
  fi
  python3 /event-daemon.py &
  daemon_pid=$!
  echo "[$(now_utc)] event daemon started pid=${daemon_pid}"
}

fail_count=0
last_alert=0
daemon_pid=""
echo "[$(now_utc)] replicator starting; interval=${INTERVAL}s min_age=${MIN_AGE} src=${SRC} dst=${DST}"
ensure_daemon

while true; do
  ts="$(now_utc)"
  ensure_daemon

  # Guard: never run two sweeps at once. mkdir is atomic; the loop is already
  # sequential, so this only trips if something external starts a second sweep.
  if ! mkdir "$LOCK" 2>/dev/null; then
    echo "[$ts] a sweep is already in progress; skipping this tick"
    sleep "$INTERVAL"; continue
  fi

  log="/tmp/sweep.log"
  missing=/tmp/missing.txt
  differ=/tmp/differ.txt
  cands=/tmp/candidates.txt
  list=/tmp/copied.txt
  start="$(date +%s)"
  echo "[$ts] sweep start"
  : > "$missing"; : > "$differ"; : > "$list"
  # Phase 1 -- ask which files the backup lacks or holds at a different size.
  # --size-only: uploads are write-once/immutable, and a delivered file carries
  # its own mtime, so a size+mtime compare would re-copy every large file that
  # arrived fine. Size uniquely identifies a complete file.
  { rclone check "$SRC" "$DST" --one-way --min-age "$MIN_AGE" --size-only \
      --missing-on-dst "$missing" --differ "$differ" --checkers 8 \
      --log-level NOTICE 2>&1; echo $? >/tmp/rc; } | tee "$log"
  rc="$(cat /tmp/rc 2>/dev/null || echo 1)"
  # rclone check exits 1 when it finds differences: an ordinary result here.
  [ "$rc" -eq 1 ] && rc=0

  # Phase 2 -- confirm each candidate with a direct stat before copying it.
  # A recursive listing over this tree intermittently omits a freshly written
  # file (a copy was observed replacing a file that was present and complete
  # the whole time); a stat resolves one path and cannot skip an entry, so it
  # tells a real miss from a listing artefact. Candidates are normally zero.
  false_pos=0
  if [ "$rc" -eq 0 ]; then
    sort -u "$missing" "$differ" > "$cands"
    while IFS= read -r rel; do
      [ -n "$rel" ] || continue
      lsz="$(stat -c %s "$SRC/$rel" 2>/dev/null)" || continue
      rsz="$(rclone lsjson --stat "$DST$rel" 2>/dev/null \
             | python3 -c 'import json,sys
d = json.load(sys.stdin) or {}
print(d.get("Size", -1))' 2>/dev/null || echo -1)"
      if [ "$rsz" = "$lsz" ]; then
        false_pos=$(( false_pos + 1 ))
        echo "[$ts] listing called it missing, a direct stat found it at ${rsz} bytes: $rel"
        continue
      fi
      if rclone copyto "$SRC/$rel" "$DST$rel" --log-level INFO >> "$log" 2>&1; then
        printf '%s\n' "$rel" >> "$list"
        echo "[$ts] copied (backup had ${rsz}, source has ${lsz}): $rel"
      else
        echo "[$ts] copy FAILED: $rel"
      fi
    done < "$cands"
  fi
  elapsed=$(( $(date +%s) - start ))
  rmdir "$LOCK" 2>/dev/null

  if [ "$rc" -eq 0 ]; then
    fail_count=0
    n="$(wc -l < "$list" | tr -d ' ')"
    if [ "${n:-0}" -gt 0 ]; then
      csv=/tmp/replication-report.csv
      zipf=/tmp/replication-report.zip
      total=0
      printf 'path,copied size (bytes),info,error\r\n' > "$csv"
      while IFS= read -r rel; do
        [ -n "$rel" ] || continue
        sz="$(stat -c %s "$SRC/$rel" 2>/dev/null || echo 0)"
        total=$(( total + sz ))
        printf '%s,%s,copied,\r\n' "$rel" "$sz" >> "$csv"
      done < "$list"
      printf 'TOTAL (%s files),%s,,\r\n' "$n" "$total" >> "$csv"
      rm -f "$zipf"; ( cd /tmp && zip -q "$(basename "$zipf")" "$(basename "$csv")" )
      body="Replication sweep OK.
Time: ${ts}
Copied: ${n} file(s), ${total} bytes
Source: ${SRC}
Target: ${DST}
Elapsed: ${elapsed}s

These files were NOT delivered by the event daemon, which is the normal path --
the sweep only copies what the daemon left behind, and each copy was confirmed
missing by a direct stat, not just by a directory listing. To see what the
daemon did with one of them:

  <your log viewer for this container> | grep '<file name>'"
      send_mail "${SUBJECT_PREFIX} ${n} file(s) copied" "$body" "$zipf"
      echo "[$ts] copied ${n} files (${total} bytes) in ${elapsed}s; report emailed"
    else
      echo "[$ts] sweep ok in ${elapsed}s; nothing new to copy (${false_pos} listing artefact(s))"
    fi
    # Reap pre-rename leftovers: SFTPGo's realtime push can land the pre-rename
    # name on the backup when a rename beat the push (see reap-orphans.py
    # for the deletion criteria -- it only removes a duplicate whose renamed twin
    # exists on both sides with the same size, never files the source's retention pruned).
    reap_out="$(python3 /reap-orphans.py 2>&1)"
    printf '%s\n' "$reap_out"          # it stamps its own lines, same format
    reaped="$(printf '%s\n' "$reap_out" | grep -c ' reap: orphan ')"
    if [ "${reaped:-0}" -gt 0 ]; then
      send_mail "${SUBJECT_PREFIX} ${reaped} pre-rename duplicate(s) deleted" "Deleted pre-rename duplicates on the backup.
Time: ${ts}
Target: ${DST}

$(printf '%s\n' "$reap_out" | grep ' reap: orphan ')

Each deleted file had a byte-identical renamed twin present on both the source and
the backup; files the source's retention has pruned are never touched."
    fi
  else
    fail_count=$(( fail_count + 1 ))
    echo "[$ts] sweep FAILED rc=${rc} in ${elapsed}s (streak=${fail_count})"
    now="$(date +%s)"
    if [ "$fail_count" -ge "$FAIL_THRESHOLD" ] && [ $(( now - last_alert )) -ge "$ALERT_MIN_GAP" ]; then
      body="Replication FAILED.
Time: ${ts}
rclone exit code: ${rc}
Consecutive failed sweeps: ${fail_count}
Source: ${SRC}
Target: ${DST}
Sweep interval: ${INTERVAL}s

--- last 30 log lines ---
$(tail -n 30 "$log")"
      send_mail "${SUBJECT_PREFIX} FAILED (rc=${rc}, ${fail_count} in a row)" "$body" ""
      last_alert="$now"
      echo "[$ts] failure alert emailed"
    fi
  fi
  echo "[$ts] next sweep in ${INTERVAL}s"
  sleep "$INTERVAL"
done
