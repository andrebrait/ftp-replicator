#!/bin/sh
# Launcher. The rclone image ships neither python3 nor a mail client, so the
# only job here is to make python3 available and hand over. Everything else --
# the event endpoints, the sweep, the reports, the logging -- is replicator.py,
# in one process.
#
# There is deliberately no supervision loop: if the process dies the container
# should die with it, so `restart: unless-stopped` (or your runtime's
# equivalent) restarts it and the failure is visible rather than papered over.
set -eu

if ! command -v python3 >/dev/null 2>&1; then
  # ca-certificates is for the SMTP TLS handshake, not for rclone.
  apk add --no-cache python3 ca-certificates >/dev/null 2>&1 || {
    echo '{"level":"error","component":"replicator","msg":"apk add python3 failed"}'
    exit 1
  }
fi

# rclone will not take a plaintext SFTP password: the value in
# RCLONE_CONFIG_BACKUP_PASS has to be obscured. Supply either that (already
# obscured) or BACKUP_PASS_PLAINTEXT, which is obscured here so nobody has to
# run `rclone obscure` by hand.
if [ -z "${RCLONE_CONFIG_BACKUP_PASS:-}" ] && [ -n "${BACKUP_PASS_PLAINTEXT:-}" ]; then
  RCLONE_CONFIG_BACKUP_PASS="$(rclone obscure "$BACKUP_PASS_PLAINTEXT")"
  export RCLONE_CONFIG_BACKUP_PASS
  unset BACKUP_PASS_PLAINTEXT
fi

exec python3 "$(dirname "$0")/replicator.py"
