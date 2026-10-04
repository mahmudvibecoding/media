#!/bin/sh
# Reopen the server dashboard through this checkout's private SSH connection.
set -eu
cd "$(dirname "$0")/.."
mkdir -p .local
dashboard_socket="$(pwd)/.local/dashboard-ssh.sock"
if ! ssh -S "$dashboard_socket" -O check root@198.163.196.164 >/dev/null 2>&1; then
  if [ -S "$dashboard_socket" ]; then rm -f "$dashboard_socket"; fi
  ssh -fN -M -S "$dashboard_socket" -o BatchMode=yes -o ExitOnForwardFailure=yes \
    -o ServerAliveInterval=30 -o ServerAliveCountMax=3 \
    -L 127.0.0.1:8050:127.0.0.1:8050 root@198.163.196.164
fi
curl --connect-timeout 3 --max-time 5 -fsS http://127.0.0.1:8050/health >/dev/null
echo 'Media Library: http://127.0.0.1:8050'
echo "Login details: $(pwd)/.local/dashboard-access.txt"
if [ "${1:-}" != '--no-open' ] && [ "$(uname -s)" = 'Darwin' ]; then
  open http://127.0.0.1:8050
fi
