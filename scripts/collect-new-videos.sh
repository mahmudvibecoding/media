#!/bin/sh
# Scan every saved channel through the ranked proxy pool and save new IDs.
set -eu
cd "$(dirname "$0")/.."
exec docker compose run --rm -T discovery python discover_all.py "$@"
