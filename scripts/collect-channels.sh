#!/bin/sh
# Collect every pending channel profile; committed profiles survive interruption.
set -eu
cd "$(dirname "$0")/.."
exec docker compose run --rm -T channels python collect_channels.py "$@"
