#!/bin/sh
# Collect pending video metadata through ranked proxies and a batch writer.
set -eu
cd "$(dirname "$0")/.."
exec docker compose run --rm -T metadata python collect_metadata_all.py "$@"
