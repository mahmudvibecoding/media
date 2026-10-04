#!/bin/sh
# Import GitHub, refresh source lists, then test and rank the updated catalog.
set -eu
cd "$(dirname "$0")/.."
exec docker compose run --rm -T proxy-service python proxy_service.py refresh "$@"
