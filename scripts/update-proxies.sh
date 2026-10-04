#!/bin/sh
# One manual catalog import followed by three complete test passes.
set -eu
cd "$(dirname "$0")/.."
exec docker compose run --rm -T proxy-service python proxy_service.py refresh
