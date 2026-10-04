#!/bin/sh
# Test the stored catalog three times and write the ranked file.
set -eu
cd "$(dirname "$0")/.."
exec docker compose run --rm -T proxy-service python proxy_service.py refresh
