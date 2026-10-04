#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
python3 configure_dashboard.py configure
compose() { docker compose --env-file .env --env-file .env.dashboard "$@"; }
compose build backend
compose up -d db
compose run --rm -T init
compose run --no-deps --rm -T dashboard-setup
compose run --no-deps --rm -T backend python prepare_dashboard.py
compose up -d --no-deps dashboard
echo 'Media Library is available on http://127.0.0.1:8050 (or MEDIA_DASHBOARD_PORT).'
echo 'Login details: .local/dashboard-access.txt'
