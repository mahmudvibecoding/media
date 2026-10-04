#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
umask 077

if ! command -v docker >/dev/null 2>&1; then
  echo "Install Docker with the Compose plugin, then run this script again." >&2
  exit 1
fi
docker compose version >/dev/null
docker info >/dev/null

if [ ! -e .env ]; then
  # Hex passwords work unchanged in Compose, libpq, and shell environments.
  admin_password=$(od -An -N32 -tx1 /dev/urandom | tr -d ' \n')
  app_password=$(od -An -N32 -tx1 /dev/urandom | tr -d ' \n')
  (set -C; printf 'POSTGRES_PASSWORD=%s\nMEDIA_DB_PASSWORD=%s\n' \
    "$admin_password" "$app_password" > .env)
  echo "Created private database credentials in .env."
fi

docker compose up -d --build
docker compose run --rm backend python manage.py status
echo "Backend ready. See the Docker quick start in README.md for collection commands."
