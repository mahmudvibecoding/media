#!/bin/sh
# Every resource belongs to a new disposable Compose project.
set -eu
cd "$(dirname "$0")/.."
umask 077
docker compose version >/dev/null
docker info >/dev/null
report_dir=$(mktemp -d "${TMPDIR:-/tmp}/media-docker-check.XXXXXXXX")
COMPOSE_PROJECT_NAME=$(basename "$report_dir" | tr '[:upper:].' '[:lower:]-')
export COMPOSE_PROJECT_NAME
credentials="$report_dir/credentials.env"
printf 'POSTGRES_PASSWORD=%s\nMEDIA_DB_PASSWORD=%s\n' \
  "$(od -An -N32 -tx1 /dev/urandom | tr -d ' \n')" \
  "$(od -An -N32 -tx1 /dev/urandom | tr -d ' \n')" > "$credentials"
compose() { docker compose --env-file "$credentials" -p "$COMPOSE_PROJECT_NAME" "$@"; }
cleanup() {
  result=$?
  trap - EXIT HUP INT TERM
  compose down --volumes --remove-orphans >> "$report_dir/cleanup.log" 2>&1 || true
  docker image rm "$COMPOSE_PROJECT_NAME-backend:local" >> "$report_dir/cleanup.log" 2>&1 || true
  rm -f "$credentials"
  echo "Verification reports: $report_dir"
  exit "$result"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM

echo "Building and starting isolated project $COMPOSE_PROJECT_NAME"
if ! compose up -d --build > "$report_dir/setup.log" 2>&1; then
  tail -80 "$report_dir/setup.log"
  exit 1
fi
compose run --rm backend python manage.py status
# This optional read-only role is used by the permission regression test.
compose exec -T db psql -U postgres -d postgres -v ON_ERROR_STOP=1 \
  -c 'CREATE ROLE media_viewer NOLOGIN; GRANT media_viewer TO media;'
if ! compose run --rm -e MEDIA_TEST_DATABASE_URL=dbname=media \
  -e PROXY_TEST_DATABASE_URL=dbname=proxy -e PROXY_TEST_DATABASE=1 \
  backend python -m unittest discover -s tests -v > "$report_dir/python-tests.log" 2>&1; then
  tail -100 "$report_dir/python-tests.log"
  exit 1
fi
tail -5 "$report_dir/python-tests.log"
compose run --rm backend python scripts/docker_smoke.py collect
compose down
compose up -d
compose run --rm backend python scripts/docker_smoke.py verify
echo "Docker verification passed: tests, fixture collection, replay, and persistent storage."
