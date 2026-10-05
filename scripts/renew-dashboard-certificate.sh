#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
compose() {
  docker compose --env-file .env --env-file .env.dashboard \
    -f compose.yaml -f compose.host-network.yaml -f compose.public-dashboard.yaml "$@"
}
compose run --rm --no-deps dashboard-certbot renew --quiet "$@"
compose exec -T dashboard-web nginx -t
compose exec -T dashboard-web nginx -s reload
