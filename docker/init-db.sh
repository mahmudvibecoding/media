#!/usr/bin/env bash
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  --set=app_password="$MEDIA_DB_PASSWORD" <<'SQL'
CREATE ROLE media LOGIN PASSWORD :'app_password';
CREATE DATABASE media OWNER media;
CREATE DATABASE proxy OWNER media;
SQL
