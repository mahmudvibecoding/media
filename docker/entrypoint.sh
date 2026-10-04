#!/bin/sh
set -eu
umask 077
mkdir -p "$MEDIA_STATE_DIR" "$MEDIA_OUTPUT_DIR"
exec "$@"
