#!/bin/sh
export LD_LIBRARY_PATH="/usr/local/lib/media-postgres${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
exec "/usr/local/lib/media-postgres/${0##*/}" "$@"
