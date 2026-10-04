"""Database connections and writable paths shared by the collection tools."""
import os
from pathlib import Path

import psycopg
from psycopg.conninfo import conninfo_to_dict


ROOT = Path(__file__).resolve().parent
STATE_DIR = Path(os.environ.get("MEDIA_STATE_DIR", ROOT / ".local")).expanduser().resolve()
OUTPUT_DIR = Path(os.environ.get("MEDIA_OUTPUT_DIR", ROOT / "outputs")).expanduser().resolve()
BRIDGE_BINARY = Path(os.environ.get("MEDIA_PROXY_BRIDGE_BINARY", ROOT / ".local/bin/proxy-tester"))


def database_options(name):
    if name not in {"media", "proxy"}:
        raise ValueError("Unknown collection database")
    dsn = os.environ.get(f"{name.upper()}_DATABASE_URL")
    options = conninfo_to_dict(dsn) if dsn else {}
    options.setdefault("dbname", name)
    options.setdefault("connect_timeout", 5)
    if not dsn and not any(os.environ.get(key) for key in ("PGHOST", "PGHOSTADDR", "PGSERVICE")):
        # Preserve the project-local developer socket. libpq supplies the OS
        # username or PGUSER; no machine-specific account is required.
        options["host"] = str(STATE_DIR / "postgres/socket")
    return options


def connect_database(name, **kwargs):
    # Explicit call options take precedence over the connection string.
    return psycopg.connect(**{**database_options(name), **kwargs})
