"""Allow every database test to use the disposable verification cluster."""
import os
from pathlib import Path

import psycopg


def connect_test_database(name, **kwargs):
    dsn = os.environ.get(f"{name.upper()}_TEST_DATABASE_URL")
    if dsn:
        return psycopg.connect(dsn, **kwargs)
    root = Path(__file__).resolve().parents[1]
    return psycopg.connect(dbname=name, user="mahmud",
                          host=str(root / ".local/postgres/socket"), port=5432, **kwargs)
