"""Import source URLs and parsing hints without restoring historical tables."""
import argparse
import json
from pathlib import Path
from urllib.parse import urlsplit

import psycopg
from collect_proxies import DB, ROOT


def import_lists(conn, rows):
    inserted = 0
    with conn.transaction():
        if not conn.execute("SELECT pg_try_advisory_xact_lock(hashtextextended('proxy:proxy_collection',0))").fetchone()[0]:
            raise RuntimeError('Another collector or cleanup is running')
        for row in rows:
            url = row['url']
            parts = urlsplit(url)
            if parts.scheme not in ('http','https') or not parts.hostname or parts.username or parts.password:
                raise ValueError('Source URLs must be public HTTP(S) URLs without credentials')
            enabled = row.get('enabled',bool(row.get('data_observed_without_account'))
                              and row.get('is_html') is False
                              and row['kind'] in ('feed_candidate','api_candidate'))
            inserted += conn.execute('''INSERT INTO proxy_lists (url,kind,protocol_hints,enabled)
                VALUES (%s,%s,%s,%s) ON CONFLICT (url) DO UPDATE SET
                    kind=excluded.kind,protocol_hints=excluded.protocol_hints
                WHERE (proxy_lists.kind,proxy_lists.protocol_hints) IS DISTINCT FROM
                      (excluded.kind,excluded.protocol_hints)''',
                (url,row['kind'],row.get('protocol_hints',[]),enabled)).rowcount
    return inserted


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--catalog',type=Path,default=ROOT/'outputs/research/proxy-list-database-20260930/catalog.jsonl')
    args = parser.parse_args()
    with args.catalog.open() as source, psycopg.connect(**DB,autocommit=True) as conn:
        rows = (json.loads(line) for line in source if line.strip())
        changed = import_lists(conn,rows)
        total,enabled = conn.execute('SELECT count(*),count(*) FILTER (WHERE enabled) FROM proxy_lists').fetchone()
        print(json.dumps(dict(changed_urls=changed,total_urls=total,enabled_urls=enabled)))


if __name__ == '__main__':
    main()
