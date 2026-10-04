"""Build dashboard search and pagination indexes without blocking collection writes."""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import time

from psycopg import sql

from dashboard.search import INDEXES
from runtime_config import connect_database


def prepare_table(table):
    with connect_database('media', autocommit=True, application_name='media-dashboard-indexes') as conn:
        conn.execute("SET maintenance_work_mem='2GB'")
        conn.execute('SET max_parallel_maintenance_workers=8')
        # Concurrent index builds wait for older snapshots, including builds on
        # other tables. A short lock timeout would discard successful large scans.
        conn.execute('SET lock_timeout=0')
        for name, definition in INDEXES[table].items():
            source = f'CREATE INDEX CONCURRENTLY {name} ON public.{table} {definition}'
            signature = 'media-dashboard:' + hashlib.sha256(source.encode()).hexdigest()
            row = conn.execute('''SELECT i.indisvalid, obj_description(c.oid,'pg_class')
                FROM pg_class c JOIN pg_index i ON i.indexrelid=c.oid
                WHERE c.oid=to_regclass(%s)''', ('public.'+name,)).fetchone()
            if row and row[0]:
                if row[1] != signature:
                    raise RuntimeError('Index definition must be verified: '+name)
                print(json.dumps(dict(index=name, status='ready')), flush=True)
                continue
            if row:
                conn.execute(sql.SQL('DROP INDEX CONCURRENTLY public.{}').format(sql.Identifier(name)))
            started = time.monotonic()
            print(json.dumps(dict(index=name, status='building')), flush=True)
            conn.execute(source)
            conn.execute(sql.SQL('COMMENT ON INDEX public.{} IS {}').format(sql.Identifier(name), sql.Literal(signature)))
            print(json.dumps(dict(index=name, status='ready', seconds=round(time.monotonic()-started, 2))), flush=True)
        conn.execute(sql.SQL('ANALYZE public.{}').format(sql.Identifier(table)))


def main():
    with connect_database('media', autocommit=True) as conn:
        if not conn.execute("SELECT pg_try_advisory_lock(hashtext('media.dashboard-indexes'))").fetchone()[0]:
            raise RuntimeError('Dashboard indexes are already being prepared')
        with ThreadPoolExecutor(max_workers=3) as executor:
            list(executor.map(prepare_table, INDEXES))
    print(json.dumps(dict(ready=True, indexes=sum(map(len, INDEXES.values())))), flush=True)


if __name__ == '__main__':
    main()
