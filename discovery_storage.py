"""Read discovery inputs once and commit whole tab scans in COPY batches."""
from collections import defaultdict
import json
from pathlib import Path

from proxy_catalog import CatalogProxy, SUPPORTED_PROTOCOLS
from proxy_formats import unpack_connection_settings
from runtime_config import connect_database


def load_inventory(limit=0, channel_ids=None):
    with connect_database('media') as conn:
        conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        if channel_ids:
            channels = [r[0] for r in conn.execute(
                'SELECT channel_id FROM public.channels WHERE channel_id=ANY(%s) ORDER BY channel_id',
                (channel_ids,))]
            if set(channels) != set(channel_ids):
                raise ValueError('A requested channel is not in the saved channels table')
        else:
            channels = [r[0] for r in conn.execute(
                'SELECT channel_id FROM public.channels ORDER BY channel_id LIMIT %s', (limit or None,))]
        known = defaultdict(set)
        with conn.cursor(name='discovery_existing_ids') as cursor:
            cursor.itersize = 25000
            if limit or channel_ids:
                cursor.execute('SELECT channel_id,type,video_id FROM public.videos WHERE channel_id=ANY(%s)',
                               (channels,))
            else:
                cursor.execute('SELECT channel_id,type,video_id FROM public.videos')
            for channel, kind, video in cursor:
                known[(channel, kind)].add(video)
    return channels, dict(known)


def load_ranked_proxies(path):
    with Path(path).open() as source:
        ranks = [json.loads(line) for line in source]
    ids = [r['proxy_id'] for r in ranks]
    if (not ids or len(set(ids)) != len(ids) or
            any(type(r['proxy_id']) is not int or r['proxy_id'] < 1 or
                r.get('checks') != 3 or r.get('score') not in (1, 2, 3) or
                r.get('protocol') not in SUPPORTED_PROTOCOLS for r in ranks)):
        raise ValueError('The ranked proxy file must contain unique, tested responders')
    ranks.sort(key=lambda r: (-r['score'], r['average_response_ms'], r['proxy_id']))
    with connect_database('proxy') as conn:
        conn.execute('SET TRANSACTION READ ONLY')
        rows = conn.execute('''SELECT proxy_id,connection_key,address,port,connection_settings
            FROM public.proxies WHERE proxy_id=ANY(%s)''', (ids,)).fetchall()
    configurations = {r[0]: r for r in rows}
    proxies = []
    for rank in ranks:
        row = configurations.get(rank['proxy_id'])
        if row is None or bytes(row[1]).hex() != rank['connection_key']:
            raise ValueError('A ranked proxy does not match its catalog identity')
        transport, settings = unpack_connection_settings(row[4])
        proxies.append(CatalogProxy(row[0], bytes(row[1]), row[2], row[3], transport,
                                    rank['protocol'], settings))
    return proxies, ranks


class BatchWriter:
    """The caller supplies only completed scans; each flush is one transaction."""
    def __init__(self, conn):
        self.conn = conn
        conn.execute('''CREATE TEMP TABLE discovery_incoming(
            video_id text NOT NULL,channel_id text NOT NULL,type text NOT NULL)
            ON COMMIT DELETE ROWS''')

    def write(self, scans):
        if any(not s.get('scan_complete') for s in scans):
            raise ValueError('An incomplete tab scan cannot be committed')
        rows = [(v['video_id'], s['channel_id'], s['type'])
                for s in scans for v in s['videos']]
        inserted = []
        if rows:
            with self.conn.transaction():
                with self.conn.cursor().copy('COPY discovery_incoming FROM STDIN') as copy:
                    for row in rows:
                        copy.write_row(row)
                inserted = self.conn.execute('''INSERT INTO public.videos(video_id,channel_id,type)
                    SELECT DISTINCT ON(video_id) video_id,channel_id,type FROM discovery_incoming
                    ORDER BY video_id,channel_id,type ON CONFLICT(video_id) DO NOTHING
                    RETURNING video_id,channel_id,type''').fetchall()
        counts = defaultdict(int)
        for _, channel, kind in inserted:
            counts[(channel, kind)] += 1
        for scan in scans:
            scan['videos_inserted'] = counts[(scan['channel_id'], scan['type'])]
        return inserted
