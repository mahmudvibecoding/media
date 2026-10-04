"""Freeze channel selections and commit complete profiles in COPY batches."""
from datetime import date, datetime
import json
import os
from pathlib import Path
import random

from psycopg import sql
from psycopg.types.json import Jsonb

from channel_info import FIELDS
from manage import CHANNEL_ID
from proxy_service_common import digest, write_json


def select_channels(conn, *, resume=None, refresh=False, limit=0):
    if resume:
        folder = Path(resume)
        manifest = json.loads((folder/'manifest.json').read_text())
        source = folder/'input.jsonl'
        if digest(source) != manifest['input_sha256']:
            raise ValueError('Channel input checksum changed')
        ids = [json.loads(line)['channel_id'] for line in source.read_text().splitlines()]
        if len(ids) != manifest['input_count'] or len(set(ids)) != len(ids) or any(not CHANNEL_ID.fullmatch(cid) for cid in ids):
            raise ValueError('Invalid saved channel selection')
        rows = conn.execute('SELECT channel_id,metadata_updated_at FROM public.channels WHERE channel_id=ANY(%s)', (ids,)).fetchall()
        if len(rows) != len(ids):
            raise ValueError('A selected channel is missing')
        start = datetime.fromisoformat(manifest['started_at'])
        pending = {cid for cid, updated in rows if updated is None or updated < start}
        return [cid for cid in ids if cid in pending], manifest['started_at']
    query = 'SELECT channel_id FROM public.channels'
    if not refresh:
        query += ' WHERE metadata_updated_at IS NULL'
    ids = [r[0] for r in conn.execute(query + ' ORDER BY channel_id')]
    random.Random(0).shuffle(ids)
    return ids[:limit or None], None


def save_input(folder, ids, manifest):
    path = folder/'input.jsonl'
    with path.open('w') as output:
        for cid in ids:
            output.write(json.dumps({'channel_id':cid})+'\n')
        output.flush()
        os.fsync(output.fileno())
    manifest.update(input_count=len(ids), input_sha256=digest(path))
    write_json(folder/'manifest.json', manifest)


class ChannelWriter:
    def __init__(self, conn):
        self.conn = conn
        conn.execute('''CREATE TEMP TABLE channel_incoming (
            channel_id text PRIMARY KEY,successful boolean NOT NULL,
            title text,handle text,description text,subscriber_count bigint,video_count bigint,
            view_count bigint,joined_date date,country text,avatar_url text,keywords text[],external_links jsonb,
            observed_at timestamptz NOT NULL,error text) ON COMMIT DELETE ROWS''')

    def write(self, results):
        if len({row['channel_id'] for row in results}) != len(results):
            raise ValueError('Duplicate channel in a batch')
        values = []
        for row in results:
            good = row['status'] == 'ok'
            profile = row.get('metadata') or {}
            if good and (not profile.get('title') or set(profile) != set(FIELDS)):
                raise ValueError('Incomplete normalized channel profile')
            fields = [profile.get(name) if good else None for name in FIELDS]
            if fields[6] is not None:
                fields[6] = date.fromisoformat(fields[6])
            if fields[10] is not None:
                fields[10] = Jsonb(fields[10])
            values.append((row['channel_id'], good, *fields, datetime.fromisoformat(row['observed_at']),
                           None if good else (row.get('error') or 'Unresolved channel')[:500]))
        with self.conn.transaction():
            with self.conn.cursor().copy('COPY channel_incoming FROM STDIN') as copy:
                for row in values:
                    copy.write_row(row)
            found = self.conn.execute('''SELECT count(*) FROM channel_incoming i
                JOIN public.channels c USING(channel_id)''').fetchone()[0]
            if found != len(values):
                raise ValueError('A selected channel is missing; batch rolled back')
            assignments = sql.SQL(',').join(sql.SQL('{}=i.{}').format(sql.Identifier(name), sql.Identifier(name)) for name in FIELDS)
            self.conn.execute(sql.SQL('''UPDATE public.channels c SET {},
                metadata_updated_at=i.observed_at,metadata_error=NULL
                FROM channel_incoming i WHERE c.channel_id=i.channel_id AND i.successful''').format(assignments))
            self.conn.execute('''UPDATE public.channels c SET metadata_error=i.error
                FROM channel_incoming i WHERE c.channel_id=i.channel_id AND NOT i.successful''')

