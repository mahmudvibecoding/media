"""Freeze metadata inputs and save final outcomes in short COPY transactions."""
from datetime import datetime
import json
import os
from pathlib import Path
import random

from collect_video_metadata import has_metadata, metadata_error_reason
from discover_videos import VIDEO_ID
from proxy_service_common import digest, write_json


FIELDS = ('title', 'description', 'duration_seconds', 'published_at', 'thumbnail_url')


def read_input(path):
    rows = []
    seen = set()
    with Path(path).open() as source:
        for line in source:
            row = json.loads(line)
            video, kind = row.get('video_id'), row.get('type')
            if (not isinstance(video, str) or not VIDEO_ID.fullmatch(video) or
                    kind not in ('video', 'short') or video in seen):
                raise ValueError('Metadata input must contain unique video IDs and their types')
            seen.add(video)
            rows.append((video, kind))
    return rows


def select_input(conn, source=None, limit=0):
    """Explicit lists include previous errors; the default selects fresh work."""
    if source is not None:
        requested = read_input(source)
        rows = conn.execute('''SELECT video_id,type,metadata_updated_at FROM public.videos
            WHERE video_id=ANY(%s)''', ([video for video, _ in requested],)).fetchall()
        by_id = {row[0]: row for row in rows}
        if len(rows) != len(requested) or any(by_id[video][1] != kind for video, kind in requested):
            raise ValueError('Metadata input does not match the saved video IDs and types')
        pending = [(video, kind) for video, kind in requested if by_id[video][2] is None]
        already_saved = len(requested)-len(pending)
    else:
        pending = conn.execute('''SELECT video_id,type FROM public.videos
            WHERE metadata_updated_at IS NULL AND metadata_error IS NULL ORDER BY video_id''').fetchall()
        already_saved = 0
    # Spread channels and both video types through pilots and the full run.
    random.Random(0).shuffle(pending)
    total_pending = len(pending)
    return pending[:limit or None], dict(pending_before_limit=total_pending, already_saved=already_saved)


def save_input(folder, rows, values):
    path = folder/'input.jsonl'
    with path.open('w') as output:
        for video, kind in rows:
            output.write(json.dumps(dict(video_id=video, type=kind), separators=(',', ':'))+'\n')
        output.flush()
        os.fsync(output.fileno())
    manifest = dict(version=1, input_count=len(rows), input_sha256=digest(path), **values)
    write_json(folder/'manifest.json', manifest)
    return manifest


def resume_source(folder):
    folder = Path(folder)
    manifest = json.loads((folder/'manifest.json').read_text())
    source = folder/'input.jsonl'
    if digest(source) != manifest['input_sha256'] or len(read_input(source)) != manifest['input_count']:
        raise ValueError('The saved metadata input failed its integrity check')
    return source


class MetadataWriter:
    def __init__(self, conn):
        self.conn = conn
        conn.execute('''CREATE TEMP TABLE metadata_incoming(
            video_id text PRIMARY KEY,successful boolean NOT NULL,title text,description text,
            duration_seconds integer,published_at timestamptz,thumbnail_url text,
            observed_at timestamptz NOT NULL,error text) ON COMMIT DELETE ROWS''')

    def write(self, results):
        if len({row['video_id'] for row in results}) != len(results):
            raise ValueError('A metadata batch contains duplicate final outcomes')
        values = []
        for row in results:
            if not row.get('final'):
                raise ValueError('Only final metadata outcomes can be saved')
            successful = has_metadata(row)
            metadata = row.get('metadata') or {}
            fields = [metadata.get(name) if successful else None for name in FIELDS]
            if fields[3] is not None:
                fields[3] = datetime.fromisoformat(fields[3])
            values.append((row['video_id'], successful, *fields,
                           datetime.fromisoformat(row['observed_at']),
                           None if successful else metadata_error_reason(row)))
        with self.conn.transaction():
            with self.conn.cursor().copy('COPY metadata_incoming FROM STDIN') as copy:
                for row in values:
                    copy.write_row(row)
            matched = self.conn.execute('''SELECT count(*) FROM metadata_incoming s
                JOIN public.videos v USING(video_id)''').fetchone()[0]
            if matched != len(values):
                raise ValueError('A selected metadata video is missing from the database')
            saved = self.conn.execute('''UPDATE public.videos v SET
                title=coalesce(s.title,v.title),description=coalesce(s.description,v.description),
                duration_seconds=coalesce(s.duration_seconds,v.duration_seconds),
                published_at=coalesce(s.published_at,v.published_at),
                thumbnail_url=coalesce(s.thumbnail_url,v.thumbnail_url),
                metadata_updated_at=s.observed_at,metadata_error=NULL
                FROM metadata_incoming s WHERE v.video_id=s.video_id AND s.successful
                    AND v.metadata_updated_at IS NULL
                RETURNING v.video_id,v.title IS NOT NULL,v.description IS NOT NULL,
                    v.duration_seconds IS NOT NULL,v.published_at IS NOT NULL,v.thumbnail_url IS NOT NULL''').fetchall()
            failed = self.conn.execute('''UPDATE public.videos v SET metadata_error=s.error
                FROM metadata_incoming s WHERE v.video_id=s.video_id AND NOT s.successful
                    AND v.metadata_updated_at IS NULL RETURNING v.video_id''').fetchall()
        saved = {row[0]: dict(zip(FIELDS, row[1:])) for row in saved}
        failed = {row[0] for row in failed}
        for result in results:
            video = result['video_id']
            result['saved'] = video in saved
            result['error_saved'] = video in failed
            result['already_saved'] = video not in saved and video not in failed
            result['fields_saved'] = saved.get(video, {})
