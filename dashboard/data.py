"""Indexed, bounded, read-only queries for the library."""
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import re
import threading
import time
from urllib.parse import urlencode

from psycopg.rows import dict_row
from psycopg.errors import QueryCanceled
from psycopg_pool import ConnectionPool

from dashboard.search import DOCUMENTS, NAMES, partial_name_query
from dashboard.security import Cursors


CHANNEL_ID = re.compile(r'UC[A-Za-z0-9_-]{22}\Z')
VIDEO_ID = re.compile(r'[A-Za-z0-9_-]{11}\Z')
PAGE_SIZE = 50
SORTS = {
    'channels': {
        'subscribers': ('Most subscribers', 'coalesce(subscriber_count,-1)', 'DESC', 'number'),
        'name': ('Name A–Z', 'public.media_search_normalize(title)', 'ASC', 'text'),
    },
    'videos': {
        'recent': ('Newest videos', "coalesce(published_at,'1900-01-01 00:00:00+00'::timestamptz)", 'DESC', 'date'),
        'views': ('Most viewed', 'coalesce(view_count,-1)', 'DESC', 'number'),
        'title': ('Title A–Z', 'public.media_search_normalize(title)', 'ASC', 'text'),
    },
    'comments': {
        'video': ('Video order', 'video_id', 'ASC', 'text'),
        'author': ('Author A–Z', 'public.media_search_normalize(author_name)', 'ASC', 'text'),
    },
}
DEFAULT_SORTS = {kind: next(iter(sorts)) for kind, sorts in SORTS.items()}
COLUMNS = {
    'channels': 'channel_id,title,handle,subscriber_count,video_count,view_count,country,avatar_url,description,keywords,metadata_updated_at,metadata_error',
    'videos': 'video_id,channel_id,type,published_at,title,description,duration_seconds,thumbnail_url,view_count,like_count,metadata_updated_at',
    'comments': 'video_id,comment_id,text,author_channel_id,author_name,is_pinned',
}


@dataclass(frozen=True)
class Selection:
    kind: str
    q: str = ''
    sort: str = ''
    channel: str = ''
    video: str = ''
    type: str = ''
    pinned: bool = False
    cursor: str = ''
    detail: str = ''
    detail_video: str = ''

    @classmethod
    def parse(cls, kind, values):
        if kind not in SORTS:
            raise ValueError('Unknown library section.')
        args = {key: values.get(key, '').strip() for key in ('q','sort','channel','video','type','cursor','detail','detail_video')}
        args['sort'] = args['sort'] or DEFAULT_SORTS[kind]
        if args['sort'] not in SORTS[kind]:
            raise ValueError('Choose a valid sort order.')
        if len(args['q']) > 200 or '\x00' in args['q']:
            raise ValueError('Keep searches under 200 characters.')
        if args['q'] and not any(char.isalnum() for char in args['q']):
            raise ValueError('Enter a word, name, or phrase to search.')
        if args['channel'] and (kind == 'channels' or not CHANNEL_ID.fullmatch(args['channel'])):
            raise ValueError('Invalid channel filter.')
        if args['video'] and (kind != 'comments' or not VIDEO_ID.fullmatch(args['video'])):
            raise ValueError('Invalid video filter.')
        if args['type'] and (kind != 'videos' or args['type'] not in ('video','short')):
            raise ValueError('Invalid video type.')
        if len(args['cursor']) > 4096:
            raise ValueError('Invalid page link.')
        detail = args['detail']
        if detail:
            if kind == 'channels' and not CHANNEL_ID.fullmatch(detail):
                raise ValueError('Invalid channel.')
            if kind == 'videos' and not VIDEO_ID.fullmatch(detail):
                raise ValueError('Invalid video.')
            if kind == 'comments' and (len(detail) > 200 or not re.fullmatch(r'[A-Za-z0-9_.-]+',detail)
                    or not VIDEO_ID.fullmatch(args['detail_video'])):
                raise ValueError('Invalid comment.')
        args['pinned'] = kind == 'comments' and values.get('pinned') == '1'
        return cls(kind=kind, **args)

    def params(self, **changes):
        values = {key:getattr(self,key) for key in ('q','sort','channel','video','type','cursor','detail','detail_video')}
        values['pinned'] = '1' if self.pinned else ''
        values.update(changes)
        return {key:value for key,value in values.items() if value not in ('',None,False)}

    def url(self, **changes):
        query = urlencode(self.params(**changes))
        return '/'+self.kind+('?' + query if query else '')

    @property
    def scope(self):
        value = dict(kind=self.kind, **self.params(cursor='',detail='',detail_video=''))
        return hashlib.sha256(json.dumps(value,sort_keys=True).encode()).hexdigest()[:24]


class Repository:
    def __init__(self, conninfo, secret, *, pool=None):
        self.pool = pool or ConnectionPool(conninfo, min_size=1, max_size=8, open=False, timeout=10,
            kwargs={'autocommit':True, 'row_factory':dict_row, 'application_name':'media-library',
                    'options':'-c default_transaction_read_only=on -c statement_timeout=8000 -c idle_in_transaction_session_timeout=10000'})
        self.cursors = Cursors(secret)
        self._totals = None
        self._totals_at = 0
        self._totals_lock = threading.Lock()

    def open(self):
        self.pool.open(wait=True)

    def close(self):
        self.pool.close()

    def totals(self):
        if self._totals is not None and time.monotonic()-self._totals_at < 600:
            return self._totals
        with self._totals_lock:
            if self._totals is not None and time.monotonic()-self._totals_at < 600:
                return self._totals
            with self.pool.connection() as conn, conn.transaction():
                conn.execute("SET LOCAL statement_timeout='30s'")
                result = {name:conn.execute(f'SELECT count(*) AS total FROM public.{name}').fetchone()['total']
                          for name in SORTS}
                result['updated_at'] = datetime.now(timezone.utc)
            self._totals, self._totals_at = result, time.monotonic()
            return result

    def context(self, selected):
        result = {}
        with self.pool.connection() as conn:
            if selected.channel:
                result['channel'] = conn.execute('SELECT channel_id,title,handle,avatar_url FROM public.channels WHERE channel_id=%s',
                                                 (selected.channel,)).fetchone()
                if not result['channel']:
                    raise LookupError('This channel is not in the library.')
            if selected.video:
                result['video'] = conn.execute('SELECT video_id,title,channel_id,thumbnail_url FROM public.videos WHERE video_id=%s',
                                               (selected.video,)).fetchone()
                if not result['video'] or selected.channel and result['video']['channel_id'] != selected.channel:
                    raise LookupError('This video is not in the selected library.')
                if not selected.channel:
                    result['channel'] = conn.execute('SELECT channel_id,title,handle,avatar_url FROM public.channels WHERE channel_id=%s',
                                                     (result['video']['channel_id'],)).fetchone()
        return result

    def search(self, selected):
        kind, params, where = selected.kind, [], []
        label, metric, direction, value_type = SORTS[kind][selected.sort]
        ties = ['channel_id'] if kind == 'channels' else ['video_id'] if kind == 'videos' else ['comment_id'] if selected.sort == 'video' else ['video_id','comment_id']
        keys = [metric, *ties]
        directions = [direction, *['ASC']*len(ties)]
        phrase = kind == 'comments' and '"' in selected.q and '-' not in selected.q
        search_query = "websearch_to_tsquery('simple',public.media_search_normalize(%s))"
        if selected.q:
            # GIN stores words, not their positions. Verify a positive phrase
            # after taking a bounded batch of its word matches, so a common
            # phrase does not require parsing hundreds of thousands of comments.
            candidate_query = f"regexp_replace(({search_query})::text,'<->|<[0-9]+>','&','g')::tsquery" if phrase else search_query
            match = f"public.media_search_vector({DOCUMENTS[kind]}) @@ {candidate_query}"
            params.append(selected.q)
            if kind == 'channels' and partial_name_query(selected.q):
                pattern = '%'+selected.q.replace('\\','\\\\').replace('%','\\%').replace('_','\\_')+'%'
                match = '('+match+f' OR {NAMES} LIKE public.media_search_normalize(%s))'
                params.append(pattern)
            where.append(match)
        if selected.channel:
            if kind == 'videos':
                where.append('channel_id=%s')
            else:
                where.append('video_id IN (SELECT video_id FROM public.videos WHERE channel_id=%s)')
            params.append(selected.channel)
        if selected.video:
            where.append('video_id=%s')
            params.append(selected.video)
        if selected.type:
            where.append('type=%s')
            params.append(selected.type)
        if selected.pinned:
            where.append('is_pinned IS TRUE')
        cursor = self.cursors.decode(selected.cursor, selected.scope) if selected.cursor else None
        previous = bool(cursor and cursor['direction'] == 'previous')
        page = cursor['page'] if cursor else 1
        values = None
        if cursor:
            values = cursor['keys']
            if len(values) != len(keys):
                raise ValueError('Invalid page link.')
            if value_type == 'number' and type(values[0]) is not int:
                raise ValueError('Invalid page link.')
            if value_type == 'text' and (not isinstance(values[0],str) or len(values[0]) > 2000):
                raise ValueError('Invalid page link.')
            if value_type == 'date':
                try:
                    values[0] = datetime.fromisoformat(values[0])
                except (ValueError, TypeError) as exc:
                    raise ValueError('Invalid page link.') from exc
            if any(not isinstance(value,str) or len(value)>200 for value in values[1:]):
                raise ValueError('Invalid page link.')
        ordering = [flip(direction) if previous else direction for direction in directions]
        order = ','.join(f'{key} {direction}' for key,direction in zip(keys,ordering))
        batch_size = 512 if phrase else PAGE_SIZE+1

        def build_query(boundary):
            filters, arguments = list(where), list(params)
            if boundary:
                condition, parameters = page_boundary(keys,directions,boundary,previous)
                filters.append(condition)
                arguments.extend(parameters)
            query = f"SELECT {COLUMNS[kind]},{metric} AS _key FROM public.{kind}"
            if filters:
                query += ' WHERE '+' AND '.join(filters)
            query += f' ORDER BY {order} LIMIT {batch_size}'
            if phrase:
                query = f'''WITH candidates AS MATERIALIZED ({query}) SELECT *,
                    public.media_search_vector({DOCUMENTS[kind]}) @@ {search_query} AS _matches FROM candidates'''
                arguments.append(selected.q)
            if kind == 'videos':
                query = f'''WITH matches AS MATERIALIZED ({query}) SELECT m.*,c.title AS channel_title,c.handle AS channel_handle
                    FROM matches m JOIN public.channels c USING(channel_id)
                    ORDER BY m._key {ordering[0]},m.video_id {ordering[1]}'''
            elif kind == 'comments':
                after = ','.join(f'm.{key} {order}' for key,order in zip(['_key',*ties],ordering))
                query = f'''WITH matches AS MATERIALIZED ({query}) SELECT m.*,v.title AS video_title,v.channel_id,c.title AS channel_title
                    FROM matches m JOIN public.videos v USING(video_id) JOIN public.channels c ON c.channel_id=v.channel_id
                    ORDER BY {after}'''
            return query, arguments

        began = time.monotonic()
        rows = []
        with self.pool.connection() as conn, conn.transaction():
            conn.execute("SET LOCAL work_mem='64MB'")
            while True:
                remaining = 8000-int((time.monotonic()-began)*1000)
                if remaining <= 0:
                    raise QueryCanceled('Search time limit reached')
                conn.execute("SELECT set_config('statement_timeout',%s,true)",(str(remaining),))
                query, arguments = build_query(values)
                batch = conn.execute(query,arguments).fetchall()
                rows.extend(row for row in batch if row.pop('_matches',True))
                if not phrase or len(rows)>PAGE_SIZE or len(batch)<batch_size:
                    break
                values = [batch[-1]['_key'],*[batch[-1][key] for key in ties]]
        more = len(rows) > PAGE_SIZE
        rows = rows[:PAGE_SIZE]
        if previous:
            rows.reverse()
        has_previous = more if previous else bool(cursor)
        has_next = bool(cursor) if previous else more
        def link(row, way, target_page):
            token = self.cursors.encode(dict(scope=selected.scope,keys=[row['_key'],*[row[key] for key in ties]],
                                             direction=way,page=target_page))
            return selected.url(cursor=token,detail='',detail_video='')
        return dict(rows=rows,page=page,previous=link(rows[0],'previous',max(1,page-1)) if rows and has_previous else None,
                    next=link(rows[-1],'next',page+1) if rows and has_next else None,
                    seconds=round(time.monotonic()-began,3),sort_label=label)

    def detail(self, selected):
        with self.pool.connection() as conn:
            if selected.kind == 'channels':
                row = conn.execute('SELECT * FROM public.channels WHERE channel_id=%s',(selected.detail,)).fetchone()
                if row:
                    row['saved_videos'] = conn.execute('SELECT count(*) AS total FROM public.videos WHERE channel_id=%s',
                                                       (selected.detail,)).fetchone()['total']
            elif selected.kind == 'videos':
                row = conn.execute('''SELECT v.*,c.title AS channel_title FROM public.videos v JOIN public.channels c USING(channel_id)
                    WHERE video_id=%s''',(selected.detail,)).fetchone()
                if row:
                    row['saved_comments'] = conn.execute('SELECT count(*) AS total FROM public.comments WHERE video_id=%s',
                                                         (selected.detail,)).fetchone()['total']
            else:
                row = conn.execute('''SELECT m.*,v.title AS video_title,v.channel_id,c.title AS channel_title FROM public.comments m
                    JOIN public.videos v USING(video_id) JOIN public.channels c ON c.channel_id=v.channel_id
                    WHERE m.video_id=%s AND m.comment_id=%s''',(selected.detail_video,selected.detail)).fetchone()
            if not row:
                raise LookupError('This record is not in the library.')
            return row


def flip(direction):
    return 'DESC' if direction == 'ASC' else 'ASC'


def page_boundary(keys, directions, values, previous):
    if len(set(directions)) == 1:
        operator = '>' if (directions[0] == 'ASC') != previous else '<'
        return '('+','.join(keys)+f') {operator} ('+','.join(['%s']*len(keys))+')', list(values)
    # Bound the leading index column before checking tied values.
    operator = '>=' if (directions[0] == 'ASC') != previous else '<='
    parameters, conditions = [values[0]], []
    for i,key in enumerate(keys):
        gt = (directions[i] == 'ASC') != previous
        conditions.append('('+' AND '.join([f'{keys[j]}=%s' for j in range(i)]+[f"{key} {'>' if gt else '<'} %s"])+')')
        parameters.extend(values[:i+1])
    return f'{keys[0]} {operator} %s AND ('+' OR '.join(conditions)+')', parameters
