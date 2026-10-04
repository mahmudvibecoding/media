# Local PostgreSQL databases

- YouTube database: `media`
- YouTube tables: `public.channels`, `public.videos`, `public.comments`
- Proxy database: `proxy`
- Proxy tables: `public.proxies`, `public.proxy_stats`, `public.proxy_lists`
- Channels: `channel_id TEXT PRIMARY KEY`, nullable `subscriber_count BIGINT`,
  profile fields (`title`, `handle`, `description`, `video_count`, `view_count`,
  `joined_date`, `country`, `avatar_url`, `keywords`, `external_links`), and
  `metadata_updated_at` / `metadata_error`. Migration 012 adds the profile fields.
- Last documented channel count: 55,239 (620 removed after the initial import)
- Seed: `data/channels.csv` (55,859 channel IDs)
- Runtime: Homebrew PostgreSQL 18.6
- Binaries: `/opt/homebrew/opt/postgresql@18/bin`
- Storage: `.local/postgres/cluster`
- Socket: `.local/postgres/socket`
- Authentication: local peer authentication for macOS user `mahmud`

Connect from the project directory:

```sh
/opt/homebrew/opt/postgresql@18/bin/psql -X -h "$PWD/.local/postgres/socket" -p 5432 -U mahmud -d media
```

Start the project database after a reboot:

```sh
/opt/homebrew/opt/postgresql@18/bin/pg_ctl -D "$PWD/.local/postgres/cluster" -l "$PWD/.local/postgres/postgres.log" -w start
```

Stop the project database:

```sh
/opt/homebrew/opt/postgresql@18/bin/pg_ctl -D "$PWD/.local/postgres/cluster" -m fast -w stop
```

The database accepts connections through its private Unix socket and loopback
TCP at `127.0.0.1:5432`. Both databases use this PostgreSQL instance. TCP requires
password authentication. Postico's `Media records` and `Proxy records` connections
use `media_viewer`, which has read-only access. Runtime files are excluded from Git.
The YouTube schema is in `db/schema.sql`; the proxy schema is in
`db/proxy/schema.sql`. Their migrations are in `db/migrations/` and
`db/proxy/migrations/`, respectively.

Connect to the proxy database:

```sh
/opt/homebrew/opt/postgresql@18/bin/psql -X -h "$PWD/.local/postgres/socket" -p 5432 -U mahmud -d proxy
```

The proxy tables were moved from `media` on September 30, 2026 using a PostgreSQL
dump and restore. Every table's ordered binary contents, schema, permissions,
and identity sequence were compared before removing the original proxy tables.
The backup and verification report are retained under
`.local/proxy-move-20260930/`.

## Compact proxy database

The proxy database has three tables and 32 stored columns. Each proxy has at most
one `proxy_stats` row, created when a check or collection outcome is recorded.
Connection fields are shared; each supported website has a fixed group of columns
in that same row. The installed website group is YouTube.

| Table | Columns | Contents |
| --- | ---: | --- |
| `proxies` | 6 | Connection identity, address, port, transport configuration and last discovery |
| `proxy_stats` | 18 | Shared connection observations and separate YouTube results, counters, score inputs and import marker |
| `proxy_lists` | 8 | Source URLs, protocol hints, enable setting and current collection state |

`proxy_catalog`, `proxy_health`, and `proxy_list_catalog` are views for Postico.
`media_viewer` has read access. The catalog view excludes connection settings,
which can contain credentials. It includes shared `working_protocol` and
`last_connection_attempt_at`.
`proxy_health` shows proxies with a statistics row and derives `youtube_responded`,
`youtube_score`, and `youtube_score_age_seconds`. These values are not stored again
in the table. Use `proxy_catalog` to include proxies with no recorded statistics.

Connection identity includes normalized address, port, configured transport and
its original options. Display labels are excluded. `connection_settings` keeps
the transport type with the options needed to open the connection:

```json
{"transport": "http", "options": {}}
```

`transport` is required to try an unverified configuration, including protocols
such as VLESS and VMess. Options containing binary nulls are stored as escaped
JSON text inside `options`; decode that string as JSON. Ordinary options are
objects. The helpers in `proxy_formats.py` handle both forms.

There is no separate `proxies.protocol` column. Confirmed results use shared
`proxy_stats.working_protocol`. Confirming a protocol leaves the saved transport
configuration and identity hash unchanged, so rediscovery and old journals still
match the same proxy.

The BIGINT counters are `connection_attempts`, `successful_connections`, `youtube_requests_sent`,
`youtube_responses_received`, and `youtube_successful_data_received`. An attempt
means one try of a configuration, including a failed connection or a request on
an already open connection. A legacy check can include several protocol
handshakes before one YouTube request. Responses include errors and bot challenges.
Data success comes from the metadata collector's existing outcome, before saving
the metadata. The scorer does not inspect video IDs or metadata fields.

### Shared connection fields

These seven columns belong to the proxy across all websites:

| Field | Meaning |
| --- | --- |
| `proxy_id` | Primary key and reference to the saved configuration in `proxies`. |
| `connection_attempts` | Actual attempts to use the proxy, including failed connections and requests reusing a connection. |
| `successful_connections` | Attempts with a confirmed proxy connection, counted once per check or collection request. Reusing an open connection also counts. |
| `last_connection_attempt_at` | Completion time of the latest actual attempt. NULL before any recorded attempt. |
| `last_connected_at` | Observation time of the latest confirmed connection. A later failed check does not clear it. |
| `last_connection_error` | Proxy endpoint, TLS, authentication or explicit protocol failure from the latest actual attempt. NULL when that attempt had no recorded proxy-specific error. |
| `working_protocol` | Proxy transport confirmed by a verified website response. Any website writer can supply it; failed attempts preserve it. NULL before confirmation. |

A check rejected before a network attempt does not change the shared fields.
A proxy can accept a connection and still fail during its handshake or while
reaching the website. Target-tunnel refusals, ambiguous adapter errors, website
TLS failures, response-body errors, HTTP errors and missing data stay in the
website's error field. Old missing telemetry remains unknown.
Timestamps describe observations, not exact TCP handshake times.

For example, `connect:timeout` and `proxy_handshake:proxy_http_407` can set the
shared error. `proxy_handshake:proxy_http_403`, `youtube_https:timeout` and
`youtube_body:timeout` set the YouTube error. The website error also includes
proxy failures that prevented its request, so it describes that check's outcome.

### YouTube columns

The other 11 columns keep YouTube results and import order independent of future
website groups:

| Field | Meaning |
| --- | --- |
| `youtube_last_attempt_at` | Observation time of the latest actual YouTube attempt; protects against stale attempt imports and is unchanged by checks rejected before an attempt. |
| `youtube_last_http_status` | HTTP status from the latest actual attempt, or NULL if it received no response. |
| `youtube_last_response_at` | Time of the latest verified YouTube HTTP response; retained after later failures. |
| `youtube_last_error` | Most recently imported outcome's safe `stage:code` label for setup, network, HTTP or data failure; NULL when successful or unobserved. |
| `youtube_requests_sent` | Requests actually sent to YouTube, excluding proxy negotiation. |
| `youtube_responses_received` | Verified YouTube HTTP responses, including error statuses and challenges. |
| `youtube_successful_data_received` | Responses the metadata collector reported as usable data, before saving the video. |
| `youtube_weighted_attempts` | Eligible attempt evidence with a one-hour half-life. |
| `youtube_weighted_successful_data_received` | Usable-data evidence with the same decay. |
| `youtube_last_scored_attempt_at` | Observation time anchoring the stored weights. NULL before any scored attempt. |
| `youtube_last_import_key` | SHA-256 identifier of the latest accepted YouTube batch or journal, for replay protection. |

The `proxy_health.youtube_responded` boolean describes the latest actual attempt:

| Value | Meaning |
| --- | --- |
| `TRUE` | A verified YouTube HTTP response arrived, including error statuses. |
| `FALSE` | An actual attempt finished without a YouTube response. |
| `NULL` | No actual YouTube attempt has been recorded. |

A configuration rejection can update `youtube_last_error` and the import key
while preserving the previous actual-attempt fields and counters. Its check time
is not stored, so import rejection-only journals in chronological order to keep
their error current. Response status alone does not measure usable data.

The collector records the upstream connection behind its local bridge. Merely
reaching that local helper is not a successful proxy connection. Helper failures
are excluded from proxy statistics. Statistics writes run independently and
never pause collection; unacknowledged statistics may be lost.

### Adding another website

Add the website's fixed column group, constraints and health-view fields in a new
migration and in `db/proxy/schema.sql`. Add its name to `WEBSITES` in
`proxy_statistics.py`, then pass `website="name"` to `ProxyStatistics` and, when
using catalog selection, `load_catalog`. Unknown website names are rejected until
their group is installed and allowed in code. The current metadata collector and
legacy journal importer both use YouTube.

An accepted batch updates shared connection observations plus only the chosen
website's columns. Replay checks use that website's latest attempt time and import key.
Scores use that website's attempts and data successes. Shared timestamps keep the
latest observation across websites. Keep one ordered producer per proxy and website;
independent producers for the same website can produce stale batches.

## Proxy schema migrations

Fresh databases use `db/proxy/schema.sql`. Existing 10-table catalogs first apply
migration 004, which deletes historical tables after retaining current values.
Catalogs at migration 004 apply migration 005, which rebuilds the three tables
with 26 columns. Migration 005 removes first-discovery time, first-response time,
separate last-attempt time, the total including checks rejected before an attempt,
redundant flags/protocol and the stored URL format hint. It preserves identities,
remaining counters, current collection state and the identity sequence.

Apply each missing migration once:

```sh
# Only if the old 10-table schema is still installed:
/opt/homebrew/opt/postgresql@18/bin/psql -X \
  -h "$PWD/.local/postgres/socket" -U mahmud -d proxy -v ON_ERROR_STOP=1 \
  -f db/proxy/migrations/004_compact_proxy_database.sql
# Historical reduction from 34 to 26 columns:
/opt/homebrew/opt/postgresql@18/bin/psql -X \
  -h "$PWD/.local/postgres/socket" -U mahmud -d proxy -v ON_ERROR_STOP=1 \
  -f db/proxy/migrations/005_reduce_proxy_columns.sql
# Add current names and the four scoring columns to the 26-column schema:
/opt/homebrew/opt/postgresql@18/bin/psql -X \
  -h "$PWD/.local/postgres/socket" -U mahmud -d proxy -v ON_ERROR_STOP=1 \
  -f db/proxy/migrations/006_proxy_scoring.sql
# Add the three connection fields to the 30-column schema:
/opt/homebrew/opt/postgresql@18/bin/psql -X \
  -h "$PWD/.local/postgres/socket" -U mahmud -d proxy -v ON_ERROR_STOP=1 \
  -f db/proxy/migrations/007_proxy_connections.sql
# Move website results into their own columns in the 33-column schema:
/opt/homebrew/opt/postgresql@18/bin/psql -X \
  -h "$PWD/.local/postgres/socket" -U mahmud -d proxy -v ON_ERROR_STOP=1 \
  -f db/proxy/migrations/008_proxy_website_columns.sql
# Share confirmed protocol and remove the separate input-protocol column:
/opt/homebrew/opt/postgresql@18/bin/psql -X \
  -h "$PWD/.local/postgres/socket" -U mahmud -d proxy -v ON_ERROR_STOP=1 \
  -f db/proxy/migrations/009_shared_proxy_protocol.sql
# Remove the YouTube check timestamp and duration:
/opt/homebrew/opt/postgresql@18/bin/psql -X \
  -h "$PWD/.local/postgres/socket" -U mahmud -d proxy -v ON_ERROR_STOP=1 \
  -f db/proxy/migrations/010_drop_youtube_check_fields.sql
```

Migration 005 locks the three tables and compares every retained value before
replacing them in one transaction. Rebuilding reclaims the discarded space;
no separate `VACUUM FULL` is needed afterward.
Migrations 006 and 007 preserve existing values and add defaulted columns in place.
Migration 006 introduced the request/response/data counter names and weighted
scoring. Historical connectivity results have unknown data quality and remain
unscored until an eligible metadata collection attempt is recorded.

Migration 008 renames website-specific fields into `youtube_*`, adds actual
attempt timestamps and `youtube_last_error`, and removes the stored proxy
`status`. It rebuilds `proxy_catalog` and `proxy_health` with the current fields
and preserves existing counters and scoring inputs. Old view columns `attempted`,
`youtube_responds`, `detected_protocol`, `availability` and `score` are removed;
use the timestamps, `youtube_responded`, shared `working_protocol` and
`youtube_score` as appropriate. A previous rejection with recorded earlier
attempts stops the migration: recover its attempt times from retained journals
before migrating. The migration restores view access for the existing
`media_viewer` role, which must exist when running these local migrations.

Migration 009 moves the input transport into `connection_settings.transport`
and nests the original options under `connection_settings.options`, then drops
`proxies.protocol`. It renames `youtube_working_protocol` to shared
`working_protocol`. Proxy IDs, identity hashes, counters, scoring inputs and
the identity sequence remain unchanged. Target and ambiguous failures are
cleared from `last_connection_error`, with the website error retained or filled
when it describes the same latest check. The catalog and health views expose
only the shared confirmed protocol.

Migration 010 removes `youtube_last_checked_at` and
`youtube_last_check_duration_ms` from the table and health view. Import ordering
uses the existing `youtube_last_attempt_at` plus `youtube_last_import_key`;
rejected configurations do not advance the attempt time or add counters. Proxy
selection orders by response status, response recency and proxy ID. All retained
values and the identity sequence stay unchanged.

Back up the database and stop collectors/importers before upgrading so every
writer starts with matching code and schema. The SQL files manage their own
transactions. Run each file once, after checking which migrations the installed
schema still needs.
All statistics writers and migrations coordinate through the same advisory lock.
Migration 009 also takes the source collector's lock before changing connection
storage. Metadata collection never waits for the statistics writer.

The local migration 008 committed on October 1, 2026. Its full database backup is
`.local/proxy-website-columns-20261001/proxy-before-008.dump`; the commit receipt
is `migration.json` in that directory. Python syntax and installed column names
were checked. Runtime tests and a full comparison of data before and after the
migration were not performed. These local artifacts are excluded from Git.

Migration 009's backup and receipt are under
`.local/proxy-shared-protocol-20261001/`. The backup is `proxy-before-009.dump`.
The proxy regression suite covers the new layout, preservation of binary options,
identity matching and separation of proxy errors from website errors.

Migration 010 committed locally on October 1, 2026. Its backup and receipt are
under `.local/proxy-drop-check-fields-20261001/`: `proxy-before-010.dump` and
`migration.json`. An ordered binary comparison verified every retained
statistics value across all 6,014,525 rows. Counts, the identity sequence and
viewer access also matched before and after the migration.

### Historical connection backfill

An older catalog whose connection observations have not been initialized can use
retained finished journals. The current helper targets the schema through
migration 010. The October 1 local backfill was already applied before migration
008; it does not need to be repeated.

For that retained journal set, the original command was:

```sh
.venv/bin/python proxy-tester/backfill_connections.py \
  outputs/proxy-testing/results --expected-connected 798364 --apply \
  --output outputs/proxy-connections/backfill-20261001.json
```

Omit `--apply` to validate and preview. The backfill checks journal identities,
counts, timestamps and available checksums before writing only the three connection
fields. It combines the 17 retained October 1 journals, including the canary,
and verifies that 798,364 proxies connected. Old requests recorded after these
journals also prove connections; other missing observations remain unknown.
Repeating the same backfill cannot add the old counts again. A partially
initialized or changed history is rejected rather than overwriting new counts.

```sql
-- Proxies with at least one recorded successful connection.
SELECT count(*) FROM proxy_stats WHERE successful_connections > 0;

-- Connection errors from each proxy's latest actual attempt.
SELECT last_connection_error, count(*)
FROM proxy_stats WHERE last_connection_error IS NOT NULL
GROUP BY last_connection_error ORDER BY count(*) DESC;
```

## Recent proxy score

`youtube_weighted_attempts` and `youtube_weighted_successful_data_received`
hold fractional evidence anchored at `youtube_last_scored_attempt_at`. Both lose
half their weight per hour. Each eligible new attempt adds one to attempt weight;
collector-reported data success also adds one to success weight. Cancellations,
client pool failures and local resource exhaustion are excluded from scoring.

`proxy_health` decays the stored evidence to query time and calculates:

```text
youtube_score = 100 * (success_weight + 1) / (attempt_weight + 2)
```

NULL means no scored observations. As observations become old, the adjusted score
approaches 50, reflecting uncertainty. Read it with the weighted evidence,
`youtube_last_scored_attempt_at` and `youtube_score_age_seconds`. Latency remains
separate. Reads do not update rows, and no hourly history or scheduled decay job
is required.
The weights in `proxy_stats` are anchored at the last scored attempt; the weights
exposed by `proxy_health` have already decayed to query time. The denominator uses
YouTube's scored attempts; `connection_attempts` remains a shared lifetime counter.
Parsed video-unavailability responses are recorded with a `video:` error label
and excluded from the weighted success score. They still increment connection,
request and response counters, but never the successful-data counter. Temporary
request failures remain scored failures.

```sql
SELECT proxy_id,address,port,connection_attempts,youtube_requests_sent,
       youtube_responses_received,youtube_successful_data_received,
       youtube_score,youtube_weighted_attempts,youtube_last_scored_attempt_at,
       youtube_score_age_seconds
FROM proxy_health
WHERE youtube_last_scored_attempt_at IS NOT NULL
ORDER BY youtube_score DESC NULLS LAST;
```

The metadata collector's statistics are best effort. Its daemon thread combines
pending updates by configuration and writes ordered batches; the HTTP workers
never perform statistics database I/O. A busy memory lock or failed reporting
hook can drop statistics. Database outages are retried in the background while
new outcomes accumulate. There is no queue cap, backpressure, concurrency change
or collection pause caused by statistics. Exit does not wait for a final flush;
pending statistics may be lost. Missing observations are unknown, not failures.

An immutable batch identifier is committed with its counters in
`youtube_last_import_key`. Exact retries are skipped. A batch overlapping a newer
YouTube attempt is skipped for the affected proxy, so an old retry cannot
double-count after detailed history has been discarded. Use one ordered writer
per configuration and website. Concurrent independent producers or legacy imports
can cause stale statistics to be skipped while collection continues.
The run summary's `proxy_statistics` object reports acknowledged, unacknowledged,
dropped, unmatched and stale attempts, plus write failures.

## Source collection

Download enabled public feed/API URLs:

```sh
.venv/bin/python collect_proxies.py
```

Resume the current collection using the run ID printed at startup:

```sh
.venv/bin/python collect_proxies.py --run-id RUN_UUID
```

`--take 24` processes only 24 pending URLs. `--retry-failures` with `--run-id`
requeues failed URLs. Starting a new run replaces each enabled list's previous
run state; an unfinished run must be resumed first. A list's connections and
latest download state commit together. Database write errors stop collection.

Defaults: 24 downloads at once, 8 per raw.githubusercontent.com and 2 per other
host, with two retries for transient failures. HTTP 429 stops further requests
to that host for the invocation. Downloads use HTTP/2 where supported,
compression and reused connections. Individual proxy endpoints are not contacted.

Text, CSV, JSON/JSONL, XML, safe YAML, common subscription URIs and base64
subscriptions are supported. Incomplete responses and lists exceeding 256 MiB
decoded size are recorded as failures. Supported page/cursor APIs are followed
to completion; a later-page failure saves fetched entries as `collected_partial`.
Payloads and parser caches remain under `.local/proxy-collection/`; only the
latest download's checksum, path and parser state are stored in the database.

Maintenance applies to current saved list state:

```sh
.venv/bin/python collect_proxies.py --run-id RUN_UUID --reparse
.venv/bin/python collect_proxies.py --run-id RUN_UUID --complete-pagination
```

Reparsing upserts corrected configurations. It cannot remove old configurations
based on source membership because those links have been deleted.

Import URLs and parsing hints from a JSONL source catalog:

```sh
.venv/bin/python import_proxy_lists.py --catalog PATH_TO_CATALOG_JSONL
```

Each row supplies `url`, `kind`, `protocol_hints`, and optionally `enabled`.
The collector derives format hints from the URL filename suffix. New URLs default to disabled unless explicitly enabled or the
catalog marks their feed/API check as non-HTML public proxy data. Re-importing an
existing URL preserves its enable setting and collection state. The historical
research importer delegates to this compact importer.

## Proxy connectivity results

The tester imports immutable finished journals into `proxy_stats`. Counts and
latest state update together in one transaction after the complete input has
been validated. Status must consistently determine whether the latest check
attempted a connection and received a response; inconsistent or unsupported
status results are rejected. Journal status is validated during import; it is
not stored in `proxy_stats`. An exact retry of the latest YouTube journal for a
proxy is skipped using `youtube_last_import_key`. Older or changed overlapping
YouTube attempt results are rejected before any counts change, using
`youtube_last_attempt_at`. Rejections before an attempt do not advance that time
or add counters; their errors follow import order.
Import rounds in chronological order; do not append to an already imported
journal or replay old rounds after importing newer ones for the same proxies.

See [the tester guide](../proxy-tester/README.md) for the request, server operation,
imports, verification and queries. Parser and database integration checks:

```sh
PROXY_TEST_DATABASE=1 .venv/bin/python -m unittest discover -s tests -p '*prox*.py' -v
```

Database tests run in isolated schemas and roll back their fixtures.

## Videos

Created with `db/migrations/002_videos.sql`:

- `video_id TEXT PRIMARY KEY`: shared ID namespace for videos and Shorts.
- `channel_id TEXT NOT NULL`: foreign key to `channels.channel_id`.
- `type TEXT NOT NULL`: constrained to `video` or `short`.
- `published_at TIMESTAMPTZ`: nullable until a publication timestamp is obtained.
- `title TEXT`, `description TEXT`: nullable until returned by the metadata endpoint.
- `duration_seconds INTEGER`: nullable, with a nonnegative-value constraint.
- `thumbnail_url TEXT`: nullable; the last thumbnail URL returned by the metadata endpoint.
- `metadata_updated_at TIMESTAMPTZ`: nullable; the time of the last successful metadata save.
- `metadata_error TEXT`: nullable; the latest metadata failure reason, cleared by a successful save.
- `view_count BIGINT`: nullable; the latest collected total views, constrained to nonnegative values.
- `like_count BIGINT`: nullable; the latest collected total likes, constrained to nonnegative values.
- `stats_updated_at TIMESTAMPTZ`: nullable; when the saved statistics were successfully collected.
- `stats_error TEXT`: nullable; the latest statistics collection error, cleared by a successful save.
- `comments_updated_at TIMESTAMPTZ`: nullable; when the latest successful comment scan finished importing.
- `comments_error TEXT`: nullable; the latest comment collection error, cleared after a successful scan finishes importing.
- Index: `(channel_id, type)`.
- Pending metadata index: `(video_id) WHERE metadata_updated_at IS NULL`.
- Pending comments index: `(video_id) WHERE comments_updated_at IS NULL`.

Title, description, and duration were added with `db/migrations/004_video_metadata.sql`.
`db/migrations/005_drop_video_player_status.sql` removes the former `player_status`
column. `db/migrations/006_video_thumbnail_url.sql` adds `thumbnail_url`. Widths,
heights, and image files are not collected. A missing URL preserves the existing
value. Player status is used for response validation, retries, and failure reasons.
`db/migrations/007_video_metadata_updated_at.sql` adds `metadata_updated_at` and
the pending metadata index. Existing and newly inserted rows start with `NULL`.
Each successful metadata update sets this timestamp atomically with its fields.
`db/migrations/008_video_metadata_error.sql` adds `metadata_error`, initially
`NULL`. A final failure replaces this value without changing existing metadata
or its timestamp. A successful metadata update clears it atomically. The column
stores a short code and explanation, capped at 500 characters by the collector.
The metadata collector fills the existing `published_at` column only from a full
timestamp with a timezone. It preserves previously saved values when a later
response omits metadata. Selection requires `metadata_updated_at IS NULL`;
`--skip-errors` additionally requires `metadata_error IS NULL`, including when
explicit IDs are provided. A run loads its pending IDs once, so
newly inserted videos wait for the next run. Failed requests leave timestamps
unchanged and record their latest reason in `metadata_error`. Individual failure
log files are not created. The summary contains aggregate counts, including retries.
All unsuccessful metadata fetches use the same retry limit: ten immediate retries
by default, then the final failure reason is stored. A successful response ends
retries immediately. Database write errors stop collection.
With `MEDIA_PROXY_URLS` or catalog mode, each video instead gets one attempt
through its assigned proxy. Assignment rotates between videos; a failed attempt
stores the error and processing moves to the next video.

`db/migrations/009_video_stats.sql` adds the four statistics columns with no
non-NULL defaults. Unknown or unavailable counts remain `NULL`; an actual zero
is stored as `0`. Statistics collectors should save counts and their observation
time atomically, clearing `stats_error` on success. Failed refreshes should update
only `stats_error`, preserving previous counts and `stats_updated_at`.
`metadata_updated_at` continues to track metadata independently. These columns
are ready for a statistics collector.

The discovery collector inserts the returned IDs from both tabs. The primary
key skips known IDs with `ON CONFLICT DO NOTHING`;
`RETURNING video_id` identifies newly inserted rows. Existing metadata is preserved.
Each complete tab scan commits as one transaction, and new rows have
`published_at=NULL`. Every scan follows pagination through the previously stored
range or to the end of the tab. A first scan with no stored range follows the
available history, subject to the configured page limit. A failed scan inserts
no partial results, so a retry can safely start from
page one. Recurring polling is still a separate step. The local `media_viewer`
role has SELECT on this table, granted
separately from the portable schema migration.

## Comments

`db/migrations/011_video_comments.sql` adds `public.comments`, the two comment
progress columns on `videos`, and `videos_comments_pending_idx`. Apply it once
after migration `010`. Fresh databases use the matching `db/schema.sql`.

| Column | Type | Meaning |
| --- | --- | --- |
| `video_id` | `TEXT NOT NULL` | References `videos.video_id`. |
| `comment_id` | `TEXT NOT NULL` | YouTube comment identifier. |
| `text` | `TEXT NOT NULL` | Full displayed text, including Unicode and line breaks. Empty text is allowed. |
| `author_channel_id` | `TEXT` | Author's channel ID when available. |
| `author_name` | `TEXT` | Author's displayed name when available. |
| `is_pinned` | `BOOLEAN` | TRUE or FALSE when known; NULL when unknown. |

The primary key `(video_id, comment_id)` prevents duplicate comments on a video
and supports fetching a video's saved history. The foreign key rejects unknown
videos and prevents removing a referenced video without handling its comments.
There are exactly six comment columns. Replies, likes, publication dates, and
per-comment collection times are outside this schema.

`collect_video_comments.py` implements single-video collection. `comments_bulk.py`
and `comments_bulk_import.py` provide durable page resume and local or remote
outbox import. The write contract is:

- Buffer individual pages in SQLite; incomplete scans do not insert public rows.
- After the scan and all its pages have been imported, update
  `comments_updated_at` and clear `comments_error` in the completion transaction.
- On failure, update only `comments_error`; preserve prior comments and the last
  successful `comments_updated_at` value.
- Keep continuation tokens, active scan IDs, and saved-history boundaries in the
  durable worker queue. There is no `comment_scan_state` table.

The single-video collector buffers pages in temporary SQLite storage and imports
only after it reaches saved history or the end. It copies the prior completed
history before fetching and holds a per-video advisory lock using
`hashtextextended('media.video-comments:' || video_id, 0)`. Other comment writers
must coordinate through that lock. A failed run discards its buffer; its next
attempt restarts from page one. Successful empty or disabled sections also
advance the completion timestamp. Unavailable videos retain the prior timestamp
and record an error.

The bulk importer uses the same per-video advisory lock. It verifies a whole
checksummed batch before importing any of its scans, then commits each successful
scan's rows and completion timestamp together. Its local `import.sqlite3` journal
records the proposed timestamp before PostgreSQL commits, allowing recovery after
a lost acknowledgement without advancing that timestamp twice. A snapshot whose
baseline no longer matches the database cannot overwrite a newer completed scan.
Resume tokens, frozen history, leases, and import receipts remain in run files;
the public schema still has only six comment fields and two video progress fields.

The migration gives both video progress columns NULL defaults, including for
existing videos. The partial index covers videos with no successful comment scan
yet, including those with a recorded error.

Portable schema files do not create roles or grant access. On this local setup,
grant the existing read-only viewer account access after applying the migration:

```sql
GRANT SELECT ON public.comments TO media_viewer;
```

The viewer's existing SELECT permission on `videos` also covers its new columns.
Schema, constraint, migration, and permission checks are in
`tests/test_comments_schema.py`, `tests/test_collect_video_comments.py`, and
`tests/test_comments_bulk.py`.
Database tests use an isolated schema and roll
back their fixtures:

```sh
MEDIA_TEST_DATABASE_URL="host=$PWD/.local/postgres/socket dbname=media user=mahmud" \
  .venv/bin/python -m unittest discover -s tests -p '*comments*.py' -v
```

## Remove scan initialization markers

`db/migrations/010_drop_channel_scan_state.sql` removes `channel_scan_state`.
Discovery uses saved video IDs to decide when to stop and checks both tabs on
every run, including previously empty or absent tabs. No replacement flags are
stored on `channels`, and `--initial-only` is no longer supported.

Apply the migration to an existing database after updating the discovery code:

```sh
/opt/homebrew/opt/postgresql@18/bin/psql -X \
  -h "$PWD/.local/postgres/socket" -U mahmud -d media \
  -v ON_ERROR_STOP=1 --single-transaction \
  -f db/migrations/010_drop_channel_scan_state.sql
```

Fresh databases use `db/schema.sql`, which contains no initialization table.
Migration 003 remains in the migration history for older databases.

## Upgrade

Upgraded from PostgreSQL 14.20 to 18.6 on September 29, 2026 using a database
dump and restore. All 55,859 channel IDs were compared exactly before and after
the upgrade.

Upgrade backups were deleted at the user's request. Verification is recorded in
`.local/postgres/upgrade-18.6/verification.json`.

## Subscriber count

The column was added with `db/migrations/001_subscriber_count.sql`. Counts are
the rounded public values. `NULL` means no count has been collected; failed or
missing responses preserve an existing count. There is no check-time column.

See the project `README.md` for collector commands.
