# YouTube collection and proxy catalog

Collect channel subscriber counts, discover videos and Shorts, and save video
metadata, view and like counts, and public top-level comments. Collectors use
direct requests or saved proxy configurations. The proxy catalog
stores public source lists, connection settings, and compact website statistics.
YouTube collectors use raw InnerTube HTTP requests.

The bundled `data/channels.csv` seed contains 55,859 IDs. The documented local
subscriber run retained 55,239 channels after 620 were removed. Subscriber counts
are rounded public values, such as `4.16M` → `4160000`.

## Docker quick start

Install Docker Engine with Compose v2, or Docker Desktop, then run:

```sh
git clone https://github.com/mahmudvibecoding/media.git
cd media
sh scripts/setup.sh
```

The script creates private credentials in `.env`, builds the Python collectors
and Go proxy tester, starts PostgreSQL, and initializes the `media` and `proxy`
databases. Proxy importing and testing start when you run the manual command
below. The final status command waits for database setup to finish. You do not
need Python, Go, or PostgreSQL installed on the host.

Add a channel and collect its uploads and metadata:

```sh
docker compose run --rm backend python manage.py add-channels UC4QobU6STFB0P71PMvOGN5A
docker compose run --rm backend python discover_videos.py --channel-id UC4QobU6STFB0P71PMvOGN5A
docker compose run --rm backend python collect_video_metadata.py --limit 10 --concurrency 2
docker compose run --rm backend python manage.py status
```

Each collector runs once. Discovery, metadata, subscriber, comment, proxy, and
bulk-worker commands described below use the same arguments in Docker: replace
`.venv/bin/python` with `docker compose run --rm backend python`. YouTube access
depends on the host's network and YouTube's current response. Failed collection
is recorded separately from a successful empty result.

The YouTube database starts empty. To explicitly import all bundled channel IDs:

```sh
docker compose run --rm backend python manage.py import-channels
```

### Manual proxy catalog update and three test passes

From the application directory, run:

```sh
sh scripts/update-proxies.sh
```

This command checks the latest published [proxy catalog](https://github.com/mahmudvibecoding/proxy-catalog),
verifies checksums and restored table fingerprints, and imports new configurations
while preserving IDs and server statistics. It then tests **every configuration
in that catalog three times**, in three complete passes, with **80,000 concurrent
checks** and batches of up to one million. Recently tested proxies are included.
A detected local resource overload reduces concurrency for the saved batch.

Each invocation runs once and exits with a summary. Setup does not launch the
background worker. The command runs in the foreground; keep the terminal session
open until it finishes. If interrupted, invoke the same command to resume the
saved run. Completed checks and committed imports are not repeated. A concurrent
invocation fails without starting another run. A fresh run checks for a new
catalog; a resumed run finishes its already selected catalog first.

Any verified YouTube HTTP response qualifies for the working pool, including
sign-in challenges and HTTP errors. The export contains **all qualifying proxies,
ordered by score**, without a size limit. Ranking uses the saved data-retrieval
history, recent failures, and response latency. These three reachability passes
do not add separate metadata probes. Entries expire 24 hours after their last
verified response.

The ranked IDs and statistics are saved at
`/var/lib/media/state/proxy-service/ranked-proxies.jsonl` in the shared state
volume; connection settings remain in PostgreSQL. The final summary is saved as
`last-refresh.json` beside it. Collectors can resolve these IDs through the
catalog loader; automatic collector integration is a separate step.

```sh
docker compose run --rm backend python proxy_service.py status
```

For large sweeps on Linux, set
`COMPOSE_FILE=compose.yaml:compose.host-network.yaml` in `.env`. The tester uses the
host network and PostgreSQL binds to `127.0.0.1:55432`. Set `MEDIA_DB_HOST_PORT` if
that port is already used. Database memory and WAL settings can be adjusted with
`MEDIA_DB_SHARED_BUFFERS` and `MEDIA_DB_MAX_WAL_SIZE`.

Interrupted downloads and unimported journals are preserved. Each invocation
cleans imported journals older than the configured retention period (default one
day). The two latest imported snapshot files are retained.
The catalog publisher remains independent. The legacy background worker is
available only through the explicit `automatic` Compose profile and is disabled
by default; the manual command does not enable it.

The [earlier deployment verification](docs/proxy-service-verification-20261004.json)
records the concurrency benchmarks and background-service checks before this
manual workflow was introduced.

### Storage, restart, and updates

Three named volumes preserve PostgreSQL data, collector state, and output files.
Inside the backend container, state is at `/var/lib/media/state` and output is at
`/var/lib/media/outputs`. Use those paths for `--run`, `--output`, and batch files
that must survive a command finishing. The backend runs as an ordinary user;
PostgreSQL is available only on the Compose network by default.

```sh
docker compose down          # Stop; keep all saved data
sh scripts/setup.sh         # Start again; preserve credentials and data
```

For an update, pull the new code and rerun setup:

```sh
git pull --ff-only
sh scripts/setup.sh
```

Database setup tracks migration checksums and applies new migrations once.
It refuses an existing database without migration history. Existing manually
managed installations should continue using the migration instructions in
[db/README.md](db/README.md); the Docker setup creates fresh databases.
Back up your database, volumes, and `.env` before upgrading. Keep the existing
passwords: changing `.env` does not change a PostgreSQL password already stored
in the database. `docker compose down --volumes` deletes this installation's data.

The PostgreSQL 18 volume uses `/var/lib/postgresql`, following the
[official image layout](https://hub.docker.com/_/postgres).
Compose waits for the database health check and successful migration service
before starting a collector ([startup ordering](https://docs.docker.com/compose/how-tos/startup-order/)).

### Local batch imports

Metadata and statistics importers can read completed or running batches from the
shared state volume. `--run` reads the run ID from its manifest. `--once` imports
currently available batches; omit it to wait until the collector finishes.

```sh
docker compose run --rm backend python metadata_bulk_import.py --run /var/lib/media/state/metadata-run --once
docker compose run --rm backend python video_stats_bulk_import.py --run /var/lib/media/state/statistics-run --once
docker compose run --rm backend python comments_bulk_import.py --run /var/lib/media/state/comments-run
```

The existing SSH import options remain available for workers on another host.
Use explicit container volume mounts for external catalogs, ranked proxy files,
or SSH credentials. `import_proxy_lists.py` requires the catalog path through
`--catalog`; it has no machine-specific default.

### Verify a Docker installation

```sh
sh scripts/check_docker.sh
```

This creates a separate disposable Compose project, builds the Go tester with
race checks, and runs every Python test against isolated PostgreSQL databases.
It then runs a deterministic HTTP-fixture collection cycle through discovery,
metadata, statistics, and paginated comments, including repeat discovery and
batch replay. It restarts the containers and verifies the database, state, and
output volumes. Test resources are removed afterward; logs remain at the printed
report path. The fixture cycle does not depend on live YouTube availability.

## Native development setup

Use Python 3.11 or newer:

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
```

The local PostgreSQL 18 databases are documented in [db/README.md](db/README.md).
The connection examples describe the existing macOS setup. Database files,
backups, downloaded proxy payloads, and run outputs stay under ignored `.local/`
and `outputs/` directories; cloning the repository does not restore them.
YouTube records use `media`. Proxy lists, connections, and compact statistics use
the separate `proxy` database through `collect_proxies.py`.
The server proxy tester and its summary table are documented in
[proxy-tester/README.md](proxy-tester/README.md).
To add the subscriber column to an existing channel-only database:

```sh
/opt/homebrew/opt/postgresql@18/bin/psql -X \
  -h "$PWD/.local/postgres/socket" -U mahmud -d media \
  -v ON_ERROR_STOP=1 --single-transaction \
  -f db/migrations/001_subscriber_count.sql
```

## Discover videos and Shorts

The `videos` table has `video_id` as its primary key, a `channel_id` foreign key,
`type` constrained to `video` or `short`, and nullable `published_at TIMESTAMPTZ`.
It is indexed by `(channel_id, type)`.

For an existing channels-only database, create `videos` once:

```sh
/opt/homebrew/opt/postgresql@18/bin/psql -X \
  -h "$PWD/.local/postgres/socket" -U mahmud -d media \
  -v ON_ERROR_STOP=1 --single-transaction \
  -f db/migrations/002_videos.sql
```

Discover and save uploads for 100 channels, ordered by channel ID:

```sh
.venv/bin/python discover_videos.py --limit 100
```

Fetch and save one existing channel:

```sh
.venv/bin/python discover_videos.py --channel-id UCVPst_iSyaVYpuOP4ogRhlw
```

Check all channels for new uploads:

```sh
.venv/bin/python discover_videos.py --limit 55239 --concurrency 16
```

Every scan starts with the latest uploads and follows continuation tokens until
the last ID on a fully processed page was already stored for that channel and
type, or the tab ends. A first scan with no saved IDs follows the available
history to the end, subject to `--max-pages`. A known ID near the start of a page
does not stop the scan. Empty and absent tabs are checked again on later runs.
Each completed tab scan uses one batch insert with
`ON CONFLICT (video_id) DO NOTHING RETURNING video_id`. Existing rows are preserved,
and PostgreSQL returns only newly inserted IDs. No separate seen-ID table is used.
`published_at` is left NULL for new rows until metadata is collected.

Existing databases should apply `db/migrations/010_drop_channel_scan_state.sql`
to remove the old initialization markers. The `--initial-only` option has been
removed; rerun the normal command to check the selected channels.

Pages are buffered in memory until a tab scan finishes. The new IDs commit in
one short transaction, with no transaction held during network requests.
Failed or interrupted scans save no partial IDs. A new
invocation restarts from the first page using fresh continuation tokens; buffered
IDs from the failed attempt cannot falsely stop the retry.

`results.jsonl` records each tab's pages, completion status, stop reason,
`inserted_video_ids`, `videos_inserted`, and `videos_already_present`;
`summary.json` contains totals.
Both files are under a new `outputs/` directory for every run. Newly inserted means
new to this database, not necessarily newly published. This command runs once.

Each channel starts with a Videos request and a Shorts request. Responses keep IDs,
pagination tokens, alerts, and small tab/sort fields to verify that the requested
tab is selected and sorted by Latest. HTTP/2 connections are reused and responses
use gzip. Regular videos receive `type=video`; Shorts receive `type=short`.
Publication times require a later metadata request.

The collector distinguishes missing tabs, explicit empty tabs, unavailable channels, and failed/unrecognized
responses. A tab without sort controls is accepted only when every card parses
and no next-page token exists. It is marked `complete_tab=true` and
`latest_verified=false`; discovery covers the entire returned tab regardless of
order. Other unverified sorting releases no IDs. HTTP 400/401/403/429 stops the run;
five consecutive unexpected or unverified responses also stop it. Transport and
HTTP 5xx failures retry at most twice. A failed or incomplete run exits with code 2.
Database errors stop further collection. Earlier completed tab scans remain saved.
Repeated tokens or repeated pages fail the current scan without inserting partial
data. The default limit is 100 pages per tab; reaching it before a stopping point
is an incomplete scan with no inserts. Increase `--max-pages` to cover a larger gap.
The API and response shapes are internal and may change.

Options: `--concurrency` (default 4), `--retries` (default 2), `--max-pages` (default 100), `--output` (a new
directory), and the environment variables listed below. Byte counters cover HTTP
bodies only, including retry attempts, and exclude headers and connection overhead.

## Video statistics columns

After migrations `001` through `008`, apply `009` once to add nullable
`view_count BIGINT`, `like_count BIGINT`, `stats_updated_at TIMESTAMPTZ`, and
`stats_error TEXT` to `public.videos`:

```sh
/opt/homebrew/opt/postgresql@18/bin/psql -X \
  -h "$PWD/.local/postgres/socket" -U mahmud -d media \
  -v ON_ERROR_STOP=1 --single-transaction \
  -f db/migrations/009_video_stats.sql
```

Both counts reject negative values and start as `NULL`, including for existing
videos. The statistics timestamp tracks freshness separately from metadata.
The migration leaves them empty until the [statistics collector](#collect-views-and-likes)
runs. See [the write rules](db/README.md#videos).

## Comment storage

After migration `010`, apply `011` once to add `public.comments` and the nullable
`comments_updated_at` and `comments_error` columns on `public.videos`:

```sh
/opt/homebrew/opt/postgresql@18/bin/psql -X \
  -h "$PWD/.local/postgres/socket" -U mahmud -d media \
  -v ON_ERROR_STOP=1 --single-transaction \
  -f db/migrations/011_video_comments.sql
```

Each comment stores `video_id`, `comment_id`, `text`, `author_channel_id`,
`author_name`, and `is_pinned`. The primary key is `(video_id, comment_id)`, and
the video must already exist. Replies, comment likes, publication dates, and
collection timestamps are excluded. Author fields and pinned status can be NULL.

`comments_updated_at` records a completed scan only after all of its pages have
been imported; `comments_error` records the latest failure. Both start as NULL.
The schema includes an index for videos whose first successful comment scan is
still pending. Resume tokens and scan boundaries belong in the durable worker
queue.

See [the database guide](db/README.md#comments) for permissions and write rules.

### Collect one video's comments

```sh
.venv/bin/python collect_video_comments.py ymjtt5KJiR4
```

The video must exist in `public.videos`. The collector requests Newest directly
through `/next`, verifies the returned sort, and consumes that first response as
page one. It follows only top-level comment pages. A repeat scan processes the
whole page containing a saved, non-pinned comment before stopping. Currently
pinned, previously pinned, and unknown-pin comments cannot end that scan.
Comments revisited during the scan have their text, author, and pinned status
refreshed. Older comments outside that range are left as saved.

A temporary SQLite file holds new pages and a fixed copy of the prior completed
history. On success, all comments and `videos.comments_updated_at` are committed
together. Failures preserve saved comments and the previous successful timestamp,
and set `comments_error`. Retrying starts from the first page; partial scans
cannot create a false stopping point. Empty and disabled comment sections finish
successfully; unavailable videos and unrecognized responses record an error.

Options: `--max-pages` (default 10,000), `--retries` (default 2), `--timeout`
(seconds per request attempt, default 30), `--client-version`, and `--output`.
Reaching the page limit is an incomplete scan. Increase it and rerun when needed.
`MEDIA_DATABASE_URL` and `MEDIA_PROXY_URL` use the same conventions as the other
single collectors. Concurrent runs for the same video are prevented by a
PostgreSQL advisory lock. Each run writes an operational `summary.json` under
`outputs/comments-<video>-<timestamp>/`; comment rows go into PostgreSQL.

Incremental scans do not discover every edit or comment made visible later below
the saved-history boundary.

### Collect a snapshot of videos

`comments_bulk.py` freezes video IDs and completed comment history, stores each
validated page with its next token in SQLite, and publishes checksummed gzip
batches. `comments_bulk_import.py` imports each successful video's complete scan
in one PostgreSQL transaction. Failed scans retain their partial pages in the
queue and update only `comments_error` in PostgreSQL.

```sh
.venv/bin/python comments_bulk.py export --output outputs/comments-run \
  --ranked path/to/ranked-responders.jsonl --limit 1000 --proxy-limit 512
.venv/bin/python comments_bulk.py init --run outputs/comments-run \
  --concurrency 64 --max-attempts 6 --max-pages 10000 --timeout 20
.venv/bin/python comments_bulk.py run --run outputs/comments-run
.venv/bin/python comments_bulk_import.py --run outputs/comments-run
```

The ranked file contains `proxy_id` values from the existing proxy tests. Export
loads their configurations from the local proxy database. The default selection
is a deterministic 1,000-video sample; `--limit 0` explicitly selects all videos.
`--include VIDEO_ID` adds a control video within the limit. Snapshot files contain
private proxy configurations; keep the run folder private.

To update the same videos later, export a new folder with
`--from-run outputs/comments-run`, then initialize, run, and import that folder.
The new export reads the latest completed history from PostgreSQL. Reusing the
old folder resumes its existing scan instead of starting a new update.

- **Status:** `comments_bulk.py status --run RUN`. A `complete` worker has no
  pending jobs; inspect both `completed` and `failed` counts.
- **Pause:** send SIGINT/SIGTERM to the worker or set `stop` to `true` in its
  `control.json`. Resume with the same `run --run RUN` command. It continues from
  saved pages without an extra pass at the newest comments after resuming.
- **Retry failures:** `comments_bulk.py retry --run RUN`, then run and import
  again. The original saved-history boundary remains fixed. Failed artifacts
  stay immutable, and the retry gets a new scan identity. A scan with no saved
  pages restarts its initial request; other scans keep their saved continuation.
  Add `--recoverable-only` to skip videos already confirmed unavailable, or
  `--video-id VIDEO_ID` to retry one failed video.
- **Live import:** add `--watch` to the importer. For a worker on another host,
  retain the original snapshot locally and add `--host USER@HOST --remote RUN`.
  The importer copies only published outbox files and worker status.

The runner uses the existing recent-performance proxy pool, with one active
request per proxy. Limits apply to each unfinished page. Expired continuations
may restart from Newest using the original boundary and deduplicated buffer.
Reaching `--max-pages` leaves a scan incomplete. Proxy attempt statistics are
imported separately with replay protection.

Preserve `queue.sqlite3`, `import.sqlite3`, the snapshot, and the outbox together.
The importer verifies file checksums and all six comment fields before writing.
Its durable receipts recover a PostgreSQL commit whose acknowledgement was lost;
replaying a scan changes neither rows nor its completion timestamp. Older
snapshots cannot overwrite a newer completed scan.

See [the collection design](docs/comment-collection-plan.md) and the pilot evidence
under `outputs/comments-pilot-20261003/`.

### Run independent comment queues in parallel

`comments_bulk_fleet.py` partitions one frozen snapshot into disjoint video and
proxy sets. It preserves each video's frozen history and distributes proxy ranks
across the queues. Collectors keep separate SQLite journals, so they can write
checkpoints in parallel. The importer still commits complete videos atomically.
Each collector performs its queue reads, writes, and exports on one storage
thread so disk checkpoints can run while the network loop handles responses.
Checkpoints share durable transactions, and imports group up to 64 distinct
videos per transaction. Prepared import receipts reach disk before PostgreSQL
commits; receipts are acknowledged only after that commit succeeds.

```sh
.venv/bin/python comments_bulk_fleet.py partition --source SNAPSHOT \
  --output FLEET --shards 16 --concurrency 512
.venv/bin/python comments_bulk_fleet.py collect --fleet FLEET
.venv/bin/python comments_bulk_fleet.py import --fleet LOCAL_FLEET \
  --host USER@HOST --remote REMOTE_FLEET
.venv/bin/python comments_bulk_fleet.py status --fleet LOCAL_FLEET
```

Copy the partitioned snapshot to the collection host before starting either
supervisor. The collection supervisor initializes new queues and resumes existing
ones; the import supervisor continuously retrieves and imports published batches.
Both record child process failures and restart interrupted children up to three
times. Send SIGTERM to a supervisor to stop its children gracefully.

Use `comments_bulk_fleet.py control --fleet FLEET --concurrency 1024` on the
collection host to change requests per queue. Each queue supports up to 1,024
concurrent requests. Add `--stop` to pause collectors. An ordinary resume
continues saved pages without an extra pass at the newest comments.

The full run started on October 3, 2026 uses local artifacts in
`outputs/comments-full-20261003/` and remote queues in
`/opt/media-comments/runs/20261003-all/` on the user's collection server.
Its live service is `media-comments-20261003.service`. Status files distinguish
buffered comments, completed scans, and comments committed to PostgreSQL.

After collection and imports finish, export the independent audit on the
collection host, copy its output locally, then compare it with PostgreSQL:

```sh
.venv/bin/python comments_bulk_audit.py export --fleet FLEET --output AUDIT
.venv/bin/python comments_bulk_audit.py validate --fleet LOCAL_FLEET \
  --expected LOCAL_AUDIT --output verification.json
```

The audit compares every selected video's row count, content hashes, completion
receipt, and error state. It also checks all revisited comment fields and
preserves the original saved history. A passing audit confirms that imports
match the queues; `source_counts.failed` still reports incomplete videos.

## Collect video metadata

Apply the metadata migrations in order, once per database. Run only files that
have not already been applied; a database with `004` through `007` needs only `008`:

```sh
/opt/homebrew/opt/postgresql@18/bin/psql -X \
  -h "$PWD/.local/postgres/socket" -U mahmud -d media \
  -v ON_ERROR_STOP=1 --single-transaction \
  -f db/migrations/004_video_metadata.sql -f db/migrations/005_drop_video_player_status.sql \
  -f db/migrations/006_video_thumbnail_url.sql -f db/migrations/007_video_metadata_updated_at.sql \
  -f db/migrations/008_video_metadata_error.sql
```

Migration `007` adds nullable `metadata_updated_at` and an index on pending video
IDs where it is `NULL`. A successful metadata save sets this timestamp in the same
database update. Rows with a timestamp are skipped on subsequent runs.
Migration `008` adds nullable `metadata_error`, which holds the latest failure
reason. New rows start with `NULL`.

Fetch metadata for up to 100 pending records ordered by video ID (the default run
limit). Selection depends only on the timestamp, so previously populated records
with a `NULL` timestamp are eligible:

```sh
.venv/bin/python collect_video_metadata.py --limit 100
```

Exclude records with a saved failure reason by adding `--skip-errors`:

```sh
.venv/bin/python collect_video_metadata.py --limit 10000 --skip-errors
```

This selects records where both `metadata_updated_at` and `metadata_error` are
`NULL`. The filter applies before the limit, and also applies to explicit IDs.

Select every pending record for one run:

```sh
.venv/bin/python collect_video_metadata.py --all
```

The collector reads pending IDs and their types into memory once at startup.
Up to 128 workers take distinct IDs from one shared iterator and save each result
immediately. Newly inserted videos wait for the next run. There are no processing
batches or saved cursors; restarting selects records whose timestamps are still
`NULL`.

Select an existing video or Short explicitly; repeat `--video-id` for a sample.
Use `--video-id=ID` so IDs beginning with a hyphen are accepted. Duplicate IDs are
removed and IDs with a saved timestamp are skipped. Each selected ID is requested
once, plus any retries:

```sh
.venv/bin/python collect_video_metadata.py --video-id=RtXBV0X1v1Q
```

Each attempt uses one `POST /youtubei/v1/player` request with these fields:

```text
videoDetails(videoId,title,shortDescription,lengthSeconds,thumbnail/thumbnails/url),microformat/playerMicroformatRenderer/publishDate,playabilityStatus(status,reason)
```

The collector saves `title`, `description`, `duration_seconds`, `published_at`,
and `thumbnail_url`. It takes the last thumbnail URL returned by the API. The
thumbnail field filter requests URLs only; widths, heights, and image downloads
are excluded. A missing thumbnail URL preserves the previously saved URL.
It verifies the returned video ID, preserves empty
descriptions and zero durations, and converts exact publication timestamps to
UTC. A date without a time and timezone leaves `published_at` unset. Missing
fields never erase previously collected values.

Player status is used for validation, retry handling, and failure reasons. `UNPLAYABLE` and
`LOGIN_REQUIRED` can accompany usable metadata, which is still saved unless the
response requests bot verification. A valid response for the requested ID with
at least one metadata field counts as a successful collection; missing fields
preserve existing values. The summary reports field completeness, but it does
not determine selection or success. A status-only response or bot verification
leaves metadata and its timestamp unchanged, including when HTTP status is 200.
After the final unsuccessful attempt, the collector writes a short reason to
`metadata_error`, for example `LOGIN_REQUIRED: Please sign in`, `HTTP_429: Too Many
Requests`, or `TIMEOUT`. Reasons are limited to 500 characters. Each new failure
replaces the previous reason; intermediate retry failures are not written.
A successful save clears the error in the same update as the metadata and its
timestamp. The error column affects selection only when `--skip-errors` is used.

The default concurrency is 128. Requests use selected fields, gzip, HTTP/2, and
reused connections. Without a proxy list or catalog, every unsuccessful fetch shares a limit of ten retries per
ID (`--retries`, default 10), for up to 11 total attempts including the initial
request. This includes sign-in and bot checks, unavailable videos without usable
metadata, all HTTP errors, transport errors, malformed or oversized responses,
and responses without metadata. Changing failure types does not reset the limit.
Retries start as soon as the previous attempt finishes, with no added delay,
and stop immediately when usable metadata arrives. `metadata_error` is written
only after all attempts fail, using the final failure reason. Failed videos retain
`NULL` timestamps and other videos continue. Database write errors stop new work
and further saves.
Successful row updates commit individually; an advisory lock prevents two
metadata collectors from running simultaneously against the same database.

To rotate proxies between videos, set `MEDIA_PROXY_URLS` to a JSON array of proxy URLs:

```sh
export MEDIA_PROXY_URLS='["http://USER:PASS@HOST1:PORT1","http://USER:PASS@HOST2:PORT2"]'
.venv/bin/python collect_video_metadata.py --limit 10000
```

This takes precedence over `MEDIA_PROXY_URL` and `--retries`. Each video gets one
attempt through its assigned proxy. Assignment rotates through the list in order
and wraps around for later videos. If the attempt fails, the collector saves the
error and moves on to the next video. Concurrency remains 128
across all proxies combined. Connections are reused separately for each proxy.
Proxy URLs and credentials are not written to the run summary.

### Resumable collection on a separate server

`metadata_bulk.py` exports pending videos and all supported proxy configurations
that previously received a YouTube response. Server workers keep a durable SQLite
queue and write immutable, checksummed result batches. `metadata_bulk_import.py`
copies those batches back and updates the local `media` and `proxy` databases.
PostgreSQL stays local; the server does not need database access.
A dedicated queue process serializes lease and result writes over a private Unix
socket. Workers wait for a commit acknowledgement and keep at most 1,024 completed
results in memory while writes catch up.

Export a new snapshot into a new directory:

```sh
.venv/bin/python metadata_bulk.py export --output outputs/metadata-run
```

Copy that directory to the server. Install the repository's Python requirements
there and build `proxy-tester` for the server's architecture at
`.local/bin/proxy-tester`. Keep the Python modules together so their imports and
relative paths work. Snapshot files contain proxy credentials and must remain
private. The commands create new files with mode `0600` by default.

Initialize and start from the server's checkout:

```sh
.venv/bin/python metadata_bulk.py init --run /path/to/metadata-run \
  --workers 8 --concurrency 128 --max-attempts 3
.venv/bin/python metadata_bulk.py run --run /path/to/metadata-run
```

The concurrency argument is per worker: these defaults allow 1,024 simultaneous
requests. Each proxy belongs to one worker and has at most one active request.
Workers first try their unused configurations, then prefer successful ones.
Eligible configurations are ranked by recent request success divided by average
successful response time. After the first scored result, each new result has 20%
weight; older results gradually lose influence. One selection in twenty checks
the longest-waiting eligible configuration, so slower or previously unsuccessful
proxies can recover.
Temporary connection, HTTP and invalid-response failures lower the score and
postpone reuse for 2, 4, 8, 16, 32, then at most 60 seconds. Successful requests
make the configuration eligible immediately. Local worker errors do not lower
proxy scores.
Recent scores, response times and cooldowns are committed with results in the
run's SQLite queue and restored after worker restarts. Scores older than one hour
are reset. Existing queues gain these tables when resumed; previous journals are
preserved. A new run learns its own scores from its selected proxy snapshot.
Connection setup has a five-second timeout; the entire request has a twenty-second
deadline. Idle reusable connections are kept for up to two minutes. There are no
retries inside an individual request.

Videos without previous errors come first. Failed videos enter a recovery round
after the initial queue finishes. Recovery prefers available scored configurations
and avoids the previous proxy when another is available. Each video gets up to
the configured number of network attempts. Matching video-unavailability evidence
from two different proxy configurations within an hour ends retries for that video
in the current run. Repeating the error through the same proxy does not confirm it.
HTTP errors, bot challenges and malformed responses retain their retry budget.
Valid video errors are recorded separately and excluded from proxy success scores;
the request and connection counters still record what happened. Local worker
failures do not affect proxy statistics. Five local failures leave a final video
error so the queue cannot loop indefinitely.

Run the importer on the computer holding PostgreSQL. Use the `run_id` from the
snapshot's `manifest.json`:

```sh
.venv/bin/python metadata_bulk_import.py --host USER@SERVER \
  --remote /path/to/metadata-run --local outputs/metadata-import \
  --run-id RUN_UUID
```

The importer needs `ssh` and `rsync`, holds the regular metadata collector's
advisory lock, and imports batches in sequence. It verifies checksums, video IDs,
proxy identities, and outcome classifications before writing. Successful metadata
commits before its statistics batch; after a lost acknowledgement, replay skips
already saved videos and uses the batch digest to prevent repeated statistics.
Missing metadata fields preserve existing values. Only a video's final failure
updates `metadata_error`. A database or SSH outage leaves server results available
for the next import attempt. A checksum, identity, sequence, or statistics ordering
conflict stops the importer for inspection.

Monitor `status.json` on the server and `import-status.json` locally. The former
reports queue counts, attempts, metadata successes, tested configurations and
recent successful saves per second. The latter includes committed database saves
and its last imported event. Import is complete only when the server queue is
complete and every exported event has been imported.

Change `concurrency_per_worker` in `control.json` atomically to tune throughput
(1–1,024 per worker). Set `stop` to `true` or send `SIGTERM` to the controller to
drain active requests. Run the same `run` command to resume; do not initialize the
queue again. A restarted worker recovers only its own outstanding leases. Keep
the queue, input snapshot, outbox and import cursor until the final database audit
is complete. Run the controller and importer under process supervision for long
collections.

### Use saved proxy configurations

Build the transport bridge once with the existing Go tester dependencies:

```sh
cd proxy-tester
go build -trimpath -o ../.local/bin/proxy-tester .
cd ..
.venv/bin/python collect_video_metadata.py --proxy-catalog
```

This uses the collector's existing default of 100 pending videos and 128 workers.
`--all`, `--limit`, `--video-id` and `--skip-errors` retain their normal meanings.
Catalog mode selects configurations with a previous YouTube response, including
ones whose latest check failed. It supports HTTP, HTTPS, SOCKS4, SOCKS5, VLESS,
VMess, Trojan, Shadowsocks, ShadowsocksR, Hysteria, Hysteria2, TUIC, WireGuard and
AnyTLS through the tester's existing protocol implementations.

To select particular catalog IDs or working protocols:

```sh
.venv/bin/python collect_video_metadata.py --proxy-id 123 --proxy-id 456
.venv/bin/python collect_video_metadata.py --proxy-catalog --proxy-protocol vless
```

Catalog IDs use the full stored connection settings. The shared `working_protocol`
takes precedence over the configured transport in `connection_settings`.
Explicit IDs without a confirmed working protocol can use that configured
transport. Configurations with an unknown protocol need a connectivity check
before catalog collection can use them. Catalog options and `MEDIA_PROXY_URL(S)`
are mutually exclusive.

```text
Python metadata collector → authenticated loopback CONNECT bridge
                         → saved proxy transport → verified YouTube HTTPS
```

The bridge is a child process owned by the run. It listens only on loopback,
accepts only the YouTube target, and uses a random token kept in a private
temporary directory. It reuses the tester's configuration conversion and Mihomo
adapters. Python still verifies YouTube's certificate, parses metadata and saves
video records. Credentials and individual video results are not logged.

The assigned proxy rotates between videos. Selection uses the YouTube column
group: a previous `youtube_last_response_at` makes a configuration eligible,
and ordering puts configurations with `youtube_last_http_status` first, followed
by the most recent `youtube_last_response_at` and then proxy ID. An HTTP response
does not imply usable data.
Each video gets one attempt through its assigned configuration. A failed attempt
records the error and processing moves on.
Clients are created when used and reused for later requests. The shared CA store,
open clients, bridge connections and temporary files belong to this run and are
closed or removed when it exits.

Statistics retain the original catalog ID and connection hash, independently of
the bridge's local address. Failed upstream tunnels count as connection attempts;
bridge authentication, configuration and local resource failures are excluded
from proxy reliability. Bridge CONNECT responses are never counted as YouTube
responses. Catalog mode uses the same independent statistics writer described below.
Its run summary also groups observed attempts, sent requests, responses and data
successes by working protocol. These run totals remain available when database
statistics were not acknowledged; they contain no individual video records.

Rotating assignments spread observations across configurations. Score-based
ordering should be assessed using actual data successes, evidence weight and
age; a high score from one response is weak evidence. Loading the catalog is a
startup read, and the running collector never waits for statistics writes.

### Proxy statistics and scoring

Proxy-based metadata collection, including catalog mode, reports each attempt to a separate
background statistics writer. Use the current
[`db/proxy/schema.sql`](db/proxy/schema.sql) for a fresh database, or apply the
missing [proxy migrations](db/README.md#proxy-schema-migrations) through
[`010_drop_youtube_check_fields.sql`](db/proxy/migrations/010_drop_youtube_check_fields.sql)
before starting collectors or importing tester results.
Statistics failures never pause or throttle collection, and shutdown never waits
for a statistics flush. Unwritten statistics may be lost. The existing behavior
for saving the actual video records is independent of this statistics writer.

The writer matches full configuration identities, including credentials. An exact
HTTP/HTTPS configuration takes precedence; otherwise a unique equivalent
`unknown` configuration or an HTTPS-labelled HTTP fallback can be matched.
Ambiguous or missing identities are skipped and counted in the run summary.
For an explicit choice, set `MEDIA_PROXY_IDS` to an array of catalog IDs aligned
with `MEDIA_PROXY_URLS` (or a one-item array for `MEDIA_PROXY_URL`). Each ID must
match that URL's endpoint, credentials and supported connection settings. This
does not create proxy configurations. These URL options accept HTTP/HTTPS;
catalog mode also supports the other protocols listed above.

The collector reports usable data using its existing outcome, before any metadata
database save. The scorer adds no video-ID, field or completeness validation.
Proxy negotiation is not counted as a YouTube request. A completed target send
counts even if the response later times out. Every retry is reported separately.
The database keeps one `proxy_stats` row per proxy. Shared fields count actual
connection attempts and successful connections across websites.
`last_connection_attempt_at` records the latest actual attempt, including a
request that reuses an open connection. `last_connected_at` preserves the latest
confirmed connection time. `working_protocol` holds a proxy transport confirmed
by a verified website response and survives later failed checks.
`last_connection_error` records the latest actual attempt's proxy endpoint, TLS,
authentication or explicit protocol failure as a short `stage:code` label.
Target-tunnel refusals, website TLS failures and response-body errors stay in
`youtube_last_error`. A later attempt with no proxy-specific error clears the
shared error. A check rejected before a network attempt leaves these shared
fields unchanged.

YouTube results, errors, request/response/data counters, score inputs and import
markers live in the separate `youtube_*` columns of that same row. The view's
`youtube_responded` is true when the latest actual YouTube attempt received an
HTTP response, false when it received none, and NULL before any recorded attempt.
HTTP errors and bot challenges count as responses; usable data has its own
counter. `youtube_last_error` records setup, network, HTTP or data failures.
Only the YouTube column group is installed. The [database guide](db/README.md#adding-another-website)
explains how to add another website's columns.

Older catalogs can recover missing connection observations from saved test
journals; see the [backfill guide](db/README.md#historical-connection-backfill).

See [the database scoring guide](db/README.md#recent-proxy-score) for counter names,
the one-hour weighting, the score query and replay behavior. Historical tester
results contribute reachability counters and remain unscored for data quality.

Every run writes only an aggregate `summary.json` in a new `outputs/` directory,
with progress, traffic totals, and a histogram of retries per video. Individual
video results and failure details are not written to log files; the latest
failure reason is available in `videos.metadata_error`. Exit status is zero when every
selected video was saved, or two when any were unsaved or unprocessed.
Body-byte counters exclude headers and connection overhead. The worker shares
the connection and client-version environment variables documented below.
Discovery remains manual.

## Collect subscriber counts

Fetch counts for up to 100 channels that still have `NULL` counts:

```sh
.venv/bin/python collect_subscribers.py --limit 100
```

Process every remaining channel:

```sh
.venv/bin/python collect_subscribers.py --all --concurrency 32
```

Refresh existing counts as well:

```sh
.venv/bin/python collect_subscribers.py --all --refresh --concurrency 32
```

Successful values are committed individually, so an interrupted initial run can
resume by selecting the remaining `NULL` rows. Failed requests and missing values
never overwrite an existing count. A displayed zero is saved as `0`. `NULL` can
mean unprocessed, unavailable, or a failed request; the run log distinguishes
these outcomes. The table stores no timestamps or other channel metadata.

The worker reuses HTTP/2 connections, requests gzip, and filters `/youtubei/v1/browse`
to the channel-header text containing the subscriber count. This internal API can
change. It stops on HTTP 400/401/403/429 or five consecutive unexpected responses.
Transport errors and HTTP 5xx responses receive at most two retries by default.

Each invocation writes `results.jsonl` and `summary.json` to a new directory under
`outputs/`. The worker's byte counters cover HTTP bodies; they exclude headers and
connection overhead. The measured initial collection also has separate packet
capture reports under `outputs/subscribers-20260929/`.

Optional environment variables:

- `MEDIA_DATABASE_URL` and `PROXY_DATABASE_URL`: separate PostgreSQL connection
  strings for collection data and the proxy catalog. They accept URLs or libpq
  `key=value` strings.
- `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGSERVICE`: standard PostgreSQL
  settings. With no connection string or host/service setting, native tools use
  the project-local socket; libpq selects the operating-system user or `PGUSER`.
- `MEDIA_STATE_DIR`: writable state and downloaded proxy storage; defaults to `.local/`.
- `MEDIA_OUTPUT_DIR`: collector output root; defaults to `outputs/`.
- `MEDIA_PROXY_BRIDGE_BINARY`: Go tester binary; defaults to `.local/bin/proxy-tester`.
- `MEDIA_PROXY_URL`: proxy URL, if needed; defaults to a direct connection.
- `YOUTUBE_CLIENT_VERSION`: override the tested InnerTube WEB client version.

Compose supplies the container database and storage settings. To override a
setting for a command, use `docker compose run --rm -e NAME=value backend ...`.

## Check the worker

For a fresh clone, install the Python dependencies above, Go 1.26, PostgreSQL 18
server tools, and OpenSSL. Run as a normal user on macOS or Linux:

```sh
.venv/bin/python scripts/check_backend.py
```

The command locates PostgreSQL through `pg_config`, runs the Go tests with the
race detector, builds the transport bridge, and runs every Python test against
new `media` and `proxy` databases in a private temporary cluster. It stops and
removes that cluster afterward. Existing databases and collection jobs are not
used. HTTP fixtures run locally; dependency installation may require network access.

If the PostgreSQL tools are outside `PATH`, provide their directory:

```sh
.venv/bin/python scripts/check_backend.py \
  --postgres-bin /opt/homebrew/opt/postgresql@18/bin
```

For a quicker Python run, invoke:

```sh
.venv/bin/python -m unittest discover -s tests -v
```

That command skips database tests unless their opt-in environment variables are
set, and skips bridge process tests unless `.local/bin/proxy-tester` exists.
The proxy tests cover transport identity, migration preservation, shared protocol
selection, website error separation and statistics replay. To supply an existing
disposable test database explicitly:

```sh
PROXY_TEST_DATABASE=1 \
PROXY_TEST_DATABASE_URL="host=/path/to/test/socket dbname=proxy user=test_user" \
  .venv/bin/python -m unittest discover -s tests -p '*prox*.py'
```

To include database insertion tests, supply a test connection. These tests use
transactions that roll back their rows:

```sh
MEDIA_TEST_DATABASE_URL="host=/path/to/test/socket dbname=media user=test_user" \
  .venv/bin/python -m unittest discover -s tests -v
```

## Collect views and likes

`collect_video_stats.py` requests the primary video's counts through `/youtubei/v1/next`.
It verifies the returned video ID, uses exact view and like labels, and rejects
rounded labels and live viewer counts. Missing counts remain NULL. Each saved
result retains the source labels so the importer can check the parsed numbers.

`video_stats_bulk.py export --output RUN --ranked RANKED_RESULTS` freezes the video
IDs and a ranked proxy selection. Add `--limit 10000` for a distributed pilot or
`--missing-only` to select videos with either count missing. Initialize the copied
snapshot with `video_stats_bulk.py init --run RUN --workers 32 --concurrency 512
--max-attempts 5`, then use `metadata_bulk.py run --run RUN`. The manifest selects
the statistics collector. The existing queue, committed result batches, recovery,
and proxy connection reuse also apply to statistics runs.
The same two-proxy confirmation applies when an identity-checked response exposes
neither count. Partial results are saved immediately. Statistics collection uses
`/next`; the bulk worker does not call `/player`.

On the database machine, `video_stats_bulk_import.py` accepts `--run RUN` for a local
run, or `--host`, `--remote`, `--local`, and the manifest's `--run-id` for an SSH worker.
It validates file hashes, sequence numbers,
video identities, source counts, and proxy observations before importing. Repeating
the same batch does not increase counters again. Newer partial observations retain
previously collected counts and record the missing fields in `stats_error`.

Results are stored in `media.public.videos`: `view_count`, `like_count`,
`stats_updated_at`, and `stats_error`. Metadata fields are preserved. Statistics
tests, including temporary database tables, run with:

```sh
PROXY_TEST_DATABASE=1 .venv/bin/python -m unittest discover -s tests -p 'test_video_stats.py' -v
```
