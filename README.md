# YouTube collection and proxy catalog

Collect channel subscriber counts, discover videos and Shorts, and save video
metadata through direct requests or saved proxy configurations. The proxy catalog
stores public source lists, connection settings, and compact website statistics.
YouTube collectors use raw InnerTube HTTP requests.

The bundled `data/channels.csv` seed contains 55,859 IDs. The documented local
subscriber run retained 55,239 channels after 620 were removed. Subscriber counts
are rounded public values, such as `4.16M` → `4160000`.

## Setup

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

For an existing channels-only database, apply these migrations once. If `videos`
already exists, apply only `003_channel_scan_state.sql`:

```sh
/opt/homebrew/opt/postgresql@18/bin/psql -X \
  -h "$PWD/.local/postgres/socket" -U mahmud -d media \
  -v ON_ERROR_STOP=1 --single-transaction \
  -f db/migrations/002_videos.sql -f db/migrations/003_channel_scan_state.sql
```

Discover and save uploads for 100 channels, ordered by channel ID:

```sh
.venv/bin/python discover_videos.py --limit 100
```

Fetch and save one existing channel:

```sh
.venv/bin/python discover_videos.py --channel-id UCVPst_iSyaVYpuOP4ogRhlw
```

Resume an unfinished initial scan, skipping every tab already initialized:

```sh
.venv/bin/python discover_videos.py --limit 55239 --initial-only --concurrency 16
```

Omit `--initial-only` for later monitoring runs, which must check initialized tabs
for new uploads. This option uses the existing initialization markers and sends
no requests for completed tabs.

The initial scan saves one page per tab to `videos`, including uploads that
existed before collection started. Later scans follow continuation tokens until
the last ID on a fully processed page was already stored for that channel and
type, or the tab ends. A known ID near the start of a page does not stop the scan.
Each completed tab scan uses one batch insert with
`ON CONFLICT (video_id) DO NOTHING RETURNING video_id`. Existing rows are preserved,
and PostgreSQL returns only newly inserted IDs. No separate seen-ID table is used.
`published_at` is left NULL for new rows until metadata is collected.

`channel_scan_state` stores one `(channel_id, type)` initialization marker for
each successfully checked tab, including empty or absent tabs. This lets a
previously empty tab collect multiple pages when uploads appear. The migration
initializes markers for existing videos; the local database also preserves the
25 absent tabs from the previously verified 100-channel run.

Pages are buffered in memory until a tab scan finishes. The new IDs and its
initialization marker commit in one short transaction, with no transaction held
during network requests. Failed or interrupted scans save no partial IDs. A new
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
by `youtube_last_check_duration_ms`. An HTTP response does not imply usable data.
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
[`009_shared_proxy_protocol.sql`](db/proxy/migrations/009_shared_proxy_protocol.sql)
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

- `MEDIA_DATABASE_URL`: PostgreSQL connection string; defaults to this project's local socket.
- `MEDIA_PROXY_URL`: proxy URL, if needed; defaults to a direct connection.
- `YOUTUBE_CLIENT_VERSION`: override the tested InnerTube WEB client version.

## Check the worker

The full Python suite is invoked with:

```sh
.venv/bin/python -m unittest discover -s tests -v
```

The proxy tests cover transport identity, migration preservation, shared protocol
selection, website error separation and statistics replay. To include their
database checks in isolated schemas with rollback:

```sh
PROXY_TEST_DATABASE=1 .venv/bin/python -m unittest discover -s tests -p '*prox*.py'
```

To include database insertion tests, supply a test connection. These tests use
transactions that roll back their rows:

```sh
MEDIA_TEST_DATABASE_URL="dbname=media user=mahmud host=$PWD/.local/postgres/socket" \
  .venv/bin/python -m unittest discover -s tests -v
```
