# Test the proxy catalog against YouTube

The tester sends one video metadata request through each saved proxy configuration
with gzip response compression requested. Any HTTP response received through
verified YouTube TLS counts as a response, including bot challenges, 403, 427,
429, and server errors.
It reads the full response body within the deadline. If the body times out after
headers arrive, the result still records a YouTube response and a separate body error.

The October 1 run used `root@198.163.196.164`. The local `proxy` database holds the
source catalog and imported results. Check the service for its current state.

## October 1 run

```sh
ssh root@198.163.196.164 \
  'systemctl status media-proxy-tester --no-pager'

ssh root@198.163.196.164 \
  'cat /opt/media-proxy-tester/runs/20261001/controller.json'
```

The controller measures concurrency, scans the full snapshot with 3-second
connection and 10-second request deadlines, retries failed checks with 5/15-second
deadlines, then checks every configuration that responded in any round again.
The stability check starts at least one minute after recovery finishes.

Its service sets `LimitNOFILE=1048576`, saves progress, and continues independently
of SSH. To stop or resume this run:

```sh
ssh root@198.163.196.164 'systemctl stop media-proxy-tester'
ssh root@198.163.196.164 'systemctl start media-proxy-tester'
```

Stopping preserves completed results. Requests cancelled before a YouTube
response remain eligible on resume. A restart may change concurrency; it must
retain the same input, request settings, deadlines, and run identifier.

## Request

- `POST https://www.youtube.com/youtubei/v1/player`
- Query: `prettyPrint=false` and the field mask below.
- Headers: `Accept-Encoding: gzip`, `Content-Type: application/json`.
- Fixed video: `1hzvCKusdpc`.
- Client: `WEB`, version `2.20260925.01.00`, language `en`.

```text
videoDetails(videoId,title,shortDescription,lengthSeconds,thumbnail/thumbnails/url),microformat/playerMicroformatRenderer/publishDate,playabilityStatus(status,reason)
```

Each round sends at most one metadata request per configuration after establishing
the proxy protocol. An unknown protocol tries HTTP CONNECT, SOCKS5, SOCKS4, and
HTTPS CONNECT until one works. It stops trying protocols after target TLS is
verified or a metadata request is sent. A failed TCP connection or DNS lookup
does not repeat once for each protocol. Public lists sometimes label an ordinary
HTTP CONNECT proxy as HTTPS, so that label permits an HTTP fallback.

Deadlines apply per protocol attempt. An unknown configuration can take up to
four request deadlines while trying incompatible handshakes. TLS verification
for YouTube remains enabled even when a configuration permits an unverified TLS
certificate on the outer proxy connection. A proxy's own CONNECT error does not
count as a YouTube response. No external IP lookup is performed.

## Files and database

- `catalog/manifest.json`: immutable export counts and shard checksums.
- `runs/<run>/<phase>/results.jsonl`: one completed record per configuration in
  that round, plus any separately marked local resource errors.
- `results.jsonl.meta.json`: input hash and exact probe settings.
- `results.jsonl.summary.json`: counts, state, and performance.
- `progress.log` and `resources.jsonl`: progress and server resource measurements.

Catalog exports can contain credentials. They are private files under ignored
`outputs/` locally and `/opt/media-proxy-tester` on the server. Results contain
configuration IDs, identity hashes, timing, and response classifications;
response bodies and credentials are not saved in results.

The current schema is [db/proxy/schema.sql](../db/proxy/schema.sql): three tables
with 35 stored columns, including 20 in `proxy_stats`. There is one statistics
row per configuration, with shared connection fields and a fixed `youtube_*`
column group. Individual test history stays in the retained journals.
Existing databases must apply missing [proxy migrations](../db/README.md#proxy-schema-migrations)
through [migration 008](../db/proxy/migrations/008_proxy_website_columns.sql)
before using the current importer. Fresh databases use the current schema file.
Import an immutable finished journal with its metadata and final summary beside it:

```sh
.venv/bin/python proxy-tester/import_results.py /path/to/new/results.jsonl.gz
```

Compressed files keep sidecars named `results.jsonl.meta.json` and
`results.jsonl.summary.json`, plus `results.jsonl.sealed.json` when available.
The importer validates the whole journal, checks IDs and connection hashes,
response classifications, counts and available checksums, then updates all
statistics in one transaction. Invalid or duplicate input cannot partly change
persistent counters. `--batch-size` controls temporary staging memory; it does
not divide the final database commit.

**Import YouTube rounds in chronological order.** An exact retry of the latest
imported journal is skipped using `youtube_last_import_key`. Older or changed
overlapping YouTube journals fail without changing counters. Do not append to an
already imported journal. These restrictions keep
retry state bounded to one digest per proxy and website after detailed database
history is deleted. Import ordering uses `youtube_last_checked_at` independently
of shared connection timestamps.

Shared statistics retain `connection_attempts`, `successful_connections`,
`last_connection_attempt_at`, `last_connected_at`, and `last_connection_error`.
YouTube has its own check and attempt timestamps, working protocol, HTTP status,
duration, last response, error, counters and scoring inputs. See the
[field reference](../db/README.md#youtube-columns) for their exact names.

Journal status must consistently determine the attempted/response flags;
inconsistent or unsupported results are rejected before counters change. Status
remains part of the journal format and is not stored in `proxy_stats`.
`proxy_health.youtube_responded` is true if the latest actual YouTube attempt
received an HTTP response, false if it received none, and NULL before any attempt.
A response does not imply usable metadata; historical tests did not validate
response bodies as metadata.
The legacy journal format stays compatible. Importing these journals does not add
data-quality observations or reset an existing data score. The metadata collector
fills `youtube_successful_data_received` and the recent scoring inputs through its
best-effort background writer. See [proxy scoring](../db/README.md#recent-proxy-score).

A check counts as connected when any of its protocol attempts confirms a proxy
connection. Trying several handshakes still adds at most one successful connection
per check. `last_connection_error` holds the latest actual attempt's network
failure as a safe `stage:code` label. YouTube HTTP errors and missing metadata do
not set that field. `youtube_last_error` can also record setup, response-body,
HTTP and collector-reported data failures. A check rejected before an actual
attempt updates the website's check details while preserving the shared
connection fields and the previous actual YouTube attempt.
Historical missing observations remain unknown. The retained journals can
initialize older missing connection observations without replaying other
counters; see the [database backfill instructions](../db/README.md#historical-connection-backfill).

Verify the completed October 1 run against its saved summaries and final journal:

```sh
.venv/bin/python proxy-tester/verify_results.py \
  outputs/proxy-testing/results/20261001 \
  --manifest outputs/proxy-testing/catalog-20261001/manifest.json \
  --extra-run-dir outputs/proxy-testing/results/canary \
  --output outputs/proxy-testing/column-verification-20261001.json
```

This reads one consistent database snapshot, compares cumulative totals with
all saved summaries, and checks latest values against the final stability
journal. Run it against a snapshot matching that fixed run; later tests and
metadata collection change the expected totals.
The script's column references were updated for migration 008, but it was not
rerun after that migration. Individual per-round database history is no longer
available.

Useful SQL in the `proxy` database:

```sql
-- Configurations that responded in their latest actual YouTube attempt.
SELECT proxy_id, address, port, declared_protocol, youtube_working_protocol,
       youtube_last_attempt_at, youtube_last_http_status, youtube_responses_received
FROM proxy_health
WHERE youtube_responded IS TRUE
ORDER BY youtube_last_attempt_at DESC;

-- Latest response outcomes among configurations with recorded statistics.
SELECT youtube_responded, count(*)
FROM proxy_health
GROUP BY youtube_responded ORDER BY count(*) DESC;

-- Cumulative request and response counts.
SELECT sum(connection_attempts) AS connection_attempts,
       sum(youtube_requests_sent) AS youtube_requests_sent,
       sum(youtube_responses_received) AS youtube_responses_received,
       sum(youtube_successful_data_received) AS youtube_successful_data_received
FROM proxy_stats;
```

`youtube_last_response_at` retains a previous success even when
`youtube_responded` becomes false. `youtube_last_checked_at` can be newer than
`youtube_last_attempt_at` after a configuration rejection. These fields describe
observed results; availability can change. Identity is per saved configuration,
so multiple configurations can refer to the same endpoint.

## Build and test

The Go module pins Mihomo `v1.19.32` for VMess, VLESS, Trojan, Shadowsocks,
ShadowsocksR, Hysteria, Hysteria2, TUIC, WireGuard, and AnyTLS transports.
HTTP, HTTPS, SOCKS4, and SOCKS5 use the tester's native tunnels. MTProto is
recorded as Telegram-only; incomplete keys and unsupported transports have
explicit configuration outcomes.

The `bridge` subcommand exposes those same transports to the metadata collector.
The collector manages its loopback listener, private credentials and lifetime.
The bridge forwards an opaque YouTube TLS connection; metadata parsing and data
success accounting stay in Python. Adapters and target connections can be reused
across requests, and observations retain the original catalog identity. See
[catalog collection](../README.md#use-saved-proxy-configurations) for usage.

```sh
cd proxy-tester
go test -race -count=1 -timeout=60s ./...
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath \
  -ldflags='-s -w' -o ../outputs/proxy-testing/proxy-tester-linux-amd64 .
```

The tests use local proxy and TLS servers. They cover response classification,
gzip and field selection, authentication, protocol detection, target certificate
verification, full-body deadlines, one metadata request, and journal recovery.
Bridge tests additionally cover HTTP(S), SOCKS4/5 and VLESS tunnels, connection
reuse beyond the establishment deadline, and separating helper errors from
upstream failures.

To inspect configuration compatibility without testing connectivity:

```sh
proxy-tester audit --input /path/to/catalog/manifest.json \
  --output /path/to/configuration-audit.json
```

For a new catalog, export to a new empty directory and use a new run identifier:

```sh
.venv/bin/python proxy-tester/export_catalog.py \
  --output outputs/proxy-testing/new-catalog
```

The controller's large benchmark expects `sample-large.jsonl.gz`, containing a
uniform sample of one million configurations. The initial `sample.jsonl.gz`
contains 200,000. Create the larger sample with:

```sh
.venv/bin/python proxy-tester/sample_catalog.py \
  --manifest outputs/proxy-testing/new-catalog/manifest.json \
  --output outputs/proxy-testing/new-catalog/sample-large.jsonl.gz
```

Benchmark selection uses throughput across the middle of the sample, local
resource errors, and response retention; the final slow requests in a sample
and the initial worker startup are excluded. Local resource errors cause the full scan
to resume at lower concurrency.
