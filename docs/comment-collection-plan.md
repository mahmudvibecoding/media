# Incremental YouTube comment collection

## Current implementation

`collect_video_comments.py` collects one video. `comments_bulk.py` snapshots
videos, checkpoints pages, and exports immutable results. `comments_bulk_import.py`
imports those results and proxy observations into the local databases.

The implemented scope is public top-level comments. Each fresh scan starts with
a direct **Newest** request to `/next`. The returned sort header must confirm
Newest before the first page is consumed. If YouTube returns only a sort menu,
the collector follows its explicit Newest token. There is no Top setup request.

`comments_bulk_fleet.py` partitions large runs among independent workers and
importers. `comments_bulk_audit.py` checks published results against PostgreSQL
and reports unresolved videos. Historical pilot measurements appear below;
run outputs and databases are excluded from the repository.

## Stored data

`public.comments` contains exactly six columns:

| Field | Type | Meaning |
| --- | --- | --- |
| `video_id` | `TEXT NOT NULL` | References an existing `public.videos` row. |
| `comment_id` | `TEXT NOT NULL` | YouTube comment identifier. |
| `text` | `TEXT NOT NULL` | Displayed text, including Unicode, line breaks, and valid empty text. |
| `author_channel_id` | `TEXT` | Author channel ID when exposed. |
| `author_name` | `TEXT` | Displayed author name when exposed. |
| `is_pinned` | `BOOLEAN` | Pin status; NULL means unknown. |

The primary key is `(video_id, comment_id)`. Replies, likes, publication dates,
publication labels, and per-comment collection timestamps are excluded from the
field mask and stored rows. Attachments are outside this collection.

Only two progress columns are added to `public.videos`: `comments_updated_at`
records when a successful scan finished importing, and `comments_error` records
its latest error. Both are initially NULL. A successful import clears the error.
There is no public scan-state table. Operational timestamps, tokens, leases, and
receipts live in the run's SQLite files and JSON artifacts.

## Collection and stopping rules

The parser reads top-level thread renderers and their referenced content entities.
It does not traverse reply branches. Both current entity-based responses and
legacy comment renderers are supported. Unknown responses, malformed entities,
wrong video identities, repeated tokens, and pages without progress remain errors.

YouTube can omit an empty body list when its header explicitly says zero comments.
That case is accepted. A missing body without evidence of emptiness is not treated
as a successful comment page. An initial sort menu can still be used to request
Newest when Top exposes no comments.

On a video's first successful scan, pagination continues to its normal end. On a
later scan, every page is checked against a frozen copy of the prior completed
history. When a page contains an eligible saved ID, the whole page is saved before
stopping. For example, 15 new comments followed by 5 saved comments inserts the 15
new IDs and finishes that scan. If saved IDs disappear, collection continues to
the normal end.

A stopping ID must be known to be unpinned both in the completed baseline and in
the current scan. Pinned, formerly pinned, and unknown-pin comments cannot stop
collection. A pinned occurrence earlier in a scan also excludes a later ordinary
occurrence of that ID. Revisited rows refresh their text and author data. Older
rows outside the visited range remain as saved.

## Durable queue and recovery

The export freezes video IDs, their completion timestamps and errors, completed
comment history, and selected proxy configurations. Each snapshot file has a
SHA-256 checksum. The run directory and proxy files are private.

The runner owns one SQLite connection on a dedicated storage thread. Concurrent
HTTP workers submit page results to that thread. Each transaction records all comments
on a page, the next continuation, the request outcome, and the job's metrics.
A lease prevents stale worker results from committing. Work is scheduled between
pages, so a large video's dependent chain does not occupy every worker.

The original history remains fixed throughout the scan. Comments fetched earlier
in the active scan cannot become a stopping boundary. On interruption, a resumed
job requests its saved continuation. It does not make an extra newest-page pass
after finishing the resumed range. Comments posted during an interruption wait
until the next normal run.

If a continuation expires, repeated failures on different routes can restart the
scan at Newest while preserving its original baseline and deduplicated buffer.
Retry and page limits leave a scan explicitly incomplete. A manual retry retains
the buffer and baseline, and assigns a new generation and scan ID. Earlier error
artifacts remain immutable.

Successful scans and failed outcomes are exported as complete, checksummed gzip
batches. A failed outcome exports no partial comment rows. Its buffer remains in
SQLite for a retry. Atomic receipt files mark published batches, and export can
recover a file published before its queue cursor was acknowledged.

## Import and replay

The importer verifies the checksum and every record in a batch before changing
PostgreSQL. It validates the run, selected video, frozen baseline, scan identity,
sequence, row counts, and exact six comment fields. It stages large batches on
disk rather than loading all comment history into memory.

Each successful scan uses the same per-video advisory lock as the single-video
collector. The importer commits groups of up to 64 distinct videos, keeping
these operations in the same PostgreSQL transaction:

1. Check that the current completion timestamp still matches the frozen baseline.
2. Insert new IDs and refresh changed fields on revisited IDs.
3. Choose a completion timestamp and durably prepare its local import receipt.
4. Set `comments_updated_at`, clear `comments_error`, and commit.
5. Acknowledge the receipt in the local import journal.

If PostgreSQL committed before step 5, its exact timestamp identifies the prepared
receipt on replay. Rows and timestamps are not applied twice. If the transaction
rolled back, replay retries it. An older snapshot cannot overwrite a newer
completed scan. Failed scans update only the error, preserving comments and the
last successful timestamp; later generations may replace their own earlier error.

Keep the snapshot, `queue.sqlite3`, `import.sqlite3`, and outbox together. Those
files contain the recovery state. The importer can watch a local run or copy
published batches and status from a remote worker with rsync.

## Proxy handling

The runner reuses `ProxyPool`, `CatalogClients`, and `collection_policy.py`.
It allows one active request per proxy, scores recent success and successful
response latency, and prefers another route when retrying a failed page.

Valid data and verified empty or disabled sections count as successful responses.
An unavailable video needs confirmation from different proxies and is neutral for
data-success scoring. Network, HTTP, and malformed-response failures affect the
route's score. Local resource errors and interrupted requests are excluded from
global proxy statistics. Immutable attempt batches have their own replay receipts.

## Commands and validation

See [the README commands](../README.md#collect-a-snapshot-of-videos) for export,
initialization, collection, pause/resume, retry, repeated updates, and import.

The comment tests cover schema constraints, Newest selection, top-level parsing,
full boundary pages, pin handling, page recovery, expired tokens, immutable
exports, checksum failures, transactional imports, lost acknowledgements, replay,
and protection of newer database state. Database fixtures use isolated schemas.

```sh
MEDIA_TEST_DATABASE_URL="host=$PWD/.local/postgres/socket dbname=media user=mahmud" \
  .venv/bin/python -m unittest discover -s tests -p '*comments*.py' -v
```

The pilot uses a fixed 1,000-video selection with ordinary videos, Shorts, and two
previously collected controls. It measures first and repeat scans, a controlled
pause and resume, exact artifact replay, failures, request counts, storage, and
throughput. Completion means every selected video has either a successful scan
or an explicit exception, every published batch has been processed, and the
recovery and database audits pass. Counts of failed historical attempts are
reported separately from the final status of unique videos.

### Pilot results: 2026-10-03

The fixed selection contained 444 videos and 556 Shorts. Both passes completed
997 videos: 569 with comments, 374 empty, and 54 with comments disabled. Two
videos were unavailable; one returned an unrecognized response. Those three
exceptions retain errors and have no successful completion timestamp.

| Measurement | First pass | Repeat using direct Newest |
| --- | ---: | ---: |
| Active collection time | 94.75 s | 10.99 s |
| Comment pages | 1,662 | 943 |
| HTTP attempts, including failures | 5,848 | 1,466 |
| New rows imported | 17,148 | 0 |
| Compressed comment batches | 1.78 MB | 0.48 MB |
| Worker SQLite file | 12.09 MB | 5.51 MB |

The database ended with 17,434 comment rows, including the 286 prior control
rows. Its comment table and indexes occupied 5.24 MB. Repeat import took 10.22 s.
Every successful repeat that fetched comment pages finished after one page;
565 scans stopped at saved history. The first timing includes the former setup
request, two parser fixes, and their retries, so it is not a benchmark of the
current first-pass implementation.

Forty saved continuation jobs resumed without another initial request. Replaying
all 1,306 first-pass scan artifacts and all 1,000 repeat artifacts inserted or
refreshed no rows and preserved completion timestamps. Proxy replay preserved
counters, including the final batch's lost-acknowledgement case. The 61 comment
tests and 23 existing proxy-pool and policy tests passed.

Simple scaling of this sample to the current 2,031,675-video database suggests
roughly 35 million comments and 10 GB of additional comment table/index storage.
This is a rough capacity estimate: large comment histories outside the sample
can change it substantially. Run artifacts require separate space. The pilot
does not establish a full-backfill completion time.

Evidence: [verification.json](../outputs/comments-pilot-20261003/verification.json)
and [replay-verification.json](../outputs/comments-pilot-20261003/replay-verification.json).

## Limits

The result covers the public chronological listing available during each scan.
Incremental scans cannot guarantee discovery of older comments that become
visible below the saved boundary. Saved comments remain historical records;
there is no separate edit or deletion reconciliation pass. Header counts can
include replies or comments outside the accessible top-level listing and are
not an exact expected row count.

## References

- [yt-dlp comment extraction](https://github.com/yt-dlp/yt-dlp/blob/master/yt_dlp/extractor/youtube/_video.py): `/next` continuations, comment entities, and pinned handling.
- [YouTube comment resource](https://developers.google.com/youtube/v3/docs/comments): the official Data API's comment fields; date and like fields are outside this collection.
- [Database guide](../db/README.md#comments): schema, constraints, permissions, and write rules.
