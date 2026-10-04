from collections import Counter
import gzip
import json
from pathlib import Path
import tempfile
import unittest
import uuid

import comments_bulk as bulk
import comments_bulk_fleet as fleet
from metadata_bulk import atomic_json, digest
from proxy_catalog import CatalogProxy


class PartitionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "source"
        self.source.mkdir()
        self.videos = [{"video_id": f"{i:011d}", "kind": "video" if i % 2 else "short",
                        "baseline_at": "2026-10-03T10:00:00+00:00" if i == 0 else None,
                        "baseline_error": None} for i in range(120)]
        self.history = [[self.videos[0]["video_id"], "prior", True]]
        self.proxies = [CatalogProxy(i + 1, bytes([i + 1]) * 32, "127.0.0.1", 9000 + i,
                                    "http", "https" if i % 2 else "http", {}).bridge_record() for i in range(12)]
        files = {}
        for name, rows in (("videos.jsonl.gz", self.videos), ("history.jsonl.gz", self.history),
                           ("proxies.jsonl.gz", self.proxies)):
            with bulk.gzip_writer(self.source / name) as write:
                for row in rows:
                    write(row)
            files[name] = {"sha256": digest(self.source / name)}
        atomic_json(self.source / "manifest.json", {"version": 1, "collector": "comments", "run_id": str(uuid.uuid4()),
            "videos": 120, "video": 60, "short": 60, "with_history": 1, "history_comments": 1,
            "proxies": 12, "files": files})

    def rows(self, path):
        with gzip.open(path, "rt") as source:
            return [json.loads(line) for line in source]

    def test_frozen_rows_and_proxies_partition_exactly_once_with_matching_history(self):
        output = self.root / "fleet"
        result = fleet.partition(self.source, output, shards=4, concurrency=2)
        all_videos, all_history, all_proxies, run_ids = [], [], [], set()
        for index, entry in enumerate(result["shards"]):
            folder = output / entry["name"]
            manifest = json.loads((folder / "manifest.json").read_text())
            videos = self.rows(folder / "videos.jsonl.gz")
            history = self.rows(folder / "history.jsonl.gz")
            proxies = self.rows(folder / "proxies.jsonl.gz")
            self.assertEqual(proxies, self.proxies[index::4])
            self.assertTrue(all(fleet.partition_index(v["video_id"], 4) == index for v in videos))
            self.assertTrue(all(h[0] in {v["video_id"] for v in videos} for h in history))
            self.assertEqual(manifest["history_comments"], len(history))
            self.assertEqual(manifest["with_history"], sum(v["baseline_at"] is not None for v in videos))
            self.assertEqual(manifest["protocols"], dict(Counter(p["working_protocol"] for p in proxies)))
            self.assertEqual((folder / "proxies.jsonl.gz").stat().st_mode & 0o777, 0o600)
            bulk.initialize(folder, concurrency=2)
            queue = bulk.Queue(folder)
            try:
                self.assertEqual(queue.conn.execute("SELECT count(*) FROM jobs").fetchone()[0], len(videos))
            finally:
                queue.close()
            all_videos.extend(videos)
            all_history.extend(history)
            all_proxies.extend(proxies)
            run_ids.add(manifest["run_id"])
        self.assertEqual(sorted(all_videos, key=lambda r: r["video_id"]), self.videos)
        self.assertEqual(all_history, self.history)
        self.assertEqual(sorted(all_proxies, key=lambda r: r["id"]), self.proxies)
        self.assertEqual(len(run_ids), 4)

    def test_checksum_failure_creates_no_partial_fleet(self):
        with (self.source / "history.jsonl.gz").open("ab") as output:
            output.write(b"invalid")
        destination = self.root / "fleet"
        with self.assertRaisesRegex(ValueError, "checksum"):
            fleet.partition(self.source, destination, shards=4)
        self.assertFalse(destination.exists())

    def test_status_separates_collected_rows_from_imported_rows(self):
        output = self.root / "fleet"
        result = fleet.partition(self.source, output, shards=4)
        folder = output / result["shards"][0]["name"]
        atomic_json(folder / "remote-status.json", {"state": "running", "jobs": {"completed": 5, "ready": 7},
            "buffered_comments": 100, "pages": 10, "events": 12, "metrics": {"attempts": 12}})
        atomic_json(folder / "import-status.json", {"outcomes": {"completed": 4}, "inserted": 80,
            "refreshed": 2, "scan_seq": 4, "event_seq": 10, "complete": False})
        current = fleet.status(output)
        self.assertEqual((current["selected"], current["buffered_comments"], current["inserted"]), (120, 100, 80))
        self.assertEqual((current["completed_shards"], current["imported_shards"]), (0, 0))
        self.assertEqual(current["jobs"], {"completed": 5, "ready": 7})


if __name__ == "__main__":
    unittest.main()
