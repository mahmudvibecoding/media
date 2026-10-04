import gzip
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "proxy-tester/seal_results.py"
spec = importlib.util.spec_from_file_location("seal_results", SCRIPT)
sealing = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sealing)


class JournalSealingTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.folder = Path(temporary.name)

    def journal(self, name="results.jsonl", body=b'{"result":"ok"}\n', state="complete"):
        journal = self.folder / name
        journal.write_bytes(body)
        Path(str(journal) + ".summary.json").write_text(json.dumps({"state": state}))
        return journal

    def test_streamed_compression_preserves_source_hashes_and_replays(self):
        body = b'{"data":"' + b"a" * (1 << 20) + b'"}\n{"data":2}\n'
        journal = self.journal(body=body)
        evidence = sealing.seal(journal)
        compressed = Path(str(journal) + ".gz")
        self.assertEqual(journal.read_bytes(), body)
        self.assertEqual(gzip.decompress(compressed.read_bytes()), body)
        self.assertEqual(evidence["lines"], 2)
        self.assertEqual(evidence["uncompressed_sha256"], hashlib.sha256(body).hexdigest())
        self.assertEqual(evidence["compressed_sha256"], hashlib.sha256(compressed.read_bytes()).hexdigest())
        stamp = compressed.stat().st_mtime_ns
        self.assertEqual(sealing.seal(journal), evidence)
        self.assertEqual(compressed.stat().st_mtime_ns, stamp)

    def test_running_or_truncated_journal_has_no_published_receipt(self):
        for name, body, state in (("running", b"{}\n", "running"), ("partial", b"{}", "complete")):
            with self.subTest(name=name):
                journal = self.journal(name=name, body=body, state=state)
                with self.assertRaises(ValueError):
                    sealing.seal(journal)
                self.assertFalse(Path(str(journal) + ".sealed.json").exists())
                self.assertFalse(Path(str(journal) + ".gz").exists())
                self.assertEqual(journal.read_bytes(), body)

    def test_changed_source_cannot_replace_published_artifacts(self):
        journal = self.journal()
        sealing.seal(journal)
        compressed = Path(str(journal) + ".gz").read_bytes()
        receipt = Path(str(journal) + ".sealed.json").read_bytes()
        journal.write_bytes(b'{"changed":true}\n')
        with self.assertRaisesRegex(ValueError, "sealed journal changed"):
            sealing.seal(journal)
        self.assertEqual(Path(str(journal) + ".gz").read_bytes(), compressed)
        self.assertEqual(Path(str(journal) + ".sealed.json").read_bytes(), receipt)

    def test_parallel_cli_seals_each_journal(self):
        journals = [self.journal(name=f"part-{number}.jsonl", state="interrupted") for number in range(2)]
        result = subprocess.run([sys.executable, str(SCRIPT), "--workers", "2", "--journals", *map(str, journals)],
                                check=True, capture_output=True, text=True, timeout=20)
        evidence = [json.loads(line) for line in result.stdout.splitlines()]
        self.assertEqual({row["journal"] for row in evidence}, set(map(str, journals)))
        self.assertTrue(all(Path(str(journal) + ".sealed.json").is_file() for journal in journals))


if __name__ == "__main__":
    unittest.main()
