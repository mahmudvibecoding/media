from contextlib import redirect_stderr
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from bulk_import_source import parse_import_args, source_status, sync_outbox


class ImportSourceTests(unittest.TestCase):
    def test_local_run_reads_its_identity_and_status_without_ssh(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            for collector in ('metadata', 'statistics'):
                (folder / 'manifest.json').write_text(json.dumps({'run_id': 'example', 'collector': collector}))
                (folder / 'status.json').write_text('{"state":"complete"}')
                args = parse_import_args('test', collector, ['--run', tmp, '--once'])
                self.assertEqual(args.run_id, 'example')
                self.assertEqual(args.local, folder)
                with patch('bulk_import_source.subprocess.run', side_effect=AssertionError('Unexpected SSH')):
                    sync_outbox(args, folder)
                    self.assertEqual(source_status(args, folder), {'state': 'complete'})

    def test_wrong_collector_and_ambiguous_or_incomplete_options_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / 'manifest.json').write_text('{"run_id":"example","collector":"statistics"}')
            for argv in ([], ['--host', 'worker'], ['--run', tmp], ['--run', tmp, '--host', 'worker'],
                         ['--host', 'worker', '--remote', '/run', '--local', tmp, '--run-id', 'example', '--interval', 'nan']):
                with self.subTest(argv=argv), redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                    parse_import_args('test', 'metadata', argv)

    def test_remote_mode_keeps_immutable_file_selection(self):
        args = parse_import_args('test', 'metadata', ['--host', 'worker', '--remote', '/run',
            '--local', '/tmp/import', '--run-id', 'example'])
        with patch('bulk_import_source.subprocess.run') as execute:
            sync_outbox(args, args.local)
        command = execute.call_args.args[0]
        self.assertIn('--ignore-existing', command)
        self.assertIn('--include=*.jsonl.gz.json', command)
        self.assertEqual(command[-2:], ['worker:/run/outbox/', '/tmp/import/outbox/'])
