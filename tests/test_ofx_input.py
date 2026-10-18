import configparser
import contextlib
import importlib.util
import io
import os
import re
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from ofx_input import collect_latest_ofx, _move_without_overwrite


spec = importlib.util.spec_from_file_location(
    'accounts_input', Path(__file__).resolve().parents[1] / 'accounts-analysis.py')
app = importlib.util.module_from_spec(spec)
spec.loader.exec_module(app)


class OfxInputTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.downloads = self.root / 'Downloads'
        self.downloads.mkdir()
        self.archive = self.root / 'External volume' / 'OFX'
        self.config = configparser.RawConfigParser()
        self.config.read_dict({'Directories': {'downloads': str(self.downloads), 'input': str(self.archive)},
                              'Files': {'ofx_filename_regex': r'^CA[0-9]{8}_[0-9]{6}\.ofx$'}})
        self.output = io.StringIO()

    def downloaded(self, name='CA20261015_090958.ofx', modified=1792055398):
        path = self.downloads / name
        path.write_bytes(b'OFX content')
        os.utime(path, (modified, modified))
        return path

    def collect(self, dry_run=False):
        with contextlib.redirect_stdout(self.output):
            return collect_latest_ofx(self.config, dry_run=dry_run)

    def test_only_newest_matching_file_is_moved_with_metadata(self):
        old = self.downloaded('CA20261014_090958.ofx', 1000)
        latest = self.downloaded(modified=2000)
        other = self.downloaded('other.ofx', 3000)
        partial = self.downloaded('CA20261015_100000.ofx.part', 4000)
        result = self.collect()
        self.assertEqual(result, self.archive / latest.name)
        self.assertEqual(result.read_bytes(), b'OFX content')
        self.assertEqual(result.stat().st_mtime, 2000)
        self.assertFalse(latest.exists())
        self.assertTrue(all(path.exists() for path in (old, other, partial)))
        self.assertIn('Dernière modification', self.output.getvalue())
        self.assertIn('Ancienneté', self.output.getvalue())

    def test_duplicate_newest_stops_without_falling_back_or_overwriting(self):
        old = self.downloaded('CA20261014_090958.ofx', 1000)
        latest = self.downloaded(modified=2000)
        self.archive.mkdir(parents=True)
        destination = self.archive / latest.name
        destination.write_bytes(b'archived original')
        self.assertIsNone(self.collect())
        self.assertTrue(old.exists())
        self.assertTrue(latest.exists())
        self.assertEqual(destination.read_bytes(), b'archived original')
        self.assertIn('déjà intégré', self.output.getvalue())

    def test_selects_by_modification_not_filename_date(self):
        older_name = self.downloaded('CA20200101_090958.ofx', 2000)
        self.downloaded('CA20261015_090958.ofx', 1000)
        self.assertEqual(self.collect().name, older_name.name)

    def test_dry_run_preserves_source_and_does_not_create_archive(self):
        source = self.downloaded()
        self.assertEqual(self.collect(dry_run=True), source)
        self.assertTrue(source.exists())
        self.assertFalse(self.archive.exists())

    def test_no_match_does_not_create_archive(self):
        self.downloaded('irrelevant.pdf')
        self.assertIsNone(self.collect())
        self.assertFalse(self.archive.exists())
        self.assertIn('Aucun fichier', self.output.getvalue())

    def test_missing_settings_invalid_regex_and_same_directory_fail_before_move(self):
        source = self.downloaded()
        self.config.set('Directories', 'input', '')
        with self.assertRaisesRegex(ValueError, 'input'):
            self.collect()
        self.config.set('Directories', 'input', str(self.downloads))
        with self.assertRaisesRegex(ValueError, 'différents'):
            self.collect()
        self.config.set('Directories', 'input', str(self.archive))
        self.config.set('Files', 'ofx_filename_regex', '[')
        with self.assertRaises(re.error):
            self.collect()
        self.assertTrue(source.exists())

    def test_copy_error_removes_partial_destination_and_keeps_source(self):
        source = self.downloaded()
        self.archive.mkdir(parents=True)
        with patch('ofx_input.shutil.copyfileobj', side_effect=OSError('Disk full')):
            with self.assertRaises(OSError):
                self.collect()
        self.assertTrue(source.exists())
        self.assertFalse((self.archive / source.name).exists())

    def test_destination_race_never_overwrites_existing_file(self):
        source = self.downloaded()
        self.archive.mkdir(parents=True)
        destination = self.archive / source.name
        destination.write_bytes(b'existing')
        with self.assertRaises(FileExistsError):
            _move_without_overwrite(source, destination)
        self.assertEqual(destination.read_bytes(), b'existing')
        self.assertTrue(source.exists())

    def test_file_changed_during_copy_is_not_removed(self):
        source = self.downloaded()
        def change_source(reader, writer):
            writer.write(reader.read())
            source.write_bytes(b'newly changed contents')
        with patch('ofx_input.shutil.copyfileobj', side_effect=change_source):
            with self.assertRaisesRegex(ValueError, 'changé'):
                self.collect()
        self.assertEqual(source.read_bytes(), b'newly changed contents')
        self.assertFalse((self.archive / source.name).exists())

    def test_directories_and_symlinks_are_not_candidates(self):
        (self.downloads / 'CA20261015_100000.ofx').mkdir()
        target = self.root / 'target.ofx'
        target.write_text('test')
        (self.downloads / 'CA20261015_110000.ofx').symlink_to(target)
        self.assertIsNone(self.collect())

    def call_cli(self, args):
        with patch.object(app, 'config', self.config), patch.object(self.config, 'read', return_value=['properties.ini']), \
                patch.object(app, 'process_import') as process, contextlib.redirect_stdout(self.output):
            status = app.cli(args)
        return status, process

    def test_cli_automatic_import_uses_archived_path_and_all_flags(self):
        source = self.downloaded()
        status, process = self.call_cli(['--debug', '--import', '--csv-output'])
        self.assertEqual(status, 0)
        process.assert_called_once_with(str(self.archive / source.name), False, True, True)

    def test_cli_dry_run_uses_downloaded_path_and_does_not_move(self):
        source = self.downloaded()
        status, process = self.call_cli(['--import', '--dry-run', '--debug', '--csv-output'])
        self.assertEqual(status, 0)
        process.assert_called_once_with(str(source), True, True, True)
        self.assertTrue(source.exists())
        self.assertFalse(self.archive.exists())

    def test_cli_explicit_path_bypasses_download_configuration(self):
        source = self.downloaded()
        self.config.remove_section('Directories')
        self.config.remove_section('Files')
        status, process = self.call_cli(['--import', str(source)])
        self.assertEqual(status, 0)
        process.assert_called_once_with(str(source), False, False, False)
        self.assertTrue(source.exists())

    def test_cli_no_new_file_stops_before_import(self):
        source = self.downloaded()
        self.archive.mkdir(parents=True)
        (self.archive / source.name).write_text('previous import')
        status, process = self.call_cli(['--import'])
        self.assertEqual(status, 0)
        process.assert_not_called()

    def test_import_failure_retains_archived_file_for_explicit_retry(self):
        source = self.downloaded()
        with patch.object(app, 'config', self.config), patch.object(self.config, 'read', return_value=['properties.ini']), \
                patch.object(app, 'process_import', side_effect=ValueError('Balance mismatch')), \
                contextlib.redirect_stdout(self.output):
            self.assertEqual(app.cli(['--import']), 1)
        archived = self.archive / source.name
        self.assertEqual(archived.read_bytes(), b'OFX content')
        self.assertFalse(source.exists())
        status, process = self.call_cli(['--import', str(archived)])
        self.assertEqual(status, 0)
        process.assert_called_once_with(str(archived), False, False, False)


if __name__ == '__main__':
    unittest.main()
