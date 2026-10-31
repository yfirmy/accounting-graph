import configparser
import contextlib
import importlib.util
import io
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from ofx_encryption import encryption_settings, encrypt_ofx
from ofx_input import collect_latest_ofx

spec = importlib.util.spec_from_file_location(
    'accounts_encryption', Path(__file__).resolve().parents[1] / 'accounts-analysis.py')
app = importlib.util.module_from_spec(spec)
spec.loader.exec_module(app)


class EncryptionTests(unittest.TestCase):
    def setUp(self):
        self.config = configparser.RawConfigParser()
        self.config.read_dict({'Encryption': {
            'enabled': 'true', 'recipient_key_id': 'A' * 8}})

    def test_disabled_by_default_and_short_key_id_required(self):
        self.assertIsNone(encryption_settings(configparser.RawConfigParser()))
        for invalid in ('', 'person@example.com', '1234567', '123456789', 'A' * 16, 'A' * 40, '0x' + 'A' * 8):
            self.config.set('Encryption', 'recipient_key_id', invalid)
            with self.assertRaises(ValueError):
                encryption_settings(self.config)

    def test_default_removes_plaintext_and_missing_gpg_is_reported(self):
        with patch('ofx_encryption.shutil.which', return_value='/usr/bin/gpg'):
            self.assertEqual(encryption_settings(self.config), ('A' * 8, True))
        with patch('ofx_encryption.shutil.which', return_value=None):
            with self.assertRaisesRegex(ValueError, 'installer GnuPG'):
                encryption_settings(self.config)

    def test_only_successful_non_dry_import_encrypts_after_processing(self):
        calls = []
        with patch.object(app, 'config', self.config), \
                patch('ofx_encryption.shutil.which', return_value='gpg'), \
                patch.object(app, 'main', side_effect=lambda *a: calls.append('import')), \
                patch.object(app, 'encrypt_ofx', side_effect=lambda *a: calls.append('encrypt')), \
                contextlib.redirect_stdout(io.StringIO()):
            app.process_import('file.ofx', False, False, False)
            self.assertEqual(calls, ['import', 'encrypt'])
            calls.clear()
            app.process_import('file.ofx', True, False, False)
            self.assertEqual(calls, ['import'])
        with patch.object(app, 'config', self.config), \
                patch('ofx_encryption.shutil.which', return_value='gpg'), \
                patch.object(app, 'main', side_effect=ValueError('import failed')), \
                patch.object(app, 'encrypt_ofx') as encrypt:
            with self.assertRaises(ValueError):
                app.process_import('file.ofx', False, False, False)
            encrypt.assert_not_called()

    def test_encrypted_archive_blocks_automatic_collection(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            downloads, archive = root / 'downloads', root / 'archive'
            downloads.mkdir()
            archive.mkdir()
            source = downloads / 'statement.ofx'
            source.write_bytes(b'OFX')
            (archive / 'statement.ofx.gpg').write_bytes(b'existing ciphertext')
            self.config.read_dict({'Directories': {'downloads': str(downloads), 'input': str(archive)},
                                   'Files': {'ofx_filename_regex': r'.*\.ofx'}})
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertIsNone(collect_latest_ofx(self.config))
            self.assertEqual(source.read_bytes(), b'OFX')


@unittest.skipUnless(shutil.which('gpg'), 'GnuPG required for real encryption round-trip')
class GpgIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # GnuPG agent sockets have a short path limit, especially on macOS.
        cls.directory = tempfile.TemporaryDirectory(prefix='ofx-gpg-', dir='/tmp')
        cls.root = Path(cls.directory.name)
        cls.home = cls.root / 'keyring'
        cls.home.mkdir(mode=0o700)
        cls.env = patch.dict(os.environ, {'GNUPGHOME': str(cls.home)})
        cls.env.start()
        subprocess.run(['gpg', '--batch', '--pinentry-mode', 'loopback', '--passphrase', '',
                        '--quick-generate-key', 'OFX test fixture', 'rsa2048', 'encr', '1d'],
                       check=True, capture_output=True)
        listing = subprocess.run(['gpg', '--with-colons', '--list-keys'],
                                 check=True, capture_output=True, text=True).stdout
        cls.key_id = next(line.split(':')[4][-8:] for line in listing.splitlines()
                               if line.startswith('pub:'))

    @classmethod
    def tearDownClass(cls):
        subprocess.run(['gpgconf', '--kill', 'gpg-agent'], capture_output=True)
        cls.env.stop()
        cls.directory.cleanup()

    def setUp(self):
        self.files = tempfile.TemporaryDirectory(dir=self.root)
        self.addCleanup(self.files.cleanup)
        self.source = Path(self.files.name) / 'statement.ofx'
        self.data = b'OFXHEADER:100\n<OFX>original bytes\x00\xc3\xa9</OFX>'
        self.source.write_bytes(self.data)

    def test_real_round_trip_removes_original_and_preserves_exact_bytes(self):
        public_key = subprocess.run(['gpg', '--batch', '--export', self.key_id],
                                    check=True, capture_output=True).stdout
        public_home = self.root / 'public-only'
        public_home.mkdir(mode=0o700)
        with patch.dict(os.environ, {'GNUPGHOME': str(public_home)}):
            subprocess.run(['gpg', '--batch', '--import'], input=public_key,
                           check=True, capture_output=True)
            destination = encrypt_ofx(self.source, (self.key_id, True))
            subprocess.run(['gpgconf', '--kill', 'gpg-agent'], capture_output=True)
        self.assertFalse(self.source.exists())
        result = subprocess.run(['gpg', '--batch', '--decrypt', str(destination)],
                                check=True, capture_output=True)
        self.assertEqual(result.stdout, self.data)
        self.assertEqual(destination.stat().st_mode & 0o777, 0o600)
        self.assertFalse(list(destination.parent.glob('.ofx-encrypted-*')))

    def test_unknown_key_retains_original_and_cleans_partial_output(self):
        with self.assertRaisesRegex(ValueError, 'original conservé'):
            encrypt_ofx(self.source, ('0' * 8, True))
        self.assertEqual(self.source.read_bytes(), self.data)
        self.assertEqual(list(self.source.parent.iterdir()), [self.source])

    def test_existing_ciphertext_is_never_overwritten(self):
        destination = Path(str(self.source) + '.gpg')
        destination.write_bytes(b'existing')
        with self.assertRaisesRegex(ValueError, 'existe déjà'):
            encrypt_ofx(self.source, (self.key_id, True))
        self.assertEqual(destination.read_bytes(), b'existing')
        self.assertEqual(self.source.read_bytes(), self.data)

    def test_optional_plaintext_retention(self):
        encrypt_ofx(self.source, (self.key_id, False))
        self.assertEqual(self.source.read_bytes(), self.data)

    def test_changed_source_is_retained_and_ciphertext_not_published(self):
        run = subprocess.run

        def change_after_encryption(*args, **kwargs):
            result = run(*args, **kwargs)
            self.source.write_bytes(b'changed during encryption')
            return result

        with patch('ofx_encryption.subprocess.run', side_effect=change_after_encryption):
            with self.assertRaisesRegex(ValueError, 'a changé'):
                encrypt_ofx(self.source, (self.key_id, True))
        self.assertEqual(self.source.read_bytes(), b'changed during encryption')
        self.assertEqual(list(self.source.parent.iterdir()), [self.source])

    def test_publication_race_preserves_both_existing_files(self):
        destination = Path(str(self.source) + '.gpg')
        link = os.link

        def concurrent_publication(*args, **kwargs):
            destination.write_bytes(b'concurrent archive')
            return link(*args, **kwargs)

        with patch('ofx_encryption.os.link', side_effect=concurrent_publication):
            with self.assertRaises(FileExistsError):
                encrypt_ofx(self.source, (self.key_id, True))
        self.assertEqual(self.source.read_bytes(), self.data)
        self.assertEqual(destination.read_bytes(), b'concurrent archive')
        self.assertFalse(list(destination.parent.glob('.ofx-encrypted-*')))
