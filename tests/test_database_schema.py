import contextlib
import datetime
import io
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from balance_checks import reconcile_import
from database_schema import import_transaction


class SchemaMigrationTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / 'account_test.db'
        self.connection = sqlite3.connect(str(self.path))
        self.addCleanup(self.connection.close)
        self.connection.executescript('''
            CREATE TABLE TRANSACTIONS (ID INTEGER PRIMARY KEY NOT NULL,
                DATE TEXT NOT NULL, DATE_EPOCH INTEGER NOT NULL, LABEL TEXT,
                AMOUNT REAL, TAG TEXT);
            CREATE INDEX transaction_tag ON TRANSACTIONS(TAG);
            CREATE TABLE CHECKPOINTS (DATE_EPOCH INTEGER PRIMARY KEY NOT NULL,
                DATE TEXT NOT NULL, BALANCE REAL, TRANSACTIONS_COUNT INTEGER);
            INSERT INTO CHECKPOINTS VALUES (0, '01/01/2022', 1200.5, 10);
        ''')
        start = datetime.datetime(2022, 1, 1)
        self.rows = [(i + 1, (start + datetime.timedelta(days=i)).strftime('%d/%m/%Y'),
                      i * 86400, "Bank's transaction " + str(i), (i % 200 - 100) / 100,
                      'HERITAGE' if i % 7 == 0 else None) for i in range(1461)]
        self.connection.executemany('INSERT INTO TRANSACTIONS VALUES (?, ?, ?, ?, ?, ?)', self.rows)
        self.connection.commit()

    def migrate(self, dry_run=False):
        with contextlib.redirect_stdout(io.StringIO()):
            with import_transaction(self.connection, dry_run=dry_run):
                pass

    def test_four_years_preserved_backup_restorable_and_migration_idempotent(self):
        before = list(self.connection.iterdump())
        self.migrate()
        actual = self.connection.execute('''SELECT CAST(ID AS INTEGER), DATE, DATE_EPOCH,
            LABEL, AMOUNT, TAG FROM TRANSACTIONS ORDER BY CAST(ID AS INTEGER)''').fetchall()
        self.assertEqual(actual, self.rows)
        self.assertEqual(self.connection.execute('SELECT COUNT(*) FROM CHECKPOINTS').fetchone()[0], 1)
        self.assertEqual(self.connection.execute('PRAGMA integrity_check').fetchall(), [('ok',)])
        self.assertIsNotNone(self.connection.execute(
            "SELECT name FROM sqlite_master WHERE name = 'transaction_tag'").fetchone())
        backups = list(self.path.parent.glob('backups/*.db'))
        self.assertEqual(len(backups), 1)
        backup = sqlite3.connect(str(backups[0]))
        self.addCleanup(backup.close)
        self.assertEqual(list(backup.iterdump()), before)
        restored = sqlite3.connect(':memory:')
        self.addCleanup(restored.close)
        backup.backup(restored)
        self.assertEqual(list(restored.iterdump()), before)
        migrated = list(self.connection.iterdump())
        self.migrate()
        self.assertEqual(list(self.connection.iterdump()), migrated)
        self.assertEqual(list(self.path.parent.glob('backups/*.db')), backups)

    def test_dry_run_does_not_change_database_or_create_backup(self):
        before = list(self.connection.iterdump())
        self.migrate(dry_run=True)
        self.assertEqual(list(self.connection.iterdump()), before)
        self.assertFalse((self.path.parent / 'backups').exists())

    def test_backup_failure_prevents_any_migration(self):
        before = list(self.connection.iterdump())
        with patch('database_schema.tempfile.NamedTemporaryFile', side_effect=OSError('Disk full')):
            with self.assertRaises(OSError):
                self.migrate()
        self.assertEqual(list(self.connection.iterdump()), before)

    def test_unknown_columns_are_not_dropped(self):
        self.connection.execute('ALTER TABLE TRANSACTIONS ADD COLUMN PRIVATE_NOTE TEXT')
        self.connection.execute("UPDATE TRANSACTIONS SET PRIVATE_NOTE = 'retain'")
        self.connection.commit()
        before = list(self.connection.iterdump())
        with self.assertRaisesRegex(ValueError, 'Unexpected legacy'):
            self.migrate()
        self.assertEqual(list(self.connection.iterdump()), before)

    def test_invalid_amount_rolls_back_entire_migration(self):
        self.connection.execute('UPDATE TRANSACTIONS SET AMOUNT = 1.001 WHERE ID = 1')
        self.connection.commit()
        before = list(self.connection.iterdump())
        with self.assertRaises(ValueError):
            self.migrate()
        self.assertEqual(list(self.connection.iterdump()), before)

    def test_triggers_preserved_without_firing_during_migration(self):
        self.connection.executescript('''CREATE TABLE AUDIT (ID INTEGER);
            CREATE TRIGGER audit_update AFTER UPDATE ON TRANSACTIONS BEGIN
            INSERT INTO AUDIT VALUES (NEW.ID); END;''')
        self.migrate()
        self.assertEqual(self.connection.execute('SELECT COUNT(*) FROM AUDIT').fetchone()[0], 0)
        self.connection.execute("UPDATE TRANSACTIONS SET TAG = 'changed' WHERE ID = '1'")
        self.assertEqual(self.connection.execute('SELECT ID FROM AUDIT').fetchall(), [(1,)])

    def test_foreign_keys_cannot_cascade_delete_history(self):
        self.connection.executescript('''PRAGMA foreign_keys = ON;
            CREATE TABLE NOTES (TRANSACTION_ID INTEGER REFERENCES TRANSACTIONS(ID) ON DELETE CASCADE);
            INSERT INTO NOTES VALUES (1);''')
        before = list(self.connection.iterdump())
        with self.assertRaisesRegex(ValueError, 'Foreign keys'):
            self.migrate()
        self.assertEqual(list(self.connection.iterdump()), before)

    def test_wal_backup_includes_committed_transactions(self):
        self.connection.execute('PRAGMA journal_mode = WAL')
        self.connection.execute("UPDATE TRANSACTIONS SET TAG = 'WAL' WHERE ID = 1")
        self.connection.commit()
        self.migrate()
        backup = sqlite3.connect(str(next(self.path.parent.glob('backups/*.db'))))
        self.addCleanup(backup.close)
        self.assertEqual(backup.execute('SELECT TAG FROM TRANSACTIONS WHERE ID = 1').fetchone()[0], 'WAL')

    def test_failed_import_retains_original_database_and_backup(self):
        before = list(self.connection.iterdump())
        invalid = SimpleNamespace(balance_date=None, last_balance=1000, operations={})
        with contextlib.redirect_stdout(io.StringIO()), self.assertRaises(ValueError):
            reconcile_import(invalid, self.connection)
        self.assertEqual(list(self.connection.iterdump()), before)
        self.assertEqual(len(list(self.path.parent.glob('backups/*.db'))), 1)


if __name__ == '__main__':
    unittest.main()
