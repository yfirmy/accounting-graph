import configparser
import contextlib
import datetime
from decimal import Decimal
import importlib.util
import io
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from balance_checks import reconcile_import


spec = importlib.util.spec_from_file_location(
    'accounts_replay', Path(__file__).resolve().parents[1] / 'accounts-analysis.py')
app = importlib.util.module_from_spec(spec)
spec.loader.exec_module(app)


class ReplayTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.output = self.root / 'output'
        self.rules = self.root / 'rules.ini'
        self.rules.write_text('[phone]\naccount = XXXXXXXX\nlabel_regex = Telephone\namount = 5\n')
        self.config = configparser.RawConfigParser()
        self.config.read_dict({
            'Directories': {'database': str(self.root), 'output': str(self.output)},
            'Files': {'legitimacy_rules': str(self.rules)},
            'Accounts': {'xxxx3918': 'Courant', 'xxxx0407': 'Commun'},
            'Savings accounts': {'xxxx3918': 'false', 'xxxx0407': 'false'}})
        self.patch = patch.object(app, 'config', self.config)
        self.patch.start()
        self.addCleanup(self.patch.stop)

    def seed(self, account='12343918'):
        path = self.root / ('account_' + account + '.db')
        history = app.AccountStatement(account)
        history.balance_date = datetime.datetime(2026, 10, 18, 18)
        history.last_balance = Decimal('1000')
        history.add(app.Operation('old', datetime.datetime(2026, 9, 1), 'Opening', Decimal('100')))
        history.add(app.Operation('phone', datetime.datetime(2026, 10, 18), 'PRELEVEMENT Telephone', Decimal('-10')))
        with contextlib.closing(sqlite3.connect(str(path))) as connection:
            reconcile_import(history, connection)
            connection.execute("UPDATE TRANSACTIONS SET MATCHED_RULES = '[\"old-rule\"]'")
            connection.execute('''INSERT INTO TRANSACTION_ALERTS
                (TRANSACTION_ID, RULE_ID, TRANSACTION_DATE, LABEL, AMOUNT_CENTS, REASONS)
                VALUES ('phone', 'old-rule', '2026-10-18', 'Telephone', -1000, 'old reason')''')
            connection.commit()
        return path

    def dump(self, path):
        with contextlib.closing(sqlite3.connect(str(path))) as connection:
            return list(connection.iterdump())

    def banking_data(self, path):
        with contextlib.closing(sqlite3.connect(str(path))) as connection:
            return [connection.execute('SELECT * FROM ' + table).fetchall()
                    for table in ('TRANSACTIONS', 'BALANCE_OBSERVATIONS', 'sqlite_master')]

    def test_replay_replaces_alerts_and_generates_outputs_without_changing_banking_data(self):
        paths = [self.seed(), self.seed('12340407')]
        before = {path: self.banking_data(path) for path in paths}
        output = io.StringIO()
        with patch.object(app.plt, 'show'), contextlib.redirect_stdout(output):
            app.replay_analysis()
        self.assertEqual(len(list(self.output.glob('*.pdf'))), 4)
        report = (self.output / 'transaction_alerts.csv').read_text(encoding='utf-8-sig')
        self.assertIn('phone', report)
        self.assertNotIn('old-rule', report)
        self.assertIn('Courant', report)
        self.assertIn('Commun', report)
        self.assertIn('référence initiale reste non vérifiée', output.getvalue())
        for path in paths:
            self.assertEqual(self.banking_data(path), before[path])
            with contextlib.closing(sqlite3.connect(str(path))) as connection:
                self.assertEqual(connection.execute(
                    'SELECT TRANSACTION_ID, RULE_ID FROM TRANSACTION_ALERTS').fetchall(), [('phone', 'phone')])

    def test_corrected_rules_clear_old_alerts_on_repeated_replays(self):
        path = self.seed()
        before = self.banking_data(path)
        self.rules.write_text('[phone]\naccount = XXXXXXXX\nlabel_regex = Telephone\namount = 10\n')
        with patch.object(app, 'draw_stored_history'), contextlib.redirect_stdout(io.StringIO()):
            app.replay_analysis()
            app.replay_analysis()
        with contextlib.closing(sqlite3.connect(str(path))) as connection:
            self.assertEqual(connection.execute('SELECT * FROM TRANSACTION_ALERTS').fetchall(), [])
        self.assertEqual(self.banking_data(path), before)
        self.assertEqual(len((self.output / 'transaction_alerts.csv').read_text().splitlines()), 1)

    def test_failed_alert_refresh_restores_previous_alerts(self):
        path = self.seed()
        before = self.dump(path)

        def failing_refresh(connection, *args, **kwargs):
            connection.execute('DELETE FROM TRANSACTION_ALERTS')
            raise ValueError('Refresh failed')

        with patch.object(app, 'refresh_alerts', side_effect=failing_refresh), \
                contextlib.redirect_stdout(io.StringIO()), self.assertRaisesRegex(ValueError, 'Refresh failed'):
            app.replay_analysis()
        self.assertEqual(self.dump(path), before)

    def test_replay_dry_run_checks_but_does_not_write_outputs(self):
        path = self.seed()
        before = path.read_bytes()
        with patch.object(app, 'draw_stored_history') as draw, contextlib.redirect_stdout(io.StringIO()):
            app.replay_analysis(dry_run_mode=True)
        draw.assert_not_called()
        self.assertFalse(self.output.exists())
        self.assertEqual(path.read_bytes(), before)

    def test_modified_transaction_fails_snapshot_check_without_writing(self):
        path = self.seed()
        with contextlib.closing(sqlite3.connect(str(path))) as connection:
            connection.execute("UPDATE TRANSACTIONS SET AMOUNT_CENTS = -900 WHERE ID = 'phone'")
            connection.commit()
        before = path.read_bytes()
        with contextlib.redirect_stdout(io.StringIO()), self.assertRaisesRegex(ValueError, 'dernier contrôle'):
            app.replay_analysis()
        self.assertEqual(path.read_bytes(), before)
        self.assertFalse(self.output.exists())

    def test_no_matching_database_does_not_create_one(self):
        before = list(self.root.iterdir())
        with self.assertRaisesRegex(ValueError, 'Aucune base existante'):
            app.replay_analysis()
        self.assertEqual(list(self.root.iterdir()), before)

    def test_unconfigured_account_is_not_processed(self):
        path = self.seed('12340000')
        before = path.read_bytes()
        with self.assertRaisesRegex(ValueError, 'Aucune base existante'):
            app.replay_analysis()
        self.assertEqual(path.read_bytes(), before)

    def test_missing_reference_does_not_create_observation(self):
        path = self.seed()
        with contextlib.closing(sqlite3.connect(str(path))) as connection:
            connection.execute('DELETE FROM BALANCE_OBSERVATIONS')
            connection.commit()
        before = path.read_bytes()
        with contextlib.redirect_stdout(io.StringIO()), self.assertRaisesRegex(ValueError, 'Aucun solde de référence'):
            app.replay_analysis()
        self.assertEqual(path.read_bytes(), before)

    def test_legacy_database_is_not_migrated(self):
        path = self.root / 'account_12343918.db'
        with contextlib.closing(sqlite3.connect(str(path))) as connection:
            connection.execute('CREATE TABLE TRANSACTIONS (ID INTEGER PRIMARY KEY)')
        before = path.read_bytes()
        with contextlib.redirect_stdout(io.StringIO()), self.assertRaisesRegex(ValueError, 'migrer'):
            app.replay_analysis()
        self.assertEqual(path.read_bytes(), before)
        self.assertFalse((self.root / 'backups').exists())

    def test_cli_without_import_replays_instead_of_collecting_download(self):
        with patch.object(self.config, 'read', return_value=['properties.ini']), \
                patch.object(app, 'replay_analysis') as replay, \
                patch.object(app, 'collect_latest_ofx') as collect, \
                patch.object(app, 'process_import') as import_file:
            self.assertEqual(app.cli([]), 0)
            replay.assert_called_once_with(False, False)
            collect.assert_not_called()
            import_file.assert_not_called()


if __name__ == '__main__':
    unittest.main()
