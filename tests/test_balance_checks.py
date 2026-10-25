import contextlib
import datetime as dt
from decimal import Decimal
import importlib.util
import io
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from balance_checks import cents, latest_observation, reconcile_import


spec = importlib.util.spec_from_file_location(
    'accounts_analysis', Path(__file__).resolve().parents[1] / 'accounts-analysis.py')
app = importlib.util.module_from_spec(spec)
spec.loader.exec_module(app)


def operation(identifier, when, amount):
    return app.Operation(identifier, dt.datetime.fromisoformat(when), "Bank's fee", Decimal(amount))


def statement(when, balance, *operations):
    history = app.AccountStatement('123456')
    history.balance_date = dt.datetime.fromisoformat(when)
    history.last_date = history.balance_date.replace(hour=0, minute=0, second=0, microsecond=0)
    history.last_balance = Decimal(balance)
    for op in operations:
        history.add(op)
    return history


class ReconciliationTests(unittest.TestCase):
    def setUp(self):
        self.connection = sqlite3.connect(':memory:')
        self.addCleanup(self.connection.close)

    def baseline(self):
        return reconcile_import(statement('2026-10-09T18:00:00', '1000'), self.connection)

    def test_first_import_is_explicitly_unverified(self):
        self.assertEqual(self.baseline(), 'baseline-unverified')

    def test_weekly_import_and_overlapping_reimport(self):
        self.baseline()
        history = statement('2026-10-12T18:00:00', '1299.90',
                            operation('credit', '2026-10-12T10:00:00', '500'),
                            operation('debit', '2026-10-10T10:00:00', '-200.10'))
        self.assertEqual(reconcile_import(history, self.connection), 'reconciled')
        self.assertEqual(reconcile_import(history, self.connection), 'reconciled')
        self.assertEqual(self.connection.execute('SELECT COUNT(*) FROM TRANSACTIONS').fetchone()[0], 2)

    def test_monday_fee_backdated_before_weekend(self):
        self.baseline()
        fee = operation('fee', '2026-10-09T10:00:00', '-8.50')
        history = statement('2026-10-12T18:00:00', '991.50', fee)
        self.assertEqual(reconcile_import(history, self.connection), 'reconciled-late')
        self.assertEqual(reconcile_import(history, self.connection), 'reconciled')
        self.assertEqual(reconcile_import(statement('2026-10-15T18:00:00', '981.50', fee,
            operation('next', '2026-10-14T12:00:00', '-10')), self.connection), 'reconciled')

    def test_backfill_already_in_bank_balance(self):
        self.baseline()
        history = statement('2026-10-12T18:00:00', '990',
                            operation('old', '2026-09-15T10:00:00', '-80'),
                            operation('new', '2026-10-12T10:00:00', '-10'))
        self.assertEqual(reconcile_import(history, self.connection), 'reconciled-backfill')
        self.assertEqual(reconcile_import(statement('2026-10-15T18:00:00', '990'),
                                         self.connection), 'reconciled')

    def test_unexplained_difference_rolls_back_all_operations(self):
        self.baseline()
        with self.assertRaisesRegex(ValueError, 'Balance mismatch') as failure:
            reconcile_import(statement('2026-10-12T18:00:00', '700',
                operation('late', '2026-10-09T10:00:00', '-10'),
                operation('new', '2026-10-12T10:00:00', '-20')), self.connection)
        self.assertIn('référence initiale non vérifiée', str(failure.exception))
        self.assertIn('à partir du 09/10/2026 inclus', str(failure.exception))
        self.assertEqual(self.connection.execute('SELECT COUNT(*) FROM TRANSACTIONS').fetchone()[0], 0)
        self.assertEqual(self.connection.execute('SELECT COUNT(*) FROM BALANCE_OBSERVATIONS').fetchone()[0], 1)

    def test_balance_failure_recommends_last_successful_date_not_failed_date(self):
        self.baseline()
        reconcile_import(statement('2026-10-12T18:00:00', '990',
            operation('known', '2026-10-12T12:00:00', '-10')), self.connection)
        with self.assertRaises(ValueError) as failure:
            reconcile_import(statement('2026-10-15T18:00:00', '900'), self.connection)
        self.assertIn('Solde vérifié jusqu’au 12/10/2026 inclus, à 18:00:00 UTC', str(failure.exception))
        self.assertIn('à partir du 12/10/2026 inclus', str(failure.exception))
        self.assertEqual(latest_observation(self.connection)[0], '2026-10-12T18:00:00.000000')

    def test_changed_duplicate_rolls_back_prior_insert(self):
        op = operation('known', '2026-10-09T10:00:00', '-5')
        reconcile_import(statement('2026-10-09T18:00:00', '1000', op), self.connection)
        for changed in [operation('known', '2026-10-09T10:00:00', '-6'),
                        operation('known', '2026-10-09T11:00:00', '-5')]:
            with self.assertRaisesRegex(ValueError, 'changed amount or posting date'):
                reconcile_import(statement('2026-10-12T18:00:00', '990',
                    operation('first', '2026-10-12T10:00:00', '-10'), changed), self.connection)
        self.assertEqual(self.connection.execute('SELECT COUNT(*) FROM TRANSACTIONS').fetchone()[0], 1)

    def test_same_day_imports_preserve_times(self):
        reconcile_import(statement('2026-10-12T10:00:00', '1000'), self.connection)
        self.assertEqual(reconcile_import(statement('2026-10-12T18:00:00', '990',
            operation('afternoon', '2026-10-12T15:00:00', '-10')), self.connection), 'reconciled')

    def test_conflicting_same_timestamp_is_rejected(self):
        self.baseline()
        with self.assertRaisesRegex(ValueError, 'Conflicting bank balances'):
            reconcile_import(statement('2026-10-09T18:00:00', '990',
                operation('late', '2026-10-09T10:00:00', '-10')), self.connection)

    def test_future_operation_does_not_affect_current_balance(self):
        self.baseline()
        future = operation('future', '2026-10-15T10:00:00', '-10')
        reconcile_import(statement('2026-10-12T18:00:00', '1000', future), self.connection)
        self.assertEqual(reconcile_import(statement('2026-10-15T18:00:00', '990', future),
                                         self.connection), 'reconciled')

    def test_empty_statement_still_checks_balance(self):
        self.baseline()
        with self.assertRaisesRegex(ValueError, 'Balance mismatch'):
            reconcile_import(statement('2026-10-12T18:00:00', '999'), self.connection)

    def test_dry_run_leaves_database_unchanged(self):
        self.baseline()
        before = list(self.connection.iterdump())
        reconcile_import(statement('2026-10-12T18:00:00', '990',
            operation('new', '2026-10-12T10:00:00', '-10')), self.connection, dry_run=True)
        self.assertEqual(before, list(self.connection.iterdump()))

    def test_fresh_dry_run_rolls_back_schema(self):
        reconcile_import(statement('2026-10-12T18:00:00', '1000'), self.connection, dry_run=True)
        self.assertEqual(self.connection.execute('SELECT name FROM sqlite_master').fetchall(), [])

    def test_legacy_upgrade_preserves_tags_and_dates(self):
        self.connection.executescript('''
            CREATE TABLE TRANSACTIONS (ID INTEGER PRIMARY KEY NOT NULL,
                DATE TEXT NOT NULL, DATE_EPOCH INTEGER NOT NULL, LABEL TEXT, AMOUNT REAL, TAG TEXT);
            INSERT INTO TRANSACTIONS VALUES (123, '09/10/2026', 0, 'Old', -5.5, 'HERITAGE');
        ''')
        self.assertEqual(reconcile_import(statement('2026-10-12T18:00:00', '1000',
            operation('123', '2026-10-09T10:00:00', '-5.50')), self.connection), 'baseline-unverified')
        self.assertEqual(self.connection.execute('SELECT TAG, AMOUNT_CENTS FROM TRANSACTIONS').fetchone(),
                         ('HERITAGE', -550))
        history = app.read_transactions_from_database('123456', self.connection, dt.datetime(2026, 10, 12, 18))
        self.assertIn(dt.datetime(2026, 10, 9), history.operations)

    def test_older_statement_does_not_replace_latest_balance(self):
        reconcile_import(statement('2026-10-12T18:00:00', '990',
            operation('new', '2026-10-12T10:00:00', '-10')), self.connection)
        self.assertEqual(reconcile_import(statement('2026-10-09T18:00:00', '1000',
            operation('old', '2026-09-15T10:00:00', '-80')), self.connection), 'historical-reconciled')
        self.assertEqual(latest_observation(self.connection)[1], 99000)
        self.assertEqual(reconcile_import(statement('2026-10-15T18:00:00', '990'),
                                         self.connection), 'reconciled')

    def test_invalid_older_statement_is_rolled_back(self):
        self.baseline()
        with self.assertRaisesRegex(ValueError, 'Older statement'):
            reconcile_import(statement('2026-10-01T18:00:00', '2000',
                operation('old', '2026-10-01T12:00:00', '10')), self.connection)
        self.assertEqual(self.connection.execute('SELECT COUNT(*) FROM TRANSACTIONS').fetchone()[0], 0)

    def test_cent_arithmetic(self):
        self.assertEqual(cents(Decimal('0.10')), 10)
        for value in ['0.001', 'NaN', 'Infinity']:
            with self.assertRaises(ValueError):
                cents(value)
        self.baseline()
        self.assertEqual(reconcile_import(statement('2026-10-12T18:00:00', '1000.30',
            operation('a', '2026-10-12T10:00:00', '0.10'),
            operation('b', '2026-10-12T11:00:00', '0.20')), self.connection), 'reconciled')

    def test_graph_starts_with_received_balance_before_subtracting(self):
        history = statement('2026-10-12T18:00:00', '990',
                            operation('today', '2026-10-12T10:00:00', '-10'),
                            operation('yesterday', '2026-10-11T10:00:00', '20'))
        balances, *_ = app.compute_balance_evolution(history, self.connection, False)
        self.assertEqual(balances[dt.datetime(2026, 10, 12)], 990)
        self.assertEqual(balances[dt.datetime(2026, 10, 11)], 1000)

    def test_application_validates_before_drawing(self):
        self.baseline()
        with patch.object(app, 'analyse_operations') as draw:
            with self.assertRaises(ValueError):
                app.prepare_and_analyse_history(statement('2026-10-12T18:00:00', '900'),
                                                self.connection, False, False)
            draw.assert_not_called()

    def test_ofx_uses_balance_date_not_period_end(self):
        bank_statement = SimpleNamespace(balance_date=dt.datetime(2026, 10, 12, 18),
            end_date=dt.datetime(2026, 10, 10), balance=Decimal('1000'), transactions=[], discarded_entries=[])
        ofx = SimpleNamespace(accounts=[SimpleNamespace(account_id='123456', statement=bank_statement)])
        with patch.object(app, 'read_ofx', return_value=ofx), contextlib.redirect_stdout(io.StringIO()):
            result = app.parse_ofx('test.ofx', False)[0]
            self.assertEqual(result.balance_date, bank_statement.balance_date)
            bank_statement.balance_date = None
            with self.assertRaisesRegex(ValueError, 'DTASOF'):
                app.parse_ofx('test.ofx', False)

    def test_real_ofx_parsing_and_full_import(self):
        self.baseline()
        ofx = '''<?xml version="1.0" encoding="UTF-8"?>
        <OFX><BANKMSGSRSV1><STMTTRNRS><TRNUID>1</TRNUID>
        <STATUS><CODE>0</CODE><SEVERITY>INFO</SEVERITY></STATUS>
        <STMTRS><CURDEF>EUR</CURDEF><BANKACCTFROM><BANKID>TEST</BANKID>
        <ACCTID>123456</ACCTID><ACCTTYPE>CHECKING</ACCTTYPE></BANKACCTFROM>
        <BANKTRANLIST><DTSTART>20261001000000</DTSTART><DTEND>20261011000000</DTEND>
        <STMTTRN><TRNTYPE>DEBIT</TRNTYPE><DTPOSTED>20261009090000</DTPOSTED>
        <DTUSER>20261008000000</DTUSER><TRNAMT>-8.50</TRNAMT>
        <FITID>fee-october</FITID><NAME>Bank fee</NAME><MEMO>Fee</MEMO></STMTTRN>
        </BANKTRANLIST><LEDGERBAL><BALAMT>991.50</BALAMT>
        <DTASOF>20261012180000</DTASOF></LEDGERBAL></STMTRS>
        </STMTTRNRS></BANKMSGSRSV1></OFX>'''
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'statement.ofx'
            path.write_text(ofx)
            with contextlib.redirect_stdout(io.StringIO()):
                history = app.parse_ofx(str(path), False)[0]
                with patch.object(app, 'analyse_operations') as draw:
                    app.prepare_and_analyse_history(history, self.connection, False, False)
                    graphed = draw.call_args.args[0]
            self.assertEqual(graphed.last_balance, 991.5)
            self.assertEqual(history.balance_date, dt.datetime(2026, 10, 12, 18))
            self.assertIn(dt.datetime(2026, 10, 9), history.operations)
            self.assertEqual(latest_observation(self.connection)[1], 99150)

    def test_legacy_migration_failure_is_atomic(self):
        self.connection.executescript('''CREATE TABLE TRANSACTIONS (
            ID INTEGER PRIMARY KEY NOT NULL, DATE TEXT NOT NULL,
            DATE_EPOCH INTEGER NOT NULL, LABEL TEXT, AMOUNT REAL, TAG TEXT);
            INSERT INTO TRANSACTIONS VALUES (1, '09/10/2026', 0, 'Fee', -10, 'keep');''')
        before = list(self.connection.iterdump())
        with self.assertRaises(ValueError):
            reconcile_import(statement('2026-10-12T18:00:00', '1000',
                operation('1', '2026-10-09T10:00:00', '-20')), self.connection)
        self.assertEqual(before, list(self.connection.iterdump()))

    def test_nested_transaction_survives_rejected_import(self):
        self.baseline()
        self.connection.execute('CREATE TABLE NOTES (NOTE TEXT)')
        self.connection.execute("INSERT INTO NOTES VALUES ('keep')")
        with self.assertRaises(ValueError):
            reconcile_import(statement('2026-10-12T18:00:00', '500'), self.connection)
        self.assertTrue(self.connection.in_transaction)
        self.assertEqual(self.connection.execute('SELECT NOTE FROM NOTES').fetchone()[0], 'keep')

    def test_legacy_transactions_on_baseline_day_are_not_counted_again(self):
        self.connection.executescript('''CREATE TABLE TRANSACTIONS (
            ID INTEGER PRIMARY KEY NOT NULL, DATE TEXT NOT NULL,
            DATE_EPOCH INTEGER NOT NULL, LABEL TEXT, AMOUNT REAL, TAG TEXT);
            INSERT INTO TRANSACTIONS VALUES (1, '09/10/2026', 0, 'Fee', -10, NULL);''')
        self.baseline()
        self.assertEqual(reconcile_import(statement('2026-10-12T18:00:00', '1000'),
                                         self.connection), 'reconciled')

    def test_verified_import_generates_graphs(self):
        with tempfile.TemporaryDirectory() as directory:
            history = statement('2026-10-12T18:00:00', '1000',
                                operation('old', '2026-09-01T10:00:00', '50'),
                                operation('new', '2026-10-12T10:00:00', '-10'))
            with patch.object(app, 'get_graph_filename', side_effect=lambda name, account:
                              str(Path(directory) / (name + '.pdf'))), \
                    patch.object(app, 'get_account_name', return_value='Test'), \
                    patch.object(app, 'is_savings_account', return_value=True), \
                    patch.object(app.plt, 'show'), contextlib.redirect_stdout(io.StringIO()):
                app.prepare_and_analyse_history(history, self.connection, False, False)
            pdfs = list(Path(directory).glob('*.pdf'))
            self.assertEqual(len(pdfs), 3)
            self.assertTrue(all(path.stat().st_size > 0 for path in pdfs))


if __name__ == '__main__':
    unittest.main()
