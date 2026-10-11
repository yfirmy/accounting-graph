import configparser
import contextlib
import csv
import datetime
from decimal import Decimal
import importlib.util
import io
import json
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from balance_checks import reconcile_import
from database_schema import import_transaction
from legitimacy_checks import load_rules, refresh_alerts, export_alerts, UNEXPECTED_DIRECT_DEBIT


spec = importlib.util.spec_from_file_location(
    'accounts_analysis_alerts', Path(__file__).resolve().parents[1] / 'accounts-analysis.py')
app = importlib.util.module_from_spec(spec)
spec.loader.exec_module(app)


class LegitimacyTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name)
        self.connection = sqlite3.connect(':memory:')
        self.addCleanup(self.connection.close)
        with import_transaction(self.connection):
            pass

    def rules(self, options='', pattern='^Merchant$', account='XXXX3918'):
        path = self.path / 'rules.ini'
        path.write_text('[merchant]\naccount = {}\nlabel_regex = {}\n{}\n'.format(
            account, pattern, options), encoding='utf-8')
        return load_rules(path)

    def add(self, identifier, when, amount=-46, label='Merchant', legacy=False):
        date = datetime.datetime.fromisoformat(when)
        self.connection.execute('''INSERT INTO TRANSACTIONS
            (ID, DATE, DATE_EPOCH, LABEL, AMOUNT, POSTED_AT, AMOUNT_CENTS)
            VALUES (?, ?, 0, ?, ?, ?, ?)''',
            (identifier, date.strftime('%d/%m/%Y'), label, amount / 100,
             None if legacy else date.isoformat(timespec='microseconds'), amount))

    def check(self, rules, account='12343918'):
        return refresh_alerts(self.connection, account, rules)

    def test_fifth_purchase_flagged_even_when_each_amount_is_correct(self):
        for i in range(5):
            self.add(str(i), '2026-10-12T{:02d}:00:00'.format(10 + i))
        alerts = self.check(self.rules('amount = 0,46\nmax_per_day = 4'))
        self.assertEqual([a[0] for a in alerts], ['4'])
        self.assertIn('5 > 4', alerts[0][-1])

    def test_monthly_limit_counts_existing_rows_across_imports(self):
        self.add('first', '2026-10-01', -2899, legacy=True)
        self.add('second', '2026-10-20', -2899)
        self.add('next-month', '2026-11-01', -2899)
        alerts = self.check(self.rules('amount = 28.99\nmax_per_month = 1'))
        self.assertEqual([a[0] for a in alerts], ['second'])

    def test_unmatched_accounts_and_labels_ignored(self):
        self.add('ignored', '2026-10-12', -100, label='Unwatched')
        self.add('matched', '2026-10-12', -100)
        rules = self.rules('amount = 0.46')
        self.assertEqual(self.check(rules, account='12343919'), [])
        self.assertEqual([a[0] for a in self.check(rules)], ['matched'])

    def matched(self, identifier):
        return json.loads(self.connection.execute(
            'SELECT MATCHED_RULES FROM TRANSACTIONS WHERE ID = ?', (identifier,)).fetchone()[0])

    def test_all_matching_rule_names_are_stored_even_if_limits_fail(self):
        self.add('one', '2026-10-12', -100, 'PRELEVEMENT Telephone')
        specific = self.rules('amount = 0.46', pattern='Telephone')
        specific[0].name = 'Téléphone'
        general = self.rules('max_per_month = 1', pattern='^PRELEVEMENT')
        general[0].name = 'Prélèvements'
        alerts = self.check(specific + general)
        self.assertEqual(self.matched('one'), ['Téléphone', 'Prélèvements'])
        self.assertEqual([a[1] for a in alerts], ['Téléphone'])

    def test_unexpected_direct_debits_detect_all_three_prefixes(self):
        for i, prefix in enumerate(['PRELEVEMENT', 'PrÃ©lÃ¨vement', 'Prélèvement']):
            self.add(str(i), '2026-10-12', label=prefix + ' Inconnu')
        self.add('card', '2026-10-12', label='Paiement par carte Inconnu')
        self.add('middle', '2026-10-12', label='ANNULATION PRELEVEMENT Inconnu')
        alerts = self.check([])
        self.assertEqual({a[0] for a in alerts}, {'0', '1', '2'})
        self.assertTrue(all(a[1] == UNEXPECTED_DIRECT_DEBIT for a in alerts))
        self.assertTrue(all('Prélèvement inattendu' in a[-1] for a in alerts))
        self.assertEqual(self.matched('0'), [])

    def test_rule_for_other_account_does_not_cover_direct_debit(self):
        self.add('one', '2026-10-12', label='PRELEVEMENT Telephone')
        alerts = self.check(self.rules(pattern='Telephone', account='XXXX0407'))
        self.assertEqual([a[1] for a in alerts], [UNEXPECTED_DIRECT_DEBIT])
        self.assertEqual(self.matched('one'), [])

    def test_disabled_or_removed_rule_clears_classification_and_raises_alert(self):
        self.add('one', '2026-10-12', label='PRELEVEMENT Telephone')
        self.assertEqual(self.check(self.rules(pattern='Telephone')), [])
        self.assertEqual(self.matched('one'), ['merchant'])
        for rules in [self.rules('enabled = false', pattern='Telephone'), []]:
            alerts = self.check(rules)
            self.assertEqual(self.matched('one'), [])
            self.assertEqual([a[1] for a in alerts], [UNEXPECTED_DIRECT_DEBIT])
        self.assertEqual(self.connection.execute('SELECT COUNT(*) FROM TRANSACTION_ALERTS').fetchone()[0], 1)
        self.assertEqual(self.check(self.rules(pattern='Telephone')), [])

    def test_global_start_date_applies_even_without_rules_for_account(self):
        path = self.path / 'rules.ini'
        path.write_text('[DEFAULT]\nstart_date = 2026-07-01\n')
        self.add('old', '2026-06-30', label='PRELEVEMENT Ancien', legacy=True)
        self.add('current', '2026-07-01', label='PRELEVEMENT Inconnu')
        self.assertEqual([a[0] for a in self.check(load_rules(path))], ['current'])

    def test_without_global_start_coverage_starts_at_earliest_account_rule(self):
        self.add('old', '2026-06-30', label='PRELEVEMENT Ancien')
        self.add('current', '2026-07-01', label='PRELEVEMENT Inconnu')
        rules = self.rules('start_date = 2026-07-01', pattern='Telephone')
        self.assertEqual([a[0] for a in self.check(rules)], ['current'])

    def test_not_yet_active_rule_does_not_classify_current_direct_debit(self):
        path = self.path / 'rules.ini'
        path.write_text('[DEFAULT]\nstart_date = 2026-07-01\n'
                        '[telephone]\naccount = XXXX3918\nlabel_regex = Telephone\n'
                        'start_date = 2026-10-01\n')
        self.add('early', '2026-08-01', label='PRELEVEMENT Telephone')
        self.add('active', '2026-10-01', label='PRELEVEMENT Telephone')
        alerts = self.check(load_rules(path))
        self.assertEqual([a[0] for a in alerts], ['early'])
        self.assertEqual(self.matched('early'), [])
        self.assertEqual(self.matched('active'), ['telephone'])

    def test_unexpected_direct_debit_exported_and_printed(self):
        history = app.AccountStatement('12343918')
        history.balance_date = datetime.datetime(2026, 10, 12, 18)
        history.last_balance = Decimal('1000')
        history.add(app.Operation('new', datetime.datetime(2026, 10, 12),
                                  'PRELEVEMENT Inconnu', Decimal('-10')))
        output = io.StringIO()
        with patch.object(app, 'analyse_operations'), contextlib.redirect_stdout(output):
            alerts = app.prepare_and_analyse_history(history, self.connection, False, False, [])
        self.assertIn('\033[91m    Motifs : Prélèvement inattendu', output.getvalue())
        path = export_alerts([('Courant',) + row for row in alerts], self.path)
        self.assertIn('Prélèvement inattendu', path.read_text(encoding='utf-8-sig'))
        self.assertEqual(self.matched('new'), [])

    def test_inclusive_absolute_expense_range(self):
        for i, amount in enumerate([-5001, -5000, -501, -500, -499, 500]):
            self.add(str(i), '2026-10-12', amount)
        alerts = self.check(self.rules('min_amount = 5\nmax_amount = 50'))
        self.assertEqual([a[0] for a in alerts], ['0', '4', '5'])

    def test_zero_to_thirty_accepts_only_debits_within_bounds(self):
        for i, amount in enumerate([-3001, -3000, -2899, 0, 1, 2899]):
            self.add(str(i), '2026-10-12', amount)
        alerts = self.check(self.rules('min_amount = 0\nmax_amount = 30'))
        self.assertEqual([a[0] for a in alerts], ['0', '4', '5'])
        self.assertIn('maximum', alerts[0][-1])
        self.assertIn('Crédit positif', alerts[1][-1])

    def test_one_sided_absolute_bounds(self):
        for i, amount in enumerate([-1000, -500, -100, 0, 100]):
            self.add(str(i), '2026-10-12', amount)
        self.assertEqual([a[0] for a in self.check(self.rules('max_amount = 5'))], ['0', '4'])
        self.assertEqual([a[0] for a in self.check(self.rules('min_amount = 5'))], ['2', '3', '4'])

    def test_negative_configuration_amounts_are_rejected(self):
        for key in ['amount', 'min_amount', 'max_amount']:
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, 'nonnegative'):
                self.rules(key + ' = -1')

    def test_week_starts_monday_and_iso_year_boundary(self):
        self.add('sunday', '2025-12-28')
        self.add('monday', '2025-12-29')
        self.add('thursday', '2026-01-01')
        self.add('following-monday', '2026-01-05')
        alerts = self.check(self.rules('max_per_week = 1'))
        self.assertEqual([a[0] for a in alerts], ['thursday'])
        self.assertIn('2026-W01', alerts[0][-1])

    def test_year_resets_and_all_reasons_combined(self):
        self.add('old-year', '2025-12-31')
        self.add('first', '2026-01-01T10:00:00')
        self.add('second', '2026-01-01T11:00:00', -100)
        alerts = self.check(self.rules(
            'amount = 0.46\nmax_per_day = 1\nmax_per_week = 2\nmax_per_month = 1\nmax_per_year = 1'))
        self.assertEqual(len(alerts), 1)
        for reason in ['maximum', 'jour', 'semaine', 'mois', 'année']:
            self.assertIn(reason, alerts[0][-1])

    def test_wrong_amount_still_counts_towards_limit(self):
        self.add('wrong', '2026-10-12T10:00:00', -100)
        self.add('correct', '2026-10-12T11:00:00')
        alerts = self.check(self.rules('amount = 0.46\nmax_per_day = 1'))
        self.assertEqual([a[0] for a in alerts], ['wrong', 'correct'])

    def test_date_constraints_are_alerts_not_silent_filters(self):
        self.add('early', '2026-10-09')
        self.add('sunday', '2026-10-11')
        self.add('valid', '2026-10-12')
        self.add('late', '2026-10-20')
        alerts = self.check(self.rules('allowed_from = 2026-10-10\nallowed_until = 2026-10-15\n'
                                     'weekdays = 1,2,3,4,5\nmonth_days = 12,13,14,15'))
        self.assertEqual([a[0] for a in alerts], ['early', 'sunday', 'late'])

    def test_start_date_excludes_history_from_alerts_and_all_counters(self):
        self.add('old', '2026-10-12T23:59:59', -10000, legacy=True)
        self.add('first', '2026-10-13T00:00:00')
        self.add('second', '2026-10-13T12:00:00')
        alerts = self.check(self.rules('start_date = 2026-10-13\namount = 0.46\n'
            'max_per_day = 1\nmax_per_week = 1\nmax_per_month = 1\nmax_per_year = 1'))
        self.assertEqual([a[0] for a in alerts], ['second'])
        self.assertEqual(alerts[0][-1].count('2 > 1'), 4)
        self.assertNotIn('Montant', alerts[0][-1])

    def test_changing_start_date_clears_old_alerts_without_deleting_transactions(self):
        self.add('old', '2026-09-30', -100)
        self.add('boundary', '2026-10-01', -100)
        self.check(self.rules('amount = 0.46'))
        alerts = self.check(self.rules('amount = 0.46\nstart_date = 2026-10-01'))
        self.assertEqual([a[0] for a in alerts], ['boundary'])
        self.assertEqual(self.connection.execute('SELECT TRANSACTION_ID FROM TRANSACTION_ALERTS').fetchall(),
                         [('boundary',)])
        self.assertEqual(self.connection.execute('SELECT COUNT(*) FROM TRANSACTIONS').fetchone()[0], 2)

    def test_start_date_default_can_be_overridden_per_rule(self):
        path = self.path / 'rules.ini'
        path.write_text('[DEFAULT]\nstart_date = 2026-10-01\n'
                        '[current]\naccount = XXXX3918\nlabel_regex = Merchant\nmax_per_day = 0\n'
                        '[older]\naccount = XXXX3918\nlabel_regex = Merchant\nmax_per_day = 0\n'
                        'start_date = 2026-09-01\n')
        self.add('september', '2026-09-15')
        self.assertEqual([a[1] for a in self.check(load_rules(path))], ['older'])

    def test_recomputing_report_is_idempotent_and_clears_obsolete_alerts(self):
        self.add('one', '2026-10-12', -100)
        self.check(self.rules('amount = 0.46'))
        self.check(self.rules('amount = 0.46'))
        self.assertEqual(self.connection.execute('SELECT COUNT(*) FROM TRANSACTION_ALERTS').fetchone()[0], 1)
        self.check(self.rules('amount = 1'))
        self.assertEqual(self.connection.execute('SELECT COUNT(*) FROM TRANSACTION_ALERTS').fetchone()[0], 0)

    def test_overlapping_rules_have_separate_counts(self):
        self.add('one', '2026-10-12')
        first = self.rules('max_per_day = 0')
        second = self.rules('amount = 1')
        second[0].name = 'other'
        self.assertEqual(len(self.check(first + second)), 2)

    def test_invalid_rules_fail_early(self):
        for options in ['amount = no', 'min_amount = 10\nmax_amount = 1',
                        'amount = 1\nmax_amount = 0', 'max_per_day = -1',
                        'max_per_week = 1.5', 'weekdays = 8', 'month_days = 0',
                        'allowed_from = 2026-02-30', 'start_date = 2026-02-30', 'max_per_mont = 2',
                        'allowed_from = 2026-12-31\nallowed_until = 2026-01-01']:
            with self.subTest(options=options), self.assertRaises(ValueError):
                self.rules(options)
        with self.assertRaises(ValueError):
            self.rules(pattern='[')
        with self.assertRaises(ValueError):
            self.rules(account='.*')

    def test_disabled_rule_and_percent_literal_regex(self):
        self.assertEqual(self.rules('enabled = false'), [])
        self.add('percent', '2026-10-12', label='Merchant 100%')
        self.assertEqual(len(self.check(self.rules('max_per_day = 0', pattern='100%'))), 1)

    def test_regex_examples_match_both_label_encodings(self):
        rules = self.rules('amount = 28.99\nmax_per_month = 1',
                           pattern=r'(?:Prélèvement|PrÃ©lÃ¨vement) Bouygues Telecom', account='XXXXXXX3918')
        card = self.rules('amount = 0.46\nmax_per_day = 4',
                          pattern=r'NYX\*INNOVORDERSAS', account='XXXXXXX3918')
        card[0].name = 'card'
        rules += card
        self.add('one', '2026-10-01', -2899, 'Prélèvement Bouygues Telecom 06xxxxx144')
        self.add('two', '2026-10-02', -2899, 'PrÃ©lÃ¨vement Bouygues Telecom 0612345144')
        for i in range(5):
            self.add('card' + str(i), '2026-10-03T{:02d}:00:00'.format(i + 10),
                     label='Paiement par carte X5677 NYX*INNOVORDERSAS Pa')
        self.assertEqual({a[0] for a in self.check(rules, '12345673918')}, {'two', 'card4'})

    def test_csv_unicode_semicolons_decimal_and_empty_report(self):
        path = export_alerts([('Courant', 'id', 'rule', '2026-10-12',
                              'Prélèvement; "Test"', -2899, 'Montant incorrect')], self.path)
        with path.open(encoding='utf-8-sig', newline='') as stream:
            rows = list(csv.reader(stream, delimiter=';'))
        self.assertEqual(rows[1][4], 'Prélèvement; "Test"')
        self.assertEqual(rows[1][5], '-28,99')
        export_alerts([], self.path)
        with path.open(encoding='utf-8-sig', newline='') as stream:
            self.assertEqual(len(list(csv.reader(stream, delimiter=';'))), 1)

    def test_csv_does_not_turn_bank_labels_into_formulas(self):
        path = export_alerts([('Courant', 'id', 'r', '2026-10-12', '=1+1', -46, 'motif')], self.path)
        self.assertIn("'=1+1", path.read_text(encoding='utf-8-sig'))

    def test_failed_balance_does_not_run_legitimacy_or_clear_report(self):
        history = SimpleNamespace(balance_date=datetime.datetime(2026, 10, 12),
                                  last_balance=1000, operations={})
        reconcile_import(history, self.connection)
        callback = unittest.mock.Mock()
        history.balance_date = datetime.datetime(2026, 10, 13)
        history.last_balance = 900
        with self.assertRaises(ValueError):
            reconcile_import(history, self.connection, after_reconcile=callback)
        callback.assert_not_called()

    def test_dry_run_computes_alerts_but_restores_database(self):
        rules = self.rules('amount = 1')
        history = app.AccountStatement('12343918')
        history.balance_date = datetime.datetime(2026, 10, 12, 18)
        history.last_balance = Decimal('1000')
        history.add(app.Operation('new', datetime.datetime(2026, 10, 12), 'Merchant', Decimal('-0.46')))
        before = list(self.connection.iterdump())
        with contextlib.redirect_stdout(io.StringIO()):
            alerts = app.prepare_and_analyse_history(history, self.connection, True, False, rules)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(list(self.connection.iterdump()), before)

    def test_full_application_persists_and_exports_without_rejecting_suspicious_purchase(self):
        rules = self.path / 'rules.ini'
        self.rules('amount = 1')
        config = configparser.RawConfigParser()
        config.read_dict({'Files': {'legitimacy_rules': str(rules)},
                          'Directories': {'output': str(self.path)},
                          'Accounts': {'xxxx3918': 'Courant'},
                          'Savings accounts': {'xxxx3918': 'false'}})
        history = app.AccountStatement('12343918')
        history.balance_date = datetime.datetime(2026, 10, 12, 18)
        history.last_balance = Decimal('1000')
        history.add(app.Operation('new', datetime.datetime(2026, 10, 12), 'Merchant', Decimal('-0.46')))
        with patch.object(app, 'config', config), patch.object(app, 'analyse_operations'), \
                patch.object(app, 'open_database_connection', return_value=self.connection), \
                contextlib.redirect_stdout(io.StringIO()):
            app.process_statements([history], False, False, 'HERITAGE')
            app.process_statements([history], False, False, 'HERITAGE')
        self.assertEqual(self.connection.execute('SELECT COUNT(*) FROM TRANSACTION_ALERTS').fetchone()[0], 1)
        csv_text = (self.path / 'transaction_alerts.csv').read_text(encoding='utf-8-sig')
        self.assertIn('Merchant', csv_text)
        self.assertNotIn('12343918', csv_text)

    def test_export_combines_accounts_without_combining_their_counters(self):
        rules_path = self.path / 'rules.ini'
        self.rules('max_per_day = 1', account='XXXXXXXX')
        config = configparser.RawConfigParser()
        config.read_dict({'Files': {'legitimacy_rules': str(rules_path)},
                          'Directories': {'output': str(self.path)}})
        other = sqlite3.connect(':memory:')
        self.addCleanup(other.close)
        databases = {'12343918': self.connection, '12340407': other}
        histories = []
        for account in databases:
            history = app.AccountStatement(account)
            history.balance_date = datetime.datetime(2026, 10, 12, 18)
            history.last_balance = Decimal('1000')
            for i in range(2):
                history.add(app.Operation(str(i), datetime.datetime(2026, 10, 12, 10 + i),
                                          'Merchant', Decimal('-0.46')))
            histories.append(history)
        with patch.object(app, 'config', config), patch.object(app, 'analyse_operations'), \
                patch.object(app, 'open_database_connection', side_effect=lambda account: databases[account]), \
                patch.object(app, 'is_savings_account', return_value=False), \
                patch.object(app, 'get_account_name', side_effect=lambda account: account[-4:]), \
                contextlib.redirect_stdout(io.StringIO()):
            app.process_statements(histories + [histories[0]], False, False, 'HERITAGE')
        with (self.path / 'transaction_alerts.csv').open(encoding='utf-8-sig', newline='') as stream:
            rows = list(csv.reader(stream, delimiter=';'))
        self.assertEqual(len(rows), 3)
        self.assertEqual({row[1] for row in rows[1:]}, {'1'})

    def test_invalid_configuration_prevents_opening_accounts(self):
        self.rules('amount = 1')
        (self.path / 'rules.ini').write_text('[bad]\naccount = XXXX3918\nlabel_regex = [\n')
        config = configparser.RawConfigParser()
        config.read_dict({'Files': {'legitimacy_rules': str(self.path / 'rules.ini')}})
        with patch.object(app, 'config', config), patch.object(app, 'open_database_connection') as open_db:
            with self.assertRaises(ValueError):
                app.process_statements([], False, False, 'HERITAGE')
            open_db.assert_not_called()

    def test_failed_csv_replace_preserves_previous_report(self):
        path = self.path / 'transaction_alerts.csv'
        path.write_text('previous report')
        with patch('legitimacy_checks.os.replace', side_effect=OSError('Disk error')):
            with self.assertRaises(OSError):
                export_alerts([], self.path)
        self.assertEqual(path.read_text(), 'previous report')
        self.assertEqual(list(self.path.iterdir()), [path])


if __name__ == '__main__':
    unittest.main()
