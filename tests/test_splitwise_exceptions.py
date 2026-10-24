import contextlib
import io
import json
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from database_schema import import_transaction
from splitwise_exceptions import PersonalException, load_exceptions
from splitwise_sync import Settings, sync_pending, sync_pending_many


class PersonalExceptionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.database_number = 123450
        self.connection = self.database()
        self.settings = Settings('SPLITWISE_API_KEY', [SimpleNamespace(name='carrefour')],
            [PersonalException('courses', 'carrefour', '2026-10-24', 1000, 'XXXXXX')])
        self.client = Mock()
        self.client.expenses.return_value = []
        self.client.create.return_value = '789'
        self.prompt = Mock(return_value='later')
        self.output = io.StringIO()
        redirect = contextlib.redirect_stdout(self.output)
        redirect.__enter__()
        self.addCleanup(redirect.__exit__, None, None, None)

    def database(self, filename=None):
        if filename is None:
            self.database_number += 1
            filename = self.root / ('account_' + str(self.database_number) + '.db')
        connection = sqlite3.connect(str(filename))
        self.addCleanup(connection.close)
        with import_transaction(connection):
            pass
        return connection

    def queue(self, identifier='one', amount=6000, connection=None, day='2026-10-24', state='pending'):
        connection = connection or self.connection
        payload = dict(cost='{:.2f}'.format(amount / 100), date=day + 'T12:00:00Z',
                       description='Courses', currency_code='EUR', group_id=42, split_equally=True)
        with connection:
            connection.execute('''INSERT INTO TRANSACTIONS
                (ID, DATE, DATE_EPOCH, LABEL, AMOUNT, AMOUNT_CENTS) VALUES (?, ?, 0, 'Courses', ?, ?)''',
                (identifier, '24/10/2026', -amount / 100, -amount))
            connection.execute('''INSERT INTO SPLITWISE_EXPORTS
                (TRANSACTION_ID, RULE_ID, CATEGORY, STATE, MARKER, PAYLOAD) VALUES (?, 'carrefour', 'auto', ?, ?, ?)''',
                (identifier, state, 'marker-' + identifier, json.dumps(payload, ensure_ascii=False)))

    def sync(self):
        sync_pending(self.connection, self.settings, client=self.client, prompt=self.prompt)

    def test_sixty_minus_ten_sends_fifty_without_changing_bank_or_reapplying(self):
        self.queue()
        before = self.connection.execute('SELECT * FROM TRANSACTIONS').fetchall()
        self.sync()
        self.sync()
        self.client.create.assert_called_once()
        self.assertEqual(self.client.create.call_args.args[0]['cost'], '50.00')
        self.assertEqual(self.connection.execute('SELECT * FROM TRANSACTIONS').fetchall(), before)
        self.assertEqual(self.connection.execute('SELECT ORIGINAL_CENTS, PERSONAL_CENTS FROM SPLITWISE_ADJUSTMENTS').fetchone(), (6000, 1000))
        self.prompt.assert_not_called()

    def test_duplicate_lookup_and_confirmation_use_adjusted_amount(self):
        self.queue()
        self.client.expenses.return_value = [dict(id=8, cost='50', date='2026-10-24T12:00:00Z',
                                                currency_code='EUR', description='Courses manuelles')]
        self.prompt.return_value = '1'
        self.sync()
        self.client.create.assert_not_called()
        self.assertIn('50.00 EUR', self.prompt.call_args.args[0])
        self.assertEqual(self.connection.execute('SELECT STATE, EXPENSE_ID FROM SPLITWISE_EXPORTS').fetchone(), ('linked', '8'))

    def test_full_personal_amount_excludes_expense(self):
        self.queue(amount=1000)
        self.sync()
        self.assertEqual(self.connection.execute('SELECT STATE FROM SPLITWISE_EXPORTS').fetchone()[0], 'skipped')
        self.client.create.assert_not_called()
        self.client.expenses.assert_not_called()

    def test_excess_personal_amount_blocks_send_and_can_be_corrected(self):
        self.queue(amount=900)
        self.sync()
        self.client.create.assert_not_called()
        self.assertIn('supérieure au total', self.output.getvalue())
        self.assertEqual(self.connection.execute('SELECT COUNT(*) FROM SPLITWISE_ADJUSTMENTS').fetchone()[0], 0)
        self.settings.exceptions = [PersonalException('courses', 'carrefour', '2026-10-24', 100, 'XXXXXX')]
        self.sync()
        self.assertEqual(self.client.create.call_args.args[0]['cost'], '8.00')

    def test_no_match_is_reported(self):
        self.sync()
        self.assertIn('aucune dépense', self.output.getvalue())

    def test_ambiguous_match_blocks_all_candidates_until_choice(self):
        self.queue('one')
        self.queue('two', 7000)
        self.sync()
        self.client.create.assert_not_called()
        self.prompt.return_value = '2'
        self.sync()
        self.assertEqual([c.args[0]['cost'] for c in self.client.create.call_args_list], ['60.00', '60.00'])
        self.assertEqual(self.connection.execute('SELECT TRANSACTION_ID FROM SPLITWISE_ADJUSTMENTS').fetchone()[0], 'two')

    def test_later_transaction_on_same_day_never_consumes_exception_again(self):
        self.queue()
        self.sync()
        self.queue('later', 7000)
        self.sync()
        self.assertEqual(self.client.create.call_args.args[0]['cost'], '70.00')

    def test_changed_or_renamed_applied_exception_stops_new_sends(self):
        self.queue()
        self.sync()
        for item in [PersonalException('courses', 'carrefour', '2026-10-24', 2000, 'XXXXXX'),
                     PersonalException('renamed', 'carrefour', '2026-10-24', 1000, 'XXXXXX')]:
            self.settings.exceptions = [item]
            with self.assertRaisesRegex(ValueError, 'déjà appliquée'):
                self.sync()
        self.assertEqual(self.client.create.call_count, 1)

    def test_already_sent_or_uncertain_expense_is_not_modified(self):
        for state in ('submitted', 'linked', 'sending', 'uncertain'):
            with self.subTest(state=state):
                with self.connection:
                    self.connection.execute('DELETE FROM SPLITWISE_EXPORTS')
                    self.connection.execute('DELETE FROM TRANSACTIONS')
                self.queue(state=state)
                before = list(self.connection.iterdump())
                self.sync()
                self.assertEqual(list(self.connection.iterdump()), before)
        self.client.create.assert_not_called()

    def test_missing_credentials_preserves_deduction_for_later(self):
        self.queue()
        with patch.dict('os.environ', {}, clear=True):
            sync_pending(self.connection, self.settings, prompt=self.prompt)
        self.sync()
        self.assertEqual(self.client.create.call_args.args[0]['cost'], '50.00')

    def test_adjustment_and_payload_update_are_atomic(self):
        self.queue()
        self.connection.execute('''CREATE TRIGGER reject_adjustment BEFORE INSERT ON SPLITWISE_ADJUSTMENTS
            BEGIN SELECT RAISE(ABORT, 'test failure'); END''')
        before = list(self.connection.iterdump())
        with self.assertRaises(sqlite3.IntegrityError):
            self.sync()
        self.assertEqual(list(self.connection.iterdump()), before)
        self.client.create.assert_not_called()

    def test_multiple_accounts_are_disambiguated_before_any_send(self):
        other = self.database()
        self.queue()
        self.queue('other', connection=other)
        sync_pending_many([self.connection, other], self.settings, client=self.client, prompt=self.prompt)
        self.client.create.assert_not_called()
        self.prompt.return_value = '2'
        sync_pending_many([self.connection, other], self.settings, client=self.client, prompt=self.prompt)
        self.assertEqual([c.args[0]['cost'] for c in self.client.create.call_args_list], ['60.00', '50.00'])

    def test_application_in_an_account_not_imported_today_is_still_detected(self):
        previous = self.database(self.root / 'account_123111.db')
        current = self.database(self.root / 'account_123222.db')
        self.queue('previous', connection=previous)
        sync_pending(previous, self.settings, client=self.client, prompt=self.prompt)
        self.queue('today', connection=current)
        sync_pending(current, self.settings, client=self.client, prompt=self.prompt)
        self.assertEqual([c.args[0]['cost'] for c in self.client.create.call_args_list], ['50.00', '60.00'])
        self.assertEqual(current.execute('SELECT COUNT(*) FROM SPLITWISE_ADJUSTMENTS').fetchone()[0], 0)

    def test_unimported_account_candidate_blocks_automatic_selection(self):
        other = self.database(self.root / 'account_123111.db')
        current = self.database(self.root / 'account_123222.db')
        self.queue('other', connection=other)
        self.queue('current', connection=current)
        sync_pending(current, self.settings, client=self.client, prompt=self.prompt)
        self.client.create.assert_not_called()
        self.prompt.assert_called_once()

    def test_config_has_only_rule_date_and_personal_amount(self):
        path = self.root / 'exceptions.ini'
        path.write_text('[courses]\naccount=XXXXXX\nrule=carrefour\npurchase_date=2026-10-24\npersonal_amount=10,50\n')
        entries = load_exceptions(path, self.settings.rules)
        self.assertEqual(entries, [PersonalException('courses', 'carrefour', '2026-10-24', 1050, 'XXXXXX')])
        for old, new in [('carrefour', 'unknown'), ('10,50', '-1'), ('10,50', '0'),
                         ('2026-10-24', '24/10/2026'), ('10,50', '1.001')]:
            path.write_text('[courses]\naccount=XXXXXX\nrule=carrefour\npurchase_date=2026-10-24\npersonal_amount=10,50\n'.replace(old, new))
            with self.assertRaises(ValueError):
                load_exceptions(path, self.settings.rules)

    def test_duplicate_rule_date_exceptions_are_rejected(self):
        path = self.root / 'exceptions.ini'
        fields = '\naccount=XXXXXX\nrule=carrefour\npurchase_date=2026-10-24\npersonal_amount=10\n'
        path.write_text('[one]' + fields + '[two]' + fields)
        with self.assertRaisesRegex(ValueError, 'une seule exception'):
            load_exceptions(path, self.settings.rules)

    def test_account_mask_limits_candidates_to_requested_account(self):
        target = self.database(self.root / 'account_123111.db')
        other = self.database(self.root / 'account_123222.db')
        self.queue('target', connection=target)
        self.queue('other', connection=other)
        self.settings.exceptions = [PersonalException('courses', 'carrefour', '2026-10-24', 1000, 'XXX111')]
        sync_pending_many([target, other], self.settings, client=self.client, prompt=self.prompt)
        self.prompt.assert_not_called()
        self.assertEqual([c.args[0]['cost'] for c in self.client.create.call_args_list], ['50.00', '60.00'])
        self.assertEqual(other.execute('SELECT COUNT(*) FROM SPLITWISE_ADJUSTMENTS').fetchone()[0], 0)

    def test_unrelated_database_is_not_opened_for_lookup(self):
        target = self.database(self.root / 'account_123111.db')
        self.queue('target', connection=target)
        self.settings.exceptions = [PersonalException('courses', 'carrefour', '2026-10-24', 1000, 'XXX111')]
        # Opening this unrelated file and querying its schema would fail.
        (self.root / 'account_999999.db').write_text('not a SQLite database')
        sync_pending(target, self.settings, client=self.client, prompt=self.prompt)
        self.assertEqual(self.client.create.call_args.args[0]['cost'], '50.00')

    def test_masks_are_required_valid_and_compared_for_overlap(self):
        path = self.root / 'exceptions.ini'
        fields = '\nrule=carrefour\npurchase_date=2026-10-24\npersonal_amount=10\n'
        for account in ('', 'account=\n', 'account=.*\n', 'account=123Y\n'):
            path.write_text('[one]\n' + account + fields)
            with self.assertRaises(ValueError):
                load_exceptions(path, self.settings.rules)
        path.write_text('[one]\naccount=xxx111\n' + fields + '[two]\naccount=XXX222\n' + fields)
        entries = load_exceptions(path, self.settings.rules)
        self.assertEqual([entry.account for entry in entries], ['XXX111', 'XXX222'])
        path.write_text('[one]\naccount=XXX111\n' + fields + '[two]\naccount=123XXX\n' + fields)
        with self.assertRaisesRegex(ValueError, 'une seule exception'):
            load_exceptions(path, self.settings.rules)


if __name__ == '__main__':
    unittest.main()
