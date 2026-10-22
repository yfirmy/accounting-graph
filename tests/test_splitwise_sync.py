import contextlib
import datetime as dt
from decimal import Decimal
import importlib.util
import io
import json
from pathlib import Path
import sqlite3
import ssl
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError, URLError

from balance_checks import reconcile_import
from database_schema import import_transaction
from splitwise_sync import APIError, Client, IMPORT_NOTE, ask, classify, enqueue, load_settings, sync_pending

spec = importlib.util.spec_from_file_location('splitwise_app', Path(__file__).resolve().parents[1] / 'accounts-analysis.py')
app = importlib.util.module_from_spec(spec)
spec.loader.exec_module(app)

CONFIG = '''[Splitwise]
enabled = true
group_id = 42
[bakery]
account = XXX456
label_regex = (?i)boulangerie
weekdays = 7
min_amount = 4
max_amount = 14
description = Boulangerie
'''


class SplitwiseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.filename = Path(self.temp.name) / 'rules.ini'
        self.filename.write_text(CONFIG)
        self.settings = load_settings(self.filename)
        self.connection = sqlite3.connect(':memory:')
        self.addCleanup(self.connection.close)
        with import_transaction(self.connection):
            pass
        self.client = Mock()
        self.client.expenses.return_value = []
        self.client.create.return_value = '987'
        self.prompt = Mock(return_value='later')
        self.output = contextlib.redirect_stdout(io.StringIO())
        self.output.__enter__()
        self.addCleanup(self.output.__exit__, None, None, None)

    def operation(self, amount='-10', day='2026-10-18', identifier='bank-private-id'):
        return app.Operation(identifier, dt.datetime.fromisoformat(day + 'T09:30:00'),
                             'Boulangerie compte 123456 carte 9999', Decimal(amount))

    def queue(self, amount='-10', day='2026-10-18', identifier='bank-private-id'):
        with self.connection:
            enqueue(self.connection, self.settings, '123456', self.operation(amount, day, identifier))

    def state(self):
        return self.connection.execute('SELECT STATE, EXPENSE_ID FROM SPLITWISE_EXPORTS').fetchone()

    def run_sync(self):
        sync_pending(self.connection, self.settings, client=self.client, prompt=self.prompt)

    def remote(self, **changes):
        result = dict(id=20, cost='10.00', date='2026-10-18T08:00:00Z', currency_code='EUR',
                      description='Saisie iPhone', group_id=123, details=None, deleted_at=None, payment=False)
        result.update(changes)
        return result

    def test_classification_amount_boundaries_weekday_and_account(self):
        for amount, expected in [('-4', 'auto'), ('-14', 'auto'), ('-22', 'confirm'),
                                 ('-3.99', 'confirm'), ('0', 'skip'), ('10', 'skip')]:
            with self.subTest(amount=amount):
                self.assertEqual(classify(self.settings, '123456', self.operation(amount))[0], expected)
        self.assertEqual(classify(self.settings, '123456', self.operation(day='2026-10-19'))[0], 'skip')
        self.assertEqual(classify(self.settings, '654321', self.operation())[0], 'skip')

    def test_first_matching_rule_wins_and_start_date_applies(self):
        self.filename.write_text(CONFIG.replace('min_amount = 4', 'start_date = 2026-10-25\nmin_amount = 4'))
        self.assertEqual(classify(load_settings(self.filename), '123456', self.operation())[0], 'skip')
        self.filename.write_text(CONFIG.replace('min_amount = 4', 'action = skip\nmin_amount = 4') +
                                 '\n[second]\naccount=XXX456\nlabel_regex=boulangerie\naction=confirm\ndescription=Autre\n')
        self.assertEqual(classify(load_settings(self.filename), '123456', self.operation())[0], 'skip')

    def test_invalid_configuration_fails_before_import(self):
        for old, new in [('weekdays = 7', 'weekdays = 8'), ('min_amount = 4', 'min_amount = -1'),
                         ('max_amount = 14', 'max_amount = 2'), ('max_amount = 14', ''),
                         ('group_id = 42', 'group_id = 0'), ('label_regex = (?i)boulangerie', 'label_regex = ['),
                         ('description = Boulangerie', 'description ='), ('weekdays = 7', 'week_day = 7')]:
            with self.subTest(new=new):
                self.filename.write_text(CONFIG.replace(old, new))
                with self.assertRaises(ValueError):
                    load_settings(self.filename)

    def test_missing_or_disabled_config_is_opt_in(self):
        self.assertIsNone(load_settings(None))
        self.filename.write_text('[Splitwise]\nenabled=false\n')
        self.assertIsNone(load_settings(self.filename))

    def test_automatic_delivery_uses_neutral_payload_and_is_not_repeated(self):
        self.queue()
        self.run_sync()
        self.run_sync()
        self.assertEqual(self.state(), ('submitted', '987'))
        self.client.create.assert_called_once()
        self.prompt.assert_not_called()
        payload = self.client.create.call_args[0][0]
        self.assertEqual(payload['cost'], '10.00')
        self.assertEqual(payload['description'], 'Boulangerie')
        self.assertIn(IMPORT_NOTE, payload['details'])
        self.assertEqual(payload['group_id'], 42)
        self.assertIs(payload['split_equally'], True)
        self.assertNotIn('123456', json.dumps(payload))
        self.assertNotIn('bank-private-id', json.dumps(payload))
        self.assertNotIn('9999', json.dumps(payload))

    def test_account_number_in_description_is_rejected(self):
        self.settings.rules[0].description = 'Compte 123 456'
        with self.assertRaises(ValueError):
            self.queue()

    def test_configured_category_is_sent_as_integer(self):
        self.filename.write_text(CONFIG + 'category_id = 15\n')
        self.settings = load_settings(self.filename)
        self.queue()
        self.run_sync()
        self.assertEqual(self.client.create.call_args[0][0]['category_id'], 15)

    def test_absent_category_is_left_to_splitwise(self):
        self.queue()
        self.run_sync()
        self.assertNotIn('category_id', self.client.create.call_args[0][0])

    def test_invalid_category_is_rejected(self):
        for value in ('0', '-1', 'food', '1.5', ''):
            with self.subTest(value=value):
                self.filename.write_text(CONFIG + 'category_id = ' + value + '\n')
                with self.assertRaisesRegex(ValueError, 'Règle Splitwise'):
                    load_settings(self.filename)

    def test_statement_date_survives_import_and_retry_on_later_days(self):
        # Import a Sunday expense in a statement ending the following Thursday.
        history = self.history('1000', self.operation(day='2026-10-18'))
        history.balance_date = dt.datetime(2026, 10, 22, 18)
        self.import_history(history)
        self.client.expenses.side_effect = APIError('Offline')
        self.run_sync()
        self.client.expenses.side_effect = None
        self.client.expenses.return_value = []
        self.run_sync()
        self.assertEqual(self.client.create.call_args[0][0]['date'], '2026-10-18T12:00:00Z')
        self.client.expenses.assert_called_with('2026-10-18')

    def test_amount_confirmation_and_explicit_decline(self):
        self.queue('-22')
        self.run_sync()
        self.assertEqual(self.state(), ('pending', None))
        self.client.create.assert_not_called()
        self.prompt.return_value = 'skip'
        self.run_sync()
        self.assertEqual(self.state(), ('skipped', None))

    def test_confirmation_can_create(self):
        self.queue('-22')
        self.prompt.return_value = 'create'
        self.run_sync()
        self.assertEqual(self.state(), ('submitted', '987'))

    def test_manual_duplicate_from_another_group_requires_confirmation_and_can_link(self):
        self.queue()
        self.client.expenses.return_value = [self.remote()]
        self.run_sync()
        self.client.create.assert_not_called()
        self.assertEqual(self.state(), ('pending', None))
        self.prompt.return_value = '1'
        self.run_sync()
        self.assertEqual(self.state(), ('linked', '20'))
        self.client.create.assert_not_called()

    def test_duplicate_can_be_explicitly_confirmed_as_distinct(self):
        self.queue()
        self.client.expenses.return_value = [self.remote()]
        self.prompt.return_value = 'create'
        self.run_sync()
        self.assertEqual(self.state(), ('submitted', '987'))
        self.prompt.assert_called_once()

    def test_different_day_currency_deleted_or_payment_are_not_duplicates(self):
        self.queue()
        self.client.expenses.return_value = [self.remote(date='2026-10-17T08:00:00Z'),
            self.remote(currency_code='USD'), self.remote(deleted_at='2026-10-18'), self.remote(payment=True)]
        self.run_sync()
        self.prompt.assert_not_called()
        self.client.create.assert_called_once()

    def test_network_failure_before_create_does_not_send(self):
        self.queue()
        self.client.expenses.side_effect = APIError('Offline')
        self.run_sync()
        self.assertEqual(self.state(), ('pending', None))
        self.client.create.assert_not_called()

    def test_timeout_after_create_recovers_marker_without_resubmission(self):
        self.queue()
        self.client.create.side_effect = APIError('Timeout')
        self.run_sync()
        self.assertEqual(self.state(), ('uncertain', None))
        marker = self.connection.execute('SELECT MARKER FROM SPLITWISE_EXPORTS').fetchone()[0]
        self.client.expenses.return_value = [self.remote(id=987, details=marker)]
        self.run_sync()
        self.assertEqual(self.state(), ('submitted', '987'))
        self.assertEqual(self.client.create.call_count, 1)

    def test_uncertain_without_remote_marker_requires_confirmation(self):
        self.queue()
        self.connection.execute("UPDATE SPLITWISE_EXPORTS SET STATE='sending'")
        self.connection.commit()
        self.run_sync()
        self.prompt.assert_called_once()
        self.client.create.assert_not_called()

    def test_recovery_recognizes_marker_inside_import_note(self):
        self.queue()
        payload = json.loads(self.connection.execute('SELECT PAYLOAD FROM SPLITWISE_EXPORTS').fetchone()[0])
        self.client.expenses.return_value = [self.remote(id=987, details=payload['details'])]
        self.run_sync()
        self.assertEqual(self.state(), ('submitted', '987'))
        self.client.create.assert_not_called()

    def test_previously_queued_expense_gets_note_on_delivery(self):
        self.queue()
        marker, serialized = self.connection.execute('SELECT MARKER, PAYLOAD FROM SPLITWISE_EXPORTS').fetchone()
        payload = json.loads(serialized)
        payload['details'] = marker
        with self.connection:
            self.connection.execute('UPDATE SPLITWISE_EXPORTS SET PAYLOAD = ?', (json.dumps(payload),))
        self.run_sync()
        self.assertEqual(self.client.create.call_args[0][0]['details'], IMPORT_NOTE + '\n\n' + marker)

    def test_missing_credentials_keeps_pending(self):
        self.queue()
        with patch.dict('os.environ', {}, clear=True), patch('splitwise_sync.Client') as client:
            sync_pending(self.connection, self.settings)
        client.assert_not_called()
        self.assertEqual(self.state(), ('pending', None))

    def test_noninteractive_and_eof_never_authorize(self):
        with patch('sys.stdin.isatty', return_value=False), patch('builtins.input') as read:
            self.assertEqual(ask('Send?', {'create'}), 'later')
            read.assert_not_called()
        with patch('sys.stdin.isatty', return_value=True), patch('builtins.input', side_effect=EOFError):
            self.assertEqual(ask('Send?', {'create'}), 'later')

    def history(self, balance='1000', *ops):
        history = app.AccountStatement('123456')
        history.balance_date = dt.datetime(2026, 10, 18, 18)
        history.last_balance = Decimal(balance)
        for operation in ops:
            history.add(operation)
        return history

    def import_history(self, history, **kwargs):
        return reconcile_import(history, self.connection,
            on_new_transaction=lambda conn, op: enqueue(conn, self.settings, '123456', op), **kwargs)

    def test_reimport_only_queues_new_transactions_and_dry_run_rolls_back(self):
        history = self.history('1000', self.operation())
        before = list(self.connection.iterdump())
        self.import_history(history, dry_run=True)
        self.assertEqual(list(self.connection.iterdump()), before)
        self.import_history(history)
        self.import_history(history)
        self.assertEqual(self.connection.execute('SELECT COUNT(*) FROM SPLITWISE_EXPORTS').fetchone()[0], 1)

    def test_transactions_imported_before_feature_are_never_backfilled(self):
        history = self.history('1000', self.operation())
        reconcile_import(history, self.connection)
        self.import_history(history)
        self.assertEqual(self.connection.execute('SELECT COUNT(*) FROM SPLITWISE_EXPORTS').fetchone()[0], 0)

    def test_failed_balance_or_alert_check_rolls_back_queue(self):
        reconcile_import(self.history(), self.connection)
        before = list(self.connection.iterdump())
        with self.assertRaises(ValueError):
            self.import_history(self.history('500', self.operation()))
        self.assertEqual(list(self.connection.iterdump()), before)
        with self.assertRaises(ValueError):
            self.import_history(self.history('1000', self.operation()),
                after_reconcile=Mock(side_effect=ValueError('Alert failure')))
        self.assertEqual(list(self.connection.iterdump()), before)

    def test_application_syncs_after_commit_and_never_in_dry_run(self):
        history = self.history('1000', self.operation())
        def verify_committed(connection, settings):
            self.assertFalse(connection.in_transaction)
            self.assertEqual(connection.execute('SELECT COUNT(*) FROM SPLITWISE_EXPORTS').fetchone()[0], 1)
        with patch.object(app, 'draw_stored_history'), patch.object(app, 'sync_pending', side_effect=verify_committed) as sync:
            app.prepare_and_analyse_history(history, self.connection, True, False, splitwise=self.settings)
            sync.assert_not_called()
            app.prepare_and_analyse_history(history, self.connection, False, False, splitwise=self.settings)
            sync.assert_called_once()

    def test_sync_refuses_uncommitted_transactions(self):
        self.queue()
        self.connection.execute('BEGIN')
        with self.assertRaises(ValueError):
            self.run_sync()
        self.client.expenses.assert_not_called()

    def test_application_rejects_non_eur_before_import_or_send(self):
        history = self.history('1000', self.operation())
        history.currency = 'USD'
        before = list(self.connection.iterdump())
        with patch.object(app, 'sync_pending') as sync, self.assertRaisesRegex(ValueError, 'EUR'):
            app.prepare_and_analyse_history(history, self.connection, False, False, splitwise=self.settings)
        sync.assert_not_called()
        self.assertEqual(list(self.connection.iterdump()), before)

    def test_sending_intent_is_committed_before_network_request(self):
        self.queue()
        def create(payload):
            self.assertEqual(self.state(), ('sending', None))
            self.assertFalse(self.connection.in_transaction)
            return '987'
        self.client.create.side_effect = create
        self.run_sync()
        self.assertEqual(self.state(), ('submitted', '987'))

    def test_missing_config_section_has_clear_error(self):
        self.filename.write_text('[bakery]\naction=auto\n')
        with self.assertRaisesRegex(ValueError, 'Section'):
            load_settings(self.filename)


class APITests(unittest.TestCase):
    def test_get_expenses_request_matches_openapi_contract(self):
        from urllib.parse import urlsplit, parse_qs
        with patch('splitwise_sync.request.urlopen', return_value=io.BytesIO(b'{"expenses":[]}')) as call:
            Client('test-token').expenses('2026-10-18')
        req = call.call_args.args[0]
        url = urlsplit(req.full_url)
        self.assertEqual(url.scheme, 'https')
        self.assertEqual(url.netloc, 'secure.splitwise.com')
        self.assertEqual(url.path, '/api/v3.0/get_expenses')
        self.assertEqual(req.get_method(), 'GET')
        self.assertIsNone(req.data)
        self.assertEqual(req.get_header('Authorization'), 'Bearer test-token')
        self.assertEqual(req.get_header('User-agent'), 'accounting-graph/1.0')
        self.assertEqual(req.get_header('Accept'), 'application/json')
        self.assertIsNone(req.get_header('Content-type'))
        self.assertEqual(parse_qs(url.query), {
            'dated_after': ['2026-10-17T00:00:00Z'],
            'dated_before': ['2026-10-20T00:00:00Z'], 'limit': ['100'], 'offset': ['0']})

    def test_expense_pagination_covers_all_accessible_groups(self):
        client = Client('secret')
        pages = [{'expenses': [dict(id=i) for i in range(1, 101)]},
                 {'expenses': [dict(id=101)]}, {'expenses': []}]
        with patch.object(client, '_request', side_effect=pages) as call:
            self.assertEqual(len(client.expenses('2026-10-18')), 101)
            self.assertEqual(call.call_count, 3)
            self.assertNotIn('group_id', call.call_args.kwargs['params'])
            self.assertEqual(call.call_args.kwargs['params']['offset'], 101)

    def test_bad_pagination_never_silently_ignores_duplicates(self):
        client = Client('secret')
        with patch.object(client, '_request', return_value={'expenses': [dict(id=1)] * 100}):
            with self.assertRaises(APIError):
                client.expenses('2026-10-18')

    def test_http_200_with_errors_is_not_success(self):
        response = io.BytesIO(b'{"errors":{"base":["rejected"]}}')
        response.status = 200
        with patch('splitwise_sync.request.urlopen', return_value=response):
            with self.assertRaises(APIError) as raised:
                Client('secret').create({'description': 'Neutral'})
        self.assertIn('200', str(raised.exception))
        self.assertIn('rejected', str(raised.exception))

    def test_create_uses_bearer_and_validates_id(self):
        with patch('splitwise_sync.request.urlopen', return_value=io.BytesIO(b'{"expenses":[{"id":17}],"errors":{}}')) as call:
            self.assertEqual(Client('secret').create({'description': 'Neutral'}), '17')
            req = call.call_args[0][0]
            self.assertEqual(req.get_header('Authorization'), 'Bearer secret')
            self.assertEqual(req.get_method(), 'POST')
            self.assertEqual(req.get_header('Content-type'), 'application/json')
            self.assertEqual(req.get_header('Accept'), 'application/json')
            self.assertEqual(req.get_header('User-agent'), 'accounting-graph/1.0')
            context = call.call_args.kwargs['context']
            self.assertTrue(context.check_hostname)
            self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)
            self.assertGreater(context.cert_store_stats()['x509_ca'], 0)
        with patch('splitwise_sync.request.urlopen', return_value=io.BytesIO(b'{"expenses":[]}')):
            with self.assertRaises(APIError):
                Client('secret').create({})

    def test_http_errors_show_status_and_body_but_mask_key(self):
        body = io.BytesIO(b'{"error":"Invalid token private-token"}')
        with patch('splitwise_sync.request.urlopen', side_effect=HTTPError('url', 401, 'Unauthorized', {}, body)):
            with self.assertRaises(APIError) as raised:
                Client('private-token').expenses('2026-10-18')
        self.assertIn('get_expenses', str(raised.exception))
        self.assertIn('401', str(raised.exception))
        self.assertIn('Unauthorized', str(raised.exception))
        self.assertIn('Invalid token', str(raised.exception))
        self.assertIn('[CLÉ API MASQUÉE]', str(raised.exception))
        self.assertNotIn('private-token', str(raised.exception))

    def test_network_error_shows_original_cause_without_key(self):
        with patch('splitwise_sync.request.urlopen', side_effect=URLError('certificate verify failed private-token')):
            with self.assertRaises(APIError) as raised:
                Client('private-token').create({})
        self.assertIn('create_expense', str(raised.exception))
        self.assertIn('URLError', str(raised.exception))
        self.assertIn('certificate verify failed', str(raised.exception))
        self.assertNotIn('private-token', str(raised.exception))

    def test_non_json_response_shows_status_and_body(self):
        response = io.BytesIO(b'<html>Service unavailable</html>')
        response.status = 200
        with patch('splitwise_sync.request.urlopen', return_value=response):
            with self.assertRaises(APIError) as raised:
                Client('secret').create({})
        self.assertIn('200', str(raised.exception))
        self.assertIn('JSONDecodeError', str(raised.exception))
        self.assertIn('<html>Service unavailable</html>', str(raised.exception))

    def test_missing_expense_id_displays_response_body(self):
        response = io.BytesIO(b'{"expenses":[],"errors":{}}')
        response.status = 200
        with patch('splitwise_sync.request.urlopen', return_value=response):
            with self.assertRaises(APIError) as raised:
                Client('secret').create({})
        self.assertIn('création non confirmée', str(raised.exception))
        self.assertIn('"expenses": []', str(raised.exception))

    def test_http_error_with_unreadable_body_preserves_status(self):
        failure = HTTPError('url', 503, 'Unavailable', {}, io.BytesIO())
        with patch.object(failure, 'read', side_effect=OSError('Read failed')), \
                patch('splitwise_sync.request.urlopen', side_effect=failure):
            with self.assertRaises(APIError) as raised:
                Client('secret').create({})
        self.assertIn('503', str(raised.exception))
        self.assertIn('Read failed', str(raised.exception))


if __name__ == '__main__':
    unittest.main()
