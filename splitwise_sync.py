"""Opt-in Splitwise rules and durable delivery of newly imported expenses."""
import configparser
from dataclasses import dataclass
import datetime as dt
from http.client import HTTPException
import json
import os
from pathlib import Path
import re
import ssl
import sys
import uuid
from urllib import request, parse, error

from filelock import FileLock, Timeout
import certifi
from money import cents


IMPORT_NOTE = 'Cette dépense a été ajoutée automatiquement à partir du relevé de compte.'


def expense_note(marker):
    return IMPORT_NOTE + '\n\n' + marker


@dataclass
class Rule:
    name: str
    account: object
    label: object
    weekdays: set
    minimum: int
    maximum: int
    action: str
    outside_amount: str
    description: str
    group_id: int
    start_date: object
    category_id: object = None


@dataclass
class Settings:
    token_env: str
    rules: list


def load_settings(filename):
    if not filename:
        return None
    config = configparser.ConfigParser(interpolation=None)
    with Path(filename).expanduser().open(encoding='utf-8-sig') as stream:
        config.read_file(stream)
    if 'Splitwise' not in config:
        raise ValueError('Section [Splitwise] manquante')
    section = config['Splitwise']
    if not section.getboolean('enabled', fallback=False):
        return None
    if set(section) - {'enabled', 'token_env', 'group_id'}:
        raise ValueError('Options inconnues dans [Splitwise]')
    token_env = section.get('token_env', 'SPLITWISE_API_KEY')
    if not re.fullmatch('[A-Za-z_][A-Za-z0-9_]*', token_env):
        raise ValueError('Splitwise : token_env invalide')
    rules = []
    for name in config.sections():
        if name == 'Splitwise':
            continue
        s = config[name]
        try:
            allowed = {'account', 'label_regex', 'weekdays', 'min_amount', 'max_amount',
                       'action', 'outside_amount', 'description', 'group_id', 'start_date', 'enabled',
                       'category_id'}
            if set(s) - allowed:
                raise ValueError('option inconnue')
            if not s.getboolean('enabled', fallback=True):
                continue
            mask = s['account']
            if not re.fullmatch('[0-9Xx]+', mask):
                raise ValueError('account doit contenir uniquement des chiffres ou X')
            account = re.compile(''.join('[0-9]' if c in 'Xx' else c for c in mask))
            if not s['label_regex'].strip():
                raise ValueError('label_regex vide')
            label = re.compile(s['label_regex'])
            weekdays = {int(v.strip()) for v in s.get('weekdays', '1,2,3,4,5,6,7').split(',')}
            if not weekdays or not weekdays <= set(range(1, 8)):
                raise ValueError('weekdays : 1=lundi, ..., 7=dimanche')
            action = s.get('action', 'auto')
            outside = s.get('outside_amount', 'confirm')
            if action not in {'auto', 'confirm', 'skip'} or outside not in {'confirm', 'skip'}:
                raise ValueError('action : auto/confirm/skip ; outside_amount : confirm/skip')
            minimum = cents(s.get('min_amount', '0').replace(',', '.'))
            maximum = cents(s['max_amount'].replace(',', '.')) if 'max_amount' in s else None
            if minimum < 0 or (maximum is not None and maximum < minimum):
                raise ValueError('intervalle de dépenses positives invalide')
            if action == 'auto' and maximum is None:
                raise ValueError('max_amount obligatoire pour un envoi automatique')
            description = s.get('description', '').strip()
            group_id = int(s.get('group_id', section.get('group_id', '0')))
            if action != 'skip' and (not description or group_id <= 0):
                raise ValueError('description neutre et group_id positif obligatoires')
            start = dt.date.fromisoformat(s['start_date']) if 'start_date' in s else None
            category_id = s.getint('category_id') if 'category_id' in s else None
            if category_id is not None and category_id <= 0:
                raise ValueError('category_id doit être un identifiant de sous-catégorie positif')
            rules.append(Rule(name, account, label, weekdays, minimum, maximum,
                              action, outside, description, group_id, start, category_id))
        except (KeyError, ValueError, re.error) as exc:
            raise ValueError('Règle Splitwise [{}] : {}'.format(name, exc)) from exc
    return Settings(token_env, rules)


def classify(settings, account, operation):
    amount = -cents(operation.value)
    day = operation.date.date()
    if amount <= 0:
        return 'skip', None
    for rule in settings.rules:
        if (rule.account.fullmatch(str(account)) and rule.label.search(operation.label or '')
                and day.isoweekday() in rule.weekdays
                and (rule.start_date is None or day >= rule.start_date)):
            action = rule.action
            if action != 'skip' and (amount < rule.minimum or
                                    (rule.maximum is not None and amount > rule.maximum)):
                action = rule.outside_amount
            return action, rule
    return 'skip', None


def enqueue(connection, settings, account, operation):
    """Called only for a new transaction, inside the atomic banking import."""
    action, rule = classify(settings, account, operation)
    marker = 'accounting-graph:' + uuid.uuid4().hex
    payload = None
    if action != 'skip':
        # Never copy the bank memo, account number or OFX identifier to Splitwise.
        description = rule.description
        if str(account) in re.sub(r'\D', '', description):
            raise ValueError('La description Splitwise contient le numéro de compte')
        amount = -cents(operation.value)
        # Use the statement's transaction day, never the import/retry date.
        # Noon UTC preserves that calendar day when displayed in Europe/Paris.
        payload = {'cost': '{}.{:02d}'.format(amount // 100, amount % 100),
                   'description': description, 'details': expense_note(marker),
                   'date': operation.date.date().isoformat() + 'T12:00:00Z',
                   'currency_code': 'EUR', 'group_id': rule.group_id, 'split_equally': True}
        if rule.category_id is not None:
            payload['category_id'] = rule.category_id
    connection.execute('''INSERT INTO SPLITWISE_EXPORTS
        (TRANSACTION_ID, RULE_ID, CATEGORY, STATE, MARKER, PAYLOAD)
        VALUES (?, ?, ?, ?, ?, ?)''',
        (str(operation.id), rule.name if rule else None, action,
         'skipped' if action == 'skip' else 'pending', marker,
         json.dumps(payload, ensure_ascii=False) if payload else None))
    print('Splitwise : {} | {} | {}'.format(operation.date.date(),
          rule.description if rule and rule.description else 'Transaction',
          {'auto': 'soumise automatiquement après validation', 'confirm': 'nécessite confirmation',
           'skip': 'non soumise'}[action]))


class APIError(Exception):
    pass


class Client:
    BASE = 'https://secure.splitwise.com/api/v3.0/'

    def __init__(self, token):
        self.token = token
        self._last_status = None

    def _failure(self, endpoint, reason, status=None, body=None):
        message = 'Splitwise [{}] : {}'.format(endpoint, reason)
        if status is not None:
            message += '\nStatut HTTP : {}'.format(status)
        if body is not None:
            if isinstance(body, bytes):
                body = body.decode('utf-8', errors='replace')
            message += '\nCorps de la réponse HTTP :\n' + (body or '(vide)')
        # Response bodies and exception messages can echo credentials.
        for secret in (self.token, json.dumps(self.token)[1:-1]):
            if secret:
                message = message.replace(secret, '[CLÉ API MASQUÉE]')
        return APIError(message)

    def _request(self, endpoint, params=None, payload=None):
        self._last_status = None
        url = self.BASE + endpoint
        if params:
            url += '?' + parse.urlencode(params)
        data = json.dumps(payload).encode('utf-8') if payload is not None else None
        headers = {'Authorization': 'Bearer ' + self.token,
                   'Accept': 'application/json', 'User-Agent': 'accounting-graph/1.0'}
        if data is not None:
            headers['Content-Type'] = 'application/json'
        req = request.Request(url, data=data, headers=headers)
        try:
            # Python on macOS may lack a configured default CA bundle.
            # Keep hostname and certificate verification enabled with certifi.
            context = ssl.create_default_context(cafile=certifi.where())
            with request.urlopen(req, timeout=30, context=context) as response:
                self._last_status = getattr(response, 'status', None)
                body = response.read()
        except error.HTTPError as exc:
            try:
                body = exc.read()
            except (OSError, ValueError, HTTPException) as read_error:
                body = '(corps illisible : {}: {})'.format(type(read_error).__name__, read_error)
            finally:
                exc.close()
            raise self._failure(endpoint, '{}: {}'.format(type(exc).__name__, exc.reason),
                                exc.code, body) from None
        except (OSError, ValueError, HTTPException) as exc:
            raise self._failure(endpoint, '{}: {}'.format(type(exc).__name__, exc),
                                self._last_status, getattr(exc, 'partial', None)) from None
        try:
            result = json.loads(body)
        except (ValueError, UnicodeError) as exc:
            raise self._failure(endpoint, '{}: {}'.format(type(exc).__name__, exc),
                                self._last_status, body) from None
        if not isinstance(result, dict) or result.get('errors'):
            raise self._failure(endpoint, 'requête refusée ou réponse invalide', self._last_status, body)
        return result

    def expenses(self, day):
        # Broad bounds avoid losing midnight records; then compare calendar dates.
        date = dt.date.fromisoformat(day)
        params = {'dated_after': (date - dt.timedelta(days=1)).isoformat() + 'T00:00:00Z',
                  'dated_before': (date + dt.timedelta(days=2)).isoformat() + 'T00:00:00Z',
                  'limit': 100, 'offset': 0}
        result, seen = [], set()
        while True:
            page_response = self._request('get_expenses', params=params)
            page = page_response.get('expenses')
            if not isinstance(page, list):
                raise self._failure('get_expenses', 'liste des dépenses invalide',
                                    self._last_status, json.dumps(page_response, ensure_ascii=False))
            for expense in page:
                if not isinstance(expense, dict) or not expense.get('id'):
                    raise self._failure('get_expenses', 'dépense invalide',
                                        self._last_status, json.dumps(page_response, ensure_ascii=False))
                if expense['id'] in seen:
                    raise APIError('Splitwise : pagination incohérente, envoi annulé')
                seen.add(expense['id'])
                result.append(expense)
            # Do not assume the server honored our requested page size.
            if not page:
                return result
            params['offset'] += len(page)

    def create(self, payload):
        result = self._request('create_expense', payload=payload)
        expenses = result.get('expenses')
        if (not isinstance(expenses, list) or len(expenses) != 1
                or not isinstance(expenses[0], dict) or not expenses[0].get('id')):
            raise self._failure('create_expense', 'création non confirmée',
                                self._last_status, json.dumps(result, ensure_ascii=False))
        return str(expenses[0]['id'])


def ask(message, choices):
    """Never interpret EOF, Enter or noninteractive execution as consent."""
    if not sys.stdin.isatty():
        print('Splitwise : confirmation reportée (terminal interactif requis).')
        return 'later'
    try:
        answer = input(message + '\n> ').strip().lower()
    except (EOFError, KeyboardInterrupt):
        return 'later'
    return answer if answer in choices else 'later'


def _state(connection, identifier, state, expense_id=None):
    with connection:
        connection.execute('''UPDATE SPLITWISE_EXPORTS SET STATE = ?, EXPENSE_ID = ?,
            UPDATED_AT = CURRENT_TIMESTAMP WHERE TRANSACTION_ID = ?''', (state, expense_id, identifier))


def _deliver(connection, client, prompt):
    rows = connection.execute('''SELECT TRANSACTION_ID, CATEGORY, STATE, MARKER, PAYLOAD
        FROM SPLITWISE_EXPORTS WHERE STATE IN ('pending', 'sending', 'uncertain')
        ORDER BY rowid''').fetchall()
    for identifier, category, state, marker, serialized in rows:
        payload = json.loads(serialized)
        # Also annotate expenses queued before this note was introduced.
        payload['details'] = expense_note(marker)
        day = payload['date'][:10]
        summary = '{} — {} EUR — {} (groupe {})'.format(
            day, payload['cost'], payload['description'], payload['group_id'])
        try:
            expenses = client.expenses(day)
            # A marker is stronger evidence than date/amount, including after a crash.
            owned = [e for e in expenses
                     if marker in (e.get('details') or '').splitlines()]
            if len(owned) == 1:
                _state(connection, identifier, 'submitted', str(owned[0]['id']))
                print('Splitwise : envoi déjà enregistré — ' + summary)
                continue
            if owned:
                print('Splitwise : plusieurs dépenses portent le même marqueur, vérification manuelle requise.')
                _state(connection, identifier, 'uncertain')
                continue
            duplicates = []
            for expense in expenses:
                if expense.get('deleted_at') or expense.get('payment'):
                    continue
                if not expense.get('date') or 'cost' not in expense or not expense.get('currency_code'):
                    raise APIError('Splitwise : données insuffisantes pour contrôler les doublons')
                if (expense['date'][:10] == day and expense['currency_code'] == payload['currency_code']
                        and cents(expense['cost']) == cents(payload['cost'])):
                    duplicates.append(expense)
            uncertain = state in {'sending', 'uncertain'}
            if duplicates or category == 'confirm' or uncertain:
                choices = {'create', 'skip', 'later'}
                lines = ['Splitwise : ' + summary]
                if uncertain:
                    lines.append('Résultat du précédent envoi incertain : créer à nouveau risque un doublon.')
                if category == 'confirm':
                    lines.append('La règle ou le montant impose une confirmation.')
                if duplicates:
                    lines.append('Dépenses existantes de même montant et même jour :')
                    for i, expense in enumerate(duplicates, 1):
                        lines.append('{} : {} (ID {}, groupe {})'.format(
                            i, expense.get('description', ''), expense['id'], expense.get('group_id')))
                        choices.add(str(i))
                    lines.append('Numéro = rattacher à cette dépense existante, sans la recréer.')
                lines.append('create = créer ; skip = ne pas soumettre ; Entrée/later = reporter.')
                decision = prompt('\n'.join(lines), choices)
                if decision in {str(i) for i in range(1, len(duplicates) + 1)}:
                    _state(connection, identifier, 'linked', str(duplicates[int(decision) - 1]['id']))
                    continue
                if decision == 'skip':
                    _state(connection, identifier, 'skipped')
                    continue
                if decision != 'create':
                    continue
            # Persist intent BEFORE the HTTP call. No blind retries after a timeout.
            _state(connection, identifier, 'sending')
            try:
                expense_id = client.create(payload)
            except APIError:
                _state(connection, identifier, 'uncertain')
                raise
            _state(connection, identifier, 'submitted', expense_id)
            print('Splitwise : dépense créée, ID {} — {}'.format(expense_id, summary))
        except (APIError, ValueError) as exc:
            print('Splitwise : envoi suspendu ; reprise au prochain import. ' + str(exc))
            # Stop this batch on API failures, retaining all remaining work.
            break


def sync_pending(connection, settings, client=None, prompt=ask):
    if settings is None:
        return
    if connection.in_transaction:
        raise ValueError('Splitwise exige un import validé et enregistré avant tout envoi')
    if not connection.execute("SELECT 1 FROM SPLITWISE_EXPORTS WHERE STATE IN ('pending', 'sending', 'uncertain') LIMIT 1").fetchone():
        return
    if client is None:
        token = os.environ.get(settings.token_env)
        if not token:
            print('Splitwise : variable {} absente ; envois conservés en attente.'.format(settings.token_env))
            return
        client = Client(token)
    filename = next(row[2] for row in connection.execute('PRAGMA database_list') if row[1] == 'main')
    if not filename:
        # Used by in-memory tests; real account databases are always on disk.
        _deliver(connection, client, prompt)
        return
    lock = FileLock(str(Path(filename).parent / '.splitwise-sync.lock'), timeout=0)
    try:
        with lock:
            _deliver(connection, client, prompt)
    except Timeout:
        print('Splitwise : un autre envoi est en cours ; reprise au prochain import.')
