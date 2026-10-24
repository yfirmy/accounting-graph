"""One-off personal deductions, applied atomically to the Splitwise outbox only."""
import configparser
from dataclasses import dataclass
import datetime as dt
import json
import re
from pathlib import Path

from money import cents


@dataclass(frozen=True)
class PersonalException:
    name: str
    rule: str
    purchase_date: str
    personal_cents: int
    account: str


def matches_account(mask, account):
    return re.fullmatch(mask.upper().replace('X', '[0-9]'), account) is not None


def masks_overlap(left, right):
    return len(left) == len(right) and all(a == b or 'X' in (a, b) for a, b in zip(left, right))


def database_account(connection):
    filename = next(row[2] for row in connection.execute('PRAGMA database_list') if row[1] == 'main')
    stem = Path(filename).stem if filename else ''
    return stem[len('account_'):] if stem.startswith('account_') else ''


def load_exceptions(filename, rules):
    if not filename:
        return []
    config = configparser.ConfigParser(interpolation=None)
    with Path(filename).expanduser().open(encoding='utf-8-sig') as stream:
        config.read_file(stream)
    known = {rule.name for rule in rules}
    result, seen = [], []
    for name in config.sections():
        try:
            section = config[name]
            if set(section) != {'account', 'rule', 'purchase_date', 'personal_amount'}:
                raise ValueError('indiquer uniquement account, rule, purchase_date et personal_amount')
            account = section['account'].strip().upper()
            if not re.fullmatch('[0-9X]+', account):
                raise ValueError('account doit contenir des chiffres ou X (un X par chiffre masqué)')
            rule = section['rule'].strip()
            if rule not in known:
                raise ValueError('règle Splitwise inconnue ou désactivée : ' + rule)
            day = dt.date.fromisoformat(section['purchase_date']).isoformat()
            amount = cents(section['personal_amount'].replace(',', '.'))
            if amount <= 0:
                raise ValueError('personal_amount doit être strictement positif')
            if any(r == rule and d == day and masks_overlap(a, account) for r, d, a in seen):
                raise ValueError('une seule exception est autorisée par compte, règle et date')
            seen.append((rule, day, account))
            result.append(PersonalException(name, rule, day, amount, account))
        except (KeyError, ValueError) as exc:
            raise ValueError('Exception Splitwise [{}] : {}'.format(name, exc)) from exc
    return result


def euros(value):
    return '{}.{:02d}'.format(value // 100, value % 100)


def apply_exceptions(connections, writable, exceptions, prompt):
    """Return blocked outbox IDs; caller holds the shared Splitwise file lock.

    Only inspect accounts matching each exception's mask.
    """
    blocked = {connection: set() for connection in writable}
    accounts = {connection: database_account(connection) for connection in connections}
    for item in exceptions:
        candidates, applied = [], []
        for connection in connections:
            if not matches_account(item.account, accounts[connection]):
                continue
            tables = {r[0] for r in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if 'SPLITWISE_ADJUSTMENTS' in tables:
                applied.extend(connection.execute('''SELECT EXCEPTION_ID, RULE_ID, PURCHASE_DATE, PERSONAL_CENTS
                    FROM SPLITWISE_ADJUSTMENTS WHERE EXCEPTION_ID = ? OR (RULE_ID = ? AND PURCHASE_DATE = ?)''',
                    (item.name, item.rule, item.purchase_date)).fetchall())
            if 'SPLITWISE_EXPORTS' not in tables:
                continue
            for identifier, state, serialized in connection.execute('''SELECT TRANSACTION_ID, STATE, PAYLOAD
                    FROM SPLITWISE_EXPORTS WHERE RULE_ID = ? AND PAYLOAD IS NOT NULL''', (item.rule,)):
                payload = json.loads(serialized)
                if payload['date'][:10] == item.purchase_date:
                    candidates.append((connection, identifier, state, payload))
        if applied:
            expected = (item.name, item.rule, item.purchase_date, item.personal_cents)
            if len(applied) != 1 or applied[0] != expected:
                raise ValueError('Exception [{}] déjà appliquée avec une autre définition ; '
                                 'aucun recalcul ni nouvel envoi automatique.'.format(item.name))
            print('Splitwise : exception [{}] déjà appliquée.'.format(item.name))
            continue
        if not candidates:
            print('Splitwise : exception [{}] non appliquée — aucune dépense pour le compte {}, [{}] le {}.'.format(
                item.name, item.account, item.rule, item.purchase_date))
            continue
        # Hold every possible match until the user disambiguates. Never send the
        # unadjusted amount merely because stdin is unavailable or Enter was used.
        for connection, identifier, _, _ in candidates:
            if connection in blocked:
                blocked[connection].add(identifier)
        selected = candidates[0]
        if len(candidates) > 1:
            lines = ['Exception [{}] — part personnelle {} EUR : choisir UNE dépense.'.format(
                item.name, euros(item.personal_cents))]
            for i, (candidate_connection, identifier, state, payload) in enumerate(candidates, 1):
                filename = next(r[2] for r in candidate_connection.execute('PRAGMA database_list') if r[1] == 'main')
                account = ('…' + Path(filename).stem[-3:]) if filename else 'local'
                lines.append('{} : {} — {} EUR — {} — compte {} — transaction {} ({})'.format(
                    i, item.purchase_date, payload['cost'], payload['description'], account, identifier, state))
            lines.append('Numéro = appliquer ; Entrée/later = reporter les envois concernés.')
            answer = prompt('\n'.join(lines), {str(i) for i in range(1, len(candidates) + 1)} | {'later'})
            if answer not in {str(i) for i in range(1, len(candidates) + 1)}:
                continue
            selected = candidates[int(answer) - 1]
        connection, identifier, state, payload = selected
        if connection not in writable:
            print('Splitwise : exception [{}] en attente ; réimporter un OFX contenant le compte choisi.'.format(item.name))
            continue
        if state != 'pending':
            print('Splitwise : exception [{}] non appliquée : dépense {}. '
                  'Vérifier la dépense dans Splitwise avant toute correction.'.format(item.name, state))
            continue
        original = cents(payload['cost'])
        if item.personal_cents > original:
            print('Splitwise : exception [{}] bloquée : part personnelle {} EUR supérieure au total {} EUR.'.format(
                item.name, euros(item.personal_cents), euros(original)))
            continue
        remaining = original - item.personal_cents
        adjusted = dict(payload, cost=euros(remaining))
        with connection:
            connection.execute('BEGIN IMMEDIATE')
            # Do not change an expense that another process has started sending.
            current = connection.execute('SELECT STATE, PAYLOAD FROM SPLITWISE_EXPORTS WHERE TRANSACTION_ID = ?',
                                         (identifier,)).fetchone()
            if current is None or current[0] != 'pending' or json.loads(current[1]) != payload:
                raise ValueError('La dépense a changé pendant le traitement de son exception ; relancer l’import.')
            updated = connection.execute('''UPDATE SPLITWISE_EXPORTS SET PAYLOAD = ?, STATE = ?,
                UPDATED_AT = CURRENT_TIMESTAMP WHERE TRANSACTION_ID = ? AND STATE = 'pending' AND PAYLOAD = ?''',
                (json.dumps(adjusted, ensure_ascii=False), 'pending' if remaining else 'skipped',
                 identifier, current[1]))
            if updated.rowcount != 1:
                raise ValueError('La dépense a changé pendant le traitement de son exception ; relancer l’import.')
            connection.execute('''INSERT INTO SPLITWISE_ADJUSTMENTS
                (EXCEPTION_ID, TRANSACTION_ID, RULE_ID, PURCHASE_DATE, PERSONAL_CENTS, ORIGINAL_CENTS)
                VALUES (?, ?, ?, ?, ?, ?)''',
                (item.name, identifier, item.rule, item.purchase_date, item.personal_cents, original))
        print('Splitwise : exception [{}] appliquée : {} EUR - {} EUR personnels = {} EUR à partager{}.'.format(
            item.name, euros(original), euros(item.personal_cents), euros(remaining),
            ' (aucun envoi)' if not remaining else ''))
        for candidate_connection, candidate_id, _, _ in candidates:
            if candidate_connection in blocked:
                blocked[candidate_connection].discard(candidate_id)
    return blocked
