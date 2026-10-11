"""Rule-based transaction checks and coverage checks for direct debits."""
from collections import Counter
import configparser
import csv
from dataclasses import dataclass
import datetime
import json
import os
from pathlib import Path
import re
import tempfile

from money import cents


UNEXPECTED_DIRECT_DEBIT = 'prélèvement_inconnu'
DIRECT_DEBIT_LABEL = re.compile(r'^(PRELEVEMENT|PrÃ©lÃ¨vement|Prélèvement)')


class Rules(list):
    """Carry the global cutoff even when no active rule exists for an account."""
    def __init__(self, default_start_date=None):
        super().__init__()
        self.default_start_date = default_start_date


@dataclass
class Rule:
    name: str
    account: object
    label: object
    minimum: object
    maximum: object
    allowed_from: object
    allowed_until: object
    weekdays: set
    month_days: set
    limits: dict
    start_date: object = None


def load_rules(filename):
    config = configparser.ConfigParser(interpolation=None)
    with Path(filename).expanduser().open(encoding='utf-8-sig') as stream:
        config.read_file(stream)
    try:
        default_start = config.defaults().get('start_date')
        rules = Rules(datetime.date.fromisoformat(default_start) if default_start else None)
    except ValueError as exc:
        raise ValueError('Invalid start_date in [DEFAULT]') from exc
    allowed = {'enabled', 'account', 'label_regex', 'amount', 'min_amount', 'max_amount',
               'start_date', 'allowed_from', 'allowed_until', 'weekdays', 'month_days',
               'max_per_day', 'max_per_week', 'max_per_month', 'max_per_year'}
    for name in config.sections():
        section = config[name]
        try:
            if name == UNEXPECTED_DIRECT_DEBIT:
                raise ValueError('This rule name is reserved for unexpected direct debit alerts')
            if set(section) - allowed:
                raise ValueError('Unknown options: ' + ', '.join(sorted(set(section) - allowed)))
            if not section.getboolean('enabled', fallback=True):
                continue
            mask = section['account'].strip()
            if not mask or not re.fullmatch('[0-9Xx]+', mask):
                raise ValueError('account must contain digits or X placeholders')
            account = re.compile(''.join('[0-9]' if c in 'Xx' else c for c in mask))
            if not section['label_regex'].strip():
                raise ValueError('label_regex must not be empty')
            label = re.compile(section['label_regex'])
            def money(key):
                value = cents(section[key].replace(',', '.')) if key in section else None
                if value is not None and value < 0:
                    raise ValueError(key + ' must be a nonnegative expense amount')
                return value
            minimum, maximum = money('min_amount'), money('max_amount')
            if 'amount' in section:
                if minimum is not None or maximum is not None:
                    raise ValueError('Use amount OR min_amount/max_amount')
                minimum = maximum = money('amount')
            if minimum is not None and maximum is not None and minimum > maximum:
                raise ValueError('min_amount must be <= max_amount (absolute expense amounts)')
            def date(key):
                return datetime.date.fromisoformat(section[key]) if key in section else None
            start, end = date('allowed_from'), date('allowed_until')
            if start and end and start > end:
                raise ValueError('allowed_from must be <= allowed_until')
            def days(key, maximum):
                values = {int(v.strip()) for v in section[key].split(',')} if key in section else set()
                if any(v < 1 or v > maximum for v in values):
                    raise ValueError('Invalid ' + key)
                return values
            limits = {period: section.getint('max_per_' + period)
                      for period in ('day', 'week', 'month', 'year')
                      if 'max_per_' + period in section}
            if any(value < 0 for value in limits.values()):
                raise ValueError('Occurrence limits must be nonnegative integers')
            rules.append(Rule(name, account, label, minimum, maximum, start, end,
                              days('weekdays', 7), days('month_days', 31), limits,
                              start_date=date('start_date')))
        except (ValueError, KeyError, re.error) as exc:
            raise ValueError('Invalid legitimacy rule [{}]: {}'.format(name, exc)) from exc
    return rules


def period_keys(day):
    iso_year, iso_week, _ = day.isocalendar()
    return {'day': day.isoformat(), 'week': '{}-W{:02d}'.format(iso_year, iso_week),
            'month': day.strftime('%Y-%m'), 'year': str(day.year)}


def refresh_alerts(connection, account_id, rules):
    """Recompute the full report, counting every matching transaction once per rule.

    Posting dates use the application's UTC convention; legacy DATE is retained.
    Same-day ties with no posting time are ordered by the transaction identifier.
    """
    transactions = []
    for identifier, date, posted, label, amount in connection.execute(
            'SELECT ID, DATE, POSTED_AT, LABEL, AMOUNT_CENTS FROM TRANSACTIONS'):
        when = (datetime.datetime.fromisoformat(posted) if posted else
                datetime.datetime.strptime(date, '%d/%m/%Y'))
        transactions.append((when, str(identifier), label or '', amount))
    transactions.sort(key=lambda row: (row[0], row[1]))
    alerts = []
    matched_rules = {identifier: [] for _, identifier, _, _ in transactions}
    account_rules = [rule for rule in rules if rule.account.fullmatch(str(account_id))]
    coverage_start = getattr(rules, 'default_start_date', None)
    if coverage_start is None and account_rules and all(rule.start_date is not None for rule in account_rules):
        coverage_start = min(rule.start_date for rule in account_rules)
    for rule in account_rules:
        counts = Counter()
        for when, identifier, label, amount in transactions:
            day = when.date()
            if rule.start_date is not None and day < rule.start_date:
                continue
            if not rule.label.search(label):
                continue
            matched_rules[identifier].append(rule.name)
            reasons = []
            expense = -amount
            if amount > 0 and (rule.minimum is not None or rule.maximum is not None):
                reasons.append('Crédit positif : une dépense ou un montant nul est attendu')
            else:
                if rule.minimum is not None and expense < rule.minimum:
                    reasons.append('Dépense inférieure au minimum ({:.2f} EUR)'.format(rule.minimum / 100))
                if rule.maximum is not None and expense > rule.maximum:
                    reasons.append('Dépense supérieure au maximum ({:.2f} EUR)'.format(rule.maximum / 100))
            if rule.allowed_from and day < rule.allowed_from:
                reasons.append('Date antérieure au ' + rule.allowed_from.isoformat())
            if rule.allowed_until and day > rule.allowed_until:
                reasons.append('Date postérieure au ' + rule.allowed_until.isoformat())
            if rule.weekdays and day.isoweekday() not in rule.weekdays:
                reasons.append('Jour de semaine non autorisé')
            if rule.month_days and day.day not in rule.month_days:
                reasons.append('Jour du mois non autorisé')
            for period, key in period_keys(day).items():
                counts[period, key] += 1
                if period in rule.limits and counts[period, key] > rule.limits[period]:
                    period_label = {'day': 'jour', 'week': 'semaine', 'month': 'mois', 'year': 'année'}[period]
                    reasons.append('Occurrences {} {} : {} > {}'.format(
                        period_label, key, counts[period, key], rule.limits[period]))
            if reasons:
                alerts.append((identifier, rule.name, when.isoformat(), label, amount, ' ; '.join(reasons)))
    connection.executemany('UPDATE TRANSACTIONS SET MATCHED_RULES = ? WHERE ID = ?',
                           [(json.dumps(names, ensure_ascii=False), identifier)
                            for identifier, names in matched_rules.items()])
    for when, identifier, label, amount in transactions:
        if coverage_start is not None and when.date() < coverage_start:
            continue
        if DIRECT_DEBIT_LABEL.match(label) and not matched_rules[identifier]:
            alerts.append((identifier, UNEXPECTED_DIRECT_DEBIT, when.isoformat(), label, amount,
                           'Prélèvement inattendu : aucune règle active ne correspond à cette transaction'))
    connection.execute('DELETE FROM TRANSACTION_ALERTS')
    connection.executemany('''INSERT INTO TRANSACTION_ALERTS
        (TRANSACTION_ID, RULE_ID, TRANSACTION_DATE, LABEL, AMOUNT_CENTS, REASONS)
        VALUES (?, ?, ?, ?, ?, ?)''', alerts)
    return alerts


def export_alerts(rows, output_directory):
    """Atomically replace a UTF-8 CSV report for the accounts in this import."""
    directory = Path(output_directory).expanduser()
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / 'transaction_alerts.csv'
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8-sig', newline='',
                                         dir=directory, delete=False) as stream:
            temporary = Path(stream.name)
            writer = csv.writer(stream, delimiter=';')
            writer.writerow(['Compte', 'Transaction', 'Règle', 'Date', 'Libellé', 'Montant EUR', 'Motifs'])
            def text(value):
                value = str(value)
                # Labels are bank-provided data, never spreadsheet formulas.
                return "'" + value if value.lstrip().startswith(('=', '+', '-', '@')) else value
            for account, identifier, rule, date, label, amount, reasons in rows:
                writer.writerow([text(account), text(identifier), text(rule), date, text(label),
                                 '{:.2f}'.format(amount / 100).replace('.', ','), text(reasons)])
        os.replace(str(temporary), str(path))
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()
    return path
