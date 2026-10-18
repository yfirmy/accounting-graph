#!/usr/bin/python
# -*- coding: utf-8 -*-

import datetime
import sqlite3
import sys
import configparser
import os
import re
import argparse
import math
from contextlib import closing
from pathlib import Path

import dateutil
import dateutil.relativedelta
import calendar

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from ofxparse import OfxParser
from balance_checks import cents, latest_observation, reconcile_import, verify_stored_balance
from legitimacy_checks import load_rules, refresh_alerts, export_alerts
from ofx_input import collect_latest_ofx, print_file_dates

MONTHS_IN_YEAR = 12
DAYS_IN_MONTH = 31
PAY_DAY = 28
IMPORT_FLAG = "--import"
DRY_RUN_FLAG = "--dry-run"
DEBUG_FLAG = "--debug"
CSV_OUTPUT_FLAG = "--csv-output"
CURRENCY = "€"

config = configparser.RawConfigParser()


class Operation:
    def __init__(self, operation_id, date, label, amount):
        self.id = str(operation_id)
        self.posted_at = date
        self.date = date.replace(hour=0, minute=0, second=0, microsecond=0)
        self.label = label
        self.value = amount

    def debug(self, csv_output_mode: bool):
        separator = ";" if csv_output_mode else " "
        return self.id + separator + str(self.date) + separator + "{:>8}".format(str(self.value)) + separator + "\"" + self.label + "\""


class AccountStatement:
    def __init__(self, account_id):
        self.account_id = account_id
        self.operations = {}
        self.last_date = None
        self.last_balance = None
        self.balance_date = None

    def add(self, operation):
        if operation.date not in self.operations:
            self.operations[operation.date] = []
        self.operations[operation.date].append(operation)

    def get_date_boundaries(self):
        min_date = None
        max_date = None
        if len(self.operations) > 0:
            operations_dates = list(self.operations.keys())
            if len(operations_dates) > 0:
                min_date = min(operations_dates)
                max_date = max(operations_dates)
        return min_date, max_date

    def operations_count(self, date):
        if len(self.operations) > 0 and date in self.operations:
            return len(self.operations[date])
        else:
            return 0


def parse_double(ch):
    return float(ch.replace(',', '.').replace('\xa0', '')) if len(ch) > 0 else 0.0


def get_account_config_key(section, account_id):
    matches = []
    for key in config.options(section):
        pattern = ''.join('[0-9]' if char in 'Xx' else re.escape(char) for char in key)
        if re.fullmatch(pattern, str(account_id)):
            matches.append(key)
    if not matches:
        raise configparser.NoOptionError('matching account', section)
    if len(matches) > 1:
        raise ValueError("Multiple account keys match in section " + section)
    return matches[0]


def get_account_name(account_id):
    account_name = ""
    try:
        account_name = config.get('Accounts', get_account_config_key('Accounts', account_id))
    except Exception as ex:
        print("Failed to retrieve the name of account " + str(account_id), str(ex))
    return account_name if account_name else ""

def get_masked_account_id(account_id):
    return 'X' * (len(account_id) - 3) + account_id[-3:]

def get_output_directory():
    output_dir = ""
    try:
        output_dir = config.get('Directories', 'output')
    except Exception as ex:
        print("Failed to retrieve the output directory", str(ex))
    return output_dir if output_dir else ""

def get_graph_filename(graph_type_name, account_id):
    output_dir = os.path.expanduser(get_output_directory())
    return os.path.join(output_dir, get_account_name(account_id) + ' - ' + graph_type_name + '.pdf')

def is_savings_account(account_id):
    is_savings_account_param = False
    try:
        is_savings_account_param = config.getboolean(
            'Savings accounts', get_account_config_key('Savings accounts', account_id))
    except Exception as ex:
        print("Failed to retrieve the name of account " + str(account_id), str(ex))
    return is_savings_account_param if is_savings_account_param else False


def analyse_operations(statements: AccountStatement, connection, debug_mode: bool):
    if len(statements.operations) > 0:
        (balance_over_time, min_date, max_date,
         min_balance, min_balance_date,
         max_balance, max_balance_date) = compute_balance_evolution(statements, connection, debug_mode)
        draw_balance_evolution(statements.account_id, balance_over_time, min_date, max_date,
                               min_balance, min_balance_date, max_balance, max_balance_date)
        if is_savings_account(statements.account_id):
            balance_derivative = compute_savings_derivative(balance_over_time)
            draw_savings_derivative(statements.account_id, balance_derivative)
        balance_compared = compute_balance_compared(balance_over_time, statements.last_date)
        draw_balance_comparison(statements.account_id, balance_compared)


def format_amount(value):
    return "{:,.2f}".format(value).replace(',', ' ').replace('.', ',') + " " + CURRENCY


def draw_balance_evolution(account_id, balance, min_date, max_date,
                           min_balance, min_balance_date, max_balance, max_balance_date):

    last_balance = balance[next(iter(balance))]
    offset = (max_balance - min_balance) * 0.7 / 100.0

    fig, axes = plt.subplots()
    fig.set_figwidth(20)
    lists = sorted(balance.items())
    x, y = zip(*lists)
    axes.plot(x, y, color="mediumseagreen")
    axes.xaxis.set_major_locator(mdates.MonthLocator())
    for label in axes.get_xticklabels(which='major'):
        label.set(rotation=30, horizontalalignment='right')
    axes.grid(True)
    axes.set_title("Evolution du solde - " + get_account_name(account_id) + " (" + get_masked_account_id(account_id) + ")")
    axes.set_ylabel(r'Solde')
    plt.hlines(y=0, xmin=min_date, xmax=max_date, colors='grey', linestyles='--')
    plt.plot(min_balance_date, min_balance, marker='x', color="blue")
    plt.text(min_balance_date, min_balance - offset, " " + format_amount(min_balance), color="blue",
             verticalalignment='top')
    plt.plot(max_balance_date, max_balance, marker='x', color="red")
    plt.text(max_balance_date, max_balance + offset, " " + format_amount(max_balance), color="red",
             verticalalignment='bottom')
    plt.plot(max_date, last_balance, marker='x', color='black')
    plt.text(max_date, last_balance + offset, " " + format_amount(last_balance), color="black",
             verticalalignment='top')

    plt.savefig(get_graph_filename('Evolution du solde', account_id))
    plt.show()
    plt.close()


def last_non_none(lst):
    i = len(lst) - 1
    for item in reversed(lst):
        if item is not None:
            return i, item
        i = i-1
    return None, None


def count_non_none(lst):
    return sum(1 for item in lst if item is not None)


# Multi-Month Daily Bank Balance Trend Graph
# comparing daily bank balances across multiple months
def stats_same_day(balance_compared, day):
    values_same_day = list(map(lambda balance_for_month: balance_for_month[day], balance_compared))
    filtered_values_same_day = list(filter(lambda v: v is not None, values_same_day))
    mean_value_same_day = sum(filtered_values_same_day) / len(filtered_values_same_day)
    min_value_same_day = min(filtered_values_same_day)
    max_value_same_day = max(filtered_values_same_day)
    return min_value_same_day, max_value_same_day, mean_value_same_day


def spot_value(x, y, marker, marker_color, text_color, label, h_alignment, plt, v_alignment='baseline'):
    plt.plot(x, y, marker=marker, color=marker_color)
    offset = 10 if y >= 0 else -15
    plt.text(x, y + offset, " " + label + format_amount(y),
             color=text_color, horizontalalignment=h_alignment, verticalalignment=v_alignment)


def draw_savings_derivative(account_id, savings_derivative):
    fig, axes = plt.subplots()
    fig.set_figwidth(20)
    lists = sorted(savings_derivative.items())
    timestamps = list(savings_derivative.keys())
    min_date = timestamps[0].replace(day=1)
    max_date = timestamps[len(timestamps)-1]
    max_date = max_date.replace(day=1) + dateutil.relativedelta.relativedelta(months=1)

    x, y = zip(*lists)
    savings_color = [{p<0: 'red', 0<=p<=2: 'orange', p>2: 'green'}[True] for p in y]
    axes.bar(x, y, width=8.0, color=savings_color)
    axes.set_title("Epargne par mois - " + get_account_name(account_id) + " (" + get_masked_account_id(account_id) + ")")
    axes.xaxis.set_major_locator(mdates.MonthLocator())
    axes.grid(True)
    axes.set_ylabel(r'Epargne')
    plt.hlines(y=0, xmin=min_date, xmax=max_date, colors='grey', linestyles='--')
    for item in savings_derivative:
        label = "+" if savings_derivative[item] > 0 else "" if savings_derivative[item] < 0 else ""
        color = "green" if savings_derivative[item] > 0 else "red" if savings_derivative[item] < 0 else "black"
        vertical_alignment = "bottom" if savings_derivative[item] >= 0 else "top"
        spot_value(item, savings_derivative[item], "", color, color, label, "center", plt, vertical_alignment)

    plt.savefig(get_graph_filename('Epargne par mois', account_id))
    plt.show()
    plt.close()


def draw_balance_comparison(account_id, balance_compared):
    min_grey_intensity = 0.85
    max_grey_intensity = 0.35
    a = (max_grey_intensity - min_grey_intensity) / max(1, len(balance_compared) - 1)
    fig, axes = plt.subplots()
    fig.set_figwidth(20)
    for month_age in reversed(range(0, len(balance_compared))):
        if month_age == 0:
            color = "red"
            alpha = 1
        else:
            color_intensity = max_grey_intensity - month_age * a
            alpha = 0.5
            color = (color_intensity, color_intensity, color_intensity)
        if month_age < len(balance_compared):
            plt.plot(range(1, len(balance_compared[month_age])), balance_compared[month_age][1:], color=color, alpha=alpha)
    plt.hlines(y=0, xmin=1, xmax=31, colors='grey', linestyles='--')
    axes.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=(0, 1, 2, 3, 4, 5, 6)))
    axes.xaxis.set_minor_locator(mdates.DayLocator())
    axes.grid(True)
    axes.set_title("Comparaison du solde - " + get_account_name(account_id) + " (" + get_masked_account_id(account_id) + ")")
    axes.set_ylabel(r'Solde')
    current_month_values = [value for value in balance_compared[0][1:] if value is not None]
    current_min = min(current_month_values, default=0)
    current_max = max(current_month_values, default=0)
    lower_limit = current_min - 100 if current_min < -100 else -100
    upper_limit = max(3000, math.ceil(current_max / 500) * 500)
    axes.set_ylim(lower_limit, upper_limit)

    last_month = 0 if count_non_none(balance_compared[0]) >= 1 else 1
    last_day, last_balance = last_non_none(balance_compared[last_month])

    spot_value(last_day, last_balance, "x", "red", "red", "", "left", plt, "bottom")

    min_value_same_day, max_value_same_day, mean_value_same_day = stats_same_day(balance_compared, last_day)

    spot_value(last_day, min_value_same_day, "+", "grey", "darkgrey", "min: ", "right", plt, "bottom")
    spot_value(last_day, max_value_same_day, "+", "grey", "darkgrey", "max: ", "right", plt, "bottom")
    spot_value(last_day, mean_value_same_day, "+", "grey", "darkgrey", "moy: ", "right", plt, "bottom")

    plt.savefig(get_graph_filename('Comparaison du solde', account_id))
    plt.show()
    plt.close()


def compute_balance_evolution(account_statement: AccountStatement, connection, debug_mode: bool):
    min_date, max_date = account_statement.get_date_boundaries()
    balance_over_time = {}
    current_date = account_statement.last_date
    current_balance = cents(account_statement.last_balance)
    min_balance = account_statement.last_balance
    min_balance_date = account_statement.last_date
    max_balance = account_statement.last_balance
    max_balance_date = account_statement.last_date

    while current_date >= min_date:
        balance_over_time[current_date] = current_balance / 100

        if current_balance / 100 < min_balance:
            min_balance = current_balance / 100
            min_balance_date = current_date
        if current_balance / 100 > max_balance:
            max_balance = current_balance / 100
            max_balance_date = current_date

        for operation in account_statement.operations.get(current_date, []):
            current_balance -= cents(operation.value)
        current_date = current_date - datetime.timedelta(days=1)

    balance_debug(debug_mode, account_statement, balance_over_time)

    return (balance_over_time,
            min_date, account_statement.last_date,
            min_balance, min_balance_date,
            max_balance, max_balance_date)


def compute_savings_derivative(balance_over_time):
    savings_derivative = {}
    timestamps = list(balance_over_time.keys())
    first_timestamp = timestamps[len(timestamps)-1]
    first_month = first_timestamp.replace(day=PAY_DAY, hour=0, minute=0, second=0, microsecond=0)
    last_timestamp = timestamps[0]
    timespan = dateutil.relativedelta.relativedelta(last_timestamp, first_month)
    months_list = [first_month + dateutil.relativedelta.relativedelta(months=x)
                     for x in range(0, timespan.years*12 + timespan.months + 1)]
    if first_timestamp < months_list[0]:
        months_list.insert(0, first_timestamp)
    if last_timestamp > months_list[len(months_list) - 1]:
        months_list.append(last_timestamp)
    previous_timestamp = None

    for timestamp in months_list:
        if (previous_timestamp is not None and
                timestamp in balance_over_time and
                previous_timestamp in balance_over_time):
            add_days = 15 if timestamp == last_timestamp and timestamp.day > PAY_DAY else 0
            display_month = (timestamp + dateutil.relativedelta.relativedelta(days=add_days)).replace(day=1)
            _, month_size = calendar.monthrange(timestamp.year, display_month.month)
            display_date = display_month.replace(day=int(month_size/2)+1)
            savings_derivative[display_date] = balance_over_time[timestamp] - balance_over_time[previous_timestamp]
        previous_timestamp = timestamp

    return savings_derivative


def balance_debug(debug_mode: bool, acc_statement: AccountStatement, balance_over_time):
    if debug_mode:
        print("Balance for account " + str(acc_statement.account_id) + " - " + get_account_name(
            acc_statement.account_id), )
        for date in balance_over_time:
            print(date.strftime("%d/%m/%Y") + ": " + str(balance_over_time[date]))


def calculate_month_difference(last_date, current_date):
    difference = dateutil.relativedelta.relativedelta(last_date, current_date.replace(day=1))
    return difference.months + MONTHS_IN_YEAR * difference.years


def compute_balance_compared(balance, last_date):
    balance_compared = []
    for balance_date in (balance.keys()):
        month_diff = calculate_month_difference(last_date, balance_date)
        while len(balance_compared) <= month_diff:
            balance_compared.append([None] * (DAYS_IN_MONTH + 1))
        line = balance_compared[month_diff]
        line[balance_date.day] = balance[balance_date]
    return balance_compared


def read_transactions_from_database(account_id, connection, as_of):
    account_statement = AccountStatement(account_id)
    cursor = connection.execute(
        "SELECT ID, DATE, POSTED_AT, LABEL, AMOUNT_CENTS FROM TRANSACTIONS")
    for identifier, date, posted_at, label, amount in cursor:
        posted = (datetime.datetime.fromisoformat(posted_at) if posted_at else
                  datetime.datetime.strptime(date, '%d/%m/%Y'))
        if posted <= as_of:
            account_statement.add(Operation(identifier, posted, label, amount / 100))
    return account_statement


def open_database_connection(account_id: int):
    database_dir = os.path.expanduser(config.get('Directories', 'database'))
    db_path = os.path.join(database_dir, 'account_' + str(account_id) + '.db')
    return sqlite3.connect(db_path)


def parse_file(filename: str, csv_output_mode: bool):
    if str(filename).lower().endswith('.ofx'):
        return parse_ofx(filename, csv_output_mode)
    else:
        raise ValueError("Invalid file format")


def process_statements(new_account_statements: [AccountStatement], dry_run_mode: bool, debug_mode: bool):
    rules = load_rules(config.get('Files', 'legitimacy_rules', fallback='legitimacy_rules.ini'))
    output_directory = config.get('Directories', 'output') if not dry_run_mode else None
    if not dry_run_mode and not output_directory.strip():
        raise ValueError('Missing output directory for transaction alerts')
    reporting_by_account = {}
    for new_account_statement in new_account_statements:
        with open_database_connection(new_account_statement.account_id) as connection:
            alerts = prepare_and_analyse_history(
                new_account_statement, connection, dry_run_mode, debug_mode, rules=rules)
            account_label = '{} ({})'.format(get_account_name(new_account_statement.account_id),
                                            get_masked_account_id(new_account_statement.account_id))
            reporting_by_account[new_account_statement.account_id] = [(account_label,) + row for row in alerts]
    if not dry_run_mode:
        reporting = [row for rows in reporting_by_account.values() for row in rows]
        report_path = export_alerts(reporting, output_directory)
        print('Transaction alerts: {} — {}'.format(len(reporting), report_path))


def print_transaction_alerts(account_id, alerts):
    summary = 'Transactions à examiner pour le compte {} "{}" : {}'.format(
        str(account_id), get_account_name(account_id), len(alerts))
    print("\n" + '\033[34m' + summary + '\033[0m' + "\n")
    for identifier, rule, date, label, amount, reasons in alerts:
        print('\033[91m' + '  {} | {} | Transaction {} | Règle {}'.format(
            datetime.datetime.fromisoformat(date).strftime('%d/%m/%Y %H:%M:%S'),
            format_amount(amount / 100), identifier, '\033[0m' + '"' + rule.replace("_", " ") + '"' ))
        print('\033[33m' + '    {}'.format(label) + '\033[0m')
        print('\033[91m' + '    Motifs : {}'.format(reasons) + '\033[0m')
        print("\n")


def prepare_and_analyse_history(new_statements: AccountStatement, connection, dry_run_mode: bool, debug_mode: bool, rules=None):
    alerts = []
    def check_legitimacy(database):
        alerts.extend(refresh_alerts(database, new_statements.account_id, rules))
    status = reconcile_import(new_statements, connection, dry_run=dry_run_mode,
                              after_reconcile=check_legitimacy if rules is not None else None)
    messages = {
        'baseline-unverified': 'Initial bank balance recorded; no reliable previous reference to verify it.',
        'reconciled': 'Balance reconciled exactly.',
        'reconciled-late': 'Balance reconciled including newly discovered backdated transactions.',
        'reconciled-backfill': 'Balance reconciled assuming older transactions were already included in the previous bank balance.',
        'historical-reconciled': 'Older statement reconciled with the latest bank balance.',
    }
    print(('Dry run: ' if dry_run_mode else '') + messages[status])
    if rules is not None:
        print_transaction_alerts(new_statements.account_id, alerts)
    if dry_run_mode:
        return alerts
    draw_stored_history(new_statements.account_id, connection, debug_mode)
    return alerts


def draw_stored_history(account_id, connection, debug_mode):
    latest = latest_observation(connection)
    as_of = datetime.datetime.fromisoformat(latest[0])
    whole_statements = read_transactions_from_database(account_id, connection, as_of)
    whole_statements.balance_date = as_of
    whole_statements.last_date = as_of.replace(hour=0, minute=0, second=0, microsecond=0)
    whole_statements.last_balance = latest[1] / 100
    analyse_operations(whole_statements, connection, debug_mode)


def replay_analysis(dry_run_mode=False, debug_mode=False):
    rules = load_rules(config.get('Files', 'legitimacy_rules', fallback='legitimacy_rules.ini'))
    directory = Path(config.get('Directories', 'database')).expanduser()
    if not directory.is_dir():
        raise ValueError('Répertoire des bases introuvable : ' + str(directory))
    output = config.get('Directories', 'output') if not dry_run_mode else None
    if not dry_run_mode and not output.strip():
        raise ValueError('Missing output directory for transaction alerts')
    databases = []
    for path in sorted(directory.glob('account_*.db')):
        if not path.is_file():
            continue
        account_id = path.stem[len('account_'):]
        try:
            get_account_config_key('Accounts', account_id)
        except configparser.NoOptionError:
            continue
        databases.append((account_id, path))
    if not databases:
        raise ValueError('Aucune base existante correspondant aux comptes configurés.')
    reporting = []
    for account_id, path in databases:
        print('Analyse sans import : {} ({})'.format(get_account_name(account_id), get_masked_account_id(account_id)))
        mode = 'ro' if dry_run_mode else 'rw'
        with closing(sqlite3.connect(path.resolve().as_uri() + '?mode=' + mode, uri=True)) as connection:
            try:
                # Replace alerts atomically without inserting banking data.
                with connection:
                    connection.execute('BEGIN' if dry_run_mode else 'BEGIN IMMEDIATE')
                    verify_stored_balance(connection)
                    alerts = refresh_alerts(connection, account_id, rules,
                                            persist=not dry_run_mode, update_matches=False)
                print_transaction_alerts(account_id, alerts)
                if not dry_run_mode:
                    Path(output).expanduser().mkdir(parents=True, exist_ok=True)
                    draw_stored_history(account_id, connection, debug_mode)
                label = '{} ({})'.format(get_account_name(account_id), get_masked_account_id(account_id))
                reporting.extend((label,) + row for row in alerts)
            except sqlite3.OperationalError as exc:
                raise ValueError('Impossible de relire {} : {}. Si le schéma est ancien, '
                                 'effectuer d’abord un import OFX pour le migrer.'.format(path.name, exc)) from exc
    if not dry_run_mode:
        report_path = export_alerts(reporting, output)
        print('Transaction alerts: {} — {}'.format(len(reporting), report_path))


def main(filename: str, dry_run_mode: bool, debug_mode: bool, csv_output_mode: bool):
    new_account_statements = parse_file(filename, csv_output_mode)
    process_statements(new_account_statements, dry_run_mode, debug_mode)


def parse_ofx(filename: str, csv_output_mode: bool):
    parsed_account_statements = []
    with open(filename, 'r', encoding="cp1252") as ofxFile:
        ofx = OfxParser.parse(ofxFile)
        for account in ofx.accounts:
            account_statement = AccountStatement(account.account_id)
            statement = account.statement
            print("\n" + '\033[34m' + "Account " + account.account_id + " \"" + get_account_name(account.account_id) + " ->| " + str(statement.end_date) +
                  "\": " + '\033[0m')
            if getattr(statement, 'discarded_entries', []):
                raise ValueError("OFX contains discarded transactions; import cancelled")
            account_statement.balance_date = getattr(statement, 'balance_date', None)
            if account_statement.balance_date is None:
                raise ValueError("OFX is missing the ledger balance date (LEDGERBAL/DTASOF)")
            account_statement.last_date = account_statement.balance_date.replace(
                hour=0, minute=0, second=0, microsecond=0)
            account_statement.last_balance = statement.balance
            print('\033[94m' + "Balance on " + account_statement.last_date.strftime("%d/%m/%Y") + ": "
                  + str(account_statement.last_balance) + " " + CURRENCY + '\033[0m')
            if len(statement.transactions) == 0:
                print("WARNING: No transaction in this file for account " + str(account.account_id) +
                      " - " + get_account_name(account.account_id))
            else:
                for transaction in statement.transactions:
                    operation = Operation(transaction.id,
                                          transaction.date,
                                          transaction.memo,
                                          transaction.amount)
                    print(operation.debug(csv_output_mode))
                    account_statement.add(operation)
            parsed_account_statements.append(account_statement)

    print()
    return parsed_account_statements

def process_import(filename: str, dry_run_mode: bool, debug_mode: bool, csv_output_mode: bool):
    main(filename, dry_run_mode, debug_mode, csv_output_mode)
    print()


def cli(argv=None):
    parser = argparse.ArgumentParser(description='Sans --import : rejouer les contrôles et graphiques, et remplacer les alertes sans importer de transactions.')
    parser.add_argument(IMPORT_FLAG, dest='import_file', nargs='?', const='',
                        metavar='FICHIER_OFX', help='Sans chemin : récupérer le dernier téléchargement.')
    parser.add_argument(DRY_RUN_FLAG, action='store_true')
    parser.add_argument(DEBUG_FLAG, action='store_true')
    parser.add_argument(CSV_OUTPUT_FLAG, action='store_true')
    args = parser.parse_args(argv)
    try:
        if not config.read('conf/properties.ini'):
            raise ValueError('Fichier conf/properties.ini introuvable.')
        filename = args.import_file
        if filename is None:
            replay_analysis(args.dry_run, args.debug)
            return 0
        if filename:
            filename = os.path.expanduser(filename)
            print_file_dates(filename)
        else:
            filename = collect_latest_ofx(config, dry_run=args.dry_run)
            if filename is None:
                return 0
        process_import(str(filename), args.dry_run, args.debug, args.csv_output)
        return 0
    except (OSError, ValueError, configparser.Error, re.error, sqlite3.Error) as exc:
        print('\033[91mErreur : {}\033[0m'.format(exc))
        return 1


if __name__ == "__main__":
    sys.exit(cli())
