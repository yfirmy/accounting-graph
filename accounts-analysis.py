#!/usr/bin/python
# -*- coding: utf-8 -*-

import csv
import datetime
import re
import sqlite3
import sys
import configparser

import dateutil
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from ofxparse import OfxParser

MONTHS_IN_YEAR = 12
DAYS_IN_MONTH = 31
IMPORT_FLAG = "--import"
DRY_RUN_FLAG = "--dry-run"
DEBUG_FLAG = "--debug"
CURRENCY = "€"

config = configparser.RawConfigParser()


class Operation:
    def __init__(self, operation_id, date, label, amount):
        self.id = operation_id
        self.date = date.replace(hour=0, minute=0, second=0, microsecond=0)
        self.label = label
        self.value = amount

    def debug(self):
        return self.id + " " + str(self.date) + " " + "{:>8}".format(str(self.value)) + " \"" + self.label + "\""


class AccountStatement:
    def __init__(self, account_id):
        self.account_id = account_id
        self.operations = {}
        self.last_date = None
        self.last_balance = None

    def add(self, operation):
        if operation.date not in self.operations:
            self.operations[operation.date] = []
        self.operations[operation.date].append(operation)

    def get_date_boundaries(self):
        operations_dates = list(self.operations.keys())
        min_date = operations_dates[len(operations_dates) - 1]
        max_date = operations_dates[0]
        return min_date, max_date


def parse_double(ch):
    return float(ch.replace(',', '.').replace('\xa0', '')) if len(ch) > 0 else 0.0


def get_account_name(account_id):
    account_name = ""
    try:
        account_name = config.get('Accounts', str(account_id))
    except Exception as ex:
        print("Failed to retrieve the name of account " + str(account_id), str(ex))
    finally:
        return account_name if account_name else ""


def analyse_operations(history, connection, debug_mode: bool):
    (balance_over_time, min_date, max_date,
     min_balance, min_balance_date,
     max_balance, max_balance_date) = compute_balance_evolution(history, connection, debug_mode)
    draw_balance_evolution(history.account_id, balance_over_time, min_date, max_date,
                           min_balance, min_balance_date, max_balance, max_balance_date)
    balance_compared = compute_balance_compared(balance_over_time, history.last_date)
    draw_balance_comparison(history.account_id, balance_compared)


def draw_balance_evolution(account_id, balance, min_date, max_date,
                           min_balance, min_balance_date, max_balance, max_balance_date):

    last_balance = balance[next(iter(balance))]
    offset = (max_balance - min_balance) * 0.7 / 100.0

    fig, axes = plt.subplots()
    fig.set_figwidth(20)
    lists = sorted(balance.items())
    x, y = zip(*lists)
    axes.plot(x, y)
    axes.xaxis.set_major_locator(mdates.MonthLocator())
    for label in axes.get_xticklabels(which='major'):
        label.set(rotation=30, horizontalalignment='right')
    axes.grid(True)
    axes.set_title("Evolution du solde - " + get_account_name(account_id) + " (" + str(account_id) + ")")
    axes.set_ylabel(r'Solde')
    plt.hlines(y=0, xmin=min_date, xmax=max_date, colors='grey', linestyles='--')
    plt.plot(min_balance_date, min_balance, marker='o', color="blue")
    plt.text(min_balance_date, min_balance - offset, " " + "{:.2f}".format(min_balance) + " " + CURRENCY, color="blue",
             verticalalignment='top')
    plt.plot(max_balance_date, max_balance, marker='o', color="red")
    plt.text(max_balance_date, max_balance + offset, " " + "{:.2f}".format(max_balance) + " " + CURRENCY, color="red",
             verticalalignment='bottom')
    plt.plot(max_date, last_balance, marker='o', color='black')
    plt.text(max_date, last_balance + offset, " " + "{:.2f}".format(last_balance) + " " + CURRENCY, color="black")
    plt.show()


# Multi-Month Daily Bank Balance Trend Graph
# comparing daily bank balances across multiple months
def draw_balance_comparison(account_id, balance_compared):
    fig, axes = plt.subplots()
    fig.set_figwidth(20)
    for month_age in reversed(range(0, 12)):
        if month_age == 0:
            color = "red"
        else:
            color_intensity = 0.52 + (month_age * (1 / 28))  # 0.9 - (m * (1/24))
            color = (color_intensity, color_intensity, color_intensity)
        if month_age < len(balance_compared):
            plt.plot(range(0, len(balance_compared[month_age])), balance_compared[month_age], color=color)
    plt.hlines(y=0, xmin=1, xmax=31, colors='grey', linestyles='--')
    axes.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=(0, 1, 2, 3, 4, 5, 6)))
    axes.xaxis.set_minor_locator(mdates.DayLocator())
    axes.grid(True)
    axes.set_title("Comparaison du solde - " + get_account_name(account_id) + " (" + str(account_id) + ")")
    axes.set_ylabel(r'Solde')
    plt.show()


def compute_balance_evolution(account_statement, connection, debug_mode: bool):
    min_date, max_date = account_statement.get_date_boundaries()
    balance_over_time = {}
    current_date = account_statement.last_date
    current_balance = account_statement.last_balance
    min_balance = account_statement.last_balance
    min_balance_date = account_statement.last_date
    max_balance = account_statement.last_balance
    max_balance_date = account_statement.last_date

    while current_date >= min_date:
        if current_date in account_statement.operations:
            for operation in account_statement.operations[current_date]:
                current_balance = current_balance - operation.value
        balance_over_time[current_date] = current_balance

        if current_balance < min_balance:
            min_balance = current_balance
            min_balance_date = current_date
        if current_balance > max_balance:
            max_balance = current_balance
            max_balance_date = current_date

        current_date = current_date - datetime.timedelta(days=1)

    balance_health_check(account_statement, balance_over_time, connection)
    balance_debug(debug_mode, account_statement, balance_over_time)

    return (balance_over_time,
            min_date, account_statement.last_date,
            min_balance, min_balance_date,
            max_balance, max_balance_date)


def check_balance_in_checkpoints(date, balance, cur):
    cur.execute("SELECT * FROM CHECKPOINTS WHERE DATE_EPOCH = ?", (date.strftime('%s'),))
    row = cur.fetchone()
    if row is None:
        return True, None
    else:
        return (len(row) == 3 and float(row[2]) == balance), float(row[2])


def balance_health_check(acc_statement, balance_over_time, connection):
    print("Healthcheck for balance evolution of account " +
          str(acc_statement.account_id) + " - " + get_account_name(acc_statement.account_id))
    cur = connection.cursor()
    for date in balance_over_time:
        coherent_with_checkpoint, previous_balance = check_balance_in_checkpoints(date, balance_over_time[date], cur)
        if not coherent_with_checkpoint:
            print(date.strftime("%d/%m/%Y") + ": " + str(balance_over_time[date]) +
                  ": balance does not match previous checkpoint " + str(previous_balance))
            raise ValueError("Invalid balance in checkpoints")
    print("OK")


def balance_debug(debug_mode: bool, acc_statement, balance_over_time):
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


def write_operations_in_database(history, connection):
    for opDate in history.operations:
        for op in history.operations[opDate]:
            request = "INSERT OR IGNORE INTO TRANSACTIONS (ID, DATE, DATE_EPOCH, LABEL, AMOUNT) \
                VALUES (" + str(op.id) + ", '" + op.date.strftime("%d/%m/%Y") + "', " + op.date.strftime(
                '%s') + ", '" + op.label + "', " + str(op.value) + " )"
            connection.execute(request)
    connection.commit()


def search_operations_in_database(history, connection):
    print("Searching operations in database for account " + str(history.account_id) + " (" + get_account_name(
        history.account_id) + ")")
    cur = connection.cursor()
    for opDate in history.operations:
        for op in history.operations[opDate]:
            request = "SELECT * FROM TRANSACTIONS WHERE ID =" + str(op.id)
            cur.execute(request)
            row = cur.fetchone()
            if row is None:
                print("Operation " + str(op.id) + " is new: " + op.debug())
    print()


def read_transactions_from_database(account_id, connection):
    account_statement = AccountStatement(account_id)
    cursor = connection.execute("SELECT ID, DATE, DATE_EPOCH, LABEL, AMOUNT FROM TRANSACTIONS ORDER BY DATE_EPOCH DESC")
    for row in cursor:
        op = Operation(row[0], datetime.datetime.utcfromtimestamp(int(row[2])), row[3], float(row[4]))
        account_statement.add(op)
    return account_statement


def open_database_connection(account_id):
    return sqlite3.connect('db/account_' + str(account_id) + '.db')


def parse_file(filename):
    if filename.endswith("ofx"):
        return parse_ofx(filename)
    elif filename.endswith("csv"):
        return parse_csv(filename)
    else:
        raise ValueError("Invalid file format")


def process_history(new_histories, dry_run_mode: bool, debug_mode: bool):
    for new_history in new_histories:
        with open_database_connection(new_history.account_id) as connection:
            prepare_and_analyse_history(new_history, connection, dry_run_mode, debug_mode)


def prepare_and_analyse_history(new_history, connection, dry_run_mode: bool, debug_mode: bool):
    create_transactions_table_if_not_exists(connection)
    create_checkpoints_table_if_not_exists(connection)
    if dry_run_mode:
        search_operations_in_database(new_history, connection)
    else:
        write_operations_in_database(new_history, connection)
        whole_history = read_transactions_from_database(new_history.account_id, connection)
        update_history_details(new_history, whole_history)
        update_checkpoints(whole_history, connection)
        analyse_operations(whole_history, connection, debug_mode)


def update_history_details(new_history, whole_history):
    whole_history.last_date = new_history.last_date
    whole_history.last_balance = new_history.last_balance


def update_checkpoints(acc_statement, connection):
    last_balance = acc_statement.last_balance
    last_date = acc_statement.last_date
    request = "INSERT OR IGNORE INTO CHECKPOINTS (DATE_EPOCH, DATE, BALANCE) VALUES \
               (" + last_date.strftime('%s') + ", '" + last_date.strftime("%d/%m/%Y") + "', " + str(last_balance) + " )"
    connection.execute(request)
    connection.commit()


def main(filename, dry_run_mode: bool, debug_mode: bool):
    new_histories = parse_file(filename)
    process_history(new_histories, dry_run_mode, debug_mode)


def parse_ofx(filename):
    parsed_account_statements = []
    with open(filename, 'r', encoding="cp1252") as ofxFile:
        ofx = OfxParser.parse(ofxFile)
        for account in ofx.accounts:
            account_statement = AccountStatement(account.account_id)
            statement = account.statement
            print("\nAccount " + account.account_id + " \"" + get_account_name(account.account_id) + "\": ")
            if len(statement.transactions) == 0:
                print("WARNING: No transaction in this file for account " + str(account.account_id) +
                      " - " + get_account_name(account.account_id))
            else:
                for transaction in statement.transactions:
                    operation = Operation(transaction.id,
                                          transaction.date,
                                          transaction.memo,
                                          transaction.amount)
                    print(operation.debug());
                    account_statement.add(operation)

            account_statement.last_date = statement.end_date.replace(hour=0, minute=0, second=0, microsecond=0)
            account_statement.last_balance = float(statement.balance)
            parsed_account_statements.append(account_statement)

    print()
    return parsed_account_statements


def parse_csv(filename):
    histories = []
    history = AccountStatement(0)
    with open(filename, 'r', encoding="ISO 8859-1") as csvFile:
        account_reader = csv.reader(csvFile, delimiter=';', quotechar='"')
        pattern_last_balance = re.compile(r'Solde au ([0-3][0-9]\/[0-1][0-9]\/[1-2][0-9]{3}) ([\d+\xa0]*\d+,\d\d) \x80')
        pattern_operation = re.compile(r'[0-3][0-9]\/[0-1][0-9]\/[1-2][0-9]{3}')
        for row in account_reader:
            if len(row) == 1:
                match_last_balance = pattern_last_balance.match(row[0])
                if match_last_balance:
                    history.last_balance = float(match_last_balance.group(2).replace(',', '.').replace('\xa0', ''))
                    history.last_date = datetime.datetime.strptime(match_last_balance.group(1), '%d/%m/%Y').date()
            if len(row) >= 4:
                match_operation = pattern_operation.match(row[0])
                if match_operation:
                    transaction_date = datetime.datetime.strptime(row[0], '%d/%m/%Y').date()
                    debit = parse_double(row[2])
                    credit = parse_double(row[3])
                    transaction_amount = -debit if debit > 0.0 else credit
                    history.add(Operation(None, transaction_date, row[1], transaction_amount))
    histories.append(history)
    return histories


def create_transactions_table_if_not_exists(connection):
    connection.execute('''CREATE TABLE IF NOT EXISTS TRANSACTIONS
         (ID            INTEGER  PRIMARY KEY NOT NULL,
         DATE           TEXT     NOT NULL,
         DATE_EPOCH     INTEGER  NOT NULL,
         LABEL          TEXT,
         AMOUNT         REAL);''')


def create_checkpoints_table_if_not_exists(connection):
    connection.execute('''CREATE TABLE IF NOT EXISTS CHECKPOINTS
         (DATE_EPOCH     INTEGER  PRIMARY KEY NOT NULL,
         DATE            TEXT     NOT NULL,
         BALANCE         REAL);''')


def print_usage_and_exit():
    print("usage: python3 accounts-analysis.py {} export_from_bank.ofx".format(IMPORT_FLAG))
    exit(1)


def process_import(filename, dry_run_mode: bool, debug_mode: bool):
    main(filename, dry_run_mode, debug_mode)
    print()


if __name__ == "__main__":
    if len(sys.argv) >= 3 and sys.argv[1] == IMPORT_FLAG:
        dry_run = (len(sys.argv) >= 4 and sys.argv[3] == DRY_RUN_FLAG) or (
                    len(sys.argv) == 5 and sys.argv[4] == DRY_RUN_FLAG)
        debug = (len(sys.argv) >= 4 and sys.argv[3] == DEBUG_FLAG) or (len(sys.argv) == 5 and sys.argv[4] == DEBUG_FLAG)
        try:
            config.read("conf/properties.ini")
        except Exception as e:
            print("Failed to load properties configuration file:", str(e))
        process_import(sys.argv[2], dry_run, debug)
    else:
        print_usage_and_exit()
