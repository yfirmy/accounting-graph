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

config = configparser.RawConfigParser()


class Operation:
    def __init__(self, operation_id, date, label, amount):
        self.id = operation_id
        self.date = date.replace(hour=0, minute=0, second=0, microsecond=0)
        self.label = label
        self.value = amount


class History:
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


def analyse_operations(history):
    balance, min_date, max_date = compute_balance_evolution(history)
    draw_balance_evolution(history.account_id, balance, min_date, max_date)
    balance_compared = compute_balance_compared(balance, history.last_date)
    draw_balance_comparison(history.account_id, balance_compared)


def draw_balance_evolution(account_id, balance, min_date, max_date):
    fig, axes = plt.subplots()
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
    plt.show()


def draw_balance_comparison(account_id, balance_compared):
    fig, axes = plt.subplots()
    for m in reversed(range(0, 12)):
        if m == 0:
            color = "red"
        else:
            color_intensity = 0.52 + (m * (1 / 28))  # 0.9 - (m * (1/24))
            color = (color_intensity, color_intensity, color_intensity)
        plt.plot(range(0, len(balance_compared[m])), balance_compared[m], color=color)
    plt.hlines(y=0, xmin=1, xmax=31, colors='grey', linestyles='--')
    axes.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=(0, 1, 2, 3, 4, 5, 6)))
    axes.xaxis.set_minor_locator(mdates.DayLocator())
    axes.grid(True)
    axes.set_title("Comparaison du solde - " + get_account_name(account_id) + " (" + str(account_id) + ")")
    axes.set_ylabel(r'Solde')
    plt.show()


def compute_balance_evolution(history):
    min_date, max_date = history.get_date_boundaries()
    balance = {}
    current_date = history.last_date
    current_balance = history.last_balance

    while current_date >= min_date:
        if current_date in history.operations:
            for operation in history.operations[current_date]:
                current_balance = current_balance - operation.value
        balance[current_date] = current_balance
        current_date = current_date - datetime.timedelta(days=1)

    print("Solde")
    for date1 in balance:
        print(date1.strftime("%d/%m/%Y") + ": " + str(balance[date1]))

    return balance, min_date, max_date


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


def read_transactions_from_database(account_id, connection):
    history = History(account_id)
    cursor = connection.execute("SELECT ID, DATE, DATE_EPOCH, LABEL, AMOUNT FROM TRANSACTIONS ORDER BY DATE_EPOCH DESC")
    for row in cursor:
        op = Operation(row[0], datetime.datetime.utcfromtimestamp(int(row[2])), row[3], float(row[4]))
        history.add(op)
    return history


def open_database_connection(account_id):
    return sqlite3.connect('db/account_' + str(account_id) + '.db')


def parse_file(filename):
    if filename.endswith("ofx"):
        return parse_ofx(filename)
    elif filename.endswith("csv"):
        return parse_csv(filename)
    else:
        raise ValueError("Invalid file format")


def process_history(new_histories):
    for new_history in new_histories:
        with open_database_connection(new_history.account_id) as connection:
            prepare_and_analyse_history(new_history, connection)


def prepare_and_analyse_history(new_history, connection):
    create_table_if_not_exists(connection)
    write_operations_in_database(new_history, connection)
    whole_history = read_transactions_from_database(new_history.account_id, connection)
    update_history_details(new_history, whole_history)
    analyse_operations(whole_history)


def update_history_details(new_history, whole_history):
    whole_history.last_date = new_history.last_date
    whole_history.last_balance = new_history.last_balance


def main(filename):
    new_histories = parse_file(filename)
    process_history(new_histories)


def parse_ofx(filename):
    histories = []
    with open(filename, 'r', encoding="cp1252") as ofxFile:
        ofx = OfxParser.parse(ofxFile)
        for account in ofx.accounts:
            history = History(account.account_id)
            statement = account.statement
            for transaction in statement.transactions:
                history.add(Operation(transaction.id,
                                      transaction.date,
                                      transaction.memo,
                                      transaction.amount))

            history.last_date = statement.end_date.replace(hour=0, minute=0, second=0, microsecond=0)
            history.last_balance = float(statement.balance)
            histories.append(history)

    return histories


def parse_csv(filename):
    histories = []
    history = History(0)
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


def create_table_if_not_exists(connection):
    connection.execute('''CREATE TABLE IF NOT EXISTS TRANSACTIONS
         (ID            INTEGER  PRIMARY KEY NOT NULL,
         DATE           TEXT     NOT NULL,
         DATE_EPOCH     INTEGER  NOT NULL,
         LABEL          TEXT,
         AMOUNT         REAL);''')


def print_usage_and_exit():
    print("usage: python3 accounts-analysis.py {} export_from_bank.ofx".format(IMPORT_FLAG))
    exit(1)


def process_import(filename):
    main(filename)


if __name__ == "__main__":
    if len(sys.argv) != 3 or sys.argv[1] != IMPORT_FLAG:
        print_usage_and_exit()
    try:
        config.read("conf/properties.ini")
    except Exception as e:
        print("Failed to load properties configuration file:", str(e))
    process_import(sys.argv[2])
