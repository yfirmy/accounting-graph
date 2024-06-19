#!/usr/bin/python
# -*- coding: utf-8 -*-

import csv
import datetime
import re
import sqlite3
import sys
import configparser

import dateutil
import dateutil.relativedelta
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


def is_savings_account(account_id):
    is_savings_account_param = False
    try:
        is_savings_account_param = config.getboolean('Savings accounts', str(account_id))
    except Exception as ex:
        print("Failed to retrieve the name of account " + str(account_id), str(ex))
    finally:
        return is_savings_account_param if is_savings_account_param else False


def analyse_operations(statements, connection, debug_mode: bool):
    (balance_over_time, min_date, max_date,
     min_balance, min_balance_date,
     max_balance, max_balance_date) = compute_balance_evolution(statements, connection, debug_mode)
    draw_balance_evolution(statements.account_id, balance_over_time, min_date, max_date,
                           min_balance, min_balance_date, max_balance, max_balance_date)
    if is_savings_account(statements.account_id):
        balance_derivative = compute_savings_derivative(balance_over_time, min_date, max_date)
        draw_savings_derivative(statements.account_id, balance_derivative, min_date, max_date)
    balance_compared = compute_balance_compared(balance_over_time, statements.last_date)
    draw_balance_comparison(statements.account_id, balance_compared)


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
    axes.set_title("Evolution du solde - " + get_account_name(account_id) + " (" + str(account_id) + ")")
    axes.set_ylabel(r'Solde')
    plt.hlines(y=0, xmin=min_date, xmax=max_date, colors='grey', linestyles='--')
    plt.plot(min_balance_date, min_balance, marker='x', color="blue")
    plt.text(min_balance_date, min_balance - offset, " " + "{:.2f}".format(min_balance) + " " + CURRENCY, color="blue",
             verticalalignment='top')
    plt.plot(max_balance_date, max_balance, marker='x', color="red")
    plt.text(max_balance_date, max_balance + offset, " " + "{:.2f}".format(max_balance) + " " + CURRENCY, color="red",
             verticalalignment='bottom')
    plt.plot(max_date, last_balance, marker='x', color='black')
    plt.text(max_date, last_balance + offset, " " + "{:.2f}".format(last_balance) + " " + CURRENCY, color="black",
             verticalalignment='top')
    plt.show()


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
    plt.text(x, y + offset, " " + label + "{:.2f}".format(y) + " " + CURRENCY + " ",
             color=text_color, horizontalalignment=h_alignment, verticalalignment=v_alignment)


def draw_savings_derivative(account_id, savings_derivative, min_date, max_date):
    fig, axes = plt.subplots()
    fig.set_figwidth(20)
    lists = sorted(savings_derivative.items())
    x, y = zip(*lists)
    savings_color = [{p<0: 'red', 0<=p<=2: 'orange', p>2: 'green'}[True] for p in y]
    axes.bar(x, y, width=8.0, color=savings_color)
    axes.set_title("Epargne par mois - " + get_account_name(account_id) + " (" + str(account_id) + ")")
    axes.xaxis.set_major_locator(mdates.MonthLocator())
    #axes.xaxis.set_minor_locator(mdates.MonthLocator())
    axes.grid(True)
    axes.set_ylabel(r'Epargne')
    plt.hlines(y=0, xmin=min_date, xmax=max_date, colors='grey', linestyles='--')
    for item in savings_derivative:
        label = "+" if savings_derivative[item] > 0 else "" if savings_derivative[item] < 0 else ""
        color = "green" if savings_derivative[item] > 0 else "red" if savings_derivative[item] < 0 else "black"
        vertical_alignment = "bottom" if savings_derivative[item] >= 0 else "top"
        spot_value(item, savings_derivative[item], "", color, color, label, "center", plt, vertical_alignment)
    plt.show()


def draw_balance_comparison(account_id, balance_compared):
    min_grey_intensity = 0.85
    max_grey_intensity = 0.35
    a = (max_grey_intensity - min_grey_intensity) / (len(balance_compared) - 1)
    fig, axes = plt.subplots()
    fig.set_figwidth(20)
    for month_age in reversed(range(0, len(balance_compared))):
        if month_age == 0:
            color = "red"
        else:
            color_intensity = max_grey_intensity - month_age * a
            color = (color_intensity, color_intensity, color_intensity)
        if month_age < len(balance_compared):
            plt.plot(range(1, len(balance_compared[month_age])), balance_compared[month_age][1:], color=color)
    plt.hlines(y=0, xmin=1, xmax=31, colors='grey', linestyles='--')
    axes.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=(0, 1, 2, 3, 4, 5, 6)))
    axes.xaxis.set_minor_locator(mdates.DayLocator())
    axes.grid(True)
    axes.set_title("Comparaison du solde - " + get_account_name(account_id) + " (" + str(account_id) + ")")
    axes.set_ylabel(r'Solde')

    last_month = 0 if count_non_none(balance_compared[0]) >= 1 else 1
    last_day, last_balance = last_non_none(balance_compared[last_month])

    spot_value(last_day, last_balance, "x", "red", "red", "", "left", plt, "bottom")

    min_value_same_day, max_value_same_day, mean_value_same_day = stats_same_day(balance_compared, last_day)

    spot_value(last_day, min_value_same_day, "+", "grey", "darkgrey", "min: ", "right", plt, "bottom")
    spot_value(last_day, max_value_same_day, "+", "grey", "darkgrey", "max: ", "right", plt, "bottom")
    spot_value(last_day, mean_value_same_day, "+", "grey", "darkgrey", "moy: ", "right", plt, "bottom")

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


def compute_savings_derivative(balance_over_time, min_date, max_date):
    savings_derivative = {}
    timestamps = list(balance_over_time.keys())
    first_timestamp = timestamps[len(timestamps)-1] #.replace(day=10)
    last_timestamp = timestamps[0]
    timespan = dateutil.relativedelta.relativedelta(last_timestamp, first_timestamp);
    months_list = [first_timestamp + dateutil.relativedelta.relativedelta(months=x)
                     for x in range(0, timespan.years*12 + timespan.months + 1)]
    previous_timestamp = None

    for timestamp in months_list:
        if (previous_timestamp is not None and
                timestamp in balance_over_time and
                previous_timestamp in balance_over_time):
            savings_derivative[timestamp] = balance_over_time[timestamp] - balance_over_time[previous_timestamp]
            #print(timestamp.strftime("%d/%m/%Y") + ": " + str(savings_derivative[timestamp]))
        previous_timestamp = timestamp

    return savings_derivative


def check_balance_in_checkpoints(date, balance, cur):
    cur.execute("SELECT * FROM CHECKPOINTS WHERE DATE_EPOCH = ?", (date.strftime('%s'),))
    row = cur.fetchone()
    if row is None:
        return True, None
    else:
        return len(row) == 3 and abs(float(row[2]) - balance) <= 0.00001, float(row[2])


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


def process_statements(new_account_statements, dry_run_mode: bool, debug_mode: bool):
    for new_account_statement in new_account_statements:
        with open_database_connection(new_account_statement.account_id) as connection:
            prepare_and_analyse_history(new_account_statement, connection, dry_run_mode, debug_mode)


def prepare_and_analyse_history(new_history, connection, dry_run_mode: bool, debug_mode: bool):
    create_transactions_table_if_not_exists(connection)
    create_checkpoints_table_if_not_exists(connection)
    if dry_run_mode:
        search_operations_in_database(new_history, connection)
    else:
        write_operations_in_database(new_history, connection)
        whole_history = read_transactions_from_database(new_history.account_id, connection)
        update_history_details(new_history, whole_history)
        analyse_operations(whole_history, connection, debug_mode)
        update_checkpoints(whole_history, connection)


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
    new_account_statements = parse_file(filename)
    process_statements(new_account_statements, dry_run_mode, debug_mode)


def parse_ofx(filename):
    parsed_account_statements = []
    with open(filename, 'r', encoding="cp1252") as ofxFile:
        ofx = OfxParser.parse(ofxFile)
        for account in ofx.accounts:
            account_statement = AccountStatement(account.account_id)
            statement = account.statement
            print("\nAccount " + account.account_id + " \"" + get_account_name(account.account_id) + "\": ")
            account_statement.last_date = statement.end_date.replace(hour=0, minute=0, second=0, microsecond=0)
            account_statement.last_balance = float(statement.balance)
            print("Balance on " + account_statement.last_date.strftime("%d/%m/%Y") + ": "
                  + str(account_statement.last_balance) + " " + CURRENCY)
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
            parsed_account_statements.append(account_statement)

    print()
    return parsed_account_statements


def parse_csv(filename):
    parsed_account_statements = []
    account_statement = AccountStatement(0)
    with open(filename, 'r', encoding="ISO 8859-1") as csvFile:
        account_reader = csv.reader(csvFile, delimiter=';', quotechar='"')
        pattern_last_balance = re.compile(r'Solde au ([0-3][0-9]\/[0-1][0-9]\/[1-2][0-9]{3}) ([\d+\xa0]*\d+,\d\d) \x80')
        pattern_operation = re.compile(r'[0-3][0-9]\/[0-1][0-9]\/[1-2][0-9]{3}')
        for row in account_reader:
            if len(row) == 1:
                match_last_balance = pattern_last_balance.match(row[0])
                if match_last_balance:
                    account_statement.last_balance = float(match_last_balance.group(2).replace(',', '.').replace('\xa0', ''))
                    account_statement.last_date = datetime.datetime.strptime(match_last_balance.group(1), '%d/%m/%Y').date()
            if len(row) >= 4:
                match_operation = pattern_operation.match(row[0])
                if match_operation:
                    transaction_date = datetime.datetime.strptime(row[0], '%d/%m/%Y').date()
                    debit = parse_double(row[2])
                    credit = parse_double(row[3])
                    transaction_amount = -debit if debit > 0.0 else credit
                    account_statement.add(Operation(None, transaction_date, row[1], transaction_amount))
    parsed_account_statements.append(account_statement)
    return parsed_account_statements


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
