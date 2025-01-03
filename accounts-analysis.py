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
import calendar

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from ofxparse import OfxParser

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
        self.id = operation_id
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
                min_date = operations_dates[len(operations_dates) - 1]
                max_date = operations_dates[0]
        return min_date, max_date

    def operations_count(self, date):
        if len(self.operations) > 0 and date in self.operations:
            return len(self.operations[date])
        else:
            return 0


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
    axes.set_title("Evolution du solde - " + get_account_name(account_id) + " (" + str(account_id) + ")")
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
    axes.set_title("Epargne par mois - " + get_account_name(account_id) + " (" + str(account_id) + ")")
    axes.xaxis.set_major_locator(mdates.MonthLocator())
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


def compute_balance_evolution(account_statement: AccountStatement, connection, debug_mode: bool):
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

    balance_debug(debug_mode, account_statement, balance_over_time)
    balance_health_check(account_statement, balance_over_time, connection)

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


def check_balance_in_checkpoints(date, expected_balance, expected_transaction_count, cur):
    cur.execute("SELECT * FROM CHECKPOINTS WHERE DATE_EPOCH = ?", (date.strftime('%s'),))
    row = cur.fetchone()
    if row is None:
        return True, None
    else:
        checked_balance = float(row[2])
        checked_transaction_count = row[3]
        eq_balance = abs(checked_balance - expected_balance) <= 0.00001
        eq_transaction_count = (checked_transaction_count is not None and (int(checked_transaction_count) == expected_transaction_count)) or checked_transaction_count is None
        lt_transaction_count =  checked_transaction_count is not None and (int(checked_transaction_count) < expected_transaction_count)
        incoherent_due_to_new_transactions = (not eq_transaction_count or not eq_balance) and lt_transaction_count
        if incoherent_due_to_new_transactions:
            print('\033[94m' + "balance mismatch on " + str(date) + " due to newly added transactions" + '\033[0m')
        return (eq_transaction_count and eq_balance) or lt_transaction_count, checked_balance


def balance_health_check(acc_statement: AccountStatement, balance_over_time, connection):
    print("Healthcheck for balance evolution of account " +
          str(acc_statement.account_id) + " - " + get_account_name(acc_statement.account_id))
    cur = connection.cursor()
    for date in balance_over_time:
        transaction_count = acc_statement.operations_count(date - datetime.timedelta(days=1))
        coherent_with_checkpoint, previous_balance = check_balance_in_checkpoints(date, balance_over_time[date], transaction_count, cur)
        if not coherent_with_checkpoint:
            print('\033[93m' + date.strftime("%d/%m/%Y") + ": " + str(balance_over_time[date]) +
                  ": balance does not match previous checkpoint " + str(previous_balance) + '\033[0m')
            raise ValueError("Invalid balance in checkpoints")
    print('\033[92m' + "OK" + '\033[0m')


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


def write_operations_in_database(history, connection):
    for opDate in history.operations:
        for op in history.operations[opDate]:
            request = "INSERT INTO TRANSACTIONS (ID, DATE, DATE_EPOCH, LABEL, AMOUNT) \
                VALUES (" + str(op.id) + ", '" + op.date.strftime("%d/%m/%Y") + "', " + op.date.strftime(
                '%s') + ", '" + op.label + "', " + str(op.value) + " )" \
             + " ON CONFLICT(ID) DO NOTHING"
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


def open_database_connection(account_id: int):
    return sqlite3.connect('db/account_' + str(account_id) + '.db')


def parse_file(filename: str, csv_output_mode: bool):
    if filename.endswith("ofx"):
        return parse_ofx(filename, csv_output_mode)
    elif filename.endswith("csv"):
        return parse_csv(filename)
    else:
        raise ValueError("Invalid file format")


def extract_savings(account_id: int, connection, tag: str):
    savings = 0
    if is_savings_account(account_id):
        request = ("select (select BALANCE from main.CHECKPOINTS order by DATE_EPOCH DESC LIMIT 1) - "
                   "(select sum(amount) from main.TRANSACTIONS where TAG = '") + tag + "') AS AVAILABLE"
        cur = connection.cursor()
        cur.execute(request)
        row = cur.fetchone()
        savings = row[0] if row and row[0] else 0
    return savings


def process_statements(new_account_statements: [AccountStatement], dry_run_mode: bool, debug_mode: bool, tag: str):
    savings = {}
    for new_account_statement in new_account_statements:
        with open_database_connection(new_account_statement.account_id) as connection:
            prepare_and_analyse_history(new_account_statement, connection, dry_run_mode, debug_mode)
            if is_savings_account(new_account_statement.account_id):
                savings[new_account_statement.account_id] = extract_savings(new_account_statement.account_id, connection, tag)
    report_savings(savings)


def report_savings(savings):
    print('\n' + '\033[33m' + "Savings summary:" + '\033[0m')
    total_savings = 0
    for savings_account in savings:
        total_savings += savings[savings_account] #if not None else 0)
        print("  Savings for account " + str(savings_account) + ": " + format_amount(savings[savings_account]))
    print('\n' + '\033[33m' + "Total savings: " + format_amount(total_savings) + '\033[0m')


def prepare_and_analyse_history(new_statements: AccountStatement, connection, dry_run_mode: bool, debug_mode: bool):
    create_transactions_table_if_not_exists(connection)
    create_checkpoints_table_if_not_exists(connection)
    if dry_run_mode:
        search_operations_in_database(new_statements, connection)
    else:
        write_operations_in_database(new_statements, connection)
        whole_statements = read_transactions_from_database(new_statements.account_id, connection)
        update_statements_details(new_statements, whole_statements)
        try:
            analyse_operations(whole_statements, connection, debug_mode)
            last_date_transactions_count = get_last_date_transactions_count(whole_statements, connection)
            update_checkpoints(whole_statements, last_date_transactions_count, connection)
        except ValueError as e:
            print('\033[91m' + "Error in analyse operations for account " + str(new_statements.account_id) +
                  " - " + get_account_name(new_statements.account_id) + ": " + str(e) + '\033[0m')


def update_statements_details(new_history: AccountStatement, whole_history: AccountStatement):
    whole_history.last_date = new_history.last_date
    whole_history.last_balance = new_history.last_balance

def get_last_date_transactions_count(acc_statement: AccountStatement, connection):
    request = ("SELECT COUNT(ID) FROM TRANSACTIONS WHERE DATE_EPOCH= " + acc_statement.last_date.strftime('%s'))
    cur = connection.cursor()
    cur.execute(request)
    row = cur.fetchone()
    count = row[0] if row and row[0] else 0
    return count

def update_checkpoints(acc_statement: AccountStatement, last_date_transactions_count: int, connection):
    last_balance = acc_statement.last_balance
    last_date = acc_statement.last_date
    request = ("INSERT INTO CHECKPOINTS (DATE_EPOCH, DATE, BALANCE, TRANSACTIONS_COUNT) VALUES \
               (" + last_date.strftime('%s') + ", '" + last_date.strftime("%d/%m/%Y") + "', "
                + str(last_balance) + ", " + str(last_date_transactions_count) + " )" \
                + " ON CONFLICT(DATE_EPOCH) DO NOTHING")
    connection.execute(request)
    connection.commit()


def main(filename: str, dry_run_mode: bool, debug_mode: bool, csv_output_mode: bool, tag: str):
    new_account_statements = parse_file(filename, csv_output_mode)
    process_statements(new_account_statements, dry_run_mode, debug_mode, tag)


def parse_ofx(filename: str, csv_output_mode: bool):
    parsed_account_statements = []
    with open(filename, 'r', encoding="cp1252") as ofxFile:
        ofx = OfxParser.parse(ofxFile)
        for account in ofx.accounts:
            account_statement = AccountStatement(account.account_id)
            statement = account.statement
            print("\n" + '\033[34m' + "Account " + account.account_id + " \"" + get_account_name(account.account_id) +
                  "\": " + '\033[0m')
            account_statement.last_date = statement.end_date.replace(hour=0, minute=0, second=0, microsecond=0)
            account_statement.last_balance = float(statement.balance)
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


def parse_csv(filename: str):
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
         AMOUNT         REAL,
         TAG            TEXT);''')


def create_checkpoints_table_if_not_exists(connection):
    connection.execute('''CREATE TABLE IF NOT EXISTS CHECKPOINTS
         (DATE_EPOCH        INTEGER  PRIMARY KEY NOT NULL,
         DATE               TEXT     NOT NULL,
         BALANCE            REAL,
         TRANSACTIONS_COUNT INTEGER);''')


def print_usage_and_exit():
    print("usage: python3 accounts-analysis.py {} export_from_bank.ofx".format(IMPORT_FLAG))
    exit(1)


def process_import(filename: str, dry_run_mode: bool, debug_mode: bool, csv_output_mode: bool, tag: str):
    main(filename, dry_run_mode, debug_mode, csv_output_mode, tag)
    print()


if __name__ == "__main__":
    if len(sys.argv) >= 3 and sys.argv[1] == IMPORT_FLAG:
        dry_run = (len(sys.argv) >= 4 and sys.argv[3] == DRY_RUN_FLAG) or (
                    len(sys.argv) == 5 and sys.argv[4] == DRY_RUN_FLAG)
        debug = (len(sys.argv) >= 4 and sys.argv[3] == DEBUG_FLAG) or (
                  len(sys.argv) == 5 and sys.argv[4] == DEBUG_FLAG)
        csv_output = (len(sys.argv) >= 4 and sys.argv[3] == CSV_OUTPUT_FLAG) or (
                 len(sys.argv) == 5 and sys.argv[4] == CSV_OUTPUT_FLAG)

        try:
            config.read("conf/properties.ini")
        except Exception as e:
            print("Failed to load properties configuration file:", str(e))
        process_import(sys.argv[2], dry_run, debug, csv_output, config.get("Savings tags", "exclude"))
    else:
        print_usage_and_exit()
