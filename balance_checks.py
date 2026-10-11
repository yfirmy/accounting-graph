"""Atomic OFX reconciliation, independent of graph reconstruction.

OFX dates supplied by ofxparse are naive UTC datetimes. Legacy DATE strings
are retained because the old DATE_EPOCH values used the machine's timezone.
"""
import datetime
from money import cents
from database_schema import import_transaction


def timestamp(value):
    if value is None:
        raise ValueError("Missing OFX balance or posting date")
    if value.tzinfo is not None:
        value = value.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    return value.isoformat(timespec='microseconds')


def total_at(connection, as_of):
    # Legacy operations only have a day. Treat already known operations on the
    # baseline day as included; otherwise they would be counted again tomorrow.
    # The initial baseline makes no claim about their intraday position.
    total = 0
    for date, posted_at, amount in connection.execute(
            'SELECT DATE, POSTED_AT, AMOUNT_CENTS FROM TRANSACTIONS'):
        effective = posted_at or timestamp(datetime.datetime.strptime(date, '%d/%m/%Y'))
        if effective <= as_of:
            total += amount
    return total


def latest_observation(connection):
    return connection.execute('''SELECT AS_OF, BALANCE_CENTS, KNOWN_TOTAL_CENTS
        FROM BALANCE_OBSERVATIONS ORDER BY AS_OF DESC, ID DESC LIMIT 1''').fetchone()


def insert_operations(history, connection):
    inserted = []
    for operations in history.operations.values():
        for operation in operations:
            identifier = str(operation.id)
            posted_at = timestamp(operation.posted_at)
            amount = cents(operation.value)
            existing = connection.execute(
                'SELECT DATE, POSTED_AT, AMOUNT_CENTS FROM TRANSACTIONS WHERE ID = ?',
                (identifier,)).fetchone()
            if existing is not None:
                date_matches = (existing[1] == posted_at if existing[1] else
                                existing[0] == operation.date.strftime('%d/%m/%Y'))
                if existing[2] != amount or not date_matches:
                    raise ValueError("An existing transaction has a changed amount or posting date")
                continue
            connection.execute('''INSERT INTO TRANSACTIONS
                (ID, DATE, DATE_EPOCH, LABEL, AMOUNT, POSTED_AT, AMOUNT_CENTS)
                VALUES (?, ?, ?, ?, ?, ?, ?)''',
                (identifier, operation.date.strftime('%d/%m/%Y'),
                 int(operation.date.replace(tzinfo=datetime.timezone.utc).timestamp()),
                 operation.label, amount / 100, posted_at, amount))
            inserted.append((posted_at, amount))
    return inserted


def observe(connection, as_of, balance, total, status):
    connection.execute('''INSERT INTO BALANCE_OBSERVATIONS
        (AS_OF, BALANCE_CENTS, KNOWN_TOTAL_CENTS, STATUS) VALUES (?, ?, ?, ?)''',
        (as_of, balance, total, status))


def reconcile_import(history, connection, dry_run=False, after_reconcile=None):
    """Validate and persist one account, or roll back the entire attempted import.

    Total snapshots measure newly known amounts, including late postings. An
    exact alternative excluding *all* newly discovered older postings supports
    historical backfill. Mixed ambiguous cases fail instead of being guessed.
    """
    with import_transaction(connection, dry_run=dry_run):
        as_of = timestamp(history.balance_date)
        balance = cents(history.last_balance)
        previous = latest_observation(connection)
        inserted = insert_operations(history, connection)
        total = total_at(connection, as_of)
        if previous is None:
            status = 'baseline-unverified'
        elif as_of < previous[0]:
            # Older exports may extend history, but must reconcile forward to
            # the latest bank observation, which remains the active anchor.
            latest_total = total_at(connection, previous[0])
            expected = balance + latest_total - total
            if expected != previous[1]:
                raise ValueError("Older statement cannot be reconciled with the latest balance; "
                                 "no transactions imported")
            status = 'historical-reconciled'
        else:
            expected = previous[1] + total - previous[2]
            older_amount = sum(amount for posted, amount in inserted if posted <= previous[0])
            if balance == expected:
                status = 'reconciled-late' if any(
                    posted <= previous[0] for posted, _ in inserted) else 'reconciled'
            elif older_amount and balance == expected - older_amount:
                status = 'reconciled-backfill'
            else:
                raise ValueError(
                    "Balance mismatch: expected {:.2f}, received {:.2f}, difference {:+.2f}; "
                    "no transactions imported".format(
                        expected / 100, balance / 100, (balance - expected) / 100))
            # The same bank snapshot cannot change its balance silently.
            if as_of == previous[0] and balance != previous[1]:
                raise ValueError("Conflicting bank balances at the same timestamp")
        observe(connection, as_of, balance, total, status)
        if previous is not None and as_of < previous[0]:
            observe(connection, previous[0], previous[1], latest_total, 'history-extended')
        if after_reconcile is not None:
            after_reconcile(connection)
        return status
