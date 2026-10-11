"""SQLite schema creation, backed-up upgrades and atomic import transactions."""
from contextlib import closing, contextmanager
import datetime
from pathlib import Path
import re
import sqlite3
import tempfile

from money import cents


def _columns(connection):
    return {row[1]: row[2] for row in connection.execute('PRAGMA table_info(TRANSACTIONS)')}


def backup_before_upgrade(connection, dry_run=False):
    """Back up committed, on-disk databases before their first schema upgrade.

    SQLite's backup API includes committed WAL contents. A unique destination
    prevents a later failed/retried import from overwriting an earlier backup.
    In-memory databases and dry runs do not need persistent backups.
    """
    columns = _columns(connection)
    if dry_run or not columns:
        return None
    observations = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'BALANCE_OBSERVATIONS'").fetchone()
    alerts = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'TRANSACTION_ALERTS'").fetchone()
    if (columns.get('ID', '').upper() != 'INTEGER'
            and {'POSTED_AT', 'AMOUNT_CENTS', 'MATCHED_RULES'} <= columns.keys() and observations and alerts):
        return None
    filename = next(row[2] for row in connection.execute('PRAGMA database_list') if row[1] == 'main')
    if not filename:
        return None
    if connection.in_transaction:
        raise ValueError('Commit or roll back pending changes before upgrading this database')
    database = Path(filename)
    backup_directory = database.parent / 'backups'
    backup_directory.mkdir(exist_ok=True)
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%S')
    with tempfile.NamedTemporaryFile(prefix=database.name + '.pre-upgrade-' + stamp + '-',
                                     suffix='.db', dir=backup_directory, delete=False) as file:
        backup_path = Path(file.name)
    try:
        with closing(sqlite3.connect(str(backup_path))) as backup:
            connection.backup(backup)
            if backup.execute('PRAGMA integrity_check').fetchall() != [('ok',)]:
                raise ValueError('Backup integrity check failed; database upgrade cancelled')
    except Exception:
        backup_path.unlink(missing_ok=True)
        raise
    print('Database backup before upgrade: ' + str(backup_path))
    return backup_path


def _upgrade_transaction_ids(connection, columns):
    expected = {'ID', 'DATE', 'DATE_EPOCH', 'LABEL', 'AMOUNT', 'TAG'}
    if set(columns) != expected:
        raise ValueError('Unexpected legacy TRANSACTIONS columns; migration cancelled to preserve data')
    definition = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'TRANSACTIONS'").fetchone()[0]
    if re.search(r'\b(CHECK|UNIQUE|COLLATE|AUTOINCREMENT|DEFAULT|GENERATED|STRICT)\b',
                 definition, re.IGNORECASE):
        raise ValueError('Custom table constraints require a dedicated migration; upgrade cancelled')
    # Rebuilding a table referenced by another table can trigger cascades.
    tables = connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    for (table,) in tables:
        quoted = '"' + table.replace('"', '""') + '"'
        if connection.execute('PRAGMA foreign_key_list(' + quoted + ')').fetchall():
            raise ValueError('Foreign keys require a dedicated migration; automatic upgrade cancelled')
    objects = connection.execute('''SELECT sql FROM sqlite_master
        WHERE tbl_name = 'TRANSACTIONS' AND type IN ('index', 'trigger') AND sql IS NOT NULL''').fetchall()
    connection.execute('''CREATE TABLE TRANSACTIONS_UPGRADE (
        ID TEXT PRIMARY KEY NOT NULL, DATE TEXT NOT NULL,
        DATE_EPOCH INTEGER NOT NULL, LABEL TEXT, AMOUNT REAL, TAG TEXT)''')
    connection.execute('''INSERT INTO TRANSACTIONS_UPGRADE
        SELECT CAST(ID AS TEXT), DATE, DATE_EPOCH, LABEL, AMOUNT, TAG FROM TRANSACTIONS''')
    old_rows = connection.execute('''SELECT CAST(ID AS TEXT), DATE, DATE_EPOCH,
        LABEL, AMOUNT, TAG FROM TRANSACTIONS ORDER BY CAST(ID AS TEXT)''').fetchall()
    new_rows = connection.execute('''SELECT ID, DATE, DATE_EPOCH, LABEL, AMOUNT, TAG
        FROM TRANSACTIONS_UPGRADE ORDER BY ID''').fetchall()
    if old_rows != new_rows:
        raise ValueError('Transaction copy verification failed; migration cancelled')
    connection.execute('DROP TABLE TRANSACTIONS')
    connection.execute('ALTER TABLE TRANSACTIONS_UPGRADE RENAME TO TRANSACTIONS')
    return objects


def _prepare_schema(connection):
    connection.execute('''CREATE TABLE IF NOT EXISTS TRANSACTIONS (
        ID TEXT PRIMARY KEY NOT NULL, DATE TEXT NOT NULL,
        DATE_EPOCH INTEGER NOT NULL, LABEL TEXT, AMOUNT REAL, TAG TEXT,
        POSTED_AT TEXT, AMOUNT_CENTS INTEGER,
        MATCHED_RULES TEXT NOT NULL DEFAULT '[]')''')
    columns = _columns(connection)
    objects = []
    if columns['ID'].upper() == 'INTEGER':
        objects = _upgrade_transaction_ids(connection, columns)
        columns = _columns(connection)
    for name, kind in [('POSTED_AT', 'TEXT'), ('AMOUNT_CENTS', 'INTEGER'),
                       ('MATCHED_RULES', "TEXT NOT NULL DEFAULT '[]'")]:
        if name not in columns:
            connection.execute('ALTER TABLE TRANSACTIONS ADD COLUMN ' + name + ' ' + kind)
    for identifier, amount in connection.execute(
            'SELECT ID, AMOUNT FROM TRANSACTIONS WHERE AMOUNT_CENTS IS NULL').fetchall():
        connection.execute('UPDATE TRANSACTIONS SET AMOUNT_CENTS = ? WHERE ID = ?',
                           (cents(amount), identifier))
    connection.execute('''CREATE TABLE IF NOT EXISTS BALANCE_OBSERVATIONS (
        ID INTEGER PRIMARY KEY, AS_OF TEXT NOT NULL,
        BALANCE_CENTS INTEGER NOT NULL, KNOWN_TOTAL_CENTS INTEGER NOT NULL,
        STATUS TEXT NOT NULL, IMPORTED_AT TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)''')
    connection.execute('''CREATE TABLE IF NOT EXISTS TRANSACTION_ALERTS (
        TRANSACTION_ID TEXT NOT NULL, RULE_ID TEXT NOT NULL,
        TRANSACTION_DATE TEXT NOT NULL, LABEL TEXT NOT NULL,
        AMOUNT_CENTS INTEGER NOT NULL, REASONS TEXT NOT NULL,
        CHECKED_AT TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (TRANSACTION_ID, RULE_ID))''')
    # Restore triggers only after backfilling the new columns, so migration
    # does not fire application-specific update triggers on historical rows.
    for (sql,) in objects:
        connection.execute(sql)


@contextmanager
def import_transaction(connection, dry_run=False):
    """Back up before any writes, then migrate and import in one transaction."""
    backup_before_upgrade(connection, dry_run=dry_run)
    connection.execute('SAVEPOINT ofx_import')
    try:
        _prepare_schema(connection)
        yield
        if dry_run:
            connection.execute('ROLLBACK TO ofx_import')
        connection.execute('RELEASE ofx_import')
    except Exception:
        connection.execute('ROLLBACK TO ofx_import')
        connection.execute('RELEASE ofx_import')
        raise
