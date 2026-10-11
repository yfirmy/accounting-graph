# accounting-graph
Scripts to store and graph personal bank accounts data

## accounts-analysis
 - import OFX files (or CSV files)
 - stores imported transactions into a SQLite table
 - graph the balance evolution over time
 - graph a comparison of the balance evolution between current month and the previous months

## Balance verification

The ledger balance and its exact OFX date (`LEDGERBAL/DTASOF`) are recorded in
`BALANCE_OBSERVATIONS`. Transaction dates come from `DTPOSTED` (posting date,
not value date). Reconciliation uses integer cents and is separate from graphs.

Each observation also records the total of known transactions up to its date.
For the next import, the expected balance is the previous bank balance plus the
change in that total. This includes newly discovered backdated bank fees, while
ignoring duplicate transactions from overlapping weekly exports. Transactions
after the balance timestamp only affect subsequent balances.

If that check fails, an alternative is accepted only when excluding **all newly
imported transactions dated at or before the previous balance** explains the
difference exactly. This is reported as historical backfill, not an unconditional
exception. Mixed backfill and late corrections that cannot be reconciled are
rejected. Matching net amounts does not prove that every transaction is present:
offsetting omissions remain undetectable from balances alone.

An older statement must reconcile forward to the latest bank balance; it does
not replace the latest reference. Conflicting balances at the same timestamp,
changed dates or amounts for an existing transaction ID, and unexplained balance
differences fail the import. Inserts, schema migration and the new observations
are rolled back together for that account. Graphs are generated after validation;
a graph rendering failure does not undo an already accepted import.

**Upgrade:** the first successful import establishes an explicitly unverified
baseline. Legacy `CHECKPOINTS` are preserved but cannot be trusted as dated ledger
balances because the old importer used the statement end date and discarded the
time. Existing transactions and tags are retained, transaction IDs become text,
and cent amounts are added automatically. Legacy transaction dates are read from
`DATE` to avoid the previous local-time/UTC conversion problem. Old rows without
a posting time use the start of their recorded day: already known transactions
on the baseline day are treated as included, not counted again the next day.

`--dry-run` performs the same reconciliation and rolls back all database changes,
including schema upgrades. It does not generate graphs.

## Preserving existing account databases

Schema creation and migration live in `database_schema.py`; reconciliation stays
in `balance_checks.py`. No historical OFX re-import is needed.

Before upgrading each existing account database, the importer creates a unique
SQLite backup under `backups/` beside the database. It uses SQLite's backup API
(including committed WAL data) and checks the backup's integrity. The backup path
is printed. Failure to create or verify a backup stops the import before schema
changes. An already upgraded database does not get another migration backup.

The migration copies all six original transaction fields, verifies every copied
row, then switches tables inside the same transaction as the import. Dates,
labels, amounts and tags are retained; IDs change from integers to text. Indexes
and triggers are recreated, with triggers disabled during the cent backfill.
Unexpected extra columns, custom constraints or foreign keys stop automatic
migration rather than silently discarding data. A failed migration/import rolls
back to the original database, while the independent backup remains available.

There is only **one new permanent table**, `BALANCE_OBSERVATIONS`: it stores the
bank balance, its exact date, the known transaction total and the reconciliation
status at each import. `CHECKPOINTS` is retained unchanged as historical data.
`TRANSACTIONS_UPGRADE` exists only while replacing the old integer-ID table; after
success it has become `TRANSACTIONS`, and after failure it is rolled back.

The first import establishes a new verification baseline without deleting any
history. `--dry-run` can preview the migration and reconciliation with no retained
database changes and no backup file. The backups are complete databases that can
be restored independently; keep a separate copy on another disk for protection
against loss of the drive containing both the database and its local backups.

Run the regression suite with:

```sh
MPLBACKEND=Agg python3 -m unittest discover -s tests -v
```
