# accounting-graph
Scripts to store and graph personal bank accounts data

## accounts-analysis
 - import OFX files (or CSV files)
 - stores imported transactions into a SQLite table
 - graph the balance evolution over time
 - graph a comparison of the balance evolution between current month and the previous months

## Relancer les contrôles et graphiques sans import

```sh
python3 accounts-analysis.py

# Contrôles seuls, sans modifier les bases ni générer de fichiers
python3 accounts-analysis.py --dry-run
```

Sans `--import`, le programme analyse les bases `account_*.db` existantes dans
le répertoire `database`, pour les comptes déclarés dans la configuration.
Il recalcule les alertes avec les règles actuelles, remplace `TRANSACTION_ALERTS`
et régénère les graphiques et `transaction_alerts.csv` dans `output`.
Le remplacement des alertes est atomique pour chaque compte : un échec des
contrôles laisse ses anciennes alertes intactes.

Les transactions (y compris `MATCHED_RULES`) et les observations de solde restent
inchangées. Les correspondances des règles sont recalculées en mémoire pour les
contrôles. Aucun téléchargement n'est récupéré et aucune migration n'est lancée.
Une base ancienne ou sans référence de solde nécessite d'abord un import OFX.

Le contrôle du solde vérifie que les transactions stockées correspondent toujours
au total enregistré lors de la dernière observation. Sans nouveau relevé, il ne
peut pas comparer le résultat à un nouveau solde bancaire. Les graphiques utilisent
la date et le solde de la dernière observation disponible.

## Récupération automatique des téléchargements OFX

Ajouter les clés suivantes aux sections existantes de `conf/properties.ini` :

```ini
[Directories]
downloads = ~/Downloads
input = /Volumes/MonDisque/Comptes/OFX

[Files]
ofx_filename_regex = ^CA[0-9]{8}_[0-9]{6}\.ofx$
```

La regex correspond à un nom tel que `CA20261015_090958.ofx`. Elle s'applique au
nom complet, sans le chemin, et est sensible à la casse. `~` est accepté dans les
chemins. `input` doit être renseigné avant le premier import automatique.

```sh
# Dernier téléchargement correspondant, déplacement dans input, puis import
python3 accounts-analysis.py --import

# Simulation : analyse depuis Downloads, sans déplacement ni modifications de base
python3 accounts-analysis.py --import --dry-run

# Import explicite : fichier utilisé sur place, sans déplacement
python3 accounts-analysis.py --import /chemin/vers/releve.ofx
```

Le fichier choisi est celui dont la **date de modification est la plus récente**,
indépendamment de la date inscrite dans son nom. À date égale, le nom le plus grand
dans l'ordre lexical départage les candidats. Seuls les fichiers ordinaires
directement dans `downloads` sont examinés, sans sous-répertoires ni liens
symboliques. Les dates de modification et de création (si disponible), ainsi que
l'ancienneté, sont affichées avant toute opération. Les dates du relevé et du solde
continuent d'être affichées lors de la lecture OFX.

Si **le même nom existe déjà dans `input`**, le programme s'arrête avec un message,
sans écraser le fichier ni chercher un candidat plus ancien. Sans correspondance,
il s'arrête également sans importer. Un fichier archivé n'est pas forcément déjà
importé en base : cette détection repose uniquement sur sa présence dans `input`.

Le déplacement fonctionne aussi vers un autre volume : copie sans écrasement,
conservation de la date de modification, puis suppression de l'original après
réussite. Si la copie échoue ou si le téléchargement change pendant la copie,
l'original reste disponible. En cas d'échec ultérieur de l'analyse, l'OFX reste
archivé : utiliser la commande avec son chemin explicite pour relancer l'import.
`--dry-run` ne crée pas le répertoire `input` et ne déplace aucun fichier.

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

`BALANCE_OBSERVATIONS` stores the bank balance, its exact date, the known
transaction total and the reconciliation status at each import.
`TRANSACTION_ALERTS` stores the current transaction legitimacy report.
`CHECKPOINTS` is retained unchanged as historical data.
`TRANSACTIONS_UPGRADE` exists only while replacing the old integer-ID table; after
success it has become `TRANSACTIONS`, and after failure it is rolled back.

The first import establishes a new verification baseline without deleting any
history. `--dry-run` can preview the migration and reconciliation with no retained
database changes and no backup file. The backups are complete databases that can
be restored independently; keep a separate copy on another disk for protection
against loss of the drive containing both the database and its local backups.

## Surveillance des transactions

Le fichier **`conf/legitimacy_rules.ini`**, référencé par `[Files] legitimacy_rules`, contient une section
par règle. Les deux règles Bouygues et NYX du compte courant sont déjà actives.
Pour déplacer ce fichier, ajouter dans `conf/properties.ini` :

```ini
[Files]
legitimacy_rules = /chemin/vers/legitimacy_rules.ini
```

Les chemins relatifs sont résolus depuis le répertoire d'exécution, comme le
fichier de configuration principal. Le fichier de règles est lu en UTF-8.
Une configuration manquante ou invalide interrompt le traitement avant les
imports, au lieu de désactiver silencieusement les contrôles.

Exemple :

```ini
[telephone]
account = XXXXXXX3918
label_regex = ^(?:Prélèvement|PrÃ©lÃ¨vement) Bouygues Telecom 06[xX0-9]{5}144$
min_amount = 0
max_amount = 30
max_per_month = 1
```

| Option | Signification |
| --- | --- |
| `account` | Numéro de compte complet ou masqué : chaque `X` ou `x` remplace exactement un chiffre. |
| `label_regex` | Expression régulière Python, sensible à la casse, recherchée dans le libellé. Utiliser `^` et `$` pour imposer le libellé entier, ou `(?i)` pour ignorer la casse. |
| `amount` | Valeur absolue de la dépense en euros : `28.99` exige une transaction de −28,99 €. Les points et virgules décimales sont acceptés. |
| `min_amount`, `max_amount` | Bornes positives inclusives, à utiliser à la place de `amount`. `min_amount = 0` et `max_amount = 30` acceptent les transactions de −30 à 0 €. Une seule borne est possible. |
| `max_per_day`, `max_per_week`, `max_per_month`, `max_per_year` | Plafonds d'occurrences indépendants, entiers positifs ou nuls. Une option absente n'impose pas de plafond. |
| `start_date` | Date de démarrage inclusive (`YYYY-MM-DD`). Les transactions antérieures sont ignorées, y compris dans les compteurs. Sans cette option, tout l'historique est examiné. |
| `allowed_from`, `allowed_until` | Bornes de dates autorisées inclusives, au format `YYYY-MM-DD`. Les transactions correspondantes en dehors de ces dates sont signalées, pas ignorées. |
| `weekdays` | Jours autorisés séparés par des virgules : 1 = lundi, 7 = dimanche. |
| `month_days` | Jours autorisés du mois, séparés par des virgules, entre 1 et 31. |
| `enabled` | `false` désactive la règle ; `true` par défaut. |

Les montants de configuration doivent être positifs ou nuls : une valeur négative
est signalée comme erreur de configuration. Un crédit positif ne satisfait pas
une règle de montant de dépense, même si sa valeur absolue est dans l'intervalle.
Le signe bancaire original est conservé dans la base, la console et le CSV.
Une règle sans aucune borne de montant continue à contrôler seulement les dates
et les occurrences, quel que soit le signe de la transaction.

Pour limiter une règle aux habitudes courantes, ajouter par exemple
`start_date = 2026-10-01` dans sa section. Les transactions à partir du 1er octobre
inclus seront vérifiées. Celles de septembre ne produiront aucune alerte et ne
compteront pas, même dans les plafonds hebdomadaires ou annuels couvrant les deux
périodes. Les périodes restent calendaires ; les plafonds ne sont pas proratisés.
`allowed_from` reste différent : il signale une date interdite au lieu d'exclure
la transaction de l'analyse.

Une date commune peut être placée en tête du fichier, puis remplacée dans les
règles qui nécessitent une autre date :

```ini
[DEFAULT]
start_date = 2026-10-01
```

Au prochain import, les anciennes alertes exclues par cette date disparaissent
de la table et du CSV. Les transactions historiques restent intactes et continuent
à être utilisées pour la vérification du solde et les graphiques.

Les règles s'appliquent **après l'acceptation du solde**, sans bloquer un import
pour une simple alerte. Elles examinent **tout l'historique stocké des comptes
présents dans le fichier OFX**, à partir du `start_date` de chaque règle lorsqu'il
est défini, y compris au premier import servant de référence
de solde. Les règles d'un compte ne comptent jamais les transactions d'un autre.
Une transaction sans correspondance de libellé n'est pas signalée, sauf s'il
s'agit d'un prélèvement inattendu (voir ci-dessous).

Les compteurs utilisent des jours calendaires, des semaines ISO (lundi à
dimanche), des mois calendaires et des années civiles. Les dates suivent la
convention UTC utilisée par l'application pour `POSTED_AT` ; les anciennes
transactions utilisent leur champ `DATE`. Le comptage suit l'ordre chronologique,
puis l'identifiant en cas d'égalité : avec une limite de quatre par jour, la
cinquième occurrence et les suivantes sont signalées. Toutes les occurrences
correspondantes comptent, même si leur montant ou leur date est déjà anormal.

Chaque compte dispose d'une table `TRANSACTION_ALERTS`, contenant une ligne par
transaction et règle enfreinte, avec tous les motifs de cette règle. Elle est
recalculée à chaque import : une modification/suppression de règle corrige aussi
les anciennes alertes, et un réimport ne crée pas de doublons. La création de
cette table déclenche une sauvegarde avant migration pour les bases existantes.
L'historique des transactions et les tags ne sont pas modifiés par ce contrôle.

### Couverture des prélèvements

La colonne `TRANSACTIONS.MATCHED_RULES` contient les noms des règles applicables
sous forme de liste JSON, par exemple `["bouygues_telecom", "Prélèvements"]`.
Une liste vide `[]` signifie qu'aucune règle ne correspond. Les noms sont ceux
des sections du fichier INI. La correspondance repose sur le compte, le libellé
et le `start_date` d'une règle active : un montant ou une fréquence anormale ne
supprime pas cette classification et déclenche toujours son alerte habituelle.
La colonne est recalculée à chaque import ; les références à une règle supprimée,
renommée ou désactivée ne restent pas dans les transactions.

Après cette classification, tout libellé commençant par
`(PRELEVEMENT|PrÃ©lÃ¨vement|Prélèvement)` sans règle applicable produit une alerte
**« Prélèvement inattendu »**, sous l'identifiant réservé
`prélèvement_inconnu`. Cette alerte utilise la même table, la même sortie
console et le même CSV que les dépassements. Les autres opérations non classées
restent ignorées. Un compte sans aucune règle bénéficie aussi de ce contrôle.

La date de démarrage de cette vérification générale est celle de
`[DEFAULT] start_date`. Sans date globale, elle est la plus ancienne date de
démarrage des règles actives du compte, si elles en possèdent toutes une ; sinon
tout l'historique est vérifié. Une règle dont la date de démarrage est ultérieure
ne couvre pas un prélèvement situé dans cette période de surveillance, mais avant
sa propre activation. Les opérations avant la date générale ne génèrent pas
d'alertes de prélèvement inattendu.

**Une règle générale qui correspond à tous les prélèvements les classe tous
comme connus.** Pour détecter les créanciers inconnus, désactiver cette règle
générale et conserver des règles spécifiques aux créanciers attendus.

L'ajout de `MATCHED_RULES` passe par la migration avec sauvegarde préalable.
Les dates, montants, libellés, identifiants et tags historiques sont conservés.
`--dry-run` annule aussi la mise à jour de cette colonne.

À la fin du traitement, **`transaction_alerts.csv`** est écrit dans le répertoire
`[Directories] output`, avec les graphiques. Ce fichier regroupe les alertes des
comptes traités durant cette exécution et remplace le précédent rapport. Il est
en UTF-8 avec BOM, utilise `;` comme séparateur et la virgule décimale. Les comptes
sont masqués ; les libellés et identifiants de transactions restent présents.
S'il n'y a aucune alerte, le fichier contient seulement les en-têtes, évitant de
laisser un ancien rapport périmé. `--dry-run` calcule le nombre d'alertes mais ne
conserve ni modifications de table ni export CSV.

Run the regression suite with:

```sh
MPLBACKEND=Agg python3 -m unittest discover -s tests -v
```
