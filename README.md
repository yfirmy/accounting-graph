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
contrôles. Aucun téléchargement n'est récupéré et aucune migration n'est lancée
sans import.
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
baseline. Existing transactions and tags are retained, transaction IDs become text,
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

## Envoi des nouvelles dépenses à Splitwise

L'intégration est facultative et désactivée par défaut. Elle intervient uniquement
lors d'un import OFX, après la validation du solde et des contrôles locaux et après
l'enregistrement des transactions. Elle utilise exclusivement le partage
**« j'ai tout payé, partage égal entre tous les membres du groupe »**. Le payeur
est l'utilisateur de la clé API. Les montants sont en EUR ; un relevé d'une autre
devise ou sans devise déclarée est refusé lorsque l'intégration est activée.

Copier `examples/splitwise_rules.ini` vers `conf/splitwise_rules.ini`, puis ajouter
à la section existante de `conf/properties.ini` :

```ini
[Files]
splitwise_rules = conf/splitwise_rules.ini
```

Renseigner dans ce nouveau fichier le `group_id` Splitwise, les masques des
comptes et les règles, puis passer `enabled` à `true`. Un `group_id` différent
peut être donné par règle. L'identifiant du groupe se trouve dans son URL sur le
site Splitwise ou via `get_groups` de l'API.

Créer une application et une clé personnelle depuis
[Splitwise Apps](https://secure.splitwise.com/apps), puis définir la variable
d'environnement `SPLITWISE_API_KEY` dans le terminal qui lance l'application.
Ne pas mettre la clé dans les fichiers versionnés. Le nom de cette variable est
configurable avec `token_env`. Sans clé, l'import bancaire peut aboutir et les
envois restent en attente. Si le chemin de règles est renseigné mais le fichier
est absent ou invalide, l'import s'arrête avant de modifier les données.

Une règle correspond à un compte (avec `X` pour masquer chaque chiffre), une
regex recherchée n'importe où dans le libellé et des jours de semaine
(`1=lundi`, ..., `7=dimanche`). `start_date`, facultatif, exclut les opérations
plus anciennes. **Par défaut, la date utilisée est la date de comptabilisation `DTPOSTED`
disponible dans l'OFX, suivant la convention UTC de l'application. Ce n'est pas
une date de valeur distincte ni nécessairement le jour de l'achat.** Les jours de
semaine doivent donc être choisis en tenant compte des dates présentes dans les
relevés de la banque, sauf si l'extraction de la date d'achat ci-dessous est activée.

Pour les paiements par carte dont le libellé se termine par une date `JJ/MM`,
ajouter dans chaque règle concernée :

```ini
purchase_date_taken = end_of_label
```

Une regex commune au programme extrait le `JJ/MM` final, avec des espaces finaux
éventuels. Aucune regex n'est à fournir dans les règles. Sans cette option, ou avec
`purchase_date_taken = posting_date`, la date de comptabilisation `DTPOSTED` est
utilisée, même si le libellé contient une autre date. Le terme `posting_date` est
préféré à `value_date`, qui désigne une date de valeur distincte. Les seules valeurs
acceptées sont `posting_date` et `end_of_label`. L'ancien paramètre
`purchase_date_regex` est remplacé par `purchase_date_taken = end_of_label`.

L'année est déduite
de la date bancaire de la transaction, jamais de la date d'exécution : un achat
`31/12` comptabilisé le 3 janvier 2027 devient le 31 décembre 2026. Cela fonctionne
aussi pour les relevés anciens et les années bissextiles. La date d'achat ne peut
pas être postérieure à la date bancaire. Le délai maximal autorisé entre achat et
comptabilisation est fixé globalement à **six jours inclus**, via la constante
`PURCHASE_DATE_MAX_DELAY_DAYS` dans `splitwise_sync.py`. Il n'est pas configurable
par règle ; l'ancien paramètre `purchase_date_max_delay_days` doit être retiré des INI.

Cette date d'achat sert à `weekdays`, `start_date`, à la date envoyée et à la
recherche des doublons. Activer les mêmes options sur les règles complémentaires
d'un même commerçant pour qu'elles utilisent toutes le jour d'achat.
Si une règle correspond au compte et au libellé mais que sa date d'achat est
absente, invalide ou hors délai, l'import du compte est annulé avec un message
explicite, avant tout envoi pour ce compte. Il n'y a pas de remplacement silencieux
par la date bancaire. Les opérations comptabilisées avant `start_date` sont ignorées.
Les dates stockées dans `TRANSACTIONS` et les contrôles de solde restent inchangés.
Les dépenses déjà envoyées ou en attente ne sont pas recalculées.

Les règles sont évaluées dans l'ordre du fichier ; la première correspondant au
compte, au libellé, au jour et à la date de démarrage s'applique. Sans règle, la
transaction est « non soumise ». Les crédits et montants nuls sont également
exclus. Les valeurs `min_amount` et `max_amount` sont positives, en euros, avec
bornes incluses : `4` à `14` correspond aux débits de `-14` à `-4`.

- `action = auto` : soumission automatique si le montant est dans l'intervalle.
  `max_amount` est obligatoire pour éviter une autorisation sans plafond.
- `action = confirm` : confirmation interactive obligatoire.
- `action = skip` : non soumise.
- `outside_amount = confirm` (par défaut) ou `skip` : comportement hors intervalle.

L'exemple fourni soumet automatiquement une boulangerie le dimanche entre 4 et
14 euros, demande confirmation à 22 euros et exclut le lundi. `description` est
un libellé neutre choisi dans la configuration, par exemple « Boulangerie ». Le
libellé bancaire brut, le numéro de compte et l'identifiant OFX ne sont jamais
copiés vers Splitwise. La note indique « Cette dépense a été ajoutée automatiquement
à partir du relevé de compte. », suivie d'un marqueur aléatoire de suivi,
sans information personnelle. Ne pas mettre d'information personnelle dans les
descriptions configurées.

La date de la dépense envoyée à Splitwise est la date d'achat extraite si configurée,
sinon celle de la transaction du relevé,
jamais la date d'import ni celle d'une reprise d'envoi. Elle est transmise à midi
UTC pour conserver le même jour dans l'affichage Europe/Paris.

Chaque règle peut aussi définir `category_id`, un entier positif correspondant
à une **sous-catégorie** Splitwise (et donc à son icône), par exemple :

```ini
# Dans la section de la règle concernée ; remplacer par l'identifiant souhaité.
category_id = 15
```

La liste des identifiants et noms est disponible via
[get_categories](https://secure.splitwise.com/api/v3.0/get_categories) : utiliser
un identifiant contenu dans `subcategories`, pas celui de la catégorie parente.
Le numéro ci-dessus est illustratif. Sans `category_id`, le champ est omis et
Splitwise utilise sa catégorie par défaut. Ce réglage s'applique aux nouvelles
transactions évaluées ; les dépenses déjà envoyées ou en attente conservent leur
contenu enregistré.

Avant chaque création, l'application consulte toutes les pages des dépenses
accessibles à l'utilisateur autour de cette date, tous groupes confondus. Une
dépense active de même jour, même devise et même montant total impose une
confirmation, même pour une règle automatique. Les remboursements et dépenses
supprimées ne sont pas considérés comme des doublons potentiels.

Dans le terminal :

- `create` autorise la création, y compris si une dépense similaire existe ;
- le numéro proposé rattache la transaction à une dépense existante sans la recréer ;
- `skip` exclut définitivement cet envoi ;
- Entrée ou `later` reporte la décision.

Sans terminal interactif, aucune confirmation n'est supposée : les transactions
qui en exigent une restent en attente. Les dépenses automatiques sans doublon
peuvent être envoyées sans interaction.

La table `SPLITWISE_EXPORTS` conserve, pour chaque nouvelle transaction évaluée,
la règle et la catégorie (`auto`, `confirm`, `skip`), le contenu prévu pour l'API,
un marqueur aléatoire, l'état de traitement et l'identifiant de dépense Splitwise.
Elle est ajoutée au prochain import avec la sauvegarde préalable habituelle ;
les transactions historiques restent intactes et ne sont pas envoyées rétroactivement.
Un réimport d'opérations déjà connues ne crée aucune nouvelle candidature.

Les états sont `pending` (en attente), `sending` (envoi commencé), `uncertain`
(résultat inconnu), `submitted` (création confirmée), `linked` (rattachée à une
saisie existante) et `skipped` (exclue). Une dépense soumise ou rattachée n'est plus
envoyée. Le marqueur permet de retrouver une création après une coupure réseau.
Si le résultat d'un envoi reste incertain et qu'aucun marqueur n'est retrouvé,
l'application demande une confirmation avant toute nouvelle tentative. Un verrou
empêche deux instances utilisant le même répertoire de bases d'envoyer simultanément.
L'API ne fournit pas ici de garantie d'idempotence : une saisie concurrente sur
iPhone, un retard de visibilité ou une modification de date/note peut encore
nécessiter une vérification manuelle.

Un échec réseau ne remet pas en cause un import bancaire déjà validé. Pour
reprendre les envois en attente, réimporter explicitement le même fichier OFX :

```sh
python3 accounts-analysis.py --import /chemin/vers/releve.ofx
```

La reprise concerne les comptes présents dans ce fichier. Les envois en attente
conservent la règle, le groupe et le contenu fixés lors de leur premier import :
modifier les règles affecte les nouvelles transactions, pas les candidatures
existantes. Désactiver l'intégration suspend aussi les reprises.

Sans `--import`, aucun appel Splitwise n'est effectué. Avec `--dry-run`, les
catégories sont évaluées localement, mais aucun appel réseau, aucune confirmation
ni aucun suivi persistant n'est effectué ; les doublons distants ne sont donc pas
vérifiés dans ce mode.

Référence : [documentation officielle de l'API Splitwise](https://dev.splitwise.com/),
notamment `get_expenses` et `create_expense`. Les erreurs renvoyées dans une réponse
HTTP 200 sont également contrôlées.
En cas d'échec, le diagnostic affiche l'opération API concernée, le type et le
message de l'erreur, ainsi que le statut et le corps de la réponse HTTP lorsqu'ils
sont disponibles. La clé API est masquée dans ces diagnostics.
Les connexions HTTPS utilisent explicitement les certificats racines de `certifi`
(déjà inclus dans les dépendances), notamment pour les installations Python macOS
dont le magasin de certificats par défaut n'est pas configuré. La validation du
certificat et du nom du serveur reste activée.

## Exceptions Splitwise : achats personnels dans une dépense de groupe

Le fichier `conf/splitwise_exceptions.ini` permet de retirer une part personnelle
ponctuelle sans changer les règles habituelles ni le montant bancaire. Il est
référencé dans `conf/properties.ini` :

```ini
[Files]
splitwise_exceptions = conf/splitwise_exceptions.ini
```

Exemple, si la règle porte le nom `[carrefour]` dans `splitwise_rules.ini` :

```ini
[courses_24_octobre]
account = XXXXXXX3918
rule = carrefour
purchase_date = 2026-10-24
personal_amount = 10
```

Aucun montant total ni regex supplémentaire n'est nécessaire. `account` est
obligatoire : le numéro du compte avec un `X` (ou `x`) pour chaque chiffre masqué.
Le masque doit avoir la même longueur que le numéro de compte. Seuls les comptes
correspondants sont examinés pour cette exception. `rule` est le nom
exact, sensible à la casse, de la section d'une règle active. `purchase_date`
est le jour envoyé à Splitwise : date d'achat extraite du libellé si configurée,
sinon date bancaire. `personal_amount` est un montant positif en euros (virgule
ou point accepté). Le nom de l'exception doit rester stable. Une seule exception
est autorisée pour un même compte, une même règle et une même date. Deux exceptions
de même règle et date peuvent viser des comptes distincts ; des masques qui se
chevauchent sont refusés.

Préparer l'exception **avant l'import et l'envoi**. Elle peut aussi être ajoutée
pour une dépense déjà en attente, avant de réimporter explicitement un OFX du
compte concerné. Les règles d'éligibilité restent évaluées sur le montant bancaire
original ; une déduction ne rend pas éligible une dépense exclue par les règles.

Une correspondance unique reçoit automatiquement la déduction : une dépense de
60 euros avec 10 euros personnels devient 50 euros dans Splitwise. Le montant
bancaire reste à 60 euros, et les contrôles de solde restent inchangés. La recherche
de doublons utilise les 50 euros réellement partagés. La note envoyée ne détaille
pas les achats personnels.

S'il existe plusieurs correspondances, l'application affiche les candidats et
demande leur numéro. Entrée, `later` ou l'absence de terminal interactif conserve
**tous les candidats en attente**, sans en envoyer un au montant complet par
accident. Les imports de tous les comptes du fichier sont enregistrés avant cette
sélection. Seules les autres bases `account_*.db` dont le numéro correspond au
masque `account` sont ouvertes en lecture
pour retrouver une application passée de l'exception ou une correspondance
supplémentaire. Une dépense appartenant à un compte absent de l'import ne peut pas
être modifiée : réimporter un relevé contenant ce compte pour la traiter.

Une exception sans correspondance est signalée. Une part personnelle supérieure
au total bloque l'envoi concerné, jusqu'à correction du fichier. Si elle égale le
total, la dépense n'est pas envoyée. Une dépense déjà envoyée, rattachée ou dont
l'envoi précédent est incertain n'est jamais corrigée automatiquement ; vérifier
son état dans Splitwise avant de poursuivre.

La table `SPLITWISE_ADJUSTMENTS` conserve le nom de l'exception, la transaction,
la règle, la date, le montant initial et la déduction. Sa création bénéficie de
la sauvegarde automatique avant migration. La déduction et la mise à jour du
montant prévu pour l'API sont enregistrées dans une même transaction SQLite :
une reprise ne soustrait jamais une seconde fois la part personnelle.

Après application, l'exception peut rester dans le fichier ou être supprimée.
Ne pas la renommer ni changer ses valeurs : une définition différente d'une
exception déjà appliquée interrompt les envois pour éviter une double déduction.
Même en l'absence de clé API, la déduction peut être enregistrée localement et
l'envoi reste en attente avec le bon montant.

Sans `--import`, aucune exception n'est appliquée. Avec `--dry-run`, le fichier
est validé, mais aucune déduction persistante ni sélection interactive n'a lieu.
Un fichier configuré mais absent, une règle inconnue, une date ou un montant
invalide interrompt le traitement avant les imports.
