# Copy silver tables to Azure SQL

Source: exactly these 14 objects in `payer_01_dev.silver`: address, benefit_plan,
claim_header, claim_line, eligibility, entity_address, group, member, practitioner,
practitioner_role, product, subgroup, supplier, supplier_location.
Internal `__materialization*` objects and all other names are excluded. Missing requested
tables stop preflight before any Azure SQL changes.
Destination: `ihapocsqldbserver.database.windows.net`, database `payer_db`, schema `silver`.
Streaming tables and materialized views are copied as ordinary SQL tables containing
their current query results. Ordinary views and temporary tables are excluded.
Discovery prints all visible objects and their types, selecting only the 14 names above.
Use `--profile DATABRICKS_FREE` to explicitly select your existing CLI profile.
Source access requires USE CATALOG, USE SCHEMA,
and SELECT on the tables. Run locally using your existing Databricks Connect interpreter
and authenticated CLI profile. Do not submit this script as a Databricks job.

## Setup in PowerShell

Install Microsoft ODBC Driver 18 for SQL Server on this computer, then:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements-copy.txt
Copy-Item .env.example .env
```

Edit `.env` and fill in `SQL_PASSWORD` locally. Both SQL_USER and the older
SQL_READONLY_USER names are accepted (likewise for passwords). The script loads `.env`
from the repository root; existing environment variables take precedence.
The password previously shared in chat should be replaced. No password is included
in the supplied files. `.env` files are ignored by Git.

If needed, choose the same profile as your working Databricks Connect configuration:

```powershell
$env:DATABRICKS_CONFIG_PROFILE = "your-profile-name"
```

The Azure SQL firewall must allow this computer's public IP, or this computer must
have private network access to the server. The SQL user needs CREATE TABLE in the
database, ALTER on schema silver, and INSERT/SELECT for the new tables. TLS certificate
validation is enabled.

## Preview and copy

If the ODBC interactive dialog fails, use Azure CLI browser authentication instead.
Install Azure CLI using Microsoft's Windows installer, restart the terminal, and install
the updated requirements-copy.txt. Then run:

```powershell
az login --tenant atos365.onmicrosoft.com
az account set --subscription aa4f029e-8328-42e1-b015-bc23602d7251
az account show --query "{user:user.name,tenant:tenantId,subscription:name}" -o table
.\.venv\Scripts\python.exe src/copy_silver_to_azure_sql.py --test-sql --sql-auth azure-cli
```

The tenant and subscription above come from the supplied Azure Portal screenshot;
adjust them if the database belongs to a different tenant/subscription. This mode uses
the active Azure CLI account, not --sql-user. The script obtains an Azure SQL token
in memory without printing or storing it. Pass --sql-auth azure-cli when copying too.
Database permissions and server Entra configuration are still required.

To test only Azure SQL connectivity and permissions without touching Databricks or writing data:

```powershell
.\.venv\Scripts\python.exe src/copy_silver_to_azure_sql.py --test-sql
```

For interactive Microsoft Entra authentication (including MFA), use your Microsoft email:

```powershell
.\.venv\Scripts\python.exe src/copy_silver_to_azure_sql.py --test-sql --sql-auth entra-interactive --sql-user user@company.com
```

Complete the Microsoft sign-in dialog. The SQL server must have Entra authentication
configured and your identity must have database access and the required permissions.
Azure Portal access alone is insufficient. Pass the same `--sql-auth` and `--sql-user`
options with `--execute` when copying. Alternatively persist `SQL_AUTH=entra-interactive`
and `SQL_ENTRA_USER=user@company.com` in `.env`; no SQL password is sent in this mode.

```powershell
# Print CREATE TABLE statements; no Azure SQL connection or writes.
.\.venv\Scripts\python.exe src/copy_silver_to_azure_sql.py

# Create tables and copy rows.
.\.venv\Scripts\python.exe src/copy_silver_to_azure_sql.py --execute
```

All source column types are checked before any destination writes. Existing destination
objects cause the entire preflight to fail. The script never drops, clears, or appends
to existing tables. Each new table and its data are committed in one transaction;
failure rolls back that table. Previously committed tables remain. Before retrying a
partially completed run, reconcile those tables manually; automatic resume is not provided.

Rows are read using a local iterator and inserted in batches (default 1000; change with
`--batch-size`). Spark can buffer a partition locally, so this is intended for modest
development datasets. Large tables also require enough Azure SQL transaction log space
for the entire table transaction. Avoid source changes during the copy: this is not an
atomic snapshot across tables. Validation compares inserted rows with the destination
COUNT_BIG before commit; it is not a content checksum or a separate source-count check.

Column order, supported scalar datatypes, precision/scale, and nullability are preserved
through explicit mappings. Signed Spark TINYINT becomes SMALLINT. Strings become Unicode
NVARCHAR(MAX). Timestamps become DATETIME2(6), with timestamp instants represented in UTC;
timestamp_ntz retains its wall-clock value. Complex types (array/map/struct), intervals,
variant, and unknown types stop the run rather than silently converting data. SQL row-size
and identifier/collation restrictions can also prevent a table from being created.

Keys, indexes, comments, defaults, generated/identity behavior, Delta properties,
constraints beyond NOT NULL, and incremental synchronization are not copied.

## Selecting tables and accelerated inserts

Stop an older copy with Ctrl+C and wait for the terminal prompt before starting another.
Committed tables remain; the interrupted table is rolled back if rollback completes.

```powershell
$env:DATABRICKS_SERVERLESS_COMPUTE_ID = "auto"
.\.venv\Scripts\python.exe src/copy_silver_to_azure_sql.py --execute --sql-auth azure-cli --profile DATABRICKS_FREE --table member
```

Repeat `--table` to select several tables, such as `--table member --table address`.
Omit it to select all 14. Only selected tables are checked for source availability and
destination existence. Existing selected tables still stop the entire run; nothing is
dropped, overwritten, skipped automatically, or appended.

ODBC `fast_executemany` is now enabled by default. Batch size remains 1000; use
`--batch-size 5000` to experiment with larger batches at the cost of more client memory.
Each complete table still commits once. If accelerated insertion encounters a driver
or datatype compatibility error, inspect the failure and retry the rolled-back table
with `--no-fast-executemany`. There is no automatic retry. Actual speed and long-string,
binary, decimal, timestamp, and NULL compatibility should be verified on representative
data before a large load.

## Offline tests

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -p test_sql_copy.py -v
```
