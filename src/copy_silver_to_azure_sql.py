"""Preview or copy Databricks silver tables to Azure SQL. See COPY_TO_AZURE_SQL.md."""

import argparse
from datetime import datetime
from itertools import islice
import os
from pathlib import Path
import re
import struct


def sql_name(value):
    if not value or len(value) > 128:
        raise ValueError("SQL identifiers must contain 1 to 128 characters")
    return "[" + value.replace("]", "]]") + "]"


def spark_name(value):
    return "`" + value.replace("`", "``") + "`"


COPYABLE_TABLE_TYPES = frozenset({
    "MANAGED", "EXTERNAL", "BASE TABLE", "MANAGED_SHALLOW_CLONE",
    "EXTERNAL_SHALLOW_CLONE", "FOREIGN", "STREAMING_TABLE", "MATERIALIZED_VIEW",
})
SOURCE_TABLES = (
    "address", "benefit_plan", "claim_header", "claim_line", "eligibility",
    "entity_address", "group", "member", "practitioner", "practitioner_role",
    "product", "subgroup", "supplier", "supplier_location",
)


def discover_tables(spark, catalog, schema, requested_tables=SOURCE_TABLES):
    """Select requested business tables, excluding internal objects."""
    from pyspark.sql import functions as F
    metadata = spark.table(f"{spark_name(catalog)}.information_schema.tables")
    rows = metadata.where(F.lower(F.col("table_schema")) == schema.lower()).select(
        "table_name", "table_type").collect()
    print(f"Source: {catalog}.{schema}; visible objects: {len(rows)}", flush=True)
    names = []
    for row in sorted(rows, key=lambda row: row.table_name):
        included = (row.table_name in requested_tables
                    and row.table_type.upper() in COPYABLE_TABLE_TYPES)
        print(f"  {row.table_name}: {row.table_type} ({'include' if included else 'skip'})", flush=True)
        if included:
            names.append(row.table_name)
    missing = sorted(set(requested_tables) - set(names))
    if missing:
        raise ValueError(f"Requested tables missing or not copyable in {catalog}.{schema}: {', '.join(missing)}. "
                         "Check the selected Databricks profile, catalog/schema and USE/SELECT permissions. "
                         "See the object types listed above; ordinary views are excluded.")
    return names


def sql_type(source):
    """Map Spark simpleString types, rejecting types needing a manual policy."""
    types = {
        "tinyint": "SMALLINT",  # Spark tinyint is signed; SQL TINYINT is unsigned.
        "smallint": "SMALLINT", "int": "INT", "bigint": "BIGINT",
        "boolean": "BIT", "float": "REAL", "double": "FLOAT(53)",
        "string": "NVARCHAR(MAX)", "binary": "VARBINARY(MAX)",
        "date": "DATE", "timestamp": "DATETIME2(6)",
        "timestamp_ntz": "DATETIME2(6)",
    }
    if source in types:
        return types[source]
    match = re.fullmatch(r"decimal\((\d+),(\d+)\)", source)
    if match:
        precision, scale = map(int, match.groups())
        if 1 <= precision <= 38 and 0 <= scale <= precision:
            return f"DECIMAL({precision},{scale})"
    match = re.fullmatch(r"(?:var)?char\((\d+)\)", source)
    if match:
        length = int(match.group(1))
        return f"NVARCHAR({length})" if 1 <= length <= 4000 else "NVARCHAR(MAX)"
    raise ValueError(f"Unsupported source datatype: {source}. Define a conversion before copying.")


def create_ddl(schema, name, fields, source_table=None):
    if len(fields) > 1024:
        raise ValueError(f"{name}: exceeds SQL Server's 1024 column limit")
    names = [field.name.casefold() for field in fields]
    if len(names) != len(set(names)):
        raise ValueError(f"{name}: column names differ only in case")
    columns = []
    errors = []
    for field in fields:
        datatype = field.dataType.simpleString()
        try:
            mapped = sql_type(datatype)
            column = sql_name(field.name)
        except ValueError as error:
            errors.append(f"Table: {source_table or name} | Column: {field.name} | "
                          f"Datatype: {datatype} | {error}")
            continue
        columns.append(f"  {column} {mapped} {'NULL' if field.nullable else 'NOT NULL'}")
    if errors:
        raise ValueError("\n".join(errors))
    return f"CREATE TABLE {sql_name(schema)}.{sql_name(name)} (\n" + ",\n".join(columns) + "\n);"


def connection_string(auth=None, user=None):
    def escaped(value):
        return "{" + value.replace("}", "}}") + "}"
    auth = auth or os.getenv("SQL_AUTH", "sql")
    settings = {
        "DRIVER": "ODBC Driver 18 for SQL Server",
        "SERVER": "tcp:" + os.getenv("SQL_SERVER_HOST", "ihapocsqldbserver.database.windows.net") + ",1433",
        "DATABASE": os.getenv("SQL_DATABASE", "payer_db"),
    }
    if auth == "entra-interactive":
        identity = user or os.getenv("SQL_ENTRA_USER")
        if not identity:
            raise ValueError("Set SQL_ENTRA_USER or pass --sql-user with your Microsoft sign-in email")
        settings.update(UID=identity, Authentication="ActiveDirectoryInteractive")
    elif auth == "azure-cli":
        if user:
            raise ValueError("--sql-user does not select an Azure CLI account. Use az login and az account set instead.")
        # Token authentication must omit UID, PWD and Authentication.
    elif auth == "sql":
        password = os.environ.get("SQL_PASSWORD") or os.environ.get("SQL_READONLY_PASSWORD")
        if not password:
            raise ValueError("Set SQL_PASSWORD in your local .env or environment")
        settings.update(UID=user or os.getenv("SQL_USER", os.getenv("SQL_READONLY_USER", "IHA_POC")), PWD=password)
    else:
        raise ValueError(f"Unsupported SQL_AUTH: {auth}")
    return ";".join(f"{key}={escaped(value)}" for key, value in settings.items()) + ";Encrypt=yes;TrustServerCertificate=no;"


def connect_sql(auth=None, user=None):
    import pyodbc
    if "ODBC Driver 18 for SQL Server" not in pyodbc.drivers():
        raise RuntimeError("Install Microsoft ODBC Driver 18 for SQL Server (x64). "
                           "The Python pyodbc package alone does not install this Windows driver.")
    auth = auth or os.getenv("SQL_AUTH", "sql")
    options = {}
    connstr = connection_string(auth, user)
    if auth == "azure-cli":
        from azure.identity import AzureCliCredential
        credential = AzureCliCredential()
        try:
            token = credential.get_token("https://database.windows.net/.default").token.encode("utf-16-le")
        finally:
            credential.close()
        options["attrs_before"] = {1256: struct.pack("<I", len(token)) + token}
    return pyodbc.connect(connstr, autocommit=False, timeout=120, **options)


def copy_table(connection, dataframe, schema, name, ddl, batch_size, fast_executemany=True):
    """Commit once per table; any failure rolls back its DDL and inserts."""
    cursor = connection.cursor()
    try:
        target = f"{sql_name(schema)}.{sql_name(name)}"
        cursor.execute(ddl)
        cursor.fast_executemany = fast_executemany
        columns = ", ".join(sql_name(field.name) for field in dataframe.schema.fields)
        statement = f"INSERT INTO {target} ({columns}) VALUES ({','.join('?' for _ in dataframe.schema.fields)})"
        # Cast instants to UTC text on the server to avoid client-local timezone conversion.
        from pyspark.sql import functions as F
        timestamp_indexes = {i for i, field in enumerate(dataframe.schema.fields)
                             if field.dataType.simpleString() in ("timestamp", "timestamp_ntz")}
        projected = dataframe.select(*[
            F.col(spark_name(field.name)).cast("string").alias(field.name)
            if i in timestamp_indexes else F.col(spark_name(field.name))
            for i, field in enumerate(dataframe.schema.fields)])
        rows = iter(projected.toLocalIterator())
        copied = 0
        while batch := list(islice(rows, batch_size)):
            values = [tuple(datetime.fromisoformat(v) if i in timestamp_indexes and v is not None
                            else bytes(v) if isinstance(v, bytearray) else v
                            for i, v in enumerate(row)) for row in batch]
            cursor.executemany(statement, values)
            copied += len(values)
            print(f"  {name}: {copied:,} rows inserted (uncommitted)", flush=True)
        cursor.execute(f"SELECT COUNT_BIG(*) FROM {target}")
        if cursor.fetchone()[0] != copied:
            raise RuntimeError(f"Row count validation failed for {name}")
        connection.commit()
        print(f"COMMITTED {target}: {copied:,} rows")
    except BaseException:
        connection.rollback()
        raise
    finally:
        cursor.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true", help="Create tables and copy data; otherwise preview only")
    parser.add_argument("--catalog", default="payer_01_dev")
    parser.add_argument("--source-schema", default="silver")
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--profile", help="Explicit Databricks CLI authentication profile")
    parser.add_argument("--table", action="append", choices=SOURCE_TABLES,
                        help="Copy only this table; repeat to select multiple tables. Default: all 14")
    parser.add_argument("--no-fast-executemany", action="store_true",
                        help="Use ordinary ODBC inserts if accelerated batches have compatibility issues")
    parser.add_argument("--sql-auth", choices=("sql", "entra-interactive", "azure-cli"))
    parser.add_argument("--sql-user", help="SQL login or Microsoft sign-in email, depending on --sql-auth")
    parser.add_argument("--test-sql", action="store_true", help="Test Azure SQL access only; no Databricks connection or writes")
    args = parser.parse_args()
    if args.batch_size < 1:
        parser.error("--batch-size must be positive")
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    if args.test_sql:
        connection = connect_sql(args.sql_auth, args.sql_user)
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT DB_NAME(), USER_NAME(), HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'CREATE TABLE'), HAS_PERMS_BY_NAME(?, 'SCHEMA', 'ALTER'), HAS_PERMS_BY_NAME(?, 'SCHEMA', 'INSERT'), HAS_PERMS_BY_NAME(?, 'SCHEMA', 'SELECT')",
                           *([os.getenv("SQL_SCHEMA", "silver")] * 3))
            database, identity, create, alter, insert, select = cursor.fetchone()
            print(f"Connected: database={database}, database_user={identity}")
            print(f"Permissions (1=yes, 0=no, None=unknown): CREATE TABLE={create}, ALTER schema={alter}, INSERT={insert}, SELECT={select}")
            cursor.close()
            connection.rollback()
        finally:
            connection.close()
        return
    from databricks.connect import DatabricksSession
    target_schema = os.getenv("SQL_SCHEMA", "silver")
    sql_name(target_schema)
    builder = DatabricksSession.builder
    if args.profile:
        builder = builder.profile(args.profile)
    spark = builder.getOrCreate()
    # Spark timestamps represent instants; copy them as UTC without a timezone suffix.
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    requested_tables = tuple(dict.fromkeys(args.table)) if args.table else SOURCE_TABLES
    names = discover_tables(spark, args.catalog, args.source_schema, requested_tables)
    # Preflight every datatype before any destination changes.
    plans = []
    preflight_errors = []
    for name in names:
        source = ".".join(spark_name(part) for part in (args.catalog, args.source_schema, name))
        dataframe = spark.table(source)
        try:
            ddl = create_ddl(target_schema, name, dataframe.schema.fields, source_table=source)
        except ValueError as error:
            preflight_errors.append(str(error))
            continue
        plans.append((name, dataframe, ddl))
        print(ddl)
    if preflight_errors:
        parser.exit(1, "\nSchema preflight failed:\n" + "\n".join(preflight_errors)
                    + "\nNo Azure SQL connection or changes were made. Correct the mappings and rerun.\n")
    if not args.execute:
        print(f"PREVIEW: {len(plans)} tables. No Azure connection or changes. Use --execute to copy.")
        return
    connection = connect_sql(args.sql_auth, args.sql_user)
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT SCHEMA_ID(?)", target_schema)
        if cursor.fetchone()[0] is None:
            raise ValueError(f"Destination schema {target_schema} does not exist")
        for name, _, _ in plans:
            cursor.execute("SELECT 1 FROM sys.objects WHERE schema_id = SCHEMA_ID(?) AND name = ?", target_schema, name)
            if cursor.fetchone():
                raise ValueError(f"Destination object already exists: {target_schema}.{name}. No copy started.")
        cursor.close()
        connection.rollback()
        for name, dataframe, ddl in plans:
            copy_table(connection, dataframe, target_schema, name, ddl, args.batch_size,
                       fast_executemany=not args.no_fast_executemany)
    finally:
        connection.close()


if __name__ == "__main__":
    main()
