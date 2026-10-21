"""Offline checks for SQL type mapping and transactional rollback."""
import unittest
from unittest.mock import Mock, patch
from types import SimpleNamespace

from src.copy_silver_to_azure_sql import SOURCE_TABLES, connection_string, copy_table, create_ddl, discover_tables, sql_name, sql_type


class SqlCopyTests(unittest.TestCase):
    def test_token_auth_omits_conflicting_connection_options(self):
        with patch.dict("os.environ", {"SQL_PASSWORD": "test-secret", "SQL_USER": "sql-user"}):
            result = connection_string("azure-cli")
        for keyword in ("UID=", "PWD=", "Authentication=", "test-secret"):
            self.assertNotIn(keyword, result)
        with self.assertRaises(ValueError):
            connection_string("azure-cli", "user@example.com")

    def test_interactive_auth_omits_sql_password(self):
        with patch.dict("os.environ", {"SQL_PASSWORD": "test-secret"}):
            result = connection_string("entra-interactive", "user@example.com")
        self.assertIn("Authentication={ActiveDirectoryInteractive}", result)
        self.assertIn("UID={user@example.com}", result)
        self.assertNotIn("PWD=", result)
        self.assertNotIn("test-secret", result)

    def test_discovery_selects_exactly_requested_tables(self):
        spark = Mock()
        spark.table.return_value.where.return_value.select.return_value.collect.return_value = [
            *[SimpleNamespace(table_name=name, table_type="MATERIALIZED_VIEW") for name in SOURCE_TABLES],
            SimpleNamespace(table_name="__materialization_123_mat_456", table_type="MANAGED"),
            SimpleNamespace(table_name="new_table", table_type="STREAMING_TABLE"),
            SimpleNamespace(table_name="ordinary", table_type="MANAGED"),
            SimpleNamespace(table_name="view_only", table_type="VIEW"),
        ]
        with patch("pyspark.sql.functions.col"), patch("pyspark.sql.functions.lower"), patch("builtins.print"):
            self.assertEqual(discover_tables(spark, "payer_01_dev", "silver"),
                             sorted(SOURCE_TABLES))

    def test_discovery_reports_missing_requested_tables(self):
        spark = Mock()
        spark.table.return_value.where.return_value.select.return_value.collect.return_value = [
            SimpleNamespace(table_name=name, table_type="STREAMING_TABLE")
            for name in SOURCE_TABLES if name != "member"
        ]
        with patch("pyspark.sql.functions.col"), patch("pyspark.sql.functions.lower"), patch("builtins.print"):
            with self.assertRaisesRegex(ValueError, "not copyable.*member"):
                discover_tables(spark, "payer_01_dev", "silver")

    def test_single_table_does_not_require_other_tables(self):
        spark = Mock()
        spark.table.return_value.where.return_value.select.return_value.collect.return_value = [
            SimpleNamespace(table_name="member", table_type="MANAGED"),
            SimpleNamespace(table_name="address", table_type="MANAGED"),
        ]
        with patch("pyspark.sql.functions.col"), patch("pyspark.sql.functions.lower"), patch("builtins.print"):
            self.assertEqual(discover_tables(spark, "payer_01_dev", "silver", ("member",)), ["member"])
            with self.assertRaisesRegex(ValueError, "not copyable.*claim_line"):
                discover_tables(spark, "payer_01_dev", "silver", ("claim_line",))

    def test_batched_copy_commits_after_count_validation(self):
        for fast in (True, False):
            with self.subTest(fast=fast):
                connection = Mock()
                cursor = connection.cursor.return_value
                cursor.fetchone.return_value = (3,)
                dataframe = Mock()
                datatype = Mock()
                datatype.simpleString.return_value = "int"
                dataframe.schema.fields = [SimpleNamespace(name="id", dataType=datatype)]
                dataframe.select.return_value.toLocalIterator.return_value = iter([(1,), (2,), (3,)])
                with patch("pyspark.sql.functions.col"), patch("builtins.print"):
                    copy_table(connection, dataframe, "silver", "member", "CREATE TABLE", 2, fast)
                self.assertEqual(cursor.fast_executemany, fast)
                self.assertEqual(cursor.executemany.call_args_list[0].args[1], [(1,), (2,)])
                self.assertEqual(cursor.executemany.call_args_list[1].args[1], [(3,)])
                connection.commit.assert_called_once()
                connection.rollback.assert_not_called()

    def test_reports_all_unsupported_columns_with_source_table(self):
        fields = []
        for name, datatype in (("flags", "map<string,boolean>"), ("items", "array<int>")):
            field = Mock()
            field.name = name
            field.dataType.simpleString.return_value = datatype
            fields.append(field)
        with self.assertRaises(ValueError) as raised:
            create_ddl("silver", "member", fields, "payer_01_dev.silver.member")
        message = str(raised.exception)
        self.assertIn("payer_01_dev.silver.member", message)
        self.assertIn("Column: flags | Datatype: map<string,boolean>", message)
        self.assertIn("Column: items | Datatype: array<int>", message)

    def test_signed_byte_does_not_map_to_unsigned_sql_tinyint(self):
        self.assertEqual(sql_type("tinyint"), "SMALLINT")

    def test_decimal_precision_preserved(self):
        self.assertEqual(sql_type("decimal(38,12)"), "DECIMAL(38,12)")

    def test_unsupported_type_rejected(self):
        for datatype in ("array<int>", "struct<a:int>", "variant", "decimal(39,2)"):
            with self.subTest(datatype=datatype), self.assertRaises(ValueError):
                sql_type(datatype)

    def test_identifier_escaping(self):
        self.assertEqual(sql_name("a]b"), "[a]]b]")

    def test_failed_create_rolls_back(self):
        connection = Mock()
        connection.cursor.return_value.execute.side_effect = RuntimeError("create failed")
        with self.assertRaises(RuntimeError):
            copy_table(connection, Mock(), "silver", "member", "CREATE TABLE", 100)
        connection.rollback.assert_called_once()
        connection.commit.assert_not_called()
        connection.cursor.return_value.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
