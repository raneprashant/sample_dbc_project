from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.range(5)

df.show()

print("Hello from sample_dbc_project Databricks Job")

# CI/CD smoke test - Databricks token rotation - 2026-11-16

print("Hello!!! We added new Databricks token... Also deleted old token from Databricks Profile page")
