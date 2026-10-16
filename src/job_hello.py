from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.range(5)

df.show()

print("Hello from sample_dbc_project Databricks Job")