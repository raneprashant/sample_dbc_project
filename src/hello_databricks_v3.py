from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()

df = spark.table("payer_01_dev.silver.member")

df.printSchema()
df.show(10, truncate=False)