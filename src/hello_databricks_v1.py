from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()

df = spark.range(5)

df.show()