from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()

spark.sql("SHOW CATALOGS").show(truncate=False)