# Bronze Layer - Ingest JSON from ADLS
df_bronze = spark.read.json("abfss://raw@<storage_account>.dfs.core.windows.net/bronze/retail_transactions/")
df_bronze.write.mode("overwrite").parquet("abfss://clean@<storage_account>.dfs.core.windows.net/bronze/retail_transactions/")
