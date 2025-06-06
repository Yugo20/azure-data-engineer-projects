# Silver Layer - Filter purchase & clean data
from pyspark.sql.functions import col, to_date, lower

df_bronze = spark.read.parquet("abfss://clean@<storage_account>.dfs.core.windows.net/bronze/retail_transactions/")
df_silver = (
    df_bronze
    .filter(col("event_type") == "purchase")
    .dropna(subset=["customer_id", "amount"])
    .withColumn("event_date", to_date(col("event_timestamp")))
    .withColumn("payment_method", lower(col("payment_method")))
    .withColumn("amount", col("amount").cast("float"))
    .select(
        "event_id", "customer_id", "event_date", "product_id",
        "product_category", "payment_method", "amount", "location"
    )
)
df_silver.write.mode("overwrite").parquet("abfss://clean@<storage_account>.dfs.core.windows.net/silver/purchase_clean/")
