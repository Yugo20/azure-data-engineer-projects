# Gold Layer - Aggregates
from pyspark.sql.functions import sum, count, col

df_silver = spark.read.parquet("abfss://clean@<storage_account>.dfs.core.windows.net/silver/purchase_clean/")

df_daily_revenue = (
    df_silver.groupBy("event_date")
    .agg(sum("amount").alias("daily_revenue"), count("*").alias("total_purchases"))
)
df_top_categories = (
    df_silver.groupBy("product_category")
    .agg(sum("amount").alias("total_sales"))
    .orderBy(col("total_sales").desc())
)

df_daily_revenue.write.mode("overwrite").parquet("abfss://gold@<storage_account>.dfs.core.windows.net/gold/daily_revenue/")
df_top_categories.write.mode("overwrite").parquet("abfss://gold@<storage_account>.dfs.core.windows.net/gold/top_categories/")
