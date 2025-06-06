
-- External table for daily revenue
CREATE EXTERNAL TABLE gold.daily_revenue (
    event_date DATE,
    daily_revenue FLOAT,
    total_purchases INT
)
WITH (
    LOCATION = 'gold/daily_revenue/',
    DATA_SOURCE = your_adls_datasource,
    FILE_FORMAT = ParquetFormat
);

-- External table for top product categories
CREATE EXTERNAL TABLE gold.top_categories (
    product_category STRING,
    total_sales FLOAT
)
WITH (
    LOCATION = 'gold/top_categories/',
    DATA_SOURCE = your_adls_datasource,
    FILE_FORMAT = ParquetFormat
);
