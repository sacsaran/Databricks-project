# Bean Counter: Sales Data Pipeline

A Databricks project that takes raw sales files (orders and customers) through a Bronze, Silver and Gold pipeline to produce daily sales and product ranking tables.

## What it does

- Streams new order (CSV) and customer (JSON) files into Bronze as they land
- Cleans orders: removes duplicates, fills missing unit prices from the products file, and parses messy timestamps into a date and time
- Sends bad orders (quantity of 0 or less, or no customer ID) to a quarantine table with a rejection reason
- Keeps only the latest record per customer and upserts it with `MERGE`
- Builds Gold tables for daily sales by store and product revenue ranking

## Pipeline layers

| Layer | Table | What happens |
|---|---|---|
| Bronze | `bronze.order` | Streaming ingestion of order CSV files |
| Bronze | `bronze.customer` | Streaming ingestion of customer JSON files |
| Silver | `silver.silver_order` | Cleaned, deduplicated orders with date and time |
| Silver | `silver.silver_orders_quarantine` | Rejected orders with a rejection reason |
| Silver | `silver.silver_customer` | Latest version of each customer, upserted |
| Gold | `gold.gold_daily_sales` | Orders, revenue and units sold per store per day |
| Gold | `gold.gold_product_ranking` | Products ranked by total revenue |

All tables are in the `tech_assessment` catalog. Raw files land in the `bean_counter` volume.

## Data quality

| Issue | How it's handled |
|---|---|
| Missing unit price | Filled from `products.csv` |
| Different timestamp formats | Parsed with multiple formats into `order_date` and `order_time` |
| Duplicate orders | Removed with `DISTINCT` |
| Quantity of 0 or less, or missing customer ID | Sent to the quarantine table |
| Several versions of the same customer | Latest `updated_at` kept, then merged |

## Tech
Databricks, PySpark, Spark SQL, Delta Lake, Streaming Tables, Unity Catalog
