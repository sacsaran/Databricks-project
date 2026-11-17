# UK Housing Market: Star Schema

A Databricks project that takes raw UK housing market data through a Bronze, Silver and Gold pipeline and models it as a star schema for analytics.

## What it does

- Loads new housing data incrementally (only rows after the last load date)
- Cleans the data: converts dates, adds year, month, day and quarter, and replaces null changes with 0
- Upserts into Silver with `MERGE`, so reruns don't create duplicates
- Builds dimension and fact tables in Gold, split by **cash buyers** and **mortgage buyers**

## Pipeline layers

| Layer | Table | What happens |
|---|---|---|
| Bronze | `bronze.ukhousing` | Raw data, loaded incrementally |
| Silver | `silver.ukhousing` | Cleaned, typed, upserted with MERGE |
| Gold | dimension and fact tables | Star schema for reporting |

All tables are in the `datamodelling` catalog.

## Star schema

**Dimension tables** (the "who, where, when"):

| Table | Contains |
|---|---|
| `gold.dimregion` | Region names |
| `gold.dimareacode` | Area codes |
| `gold.dimdate` | Date, year, month, day, quarter |

**Fact tables** (the numbers):

| Table | Contains |
|---|---|
| `gold.factcashbuyer` | Cash buyer average price, index, monthly and annual change, sales volume |
| `gold.factmortgagebuyer` | Mortgage buyer average price, index, monthly and annual change, sales volume |

Each dimension has a surrogate key (for example `dimregionkey`). Each fact table links to all three dimensions through these keys.

## Tech
Databricks, PySpark, Spark SQL, Delta Lake, Unity Catalog

1. Import the notebooks into Databricks
2. Run them in order: Bronze, Silver, Gold
3. Query the gold tables
