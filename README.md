
# E-Commerce Spark-Declarative-Pipeline Data Engineering Project

![Architecture](images/architecture.png)

## Project Overview

This project demonstrates an end-to-end **E-Commerce Data Engineering and Analytics pipeline** built using **Databricks, Apache Spark, Unity Catalog, and Power BI**.

The project takes raw e-commerce CSV data through a layered data architecture:

**Landing → Bronze → Silver → Gold → Power BI**

The objective was to build a scalable data pipeline that ingests raw data, cleans and transforms it, creates analytical data models, performs data quality validation, and delivers business insights through an interactive Power BI dashboard.

## Architecture

The pipeline follows a medallion-style architecture:

* **Landing** — Raw CSV files stored in a Unity Catalog Volume
* **Bronze** — Raw data ingestion using Spark Declarative Pipelines and Auto Loader
* **Silver** — Cleaned, deduplicated and structured data
* **Gold** — Business-ready fact, dimension and aggregated tables
* **Power BI** — Interactive analytics and business intelligence dashboards

## Technologies Used

| Technology                      | Purpose                                                         |
| ------------------------------- | --------------------------------------------------------------- |
| **Databricks**                  | Data engineering platform and pipeline development              |
| **Spark / PySpark**             | Data ingestion, transformation and processing                   |
| **Spark Declarative Pipelines** | Building and orchestrating the Bronze and Silver data pipelines |
| **Unity Catalog**               | Data governance, cataloging and access management               |
| **Auto Loader**                 | Incremental ingestion of CSV files from the Landing volume      |
| **Databricks SQL**              | Creating Gold analytical tables and performing data validation  |
| **Power BI**                    | Data modeling, DAX measures and interactive dashboards          |
| **GitHub**                      | Project documentation and portfolio management                  |

---

## Dataset

The project uses an **E-Commerce Sales and Customer Analytics dataset** containing information about orders, customers, products and order items.

The raw dataset consists of four CSV files:

* `ecommerce_sales_customer_analytics_150k.csv`
* `order_items.csv`
* `customer_master.csv`
* `product_catalog.csv`

### Dataset Overview

| Dataset                                       | Description                                                                      |
| --------------------------------------------- | -------------------------------------------------------------------------------- |
| `ecommerce_sales_customer_analytics_150k.csv` | Order-level sales, customer, payment, delivery, marketing and review information |
| `order_items.csv`                             | Product-level information for each order                                         |
| `customer_master.csv`                         | Customer demographics and customer acquisition information                       |
| `product_catalog.csv`                         | Product, category, brand, supplier and pricing information                       |

The data was first uploaded to a **Unity Catalog Volume**:

```text
/Volumes/ecommerce_pipeline/landing/ecommerce_files/
```

---

# Data Engineering Pipeline

The project follows a **Medallion Architecture** consisting of Landing, Bronze, Silver and Gold layers.

```text
Raw CSV Files
     │
     ▼
Landing Volume
     │
     ▼
Bronze Layer
     │
     ▼
Silver Layer
     │
     ▼
Gold Layer
     │
     ▼
Power BI
```

---

## 1. Landing Layer

The Landing layer contains the original CSV files before transformation.

The files are stored in a Unity Catalog Volume:

```text
ecommerce_pipeline
└── landing
    └── ecommerce_files
        ├── ecommerce_sales_customer_analytics_150k.csv
        ├── order_items.csv
        ├── customer_master.csv
        └── product_catalog.csv
```

The Landing layer preserves the raw source data and provides the starting point for automated ingestion.

---

# 2. Bronze Layer

The Bronze layer ingests the raw CSV files using **Spark Declarative Pipelines and Auto Loader**.

### Bronze Tables

```text
ecommerce_pipeline.bronze
│
├── ecommerce_sales_customer_analytics
├── order_items
├── customer_master
└── product_catalog
```

### Bronze Processing

The ingestion pipeline uses:

* PySpark
* Spark Declarative Pipelines
* Auto Loader
* CSV format
* Header-based column detection
* Schema inference
* `_rescued_data` for unexpected records

The Bronze layer is primarily responsible for **reliable ingestion and preserving the source structure**, rather than performing extensive business transformations.

### Bronze Validation

The ingested datasets were validated using row counts, distinct-key checks and duplicate checks.

Key results included:

| Table                                |    Rows |
| ------------------------------------ | ------: |
| `ecommerce_sales_customer_analytics` | 138,116 |
| `order_items`                        | 397,569 |
| `customer_master`                    |  25,000 |
| `product_catalog`                    |   1,175 |

---

# 3. Silver Layer

The Silver layer transforms the Bronze data into cleaner and more structured datasets.

### Silver Tables

```text
ecommerce_pipeline.silver
│
├── orders
├── order_items
├── customers
└── products
```

### Silver Transformations

The Silver pipeline performs:

* Removal of `_rescued_data`
* Deduplication
* Key-based record validation
* Separation of orders, customers, products and order items
* Preparation of clean datasets for analytical modeling

Examples of deduplication logic include:

```python
.dropDuplicates(["order_id"])
```

for orders and:

```python
.dropDuplicates(["order_id", "product_id"])
```

for order items.

### Silver Validation

The resulting Silver tables were validated:

| Table         | Records |
| ------------- | ------: |
| `orders`      | 138,116 |
| `order_items` | 397,482 |
| `customers`   |  25,000 |
| `products`    |   1,175 |

Referential integrity checks were also performed to verify that:

* Orders had valid customers
* Order items had valid orders
* Order items had valid products

All tested referential integrity checks returned **0 invalid records**.

---

# 4. Gold Layer

The Gold layer contains **business-ready analytical datasets** designed for reporting and BI consumption.

### Gold Tables

```text
ecommerce_pipeline.gold
│
├── fact_sales
├── dim_customer
├── dim_product
├── dim_date
├── agg_daily_sales
├── agg_monthly_sales
├── agg_product_performance
├── agg_regional_performance
└── agg_customer_performance
```

### Fact Table

The central fact table is:

```text
fact_sales
```

The grain of the table is:

> **One row per order and product combination.**

This design allows sales, profit, units and product performance to be analyzed without duplicating order-level financial values across multiple product lines.

### Dimension Tables

**`dim_customer`**

Contains customer attributes used for customer and regional analysis.

**`dim_product`**

Contains product attributes such as:

* Product name
* Category
* Subcategory
* Brand
* Supplier
* Pricing
* Product rating

**`dim_date`**

Provides the date dimension used for time-based analysis, including:

* Year
* Quarter
* Month
* Month name
* Day
* Day of week

### Gold Aggregations

Additional analytical tables were created for:

* Daily sales
* Monthly sales
* Product performance
* Regional performance
* Customer performance

These provide pre-aggregated datasets for analytical use cases and validation.

# Power BI Analytics

The Gold layer was connected to **Power BI** to create an interactive business intelligence dashboard.

The Power BI model uses a **star schema**, with `fact_sales` as the central fact table and three supporting dimension tables.

## Power BI Data Model

```text
                    dim_date
                       │
                       │ 1 → *
                       ▼
dim_customer ──────► fact_sales ◄────── dim_product
     1 → *                              1 → *
```

### Fact Table

**`fact_sales`**

Contains the transactional product-level sales data used for the majority of analytical calculations.

### Dimension Tables

* **`dim_customer`** — customer attributes and segmentation
* **`dim_product`** — product, category and brand attributes
* **`dim_date`** — calendar attributes for time-based analysis

Relationships were configured using **one-to-many cardinality**, with the dimension tables filtering the `fact_sales` table.

The `dim_date` table was also configured as the Power BI **Date Table** to support time-intelligence calculations.

---

# DAX Measures

A set of DAX measures was created to support the dashboard's KPIs, financial analysis and time-based comparisons.

### Core Business Measures

```DAX
Total Sales =
SUM(fact_sales[net_sales])

Total Profit =
SUM(fact_sales[profit])

Profit Margin % =
DIVIDE(
    [Total Profit],
    [Total Sales],
    0
)

Total Orders =
DISTINCTCOUNT(fact_sales[order_id])

Units Sold =
SUM(fact_sales[quantity])

Total Customers =
DISTINCTCOUNT(fact_sales[customer_id])

Average Order Value =
DIVIDE(
    [Total Sales],
    [Total Orders],
    0
)

Average Selling Price =
DIVIDE(
    [Total Sales],
    [Units Sold],
    0
)

Total Discount =
SUM(fact_sales[discount_amount])

Gross Sales =
SUM(fact_sales[gross_sales])
```

### Time Intelligence Measures

```DAX
Sales YTD =
TOTALYTD(
    [Total Sales],
    dim_date[order_date]
)

Profit YTD =
TOTALYTD(
    [Total Profit],
    dim_date[order_date]
)

Sales Previous Year =
CALCULATE(
    [Total Sales],
    SAMEPERIODLASTYEAR(dim_date[order_date])
)

Sales YoY % =
DIVIDE(
    [Total Sales] - [Sales Previous Year],
    [Sales Previous Year],
    0
)

Profit Previous Year =
CALCULATE(
    [Total Profit],
    SAMEPERIODLASTYEAR(dim_date[order_date])
)

Profit YoY % =
DIVIDE(
    [Total Profit] - [Profit Previous Year],
    [Profit Previous Year],
    0
)
```

Additional measures were created for:

* Previous-year orders
* Orders YoY %
* Average discount %
* Profit per order
* Average delivery days

---

# Power BI Dashboard

The final Power BI report consists of **five analytical pages**, each focused on a different area of the e-commerce business.

## 1. Executive Overview

Provides a high-level view of overall business performance.

Key visuals include:

* Total Sales
* Total Profit
* Profit Margin
* Total Orders
* Total Customers
* Monthly Sales Trend
* Monthly Profit Trend
* Sales by Product Category

---

## 2. Sales & Profitability Analysis

Focuses on financial performance and sales trends.

Key visuals include:

* Total Sales
* Total Profit
* Profit Margin
* Average Order Value
* Sales Trend by Month and Year
* Sales by Product Category
* Profit by Product Category
* Product-level sales and profitability table
* Date filtering

---

## 3. Customer & Regional Analysis

Examines customer behavior and geographical performance.

Key visuals include:

* Total Customers
* Total Orders
* Total Sales
* Average Order Value
* Sales by Region
* Profit by Region
* Sales by Customer Segment
* Sales by Customer Type
* Top Customer analysis
* Regional and customer filtering

---

## 4. Marketing, Delivery & Returns

Analyzes marketing performance and operational metrics.

Key visuals include:

* Total Orders
* Total Discount
* Sales by Marketing Channel
* Profit by Marketing Channel
* Sales by Campaign
* Orders by Delivery Status
* Average Delivery Days
* Delivery and marketing filtering

> Note: Return-specific fields were not included in the final `fact_sales` Power BI model, so return-status and return-reason visuals were not included in the final dashboard.

---

## 5. Product Performance

Provides a detailed view of product-level performance.

Key visuals include:

* Total Sales
* Total Profit
* Units Sold
* Profit Margin
* Top 10 Products by Sales
* Top 10 Products by Profit
* Sales by Product Category
* Profit by Product Category
* Product performance table
* Product category, brand and date filtering

---

# Dashboard Objectives

The Power BI report was designed to answer key business questions such as:

* How are sales and profitability performing?
* Which products and categories generate the most sales?
* Which products contribute the most profit?
* How do different customer segments perform?
* Which regions generate the most revenue and profit?
* Which marketing channels contribute to sales and profitability?
* How efficiently are orders being delivered?
* Which customers and products have the highest commercial value?

The dashboard provides an interactive layer on top of the Gold data model, allowing users to filter and explore the underlying business data.

# Data Quality & Validation

Data quality checks were performed throughout the Bronze, Silver and Gold layers to ensure the data was suitable for analytics.

### Validation Performed

* Row-count validation between pipeline layers
* Duplicate detection
* Distinct key validation
* Null-value checks
* Referential integrity checks
* Order and product key validation
* Customer key validation
* Financial reconciliation between Silver and Gold data
* Validation of the final Power BI data model

### Key Validation Results

| Validation                               | Result  |
| ---------------------------------------- | ------- |
| Distinct orders in Bronze analytics data | 138,116 |
| Distinct orders in Silver orders         | 138,116 |
| Silver customers                         | 25,000  |
| Silver products                          | 1,175   |
| Silver order items                       | 397,482 |
| Invalid orders without customers         | 0       |
| Invalid order items without orders       | 0       |
| Invalid order items without products     | 0       |
| Duplicate order checks                   | Passed  |
| Duplicate customer checks                | Passed  |
| Duplicate product checks                 | Passed  |

These checks helped ensure that the Gold layer was built from consistent and validated Silver data.

---

# Key Business Insights

The completed analysis provides several areas for business investigation and decision-making.

### 1. Sales and profitability should be analyzed together

Revenue alone does not provide a complete picture of business performance.

The dashboard therefore combines:

* Sales
* Profit
* Profit Margin
* Average Order Value
* Units Sold

This makes it possible to distinguish between high-revenue activity and activity that also generates strong profitability.

### 2. Product performance varies across the portfolio

The Product Performance page allows products to be compared by:

* Sales
* Profit
* Units Sold
* Profit Margin
* Category
* Brand

This provides a framework for identifying products that generate substantial revenue as well as products whose profitability differs from their sales contribution.

### 3. Customer segments provide different perspectives on performance

Customer and regional analysis compares:

* Customer segments
* Customer types
* Orders
* Sales
* Profit
* Average Order Value

These dimensions can be used to investigate differences in purchasing behavior and commercial contribution across customer groups.

### 4. Regional performance can be analyzed independently from customer performance

The dashboard separates regional sales and profit analysis from customer segmentation.

This allows geographic performance to be examined using:

* Sales by region
* Profit by region
* Customer segment
* Customer type

This can help identify geographic differences that may not be visible from overall sales figures.

### 5. Marketing performance can be evaluated using both sales and profit

Marketing analysis includes:

* Marketing channel
* Campaign
* Sales
* Profit
* Discounts

Comparing sales and profit provides a more complete view of marketing performance than looking at revenue alone.

### 6. Delivery performance can be monitored alongside commercial performance

The dashboard includes:

* Delivery status
* Average delivery days
* Order volume

Combining operational metrics with sales information provides a basis for investigating relationships between fulfillment performance and customer/business outcomes.

---

# Project Structure

The project can be organized in GitHub using the following structure:

```text
ecommerce-data-engineering-project/
│
├── images/
│   └── architecture.png
│
├── notebooks/
│   └── ...
│
├── pipelines/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── sql/
│   └── gold_tables.sql
│
├── powerbi/
│   └── ecommerce_dashboard.pbix
│
├── data/
│   └── README.md
│
└── README.md
```

> The exact folder structure can be adjusted to match the files actually stored in the repository.

---

# Skills Demonstrated

This project demonstrates practical experience across several areas of modern data engineering and analytics.

### Data Engineering

* Data ingestion
* ETL/ELT pipeline development
* Medallion architecture
* Bronze/Silver/Gold data modeling
* Incremental data ingestion
* Data transformation
* Deduplication
* Data validation

### Databricks & Spark

* Databricks
* PySpark
* Spark Declarative Pipelines
* Auto Loader
* Unity Catalog
* Databricks SQL
* Delta-based data processing

### Data Modeling

* Fact and dimension modeling
* Star schema design
* Transaction-level data modeling
* Date dimension design
* Analytical aggregations
* Referential integrity

### Power BI

* Power BI data modeling
* DAX
* Time intelligence
* KPI development
* Interactive dashboards
* Slicers and filtering
* Data visualization
* Business analysis

### Supporting Skills

* SQL
* Python
* Git/GitHub
* Data quality testing
* Technical documentation
* End-to-end analytics pipeline development

---

# Future Improvements

Potential future enhancements include:

* Add automated data-quality monitoring
* Add pipeline scheduling and monitoring
* Implement more advanced incremental processing
* Add return-status and return-reason fields to the Gold model
* Expand Power BI time-intelligence analysis
* Add automated alerting for significant changes in sales or profitability
* Introduce additional customer lifetime-value analysis
* Add product profitability and inventory-oriented analysis
* Integrate additional data sources
* Implement CI/CD for Databricks pipeline deployments
* Add automated testing to the transformation layer
* Introduce workflow orchestration for production-style deployments

---

# Project Outcome

This project demonstrates an end-to-end workflow from **raw CSV data to an interactive business intelligence solution**.

The completed pipeline covers:

```text
Raw Data
   ↓
Unity Catalog Volume
   ↓
Auto Loader
   ↓
Bronze
   ↓
Silver
   ↓
Gold
   ↓
Star Schema
   ↓
Power BI
   ↓
Business Analysis
```

The project combines **data engineering, data modeling, data quality, SQL, PySpark, Databricks and Power BI** into a single end-to-end portfolio project.

