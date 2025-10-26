# Gold Layer Documentation
## Data Warehouse - Medallion Architecture

---

## Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Design Philosophy](#design-philosophy)
4. [Gold Layer Views](#gold-layer-views)
5. [Data Model](#data-model)
6. [Implementation Details](#implementation-details)
7. [Query Examples](#query-examples)
8. [Performance Optimization](#performance-optimization)
9. [Usage Guidelines](#usage-guidelines)
10. [Monitoring & Validation](#monitoring--validation)

---

## Overview

The **Gold Layer** represents the top tier in the Medallion Architecture, serving as the business-ready, analytics-optimized consumption layer. This layer transforms cleansed Silver layer data into dimension and fact tables following a **star schema** design pattern.

### Purpose
- **Business Intelligence**: Provide optimized views for reporting and analytics
- **Dimensional Modeling**: Implement star schema with fact and dimension tables
- **Data Integration**: Combine data from multiple Silver sources into unified business views
- **Performance**: Enable fast query performance for BI tools and dashboards
- **Consistency**: Standardize business metrics and KPIs across the organization

### Key Characteristics
- **Schema**: `gold`
- **Data Quality**: Business-ready, analytics-optimized
- **Data Format**: Star schema (facts and dimensions)
- **Update Mechanism**: Views refreshed automatically from Silver layer
- **Access Pattern**: Read-optimized for BI tools and analysts

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      SILVER LAYER                            │
│  (Cleansed & Validated Data)                                 │
│                                                               │
│  ┌─────────────────┐  ┌─────────────────┐                   │
│  │ CRM Domain      │  │ ERP Domain      │                   │
│  │ • cust_info     │  │ • cust_a1z12    │                   │
│  │ • prod_info     │  │ • loc_a101      │                   │
│  │ • sales_details │  │ • px_cat_g1v2   │                   │
│  └─────────────────┘  └─────────────────┘                   │
└───────────────────────────┬─────────────────────────────────┘
                            │
                    ┌───────▼────────┐
                    │  INTEGRATION   │
                    │  • Join Logic  │
                    │  • Surrogate   │
                    │    Keys        │
                    │  • Business    │
                    │    Rules       │
                    └───────┬────────┘
                            │
┌───────────────────────────▼─────────────────────────────────┐
│                      GOLD LAYER                              │
│  (Analytics-Ready Star Schema)                               │
│                                                               │
│  ┌──────────────────┐         ┌──────────────────┐          │
│  │ DIM_CUSTOMER     │         │ DIM_PRODUCTS     │          │
│  │                  │         │                  │          │
│  │ • customer_key   │         │ • product_key    │          │
│  │ • demographics   │         │ • categories     │          │
│  │ • location       │         │ • pricing        │          │
│  └──────────────────┘         └──────────────────┘          │
│           │                            │                     │
│           └────────────┬───────────────┘                     │
│                        │                                     │
│                ┌───────▼────────┐                            │
│                │  FACT_SALES    │                            │
│                │                │                            │
│                │ • order_number │                            │
│                │ • product_key  │                            │
│                │ • customer_key │                            │
│                │ • dates        │                            │
│                │ • metrics      │                            │
│                └────────────────┘                            │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
                    ┌───────────────┐
                    │  BI TOOLS     │
                    │  • Dashboards │
                    │  • Reports    │
                    │  • Analytics  │
                    └───────────────┘
```

---

## Design Philosophy

### Star Schema Implementation

The Gold layer implements a **star schema** design pattern consisting of:

1. **Dimension Tables**: Descriptive attributes for analysis
   - `dim_customer`: Customer master data
   - `dim_products`: Product catalog

2. **Fact Tables**: Measurable business events
   - `fact_sales`: Sales transactions

### Key Design Principles

#### 1. **Surrogate Keys**
- Each dimension has a system-generated surrogate key (`customer_key`, `product_key`)
- Generated using `ROW_NUMBER()` for uniqueness and stability
- Enables SCD (Slowly Changing Dimension) implementation in future

#### 2. **Data Integration**
- Combines CRM and ERP data sources
- Resolves conflicts using source system priority rules
- Provides unified business view

#### 3. **Business-Friendly Naming**
- Descriptive column names aligned with business terminology
- Clear, intuitive structure for business users
- Standardized naming conventions

#### 4. **View-Based Implementation**
- Implemented as database views (not materialized)
- Always reflects current Silver layer data
- Simplified maintenance and governance

---

## Gold Layer Views

### 1. DIM_CUSTOMER
Customer dimension providing complete customer profile

| Column | Type | Description | Source Logic |
|--------|------|-------------|--------------|
| `customer_key` | NUMBER | Surrogate key (auto-generated) | ROW_NUMBER() OVER (ORDER BY cst_id) |
| `customer_id` | NUMBER | Business key from CRM | silver.crm_cust_info.cst_id |
| `customer_number` | NVARCHAR2(50) | Customer reference number | silver.crm_cust_info.cst_key |
| `first_name` | NVARCHAR2(50) | Customer first name | silver.crm_cust_info.cst_firstname |
| `last_name` | NVARCHAR2(50) | Customer last name | silver.crm_cust_info.cst_lastname |
| `country` | NVARCHAR2(50) | Customer country | silver.erp_loc_a101.cntry |
| `marital_status` | NVARCHAR2(50) | Marital status | silver.crm_cust_info.cst_material_status |
| `gender` | NVARCHAR2(50) | Gender (with conflict resolution) | Prioritized from CRM, fallback to ERP |
| `birthdate` | DATE | Date of birth | silver.erp_cust_a1z12.bdate |
| `create_date` | DATE | Customer creation date | silver.crm_cust_info.cst_create_date |

**Data Integration Rules:**
- **Primary Source**: CRM system (`crm_cust_info`)
- **Enrichment Sources**: 
  - Demographics: `erp_cust_a1z12` (birth date, gender)
  - Location: `erp_loc_a101` (country)
- **Gender Conflict Resolution**:
  ```sql
  CASE WHEN ci.cst_gndr != N'n/a' THEN ci.cst_gndr
       ELSE COALESCE(ca.gen, N'n/a')
  END AS gender
  ```
  - Use CRM gender if available and not 'n/a'
  - Otherwise, use ERP gender
  - Default to 'n/a' if both missing

**Join Logic:**
```
crm_cust_info (base)
  LEFT JOIN erp_cust_a1z12 ON cst_key = cid
  LEFT JOIN erp_loc_a101 ON cst_key = cid
```

---

### 2. DIM_PRODUCTS
Product dimension with category hierarchy

| Column | Type | Description | Source Logic |
|--------|------|-------------|--------------|
| `product_key` | NUMBER | Surrogate key (auto-generated) | ROW_NUMBER() OVER (ORDER BY prd_start_dt, prd_key) |
| `product_id` | NUMBER | Business key from CRM | silver.crm_prod_info.prd_id |
| `product_number` | NVARCHAR2(50) | Product reference number | silver.crm_prod_info.prd_key |
| `product_name` | NVARCHAR2(50) | Product name | silver.crm_prod_info.prd_nm |
| `category_id` | NVARCHAR2(50) | Category identifier | silver.crm_prod_info.cat_id |
| `category` | NVARCHAR2(50) | Category name | silver.erp_px_cat_g1v2.cat |
| `subcategory` | NVARCHAR2(50) | Subcategory name | silver.erp_px_cat_g1v2.subcat |
| `maintenance` | NVARCHAR2(50) | Maintenance flag | silver.erp_px_cat_g1v2.maintenance |
| `cost` | NUMBER | Product cost | silver.crm_prod_info.prd_cost |
| `product_line` | NVARCHAR2(50) | Product line (decoded) | silver.crm_prod_info.prd_line |
| `start_date` | DATE | Product effective start date | silver.crm_prod_info.prd_start_dt |

**Data Integration Rules:**
- **Primary Source**: CRM system (`crm_prod_info`)
- **Enrichment Source**: ERP category hierarchy (`erp_px_cat_g1v2`)
- **Filter**: Only current products (`prd_end_dt IS NULL`)
- **Rationale**: Simplifies reporting by showing only active product catalog

**Join Logic:**
```
crm_prod_info (base, filtered by prd_end_dt IS NULL)
  LEFT JOIN erp_px_cat_g1v2 ON cat_id = id
```

**Category Hierarchy:**
- `category_id` → Extracted from product key
- `category` → Top-level category (e.g., Bikes, Components)
- `subcategory` → Detailed classification (e.g., Mountain Bikes, Road Bikes)

---

### 3. FACT_SALES
Sales transactions fact table

| Column | Type | Description | Source Logic |
|--------|------|-------------|--------------|
| `order_number` | NVARCHAR2(50) | Unique order identifier | silver.crm_sales_details.sls_ord_num |
| `product_key` | NUMBER | Foreign key to dim_products | Lookup via product_number match |
| `customer_key` | NUMBER | Foreign key to dim_customer | Lookup via customer_id match |
| `order_date` | DATE | Order date | silver.crm_sales_details.sls_order_dt |
| `shipping_date` | DATE | Shipment date | silver.crm_sales_details.sls_ship_dt |
| `due_date` | DATE | Due date | silver.crm_sales_details.sls_due_dt |
| `sales_amount` | NUMBER | Total sales amount | silver.crm_sales_details.sls_sales |
| `quantity` | NUMBER | Quantity sold | silver.crm_sales_details.sls_quantity |
| `price` | NUMBER | Unit price | silver.crm_sales_details.sls_price |

**Data Integration Rules:**
- **Primary Source**: CRM sales transactions (`crm_sales_details`)
- **Dimension Lookups**:
  - Product: Join on `sls_prd_key = product_number`
  - Customer: Join on `sls_cust_id = customer_id`

**Join Logic:**
```
crm_sales_details (base)
  LEFT JOIN dim_products ON sls_prd_key = product_number
  LEFT JOIN dim_customer ON sls_cust_id = customer_id
```

**Measures Available:**
- **Additive Measures**: `sales_amount`, `quantity`
- **Semi-Additive Measures**: `price` (average, not sum)
- **Derived Metrics**: 
  - Average Order Value: `sales_amount / COUNT(DISTINCT order_number)`
  - Units per Order: `quantity / COUNT(DISTINCT order_number)`

---

## Data Model

### Star Schema Diagram

```
                    ┌──────────────────────┐
                    │   DIM_CUSTOMER       │
                    ├──────────────────────┤
                    │ *customer_key        │
                    │  customer_id         │
                    │  customer_number     │
                    │  first_name          │
                    │  last_name           │
                    │  country             │
                    │  marital_status      │
                    │  gender              │
                    │  birthdate           │
                    │  create_date         │
                    └──────────┬───────────┘
                               │
                               │ 1:N
                               │
         ┌─────────────────────▼─────────────────────┐
         │           FACT_SALES                      │
         ├───────────────────────────────────────────┤
         │  order_number                             │
         │ *product_key      (FK)                    │
         │ *customer_key     (FK)                    │
         │  order_date                               │
         │  shipping_date                            │
         │  due_date                                 │
         │  sales_amount                             │
         │  quantity                                 │
         │  price                                    │
         └─────────────────────┬─────────────────────┘
                               │
                               │ N:1
                               │
                    ┌──────────▼───────────┐
                    │   DIM_PRODUCTS       │
                    ├──────────────────────┤
                    │ *product_key         │
                    │  product_id          │
                    │  product_number      │
                    │  product_name        │
                    │  category_id         │
                    │  category            │
                    │  subcategory         │
                    │  maintenance         │
                    │  cost                │
                    │  product_line        │
                    │  start_date          │
                    └──────────────────────┘

* = Primary Key
FK = Foreign Key
```

### Cardinality

| Relationship | Type | Description |
|--------------|------|-------------|
| Customer → Sales | 1:N | One customer can have many sales orders |
| Product → Sales | 1:N | One product can appear in many sales orders |
| Sales → Customer | N:1 | Many sales orders belong to one customer |
| Sales → Product | N:1 | Many sales orders contain one product |

---

## Implementation Details

### View Creation

All Gold layer entities are implemented as **database views** for the following reasons:

**Advantages:**
- ✅ **Automatic Refresh**: Always shows current Silver layer data
- ✅ **Storage Efficiency**: No data duplication
- ✅ **Simplified Maintenance**: Single source of truth
- ✅ **Consistency**: Guaranteed synchronization with Silver layer

**Trade-offs:**
- ⚠️ **Query Performance**: May be slower than materialized views for complex queries
- ⚠️ **No Indexing**: Cannot create indexes directly on views

### Grant Permissions

```sql
-- Grant SELECT permissions from Silver to Gold
GRANT SELECT ON silver.crm_cust_info TO gold;
GRANT SELECT ON silver.erp_cust_a1z12 TO gold;
GRANT SELECT ON silver.erp_loc_a101 TO gold;
GRANT SELECT ON silver.crm_prod_info TO gold;
GRANT SELECT ON silver.erp_px_cat_g1v2 TO gold;
GRANT SELECT ON silver.crm_sales_details TO gold;
```

### View Definitions

#### DIM_CUSTOMER View

```sql
CREATE VIEW gold.dim_customer AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.cntry AS country,
    ci.cst_material_status AS marital_status,
    CASE 
        WHEN ci.cst_gndr != N'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, N'n/a')
    END AS gender,
    ca.bdate AS birthdate,
    ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_a1z12 ca ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la ON ci.cst_key = la.cid;
```

#### DIM_PRODUCTS View

```sql
CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
    pn.prd_id AS product_id,
    pn.prd_key AS product_number,
    pn.prd_nm AS product_name,
    pn.cat_id AS category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintenance,
    pn.prd_cost AS cost,
    pn.prd_line AS product_line,
    pn.prd_start_dt AS start_date
FROM silver.crm_prod_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL;
```

#### FACT_SALES View

```sql
CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num AS order_number,
    pr.product_key,
    cu.customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS shipping_date,
    sd.sls_due_dt AS due_date,
    sd.sls_sales AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customer cu ON sd.sls_cust_id = cu.customer_id;
```

---

## Query Examples

### 1. Customer Analysis

#### Total Sales by Customer
```sql
SELECT 
    c.customer_key,
    c.first_name,
    c.last_name,
    c.country,
    COUNT(f.order_number) AS total_orders,
    SUM(f.sales_amount) AS total_sales,
    AVG(f.sales_amount) AS avg_order_value
FROM gold.fact_sales f
JOIN gold.dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.customer_key, c.first_name, c.last_name, c.country
ORDER BY total_sales DESC;
```

#### Sales by Country
```sql
SELECT 
    c.country,
    COUNT(DISTINCT f.customer_key) AS customer_count,
    COUNT(f.order_number) AS order_count,
    SUM(f.sales_amount) AS total_sales
FROM gold.fact_sales f
JOIN gold.dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.country
ORDER BY total_sales DESC;
```

#### Customer Demographics Analysis
```sql
SELECT 
    c.gender,
    c.marital_status,
    COUNT(DISTINCT c.customer_key) AS customer_count,
    ROUND(AVG(EXTRACT(YEAR FROM SYSDATE) - EXTRACT(YEAR FROM c.birthdate)), 1) AS avg_age,
    SUM(f.sales_amount) AS total_sales
FROM gold.dim_customer c
LEFT JOIN gold.fact_sales f ON c.customer_key = f.customer_key
GROUP BY c.gender, c.marital_status
ORDER BY total_sales DESC;
```

### 2. Product Analysis

#### Top Selling Products
```sql
SELECT 
    p.product_key,
    p.product_name,
    p.category,
    p.subcategory,
    SUM(f.quantity) AS units_sold,
    SUM(f.sales_amount) AS total_revenue,
    ROUND(SUM(f.sales_amount) / NULLIF(SUM(f.quantity), 0), 2) AS avg_selling_price,
    p.cost,
    ROUND(SUM(f.sales_amount) - (p.cost * SUM(f.quantity)), 2) AS gross_profit
FROM gold.fact_sales f
JOIN gold.dim_products p ON f.product_key = p.product_key
GROUP BY p.product_key, p.product_name, p.category, p.subcategory, p.cost
ORDER BY total_revenue DESC;
```

#### Sales by Category
```sql
SELECT 
    p.category,
    p.subcategory,
    COUNT(DISTINCT p.product_key) AS product_count,
    SUM(f.quantity) AS total_quantity,
    SUM(f.sales_amount) AS total_sales,
    ROUND(AVG(f.price), 2) AS avg_price
FROM gold.fact_sales f
JOIN gold.dim_products p ON f.product_key = p.product_key
GROUP BY p.category, p.subcategory
ORDER BY total_sales DESC;
```

#### Product Performance by Line
```sql
SELECT 
    p.product_line,
    COUNT(DISTINCT p.product_key) AS product_count,
    SUM(f.sales_amount) AS total_sales,
    SUM(f.quantity) AS units_sold,
    ROUND(SUM(f.sales_amount) / NULLIF(SUM(f.quantity), 0), 2) AS avg_price
FROM gold.fact_sales f
JOIN gold.dim_products p ON f.product_key = p.product_key
GROUP BY p.product_line
ORDER BY total_sales DESC;
```

### 3. Time-Based Analysis

#### Monthly Sales Trend
```sql
SELECT 
    TO_CHAR(f.order_date, 'YYYY-MM') AS year_month,
    COUNT(f.order_number) AS order_count,
    SUM(f.quantity) AS total_quantity,
    SUM(f.sales_amount) AS total_sales,
    ROUND(AVG(f.sales_amount), 2) AS avg_order_value
FROM gold.fact_sales f
GROUP BY TO_CHAR(f.order_date, 'YYYY-MM')
ORDER BY year_month;
```

#### Yearly Sales Comparison
```sql
SELECT 
    EXTRACT(YEAR FROM f.order_date) AS year,
    SUM(f.sales_amount) AS total_sales,
    COUNT(DISTINCT f.customer_key) AS unique_customers,
    COUNT(DISTINCT f.order_number) AS total_orders,
    ROUND(SUM(f.sales_amount) / COUNT(DISTINCT f.customer_key), 2) AS sales_per_customer
FROM gold.fact_sales f
GROUP BY EXTRACT(YEAR FROM f.order_date)
ORDER BY year;
```

#### Day of Week Analysis
```sql
SELECT 
    TO_CHAR(f.order_date, 'Day') AS day_of_week,
    COUNT(f.order_number) AS order_count,
    SUM(f.sales_amount) AS total_sales,
    ROUND(AVG(f.sales_amount), 2) AS avg_order_value
FROM gold.fact_sales f
GROUP BY TO_CHAR(f.order_date, 'Day'), TO_CHAR(f.order_date, 'D')
ORDER BY TO_CHAR(f.order_date, 'D');
```

### 4. Combined Analysis

#### Customer-Product Cross Analysis
```sql
SELECT 
    c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    p.category,
    COUNT(f.order_number) AS order_count,
    SUM(f.sales_amount) AS total_spent,
    SUM(f.quantity) AS total_quantity
FROM gold.fact_sales f
JOIN gold.dim_customer c ON f.customer_key = c.customer_key
JOIN gold.dim_products p ON f.product_key = p.product_key
GROUP BY c.customer_id, c.first_name, c.last_name, p.category
ORDER BY total_spent DESC;
```

#### Geographic Product Performance
```sql
SELECT 
    c.country,
    p.category,
    p.product_line,
    SUM(f.sales_amount) AS total_sales,
    SUM(f.quantity) AS units_sold
FROM gold.fact_sales f
JOIN gold.dim_customer c ON f.customer_key = c.customer_key
JOIN gold.dim_products p ON f.product_key = p.product_key
GROUP BY c.country, p.category, p.product_line
ORDER BY c.country, total_sales DESC;
```

### 5. Data Quality Checks

#### Orphan Records Check
```sql
-- Check for sales records without valid dimensions
SELECT COUNT(*) AS orphan_sales_count
FROM gold.fact_sales f
WHERE f.customer_key IS NULL 
   OR f.product_key IS NULL;
```

#### Missing Dimension Attributes
```sql
-- Check for customers with incomplete data
SELECT 
    COUNT(*) AS total_customers,
    SUM(CASE WHEN country = 'n/a' OR country IS NULL THEN 1 ELSE 0 END) AS missing_country,
    SUM(CASE WHEN gender = 'n/a' THEN 1 ELSE 0 END) AS missing_gender,
    SUM(CASE WHEN birthdate IS NULL THEN 1 ELSE 0 END) AS missing_birthdate
FROM gold.dim_customer;
```

---

## Performance Optimization

### Recommended Strategies

#### 1. Materialized Views (Future Enhancement)
For production environments with large data volumes:

```sql
CREATE MATERIALIZED VIEW gold.mv_fact_sales
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND
AS
SELECT * FROM gold.fact_sales;

-- Schedule refresh
BEGIN
    DBMS_SCHEDULER.CREATE_JOB (
        job_name        => 'refresh_gold_mv',
        job_type        => 'PLSQL_BLOCK',
        job_action      => 'BEGIN DBMS_MVIEW.REFRESH(''GOLD.MV_FACT_SALES'', ''C''); END;',
        start_date      => SYSTIMESTAMP,
        repeat_interval => 'FREQ=DAILY; BYHOUR=2',
        enabled         => TRUE
    );
END;
/
```

**Benefits:**
- Pre-computed joins improve query performance
- Reduce load on Silver layer tables
- Enable indexing on materialized views

#### 2. Indexing Strategy

If using materialized views, create these indexes:

```sql
-- Fact table indexes
CREATE INDEX idx_fact_customer ON gold.mv_fact_sales(customer_key);
CREATE INDEX idx_fact_product ON gold.mv_fact_sales(product_key);
CREATE INDEX idx_fact_order_date ON gold.mv_fact_sales(order_date);
CREATE BITMAP INDEX idx_fact_year ON gold.mv_fact_sales(EXTRACT(YEAR FROM order_date));

-- Dimension table indexes
CREATE UNIQUE INDEX idx_dim_customer_pk ON gold.dim_customer(customer_key);
CREATE INDEX idx_dim_customer_country ON gold.dim_customer(country);
CREATE UNIQUE INDEX idx_dim_product_pk ON gold.dim_products(product_key);
CREATE INDEX idx_dim_product_category ON gold.dim_products(category);
```

#### 3. Partitioning (For Large Datasets)

```sql
-- Partition fact table by order date
CREATE TABLE gold.fact_sales_partitioned (
    order_number NVARCHAR2(50),
    product_key NUMBER,
    customer_key NUMBER,
    order_date DATE,
    shipping_date DATE,
    due_date DATE,
    sales_amount NUMBER,
    quantity NUMBER,
    price NUMBER
)
PARTITION BY RANGE (order_date) (
    PARTITION p_2023 VALUES LESS THAN (TO_DATE('2024-01-01', 'YYYY-MM-DD')),
    PARTITION p_2024 VALUES LESS THAN (TO_DATE('2025-01-01', 'YYYY-MM-DD')),
    PARTITION p_2025 VALUES LESS THAN (MAXVALUE)
);
```

#### 4. Query Optimization Tips

**Use Dimension Filters First:**
```sql
-- Good: Filter on dimension first
SELECT /*+ USE_NL(f p) */ 
    p.category, SUM(f.sales_amount)
FROM gold.dim_products p
JOIN gold.fact_sales f ON p.product_key = f.product_key
WHERE p.category = 'Bikes'
GROUP BY p.category;

-- Avoid: Filtering after join
SELECT p.category, SUM(f.sales_amount)
FROM gold.fact_sales f
JOIN gold.dim_products p ON f.product_key = p.product_key
WHERE p.category = 'Bikes'
GROUP BY p.category;
```

**Aggregate at the Lowest Level:**
```sql
-- Efficient: Pre-aggregate before joining
SELECT c.country, agg.total_sales
FROM (
    SELECT customer_key, SUM(sales_amount) AS total_sales
    FROM gold.fact_sales
    GROUP BY customer_key
) agg
JOIN gold.dim_customer c ON agg.customer_key = c.customer_key;
```

---

## Usage Guidelines

### For Business Analysts

#### Best Practices
1. **Use Descriptive Column Names**: Gold layer uses business-friendly names
2. **Always Join Through Keys**: Use `customer_key` and `product_key` for joins
3. **Filter Early**: Apply WHERE clauses on dimensions before joining to facts
4. **Use Aggregates**: Leverage SUM, AVG, COUNT for metrics
5. **Check for NULLs**: Handle potential NULL values in optional fields

#### Common Queries Template
```sql
-- Template for typical analytical query
SELECT 
    -- Dimensions
    dim1.attribute,
    dim2.attribute,
    -- Measures
    COUNT(DISTINCT fact.order_number) AS order_count,
    SUM(fact.sales_amount) AS total_sales,
    AVG(fact.sales_amount) AS avg_sales
FROM gold.fact_sales fact
JOIN gold.dim_customer dim1 ON fact.customer_key = dim1.customer_key
JOIN gold.dim_products dim2 ON fact.product_key = dim2.product_key
WHERE 
    -- Filters on dimensions
    dim1.country = 'United States'
    AND fact.order_date >= DATE '2024-01-01'
GROUP BY dim1.attribute, dim2.attribute
ORDER BY total_sales DESC;
```

### For BI Tool Integration

#### Connection Settings
- **Schema**: `GOLD`
- **Tables to Expose**: `dim_customer`, `dim_products`, `fact_sales`
- **Join Relationships**: Define in BI tool's data model

#### Tableau Example
```
Data Source → Oracle → GOLD schema
- Add: dim_customer
- Add: dim_products  
- Add: fact_sales

Relationships:
- fact_sales.customer_key → dim_customer.customer_key
- fact_sales.product_key →
<img width="831" height="386" alt="27" src="https://github.com/user-attachments/assets/cf6b98e2-58de-496c-8ee0-22600bd2fb83" />
<img width="1176" height="567" alt="26" src="https://github.com/user-attachments/assets/722b6084-9391-4210-b670-98bf0146bc83" />
<img width="609" height="523" alt="25" src="https://github.com/user-attachments/assets/d027c807-fc42-44d4-b619-3397f4801673" />
<img width="1112" height="698" alt="24" src="https://github.com/user-attachments/assets/8580a243-0ed1-4221-889d-c2af0593636f" />
<img width="1047" height="719" alt="23" src="https://github.com/user-attachments/assets/cdde18fb-c6b6-46e0-a12d-1c16b2829381" />
<img width="1511" height="645" alt="22" src="https://github.com/user-attachments/assets/e83d94e6-cb66-489d-a4fa-a668191f30e1" />
<img width="752" height="684" alt="21" src="https://github.com/user-attachments/assets/7b3060ee-f310-4b9a-99ed-c361e4bd69b6" />
<img width="1319" height="645" alt="20" src="https://github.com/user-attachments/assets/0fa28632-d624-4a83-ad9e-9c600899422d" />
<img width="1221" height="651" alt="19" src="https://github.com/user-attachments/assets/1b9fc2cf-63ec-45b0-895c-b53b6fbca1fe" />
<img width="608" height="593" alt="18" src="https://github.com/user-attachments/assets/678cd6c9-4cd4-4810-9498-a2b7f386a4f0" />
<img width="919" height="702" alt="17" src="https://github.com/user-attachments/assets/cd7df320-f4ad-4f0a-815a-aec66fcba53a" />
<img width="1068" height="726" alt="16" src="https://github.com/user-attachments/assets/a7200ed3-f292-432c-a47b-dd63cefbe8d3" />
<img width="928" height="725" alt="15" src="https://github.com/user-attachments/assets/df608d4b-4618-421a-8335-3b4b797e1ab4" />
<img width="612" height="293" alt="14" src="https://github.com/user-attachments/assets/130498e8-b497-4f5b-adfc-4c68ab008963" />
<img width="1191" height="392" alt="13" src="https://github.com/user-attachments/assets/0845b92c-3781-4b98-ab0c-567efa6769f8" />
<img width="579" height="600" alt="12" src="https://github.com/user-attachments/assets/6736cb46-7145-4cd8-8416-3bf1f751229b" />
<img width="580" height="447" alt="11" src="https://github.com/user-attachments/assets/6bbc11ec-76ba-49ef-865b-90b17fce1063" />
<img width="1285" height="699" alt="10" src="https://github.com/user-attachments/assets/18071f90-5edc-4a26-9ef5-c7cf53629a49" />
<img width="1100" height="714" alt="9" src="https://github.com/user-attachments/assets/f16e82e4-28a2-4371-8d91-20c4242f668f" />
<img width="1074" height="699" alt="8" src="https://github.com/user-attachments/assets/5e289979-3bb1-4b1a-b981-55b3df52b2f6" />
<img width="1155" height="710" alt="7" src="https://github.com/user-attachments/assets/0fb087df-c846-4246-b423-b78fe0c5db80" />
<img width="551" height="624" alt="6" src="https://github.com/user-attachments/assets/2dfa87a5-cdc0-4566-92f9-f5848812987d" />
<img width="405" height="620" alt="5" src="https://github.com/user-attachments/assets/c0f2fa66-6daa-4d64-a958-09629b1ba2e4" />
<img width="458" height="623" alt="4" src="https://github.com/user-attachments/assets/3679604a-3701-4589-9b07-28ca07ea471d" />
<img width="1139" height="717" alt="3" src="https://github.com/user-attachments/assets/2de94232-ac00-49d3-86d1-58ff7aab723d" />
<img width="1073" height="697" alt="2" src="https://github.com/user-attachments/assets/00bd7238-dfc7-4efa-bbfd-63b0ec5f0d25" />
<img width="829" height="734" alt="1" src="https://github.com/user-attachments/assets/058ec035-ecd5-4f8e-9ec4-8ec4cffe0d29" />
