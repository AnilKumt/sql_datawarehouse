# Silver Layer Documentation
## Data Warehouse - Medallion Architecture

---

## Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Data Transformation Strategy](#data-transformation-strategy)
4. [Silver Layer Tables](#silver-layer-tables)
5. [Data Quality Rules](#data-quality-rules)
6. [ETL Process](#etl-process)
7. [Code Examples](#code-examples)
8. [Performance Considerations](#performance-considerations)
9. [Monitoring & Validation](#monitoring--validation)

---

## Overview

The **Silver Layer** represents the second tier in the Medallion Architecture, serving as the refined and validated data zone. This layer transforms raw Bronze layer data into clean, conformed, and business-ready datasets.

### Purpose
- **Data Cleansing**: Remove duplicates, handle nulls, and fix data quality issues
- **Standardization**: Apply consistent formatting and business rules
- **Data Validation**: Ensure referential integrity and business logic compliance
- **Type Conversion**: Transform data types for analytical consumption
- **Deduplication**: Keep only the most recent or relevant records

### Key Characteristics
- **Schema**: `silver`
- **Data Quality**: Validated and cleansed
- **Data Format**: Structured tables with consistent schemas
- **Update Frequency**: Full refresh via stored procedures
- **Access Pattern**: Read-optimized for downstream gold layer

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      BRONZE LAYER                            │
│  (Raw Data - Minimal Processing)                             │
│                                                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │ CRM Tables   │  │ ERP Tables   │  │ Source Files │       │
│  └──────────────┘  └──────────────┘  └──────────────┘       │
└───────────────────────────┬─────────────────────────────────┘
                            │
                    ┌───────▼────────┐
                    │  ETL PROCESS   │
                    │  Transforming  │
                    │  Validating    │
                    │  Cleansing     │
                    └───────┬────────┘
                            │
┌───────────────────────────▼─────────────────────────────────┐
│                      SILVER LAYER                            │
│  (Cleansed & Validated Data)                                 │
│                                                               │
│  ┌─────────────────┐  ┌─────────────────┐                   │
│  │ CRM Domain      │  │ ERP Domain      │                   │
│  │                 │  │                 │                   │
│  │ • cust_info     │  │ • cust_a1z12    │                   │
│  │ • prod_info     │  │ • loc_a101      │                   │
│  │ • sales_details │  │ • px_cat_g1v2   │                   │
│  └─────────────────┘  └─────────────────┘                   │
└─────────────────────────────────────────────────────────────┘
```

---

## Data Transformation Strategy

### 1. **Deduplication Strategy**
- Use `ROW_NUMBER()` window function to identify duplicates
- Partition by business keys (e.g., `cst_id`, `prd_id`)
- Keep most recent record based on timestamp ordering
- Filter records where `flag_last = 1`

### 2. **Data Cleansing Rules**
- **String Fields**: Apply `TRIM()` to remove leading/trailing whitespace
- **Null Handling**: Use `NVL()` for default values
- **Invalid Records**: Filter out rows with NULL business keys
- **Date Validation**: Remove future dates and invalid formats

### 3. **Standardization Approach**
- **Gender Values**: Map ('F'/'FEMALE' → 'Female', 'M'/'MALE' → 'Male')
- **Status Values**: Expand abbreviations ('S' → 'Single', 'M' → 'Married')
- **Country Codes**: Convert ISO codes to full names ('DE' → 'Germany')
- **Product Lines**: Decode single-letter codes to full descriptions

---

## Silver Layer Tables

### 1. CRM_CUST_INFO
Customer information from CRM system

| Column | Type | Description | Transformation |
|--------|------|-------------|----------------|
| `cst_id` | NUMBER | Customer ID (PK) | Validated (NOT NULL) |
| `cst_key` | NVARCHAR2(50) | Business key | No change |
| `cst_firstname` | NVARCHAR2(50) | First name | TRIM() applied |
| `cst_lastname` | NVARCHAR2(50) | Last name | TRIM() applied |
| `cst_material_status` | NVARCHAR2(50) | Marital status | 'S'→'Single', 'M'→'Married' |
| `cst_gndr` | NVARCHAR2(50) | Gender | 'F'→'Female', 'M'→'Male' |
| `cst_create_date` | DATE | Creation date | No change |
| `dwh_create_date` | TIMESTAMP | DWH timestamp | Auto-generated |

**Data Quality Rules:**
- ✓ Deduplicated by `cst_id` (keeping latest by `cst_create_date`)
- ✓ NULL `cst_id` records excluded
- ✓ All string fields trimmed

---

### 2. CRM_PROD_INFO
Product catalog information

| Column | Type | Description | Transformation |
|--------|------|-------------|----------------|
| `prd_id` | NUMBER | Product ID (PK) | No change |
| `prd_key` | NVARCHAR2(50) | Product key | Extracted from position 7+ |
| `cat_id` | NVARCHAR2(50) | Category ID | Extracted from positions 1-5, '-' → '_' |
| `prd_nm` | NVARCHAR2(50) | Product name | No change |
| `prd_cost` | NUMBER | Product cost | NVL(prd_cost, 0) |
| `prd_line` | NVARCHAR2(50) | Product line | Decoded (M→Mountain, R→Road, etc.) |
| `prd_start_dt` | DATE | Start date | CAST to DATE |
| `prd_end_dt` | DATE | End date | Calculated using LEAD() |
| `dwh_create_date` | TIMESTAMP | DWH timestamp | Auto-generated |

**Data Quality Rules:**
- ✓ Product key split into `cat_id` and `prd_key`
- ✓ NULL costs replaced with 0
- ✓ End dates calculated from next start date (using LEAD window function)
- ✓ Product line codes standardized

**Product Line Mapping:**
- `M` → `Mountain`
- `R` → `Road`
- `S` → `Other Sales`
- `T` → `Touring`
- Others → `n/a`

---

### 3. CRM_SALES_DETAILS
Sales transaction details

| Column | Type | Description | Transformation |
|--------|------|-------------|----------------|
| `sls_ord_num` | NVARCHAR2(50) | Order number | No change |
| `sls_prd_key` | NVARCHAR2(50) | Product key | No change |
| `sls_cust_id` | NUMBER | Customer ID | No change |
| `sls_order_dt` | DATE | Order date | Validated & converted |
| `sls_ship_dt` | DATE | Ship date | Validated & converted |
| `sls_due_dt` | DATE | Due date | Validated & converted |
| `sls_sales` | NUMBER | Sales amount | Recalculated if inconsistent |
| `sls_quantity` | NUMBER | Quantity | No change |
| `sls_price` | NUMBER | Unit price | Recalculated from sales/quantity |
| `dwh_create_date` | TIMESTAMP | DWH timestamp | Auto-generated |

**Data Quality Rules:**
- ✓ Date validation (length = 8, value > 0)
- ✓ Date conversion from YYYYMMDD integer format
- ✓ Sales amount validation: `sls_sales = sls_quantity × sls_price`
- ✓ Price calculation when NULL or invalid

**Sales Amount Logic:**
```sql
CASE 
    WHEN sls_sales IS NULL OR sls_sales <= 0 
         OR sls_sales != sls_quantity * ABS(sls_price)
    THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales
END
```

---

### 4. ERP_CUST_A1Z12
ERP customer demographic data

| Column | Type | Description | Transformation |
|--------|------|-------------|----------------|
| `cid` | NVARCHAR2(50) | Customer ID | Remove 'NAS' prefix if present |
| `bdate` | DATE | Birth date | Set to NULL if > SYSDATE |
| `gen` | NVARCHAR2(50) | Gender | Standardized to Female/Male/n/a |
| `dwh_create_date` | TIMESTAMP | DWH timestamp | Auto-generated |

**Data Quality Rules:**
- ✓ 'NAS' prefix removal from customer IDs
- ✓ Future birth dates set to NULL
- ✓ Gender standardization (F/FEMALE → Female, M/MALE → Male)

---

### 5. ERP_LOC_A101
ERP location data

| Column | Type | Description | Transformation |
|--------|------|-------------|----------------|
| `cid` | NVARCHAR2(50) | Customer ID | Remove hyphens |
| `cntry` | NVARCHAR2(50) | Country | Standardized to full names |
| `dwh_create_date` | TIMESTAMP | DWH timestamp | Auto-generated |

**Data Quality Rules:**
- ✓ Hyphens removed from customer IDs
- ✓ Country code standardization
- ✓ Empty strings converted to 'n/a'

**Country Mapping:**
- `DE` → `Germany`
- `US`, `USA` → `United States`
- Empty/NULL → `n/a`
- Others → Trimmed value

---

### 6. ERP_PX_CAT_G1V2
ERP product category hierarchy

| Column | Type | Description | Transformation |
|--------|------|-------------|----------------|
| `id` | NVARCHAR2(50) | Category ID | No change |
| `cat` | NVARCHAR2(50) | Category | No change |
| `subcat` | NVARCHAR2(50) | Subcategory | No change |
| `maintenance` | NVARCHAR2(50) | Maintenance flag | No change |
| `dwh_create_date` | TIMESTAMP | DWH timestamp | Auto-generated |

**Data Quality Rules:**
- ✓ Direct copy from Bronze (already clean)
- ✓ No transformation required

---

## Data Quality Rules

### Summary of Transformations

| Rule Type | Bronze → Silver Transformation |
|-----------|-------------------------------|
| **Deduplication** | ROW_NUMBER() OVER (PARTITION BY key ORDER BY timestamp DESC) |
| **String Cleaning** | TRIM() on all VARCHAR/NVARCHAR fields |
| **Null Handling** | NVL() for numeric defaults, NULL for invalid dates |
| **Code Standardization** | CASE statements mapping codes to full descriptions |
| **Date Validation** | Length checks, range validation, format conversion |
| **Calculated Fields** | Derived end dates, recalculated sales amounts |
| **ID Cleansing** | Prefix removal, character replacement |
| **Reference Checking** | Filter orphan records (not implemented in current version) |

---

## ETL Process

### Stored Procedure: `silver.silver_full_load_all_tables`

#### Process Flow

```
1. TRUNCATE existing Silver tables
   ↓
2. Transform & Load CRM_CUST_INFO
   ↓
3. Transform & Load CRM_PROD_INFO
   ↓
4. Transform & Load CRM_SALES_DETAILS
   ↓
5. Transform & Load ERP_CUST_A1Z12
   ↓
6. Transform & Load ERP_LOC_A101
   ↓
7. Transform & Load ERP_PX_CAT_G1V2
   ↓
8. COMMIT Transaction
   ↓
9. Success Message / Error Handling
```

#### Execution

```sql
-- Grant necessary permissions
GRANT SELECT ON bronze.* TO silver;
GRANT INSERT ON silver.* TO silver;

-- Execute full load
BEGIN
    silver.silver_full_load_all_tables;
END;
/
```

#### Error Handling

```sql
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
        RAISE;
END;
```

---

## Code Examples

### Example 1: Customer Deduplication

```sql
-- Keep only the latest customer record per cst_id
INSERT INTO silver.crm_cust_info (
    cst_id, cst_key, cst_firstname, cst_lastname, 
    cst_material_status, cst_gndr, cst_create_date
)
SELECT
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE 
        WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
        ELSE 'n/a'
    END AS cst_material_status,
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a'
    END AS cst_gndr,
    cst_create_date
FROM (
    SELECT t.*, 
           ROW_NUMBER() OVER (
               PARTITION BY cst_id 
               ORDER BY cst_create_date DESC
           ) AS flag_last
    FROM bronze.crm_cust_info t
)
WHERE flag_last = 1 AND cst_id IS NOT NULL;
```

### Example 2: Product Key Splitting

```sql
-- Extract category ID and product key from combined key
INSERT INTO silver.crm_prod_info (
    prd_id, prd_key, cat_id, prd_nm, prd_cost, 
    prd_line, prd_start_dt, prd_end_dt
)
SELECT
    prd_id,
    SUBSTR(prd_key, 7, LENGTH(prd_key)) AS prd_key,
    REPLACE(SUBSTR(prd_key, 1, 5), '-', '_') AS cat_id,
    prd_nm,
    NVL(prd_cost, 0) AS prd_cost,
    CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'T' THEN 'Touring'
        ELSE 'n/a'
    END AS prd_line,
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    CAST(
        LEAD(prd_start_dt) OVER (
            PARTITION BY prd_key 
            ORDER BY prd_start_dt
        ) - 1 AS DATE
    ) AS prd_end_dt
FROM bronze.crm_prod_info;
```

### Example 3: Date Validation & Conversion

```sql
-- Convert integer dates to DATE type with validation
SELECT 
    sls_ord_num,
    CASE 
        WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8 
        THEN NULL
        ELSE TO_DATE(TO_CHAR(sls_order_dt), 'YYYYMMDD')
    END AS sls_order_dt,
    CASE 
        WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8 
        THEN NULL
        ELSE TO_DATE(TO_CHAR(sls_ship_dt), 'YYYYMMDD')
    END AS sls_ship_dt
FROM bronze.crm_sales_details;
```

### Example 4: Sales Amount Reconciliation

```sql
-- Ensure sales = quantity × price
CASE 
    WHEN sls_sales IS NULL 
         OR sls_sales <= 0 
         OR sls_sales != sls_quantity * ABS(sls_price)
    THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales
END AS sls_sales
```

---

## Performance Considerations

### Optimization Strategies

1. **Truncate vs Delete**
   - Use `TRUNCATE TABLE` for full refresh (faster than DELETE)
   - Resets high water mark and releases storage

2. **Bulk Insert**
   - Single INSERT...SELECT statement per table
   - No row-by-row processing
   - Minimizes redo log generation

3. **Window Functions**
   - `ROW_NUMBER()` for deduplication
   - `LEAD()` for date calculations
   - Efficient single-pass processing

4. **Indexing Strategy** (Post-Load)
   ```sql
   CREATE INDEX idx_cust_id ON silver.crm_cust_info(cst_id);
   CREATE INDEX idx_prod_id ON silver.crm_prod_info(prd_id);
   CREATE INDEX idx_sales_dates ON silver.crm_sales_details(sls_order_dt);
   ```

5. **Partition Recommendations**
   - Consider range partitioning on date columns
   - Partition `crm_sales_details` by `sls_order_dt`

### Resource Management

```sql
-- Set quotas
ALTER USER silver QUOTA UNLIMITED ON USERS;

-- Grant directory access for future file operations
GRANT CREATE ANY DIRECTORY TO bronze;
GRANT READ, WRITE ON DIRECTORY SOURCE_CRM_DIR TO bronze;
```

---

## Monitoring & Validation

### Post-Load Validation Queries

#### 1. Check for Duplicates
```sql
-- Verify no duplicate customer IDs
SELECT cst_id, COUNT(*) 
FROM silver.crm_cust_info 
GROUP BY cst_id 
HAVING COUNT(*) > 1 OR cst_id IS NULL;
```

#### 2. Validate Data Quality
```sql
-- Check trimming effectiveness
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

-- Verify standardized values
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;
```

#### 3. Referential Integrity
```sql
-- Check for orphan sales records
SELECT COUNT(*) AS orphan_sales
FROM silver.crm_sales_details s
WHERE NOT EXISTS (
    SELECT 1 FROM silver.crm_cust_info c
    WHERE c.cst_id = s.sls_cust_id
);
```

#### 4. Date Validation
```sql
-- Verify no future birth dates
SELECT DISTINCT bdate
FROM silver.erp_cust_a1z12
WHERE bdate > SYSDATE;

-- Check date ordering in sales
SELECT COUNT(*)
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
   OR sls_order_dt > sls_due_dt;
```

#### 5. Sales Amount Accuracy
```sql
-- Validate sales calculations
SELECT sls_sales, sls_quantity, sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL 
   OR sls_sales <= 0;
```

### Monitoring Dashboard Metrics

| Metric | Query | Acceptable Range |
|--------|-------|------------------|
| Row Count | `SELECT COUNT(*) FROM silver.crm_cust_info` | > 0 |
| Load Duration | Track procedure execution time | < 5 minutes |
| Duplicate Records | Validation query above | 0 |
| NULL Business Keys | Check for NULL primary keys | 0 |
| Data Freshness | `MAX(dwh_create_date)` | Today's date |

---

## Best Practices

### 1. **Idempotency**
- Full refresh approach ensures consistent results
- Each run produces identical output for same input
- No incremental update complexity

### 2. **Transaction Management**
- Single transaction per full load
- ROLLBACK on any error
- All-or-nothing data consistency

### 3. **Error Handling**
- Explicit exception blocks
- Detailed error messages via DBMS_OUTPUT
- Re-raise exceptions for upstream notification

### 4. **Documentation**
- Inline comments for complex transformations
- Business rule documentation
- Change log for procedure modifications

### 5. **Testing Strategy**
- Unit test individual transformations
- Validate each table post-load
- Compare record counts Bronze vs Silver
- Verify business rule application

---

## Change Log

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-01 | DWH Team | Initial Silver layer implementation |
| 1.1 | 2025-01 | DWH Team | Fixed typo: 'Simgle' → 'Single' |
| 1.2 | 2025-01 | DWH Team | Added column order correction for crm_prod_info |

---

## Future Enhancements

1. **Incremental Loading**
   - Implement CDC (Change Data Capture)
   - Add effective dating for SCD Type 2
   - Optimize for large data volumes

2. **Data Lineage**
   - Track source-to-target mappings
   - Implement audit columns
   - Add data quality scores

3. **Advanced Validation**
   - Cross-table referential integrity checks
   - Statistical anomaly detection
   - Automated data profiling

4. **Performance Tuning**
   - Parallel DML enablement
   - Partition pruning optimization
   - Query result caching

---

## Appendix

### A. Naming Conventions

- **Schema**: `silver`
- **Tables**: Retain source table names (e.g., `crm_cust_info`)
- **Columns**: Maintain source column names
- **Procedures**: `silver_full_load_all_tables`
- **Timestamps**: `dwh_create_date` for all tables

### B. Contact Information

- **DBA Team**: dba@company.com
- **Data Engineering**: dataeng@company.com
- **Documentation**: [Internal Wiki Link]

### C. Related Documentation

- [Bronze Layer Documentation](./bronze_layer.md)
- [Gold Layer Documentation](./gold_layer.md)
- [Overall DWH Architecture](./architecture_overview.md)

---

**Document Version**: 1.0  
