DataWarehouse - Medallion Architecture


# Bronze Layer - Medallion Architecture

## Overview

The Bronze Layer is the raw data ingestion layer in our medallion architecture data warehouse. It serves as the initial landing zone for data from multiple source systems (CRM and ERP), preserving data in its original form with minimal transformation.

## Architecture

```
Source Systems → Bronze Layer (Raw Data) → Silver Layer → Gold Layer
```

### Purpose
- **Raw Data Storage**: Store data exactly as received from source systems
- **Data Preservation**: Maintain complete historical records
- **Data Lineage**: Establish clear tracking from source to warehouse
- **Minimal Transformation**: No business logic applied at this stage

## Database Schema

### User: `BRONZE`
- **Password**: `bronze123`
- **Privileges**: `CONNECT`, `RESOURCE`
- **Tablespace**: `USERS` (Unlimited Quota)

## Data Sources

### 1. CRM System
**Directory**: `SOURCE_CRM_DIR` → `datasets\source_crm`

#### Tables:
- **crm_cust_info**: Customer information
- **crm_prod_info**: Product information
- **crm_sales_details**: Sales transaction details

### 2. ERP System
**Directory**: `SOURCE_ERP_DIR` → `datasets\source_erp`

#### Tables:
- **erp_loc_a101**: Location data
- **erp_cust_a1z12**: Customer demographic data
- **erp_px_cat_g1v2**: Product category and maintenance information

## Table Structures

### CRM Tables

#### `crm_cust_info`
| Column | Type | Description |
|--------|------|-------------|
| cst_id | NUMBER | Customer ID |
| cst_key | NVARCHAR2(50) | Customer Key |
| cst_firstname | NVARCHAR2(50) | First Name |
| cst_lastname | NVARCHAR2(50) | Last Name |
| cst_material_status | NVARCHAR2(50) | Marital Status |
| cst_gndr | NVARCHAR2(50) | Gender |
| cst_create_date | DATE | Customer Creation Date |

#### `crm_prod_info`
| Column | Type | Description |
|--------|------|-------------|
| prd_id | NUMBER | Product ID |
| prd_key | NVARCHAR2(50) | Product Key |
| prd_nm | NVARCHAR2(50) | Product Name |
| prd_cost | NUMBER | Product Cost |
| prd_line | NVARCHAR2(50) | Product Line |
| prd_start_dt | TIMESTAMP | Start Date |
| prd_end_dt | TIMESTAMP | End Date |

#### `crm_sales_details`
| Column | Type | Description |
|--------|------|-------------|
| sls_ord_num | NVARCHAR2(50) | Sales Order Number |
| sls_prd_key | NVARCHAR2(50) | Product Key |
| sls_cust_id | NUMBER | Customer ID |
| sls_order_dt | NUMBER | Order Date |
| sls_ship_dt | NUMBER | Ship Date |
| sls_due_dt | NUMBER | Due Date |
| sls_sales | NUMBER | Sales Amount |
| sls_quantity | NUMBER | Quantity |
| sls_price | NUMBER | Price |

### ERP Tables

#### `erp_loc_a101`
| Column | Type | Description |
|--------|------|-------------|
| cid | NVARCHAR2(50) | Customer ID |
| cntry | NVARCHAR2(50) | Country |

#### `erp_cust_a1z12`
| Column | Type | Description |
|--------|------|-------------|
| cid | NVARCHAR2(50) | Customer ID |
| bdate | DATE | Birth Date |
| gen | NVARCHAR2(50) | Generation |

#### `erp_px_cat_g1v2`
| Column | Type | Description |
|--------|------|-------------|
| id | NVARCHAR2(50) | Product ID |
| cat | NVARCHAR2(50) | Category |
| subcat | NVARCHAR2(50) | Subcategory |
| maintenance | NVARCHAR2(50) | Maintenance Info |

## Implementation

### External Tables Pattern

The Bronze layer uses Oracle's **External Tables** feature to read CSV files directly:

```sql
ORGANIZATION EXTERNAL (
    TYPE ORACLE_LOADER
    DEFAULT DIRECTORY [SOURCE_DIR]
    ACCESS PARAMETERS (
        RECORDS DELIMITED BY NEWLINE
        SKIP 1
        FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
        MISSING FIELD VALUES ARE NULL
    )
    LOCATION ('[filename].csv')
)
```

**Benefits**:
- No data movement during initial load
- Automatic CSV parsing
- Error handling via reject limits
- Performance optimization

### Load Process

The Bronze layer implements a **full refresh** strategy using the stored procedure `load_bronze`.

#### Stored Procedure: `bronze.load_bronze`

**Execution Authority**: `AUTHID DEFINER` (runs with BRONZE user permissions)

**Process Flow**:
1. Truncate all target tables
2. Load data from external tables
3. Commit transaction
4. Handle errors with rollback

**Transaction Management**:
- All-or-nothing approach
- Automatic rollback on failure
- Detailed logging via DBMS_OUTPUT

## Setup Instructions

### 1. Initial Database Setup

Run as SYSDBA or privileged user:

```sql
ALTER SESSION SET CONTAINER = XEPDB1;
SET SERVEROUTPUT ON SIZE 1000000;

-- Create bronze user if not exists
CREATE USER bronze IDENTIFIED BY bronze123;
GRANT CONNECT, RESOURCE TO bronze;
GRANT CREATE ANY DIRECTORY TO bronze;
```

### 2. Configure Storage and Directories

```sql
-- Grant tablespace quota
ALTER USER bronze QUOTA UNLIMITED ON USERS;

-- Create directories for CSV files
CREATE OR REPLACE DIRECTORY SOURCE_CRM_DIR AS 'C:\sem five\DBMS\sql-data-warehouse-project\datasets\source_crm';
CREATE OR REPLACE DIRECTORY SOURCE_ERP_DIR AS 'C:\sem five\DBMS\sql-data-warehouse-project\datasets\source_erp';

-- Grant directory permissions
GRANT READ, WRITE ON DIRECTORY SOURCE_CRM_DIR TO bronze;
GRANT READ, WRITE ON DIRECTORY SOURCE_ERP_DIR TO bronze;
```

### 3. Create Tables

Run the `bronze_load_scripts.sql` to create:
- External table definitions (e.g., `crm_cust_info_ext`)
- Target tables (e.g., `crm_cust_info`)

### 4. Create Stored Procedure

Run `bronze_stored_procedure.sql` to create the `load_bronze` procedure.

### 5. Execute Load Process

```sql
SET SERVEROUTPUT ON;

BEGIN
    bronze.load_bronze;
END;
/
```

## File Structure

```
bronze/
├── init.sql                          # User and schema initialization
├── bronze_load_scripts.sql           # External and target table definitions
├── bronze_stored_procedure.sql       # Load procedure
└── bronze_execute_stored_procedure.sql  # Execution script
```

## Error Handling

The Bronze layer implements comprehensive error handling:

- **Reject Limits**: `REJECT LIMIT UNLIMITED` for external tables
- **Transaction Rollback**: Automatic rollback on any error
- **Error Logging**: Detailed error messages via DBMS_OUTPUT
- **Exception Re-raising**: Errors propagated to calling application

## Monitoring and Verification

### Check Row Counts
```sql
SELECT 'crm_cust_info' as table_name, COUNT(*) FROM bronze.crm_cust_info
UNION ALL
SELECT 'crm_prod_info', COUNT(*) FROM bronze.crm_prod_info
UNION ALL
SELECT 'crm_sales_details', COUNT(*) FROM bronze.crm_sales_details
UNION ALL
SELECT 'erp_loc_a101', COUNT(*) FROM bronze.erp_loc_a101
UNION ALL
SELECT 'erp_cust_a1z12', COUNT(*) FROM bronze.erp_cust_a1z12
UNION ALL
SELECT 'erp_px_cat_g1v2', COUNT(*) FROM bronze.erp_px_cat_g1v2;
```

### View Sample Data
```sql
SELECT * FROM bronze.crm_cust_info WHERE ROWNUM <= 10;
```

## Best Practices

1. **Data Preservation**: Never modify source data in Bronze layer
2. **Full Refresh**: Use truncate and reload for simplicity
3. **Error Handling**: Always check DBMS_OUTPUT after execution
4. **Directory Paths**: Ensure CSV files exist in specified directories
5. **Permissions**: Verify directory read permissions before loading

## Dependencies

- **Oracle Database**: 11g or higher (External Tables support)
- **PDB**: XEPDB1 (Pluggable Database)
- **CSV Files**: Must be present in configured directories
- **Character Set**: UTF-8 recommended for NVARCHAR2 columns

## Next Steps

After successful Bronze layer implementation:
1. Proceed to **Silver Layer** for data cleansing and transformation
2. Implement data quality checks
3. Establish monitoring and alerting
4. Document data lineage

## Troubleshooting

### Common Issues

**Issue**: "Directory does not exist" error
- **Solution**: Verify directory paths and ensure they exist on the server

**Issue**: "Insufficient privileges" error
- **Solution**: Grant necessary directory permissions to BRONZE user

**Issue**: No data loaded (0 rows)
- **Solution**: Check CSV file format, delimiters, and ensure files are in correct directories

**Issue**: Date format errors
- **Solution**: Verify date format in CSV matches external table definition



