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
- **Privileges**: `CONNECT`, `RESOURCE`, `CREATE ANY DIRECTORY`
- **Tablespace**: `USERS` (Unlimited Quota)

## Data Sources

### 1. CRM System
**Directory**: `SOURCE_CRM_DIR` → `datasets\source_crm`

#### CSV Files & Tables:
- **cust_info.csv** → **crm_cust_info**: Customer information
- **prd_info.csv** → **crm_prod_info**: Product information  
- **sales_details.csv** → **crm_sales_details**: Sales transaction details

### 2. ERP System
**Directory**: `SOURCE_ERP_DIR` → `datasets\source_erp`

#### CSV Files & Tables:
- **loc_a101.csv** → **erp_loc_a101**: Location data
- **cust_a1z12.csv** → **erp_cust_a1z12**: Customer demographic data
- **px_cat_g1v2.csv** → **erp_px_cat_g1v2**: Product category and maintenance information

## Table Structures

### CRM Tables

#### `crm_cust_info`
| Column | Type | Description |
|--------|------|-------------|
| cst_id | NUMBER | Customer ID (Primary Key) |
| cst_key | NVARCHAR2(50) | Customer Key |
| cst_firstname | NVARCHAR2(50) | First Name |
| cst_lastname | NVARCHAR2(50) | Last Name |
| cst_material_status | NVARCHAR2(50) | Marital Status |
| cst_gndr | NVARCHAR2(50) | Gender |
| cst_create_date | DATE | Customer Creation Date |
<img width="502" height="418" alt="1" src="https://github.com/user-attachments/assets/c80f8ac8-5062-432d-a1be-4ca958d1a738" />
<img width="1320" height="522" alt="2" src="https://github.com/user-attachments/assets/65e59758-42b0-4f10-8e9a-542e4fa0357f" />

**Source**: `cust_info.csv`  
**Known Issues**: Contains duplicate `cst_id` values (resolved in Silver layer)

#### `crm_prod_info`
| Column | Type | Description |
|--------|------|-------------|
| prd_id | NUMBER | Product ID |
| prd_key | NVARCHAR2(50) | Product Key (format: CAT-ID-ProductNum) |
| prd_nm | NVARCHAR2(50) | Product Name |
| prd_cost | NUMBER | Product Cost |
| prd_line | NVARCHAR2(50) | Product Line (M/R/S/T codes) |
| prd_start_dt | TIMESTAMP | Product Start Date |
| prd_end_dt | TIMESTAMP | Product End Date |

**Source**: `prd_info.csv`  
**Date Format**: `YYYY-MM-DD HH24:MI:SS`  
**Notes**: Product key contains embedded category ID in first 5 characters

#### `crm_sales_details`
| Column | Type | Description |
|--------|------|-------------|
| sls_ord_num | NVARCHAR2(50) | Sales Order Number |
| sls_prd_key | NVARCHAR2(50) | Product Key (FK to crm_prod_info) |
| sls_cust_id | NUMBER | Customer ID (FK to crm_cust_info) |
| sls_order_dt | NUMBER | Order Date (YYYYMMDD format) |
| sls_ship_dt | NUMBER | Ship Date (YYYYMMDD format) |
| sls_due_dt | NUMBER | Due Date (YYYYMMDD format) |
| sls_sales | NUMBER | Sales Amount |
| sls_quantity | NUMBER | Quantity |
| sls_price | NUMBER | Unit Price |

**Source**: `sales_details.csv`  
**Notes**: Dates stored as numeric YYYYMMDD (e.g., 20240115)

### ERP Tables

#### `erp_loc_a101`
| Column | Type | Description |
|--------|------|-------------|
| cid | NVARCHAR2(50) | Customer ID (matches cst_key) |
| cntry | NVARCHAR2(50) | Country Code |

**Source**: `loc_a101.csv`  
**Notes**: Country codes include: DE, US, USA, etc.

#### `erp_cust_a1z12`
| Column | Type | Description |
|--------|------|-------------|
| cid | NVARCHAR2(50) | Customer ID (may have NAS prefix) |
| bdate | DATE | Birth Date |
| gen | NVARCHAR2(50) | Gender (F/M/FEMALE/MALE) |

**Source**: `cust_a1z12.csv`  
**Date Format**: `YYYY-MM-DD`  
**Notes**: Some customer IDs have "NAS" prefix that needs cleaning

#### `erp_px_cat_g1v2`
| Column | Type | Description |
|--------|------|-------------|
| id | NVARCHAR2(50) | Product Category ID |
| cat | NVARCHAR2(50) | Category |
| subcat | NVARCHAR2(50) | Subcategory |
| maintenance | NVARCHAR2(50) | Maintenance Info |

**Source**: `px_cat_g1v2.csv`  
**Notes**: Category IDs match first 5 characters of product keys (with - replaced by _)
<img width="931" height="818" alt="3" src="https://github.com/user-attachments/assets/893c6577-5001-4262-9c33-1d840ad2bdfb" />
<img width="452" height="379" alt="5" src="https://github.com/user-attachments/assets/f0959411-29a8-4c53-be13-e7b74aff7b11" />

## Implementation

### External Tables Pattern

The Bronze layer uses Oracle's **External Tables** feature to read CSV files directly without physical data loading:

```sql
CREATE TABLE bronze.[table_name]_ext (
    -- Column definitions
)
ORGANIZATION EXTERNAL (
    TYPE ORACLE_LOADER
    DEFAULT DIRECTORY [SOURCE_DIR]
    ACCESS PARAMETERS (
        RECORDS DELIMITED BY NEWLINE
        SKIP 1  -- Skip CSV header row
        FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
        MISSING FIELD VALUES ARE NULL
        (
            -- Field mappings with date formats
            field_name DATE "YYYY-MM-DD"
        )
    )
    LOCATION ('[filename].csv')
)
REJECT LIMIT UNLIMITED;
```

**Benefits**:
- No data movement during initial read
- Automatic CSV parsing with Oracle's loader
- Error handling via reject limits
- Performance optimization through external access

### Load Process

The Bronze layer implements a **full refresh** strategy using the stored procedure `load_bronze`.

#### Stored Procedure: `bronze.load_bronze`

**Execution Authority**: `AUTHID DEFINER` (runs with BRONZE user permissions)

**Process Flow**:
1. **Truncate Phase**: Remove existing data from all 6 Bronze tables
2. **Load Phase**: Insert data from external tables (reads CSV files)
3. **Commit Phase**: Commit all changes as a single transaction
4. **Error Handling**: Rollback on any failure with detailed logging

**Transaction Management**:
- All-or-nothing approach ensures data consistency
- Automatic rollback on any error
- Detailed logging via DBMS_OUTPUT
- Row counts reported for each table

**Tables Loaded** (in sequence):
1. `crm_cust_info` ← `crm_cust_info_ext`
2. `crm_prod_info` ← `crm_prod_info_ext`
3. `crm_sales_details` ← `crm_sales_details_ext`
4. `erp_loc_a101` ← `erp_loc_a101_ext`
5. `erp_cust_a1z12` ← `erp_cust_a1z12_ext`
6. `erp_px_cat_g1v2` ← `erp_px_cat_g1v2_ext`

## Setup Instructions

### Prerequisites
- Oracle Database 11g or higher
- Access to XEPDB1 pluggable database
- SYSDBA or privileged user access
- CSV files in designated directories

### 1. Initial Database Setup

Run `init_database.sql` as SYSDBA:

```sql
ALTER SESSION SET CONTAINER = XEPDB1;
SET SERVEROUTPUT ON SIZE 1000000;

-- Creates bronze user with required privileges
-- Also creates: datawarehouse, silver, gold users
```

### 2. Configure Directories and Permissions

Run `bronze_execute_stored_procedure.sql` (first section):

```sql
-- Grant tablespace quota
ALTER USER bronze QUOTA UNLIMITED ON USERS;
GRANT CREATE ANY DIRECTORY TO bronze;

-- Create directories pointing to CSV file locations
CREATE OR REPLACE DIRECTORY SOURCE_CRM_DIR AS 
    'C:\sem five\DBMS\sql-data-warehouse-project\datasets\source_crm';
CREATE OR REPLACE DIRECTORY SOURCE_ERP_DIR AS 
    'C:\sem five\DBMS\sql-data-warehouse-project\datasets\source_erp';

-- Grant directory access to bronze user
GRANT READ, WRITE ON DIRECTORY SOURCE_CRM_DIR TO bronze;
GRANT READ, WRITE ON DIRECTORY SOURCE_ERP_DIR TO bronze;
```

### 3. Create External and Target Tables

Run `bronze_load_scripts.sql`:

- Creates 6 external table definitions (`*_ext` tables)
- Creates 6 corresponding permanent Bronze tables
- Initial INSERT statements (can be replaced by stored procedure)

### 4. Create Stored Procedure

Run `bronze_stored_procedure.sql`:

```sql
CREATE OR REPLACE PROCEDURE bronze.load_bronze AS
BEGIN
    -- Truncate all tables
    -- Load from external tables
    -- Commit or rollback
END;
```

### 5. Execute Load Process

Run `bronze_execute_stored_procedure.sql` (execution section):

```sql
SET SERVEROUTPUT ON;

BEGIN
    bronze.load_bronze;
END;
/
```

**Expected Output**:
```
--- Starting Bronze Layer Load Procedure ---
Truncating target tables...
Truncate complete.
Loading data into bronze.crm_cust_info...
[X] rows inserted.
Loading data into bronze.crm_prod_info...
[X] rows inserted.
...
--- Load successful. All changes committed. ---
```

## File Structure

```
project-root/
├── README.md                                    # This file
├── scripts/
│   ├── init_database.sql                        # User initialization (SYSDBA)
│   ├── bronze_load_scripts.sql                  # External & target table DDL
│   ├── bronze_stored_procedure.sql              # Load procedure definition
│   ├── bronze_execute_stored_procedure.sql      # Directory setup + execution
│   ├── silver_and_bronze.sql                    # Combined Bronze+Silver load
│   └── [silver/gold scripts...]                 # Downstream layer scripts
└── datasets/
    ├── source_crm/
    │   ├── cust_info.csv
    │   ├── prd_info.csv
    │   └── sales_details.csv
    └── source_erp/
        ├── loc_a101.csv
        ├── cust_a1z12.csv
        └── px_cat_g1v2.csv
```

## Error Handling

The Bronze layer implements comprehensive error handling:

### External Table Level
- **Reject Limits**: `REJECT LIMIT UNLIMITED` allows load to continue despite bad rows
- **Reject Files**: Malformed rows logged to `*_ext_*.bad` files in source directory
- **Log Files**: Detailed parsing errors in `*_ext_*.log` files

### Stored Procedure Level
- **Transaction Rollback**: Any error triggers full rollback
- **Error Logging**: `SQLERRM` captured and output via DBMS_OUTPUT
- **Exception Re-raising**: Errors propagated to calling application
- **Row Count Logging**: Successful inserts reported for verification

### Common Error Scenarios
1. **ORA-29913**: Directory path doesn't exist or Oracle can't access it
2. **ORA-29400**: Data type conversion error in CSV data
3. **ORA-30653**: Reject limit reached (shouldn't happen with UNLIMITED)

## Data Quality Notes

### Known Issues in Bronze Layer
These are **intentionally preserved** in Bronze and resolved in Silver:

1. **Duplicate Customer IDs**: `crm_cust_info` contains duplicate `cst_id` values
2. **Date Inconsistencies**: Product end dates may be before start dates
3. **Missing Values**: NULLs and zeros in numeric fields
4. **Formatting Issues**: Whitespace, inconsistent gender/status codes
5. **Key Mismatches**: Some foreign key references may be invalid
6. **Data Type Issues**: Dates stored as numbers in sales_details

**Philosophy**: Bronze preserves **all** source data issues for auditability. Silver layer performs cleansing and transformation.

## Monitoring and Verification

### Check Row Counts
```sql
SELECT 'crm_cust_info' AS table_name, COUNT(*) AS row_count 
FROM bronze.crm_cust_info
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
SELECT * FROM bronze.crm_sales_details WHERE ROWNUM <= 10;
```

### Check External Tables
```sql
-- Test external table access
SELECT COUNT(*) FROM bronze.crm_cust_info_ext;

-- Check for rejected rows (should return from bad file if exists)
SELECT * FROM bronze.crm_cust_info_ext WHERE ROWNUM <= 5;
```

### Verify Directory Permissions
```sql
SELECT directory_name, directory_path, privilege
FROM dba_tab_privs 
WHERE grantee = 'BRONZE' AND table_name LIKE '%DIR';
```

## Best Practices

1. **Data Preservation**: Never modify source CSV files or Bronze data
2. **Full Refresh Strategy**: Use truncate and reload for simplicity and consistency
3. **Error Handling**: Always check DBMS_OUTPUT after procedure execution
4. **Directory Paths**: Verify CSV files exist before running load procedure
5. **Permissions**: Ensure Oracle process has OS-level read access to directories
6. **Backup**: Keep copies of source CSV files before loading
7. **Documentation**: Document any known data quality issues found in source
8. **Scheduling**: Consider scheduling Bronze loads during off-peak hours

## Integration with Silver Layer

The Bronze layer provides data to the Silver layer through:

1. **Direct Table Access**: Silver reads from Bronze tables (not external tables)
2. **Grants Required**: Bronze must grant SELECT to Silver user
3. **Load Sequence**: Bronze completes before Silver transformation starts
4. **Combined Load**: Use `silver_and_bronze.sql` for sequential execution

```sql
-- Combined load execution
BEGIN
    bronze.load_bronze;
    silver.silver_full_load_all_tables;
END;
```

## Dependencies

- **Oracle Database**: 11g or higher (External Tables support required)
- **Pluggable Database**: XEPDB1 container
- **OS Access**: Oracle must have read permissions on CSV directories
- **CSV Files**: Must be present and readable in configured directories
- **Character Set**: UTF-8 recommended for NVARCHAR2 columns
- **Disk Space**: Sufficient space in USERS tablespace for data volume

## Performance Considerations

- **External Tables**: Read CSV files on-demand (no initial load overhead)
- **Bulk Insert**: Full table scans from external tables
- **Transaction Size**: All 6 tables loaded in single transaction
- **Commit Strategy**: Single commit after all loads complete
- **Parallel Processing**: Not implemented (sequential loads)

**Estimated Load Times** (varies by data volume):
- Small datasets (<10K rows): < 1 minute
- Medium datasets (10K-100K rows): 1-5 minutes  
- Large datasets (>100K rows): 5+ minutes

## Troubleshooting

### Issue: "Directory does not exist" error
**Cause**: Directory path invalid or Oracle can't access it  
**Solution**: 
- Verify path exists on database server
- Check OS-level permissions for Oracle process
- Use correct path separator (\ for Windows, / for Unix)

### Issue: "Insufficient privileges" error
**Cause**: Missing directory permissions  
**Solution**: 
```sql
GRANT READ, WRITE ON DIRECTORY SOURCE_CRM_DIR TO bronze;
```

### Issue: No data loaded (0 rows)
**Cause**: CSV files missing or empty  
**Solution**: 
- Verify CSV files exist in directory
- Check file has data rows (not just header)
- Review reject/log files for parsing errors

### Issue: Date format errors
**Cause**: CSV date format doesn't match external table definition  
**Solution**: 
- Verify CSV date format matches `DATE "YYYY-MM-DD"` specification
- Check for invalid dates or format inconsistencies

### Issue: Character encoding errors
**Cause**: CSV file encoding doesn't match database character set  
**Solution**: 
- Convert CSV files to UTF-8
- Verify NLS_LANG settings

## Next Steps

After successful Bronze layer implementation:

1. **Proceed to Silver Layer**: Data cleansing and transformation
   - Run `silver_ddl.sql` to create Silver tables
   - Run `silver_clean_and_load_procedure.sql` to create transformation logic
   - Execute `silver_clean_and_load_exec.sql` to load Silver layer

2. **Implement Data Quality Checks**: 
   - Document data issues found in Bronze
   - Define cleansing rules for Silver transformation
   
3. **Establish Monitoring**: 
   - Track load execution times
   - Monitor row counts and data volumes
   - Set up alerts for load failures

4. **Document Data Lineage**: 
   - CSV → Bronze (raw) → Silver (cleaned) → Gold (aggregated)

## Contributing

For questions, issues, or improvements:
- Review technical documentation for implementation details
- Check troubleshooting section for common issues
- Contact the data engineering team for support

---

**Version**: 1.0  
**Last Updated**: October 2025  
**Maintained By**: Data Engineering Team  
**Project**: SQL Data Warehouse - Medallion Architecture
