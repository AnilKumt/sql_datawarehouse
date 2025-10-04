-- Enable DBMS output
SET SERVEROUTPUT ON SIZE 1000000;

-- Step 1: Give user quota on tablespace USERS
ALTER USER bronze QUOTA UNLIMITED ON USERS;

-- Step 2: Create directories for CSV files
CREATE OR REPLACE DIRECTORY SOURCE_CRM_DIR AS 'C:\sem five\DBMS\sql-data-warehouse-project\datasets\source_crm';
CREATE OR REPLACE DIRECTORY SOURCE_ERP_DIR AS 'C:\sem five\DBMS\sql-data-warehouse-project\datasets\source_erp';

GRANT READ, WRITE ON DIRECTORY SOURCE_CRM_DIR TO bronze;
GRANT READ, WRITE ON DIRECTORY SOURCE_ERP_DIR TO bronze;

CREATE TABLE bronze.crm_cust_info_ext (
    cst_id                NUMBER,
    cst_key               NVARCHAR2(50),
    cst_firstname         NVARCHAR2(50),
    cst_lastname          NVARCHAR2(50),
    cst_material_status   NVARCHAR2(50),
    cst_gndr              NVARCHAR2(50),
    cst_create_date       DATE
)
ORGANIZATION EXTERNAL (
    TYPE ORACLE_LOADER
    DEFAULT DIRECTORY SOURCE_CRM_DIR
    ACCESS PARAMETERS (
        RECORDS DELIMITED BY NEWLINE
        SKIP 1
        FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
        MISSING FIELD VALUES ARE NULL
        (
          cst_id,
          cst_key,
          cst_firstname,
          cst_lastname,
          cst_material_status,
          cst_gndr,
          cst_create_date DATE "YYYY-MM-DD"
        )
    )
    LOCATION ('cust_info.csv')
)
REJECT LIMIT UNLIMITED;


INSERT INTO bronze.crm_cust_info
SELECT * FROM bronze.crm_cust_info_ext;

COMMIT;

CREATE TABLE bronze.crm_prod_info_ext (
        prd_id NUMBER,
        prd_key NVARCHAR2(50),
        prd_nm NVARCHAR2(50),
        prd_cost NUMBER,
        prd_line NVARCHAR2(50),
        prd_start_dt TIMESTAMP,
        prd_end_dt TIMESTAMP
)
ORGANIZATION EXTERNAL (
    TYPE ORACLE_LOADER
    DEFAULT DIRECTORY SOURCE_CRM_DIR
    ACCESS PARAMETERS (
        RECORDS DELIMITED BY NEWLINE
        SKIP 1
        FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' 
        MISSING FIELD VALUES ARE NULL
        (
        prd_id,
          prd_key,
          prd_nm,
          prd_cost,
          prd_line,
          prd_start_dt DATE "YYYY-MM-DD HH24:MI:SS",
          prd_end_dt DATE "YYYY-MM-DD HH24:MI:SS"
        )
    )
    LOCATION ('prd_info.csv')
)
REJECT LIMIT UNLIMITED;

INSERT INTO bronze.crm_prod_info
SELECT * FROM bronze.crm_prod_info_ext;

COMMIT;

CREATE TABLE bronze.crm_sales_details_ext (
    sls_ord_num NVARCHAR2(50),
    sls_prd_key NVARCHAR2(50),
    sls_cust_id NUMBER,
    sls_order_dt NUMBER,
    sls_ship_dt NUMBER,
    sls_due_dt NUMBER,
    sls_sales NUMBER,
    sls_quantity NUMBER,
    sls_price NUMBER
)
ORGANIZATION EXTERNAL (
    TYPE ORACLE_LOADER
    DEFAULT DIRECTORY SOURCE_CRM_DIR
    ACCESS PARAMETERS (
        RECORDS DELIMITED BY NEWLINE
        SKIP 1
        FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' 
        MISSING FIELD VALUES ARE NULL
        (
          sls_ord_num,
          sls_prd_key,
          sls_cust_id,
          sls_order_dt,
          sls_ship_dt,
          sls_due_dt,
          sls_sales,
          sls_quantity,
          sls_price
        )
    )
    LOCATION ('sales_details.csv')
)
REJECT LIMIT UNLIMITED;


INSERT INTO bronze.crm_sales_details
SELECT * FROM bronze.crm_sales_details_ext;

COMMIT;

CREATE TABLE bronze.erp_loc_a101_ext (
    cid NVARCHAR2(50),
    cntry NVARCHAR2(50)
)
ORGANIZATION EXTERNAL (
    TYPE ORACLE_LOADER
    DEFAULT DIRECTORY SOURCE_ERP_DIR
    ACCESS PARAMETERS (
        RECORDS DELIMITED BY NEWLINE
        SKIP 1
        FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' 
        MISSING FIELD VALUES ARE NULL
        (
          cid,
          cntry
        )
    )
    LOCATION ('loc_a101.csv')
)
REJECT LIMIT UNLIMITED;

INSERT INTO bronze.erp_loc_a101
SELECT * FROM bronze.erp_loc_a101_ext;

COMMIT;

CREATE TABLE bronze.erp_cust_a1z12_ext (
    cid NVARCHAR2(50),
    bdate DATE,
    gen NVARCHAR2(50)
)
ORGANIZATION EXTERNAL (
    TYPE ORACLE_LOADER
    DEFAULT DIRECTORY SOURCE_ERP_DIR
    ACCESS PARAMETERS (
        RECORDS DELIMITED BY NEWLINE
        SKIP 1
        FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' 
        MISSING FIELD VALUES ARE NULL
        (
          cid,
          bdate DATE "YYYY-MM-DD",
          gen
        )
    )
    LOCATION ('cust_a1z12.csv')
)
REJECT LIMIT UNLIMITED;

INSERT INTO bronze.erp_cust_a1z12
SELECT * FROM bronze.erp_cust_a1z12_ext;

COMMIT;

CREATE TABLE bronze.erp_px_cat_g1v2_ext (
    id NVARCHAR2(50),
    cat NVARCHAR2(50),
    subcat NVARCHAR2(50),
    maintenance NVARCHAR2(50)
)
ORGANIZATION EXTERNAL (
    TYPE ORACLE_LOADER
    DEFAULT DIRECTORY SOURCE_ERP_DIR
    ACCESS PARAMETERS (
        RECORDS DELIMITED BY NEWLINE
        SKIP 1
        FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' 
        MISSING FIELD VALUES ARE NULL
        (
          id,
          cat,
          subcat,
          maintenance
        )
    )
    LOCATION ('px_cat_g1v2.csv')
)
REJECT LIMIT UNLIMITED;

INSERT INTO bronze.erp_px_cat_g1v2
SELECT * FROM bronze.erp_px_cat_g1v2_ext;

commit;


