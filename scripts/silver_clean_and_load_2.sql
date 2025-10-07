SELECT 
prd_id,
COUNT(*)
FROM bronze.crm_prod_info
GROUP BY prd_id
HAVING COUNT(*)>1 OR prd_id IS NULL;

SELECT
prd_id,
prd_key,
SUBSTR(prd_key,1,5) AS cat_id,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prod_info;

SELECT
prd_id,
prd_key,
REPLACE(SUBSTR(prd_key,1,5),'-','_') AS cat_id,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prod_info;

SELECT
prd_id,
prd_key,
REPLACE(SUBSTR(prd_key,1,5),'-','_') AS cat_id,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prod_info
WHERE REPLACE(SUBSTR(prd_key,1,5),'-','_') NOT IN
(SELECT DISTINCT id from bronze.erp_px_cat_g1v2);

SELECT
prd_id,
prd_key,
REPLACE(SUBSTR(prd_key,1,5),'-','_') AS cat_id,
SUBSTR(prd_key,7,LENGTH(prd_key)) AS prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prod_info;

SELECT
prd_id,
prd_key,
REPLACE(SUBSTR(prd_key,1,5),'-','_') AS cat_id,
SUBSTR(prd_key,7,LENGTH(prd_key)) AS prd_key,
prd_nm,
NVL(prd_cost,0) AS prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prod_info
WHERE SUBSTR(prd_key,7,LENGTH(prd_key)) NOT IN (
SELECT sls_prd_key FROM bronze.crm_sales_details);


SELECT prd_nm
FROM bronze.crm_prod_info
WHERE prd_nm != TRIM(prd_nm);

SELECT prd_cost
FROM bronze.crm_prod_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

SELECT DISTINCT prd_line
FROM bronze.crm_prod_info;

SELECT
prd_id,
prd_key,
REPLACE(SUBSTR(prd_key,1,5),'-','_') AS cat_id,
SUBSTR(prd_key,7,LENGTH(prd_key)) AS prd_key,
prd_nm,
NVL(prd_cost,0) AS prd_cost,
CASE WHEN UPPER(TRIM(prd_line))='M' THEN 'Mountain'
    WHEN UPPER(TRIM(prd_line))='R' THEN 'Road'
    WHEN UPPER(TRIM(prd_line))='S' THEN 'Other Sales'
    WHEN UPPER(TRIM(prd_line))='T' THEN 'Touring'
    ELSE 'n/a'
END AS prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prod_info;

SELECT
prd_id,
prd_key,
REPLACE(SUBSTR(prd_key,1,5),'-','_') AS cat_id,
SUBSTR(prd_key,7,LENGTH(prd_key)) AS prd_key,
prd_nm,
NVL(prd_cost,0) AS prd_cost,
CASE UPPER(TRIM(prd_line))
        WHEN N'M' THEN N'Mountain'
        WHEN N'R' THEN N'Road'
        WHEN N'S' THEN N'Other Sales'
        WHEN N'T' THEN N'Touring'
        ELSE N'n/a'
END AS prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prod_info;

SELECT *
FROM bronze.crm_prod_info
WHERE prd_end_dt < prd_start_dt;

SELECT
prd_id,
prd_key,
prd_nm,
prd_start_dt,
CAST (prd_end_dt AS DATE),
CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt_test
FROM bronze.crm_prod_info
WHERE prd_key IN ('AC-HE-HL-U509-R','AC-HE-HL-U509');

SELECT
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)
      - INTERVAL '1' SECOND AS prd_end_dt_test
FROM bronze.crm_prod_info
WHERE prd_key IN ('AC-HE-HL-U509-R','AC-HE-HL-U509');

-- Repair the DDL
DROP TABLE silver.crm_prod_info;
CREATE TABLE silver.crm_prod_info(
    prd_id  INT,
    cat_id  NVARCHAR2(50),
    prd_key NVARCHAR2(50),
    prd_nm  NVARCHAR2(50),
    prd_cost    INT,
    prd_line    NVARCHAR2(50),
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

SELECT
prd_id,
prd_key,
REPLACE(SUBSTR(prd_key,1,5),'-','_') AS cat_id,
SUBSTR(prd_key,7,LENGTH(prd_key)) AS prd_key,
prd_nm,
NVL(prd_cost,0) AS prd_cost,
CASE UPPER(TRIM(prd_line))
        WHEN N'M' THEN N'Mountain'
        WHEN N'R' THEN N'Road'
        WHEN N'S' THEN N'Other Sales'
        WHEN N'T' THEN N'Touring'
        ELSE N'n/a'
END AS prd_line,
CAST (prd_end_dt AS DATE),
CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt_test
FROM bronze.crm_prod_info;

INSERT INTO silver.crm_prod_info(
    prd_id,
    prd_key,
    cat_id,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT
prd_id,
REPLACE(SUBSTR(prd_key,1,5),'-','_') AS cat_id,
SUBSTR(prd_key,7,LENGTH(prd_key)) AS prd_key,
prd_nm,
NVL(prd_cost,0) AS prd_cost,
CASE UPPER(TRIM(prd_line))
        WHEN N'M' THEN N'Mountain'
        WHEN N'R' THEN N'Road'
        WHEN N'S' THEN N'Other Sales'
        WHEN N'T' THEN N'Touring'
        ELSE N'n/a'
END AS prd_line,
CAST (prd_start_dt AS DATE) AS prd_start_dt,
CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
FROM bronze.crm_prod_info;
