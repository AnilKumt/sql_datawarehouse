--1

INSERT INTO silver.crm_cust_info(
    cst_id ,
    cst_key ,
    cst_firstname,
    cst_lastname,
    cst_material_status ,
    cst_gndr ,
    cst_create_date 
)SELECT
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Simgle'
    WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
    ELSE 'n/a'
END cst_material_status,
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
    WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
    ELSE 'n/a'
END cst_gndr,
cst_create_date
FROM(
     SELECT t.*,
           ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info t
)sub WHERE flag_last=1 AND cst_id IS NOT NULL;

--2
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

--3
INSERT INTO silver.crm_sales_details (
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
SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE 
        WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
        ELSE TO_DATE(TO_CHAR(sls_order_dt), 'YYYYMMDD')
    END AS sls_order_dt,
    CASE 
        WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL
        ELSE TO_DATE(TO_CHAR(sls_ship_dt), 'YYYYMMDD')
    END AS sls_ship_dt,
    CASE 
        WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) != 8 THEN NULL
        ELSE TO_DATE(TO_CHAR(sls_due_dt), 'YYYYMMDD')
    END AS sls_due_dt,

    CASE 
        WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,

    sls_quantity,

    CASE 
        WHEN sls_price IS NULL OR sls_price <= 0
            THEN CASE 
                    WHEN sls_quantity IS NOT NULL AND sls_quantity <> 0
                        THEN (CASE 
                                WHEN sls_sales IS NULL OR sls_sales <= 0 
                                    THEN sls_quantity * ABS(sls_price) -- fallback
                                ELSE sls_sales 
                              END) / sls_quantity
                    ELSE NULL
                 END
        ELSE sls_price
    END AS sls_price

FROM bronze.crm_sales_details;

--4
INSERT INTO silver.erp_cust_a1z12 (cid, bdate, gen)
SELECT
CASE 
    WHEN cid LIKE 'NAS%' THEN SUBSTR(cid,4,LENGTH(cid))
    ELSE cid
END AS cid,
CASE WHEN bdate > SYSDATE THEN NULL
    ELSE bdate
END AS bdate,
CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
    WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
    ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_a1z12;

--5

INSERT INTO silver.erp_loc_a101 (cid,cntry)
SELECT 
REPLACE(cid,'-','') AS cid,
CASE WHEN TRIM(cntry) = N'DE' THEN N'Germany'
    WHEN TRIM(cntry) IN (N'US',N'USA') THEN N'United States'
    WHEN TRIM(cntry) =N'' OR cntry IS NULL THEN N'n/a'
    ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101;

--6

INSERT INTO silver.erp_px_cat_g1v2 (
id,
cat,
subcat,
maintenance) (
SELECT * FROM bronze.erp_px_cat_g1v2);


