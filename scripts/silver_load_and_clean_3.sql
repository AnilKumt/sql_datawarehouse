SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details;

SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num);

SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info);

SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details;

SELECT 
NULLIF(sls_order_dt,0)
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LENGTH(sls_order_dt) != 8;

SELECT 
NULLIF(sls_order_dt,0)
FROM bronze.crm_sales_details
WHERE sls_order_dt >20250510;

SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE 
    WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8
        THEN NULL
    ELSE TO_DATE(TO_CHAR(sls_order_dt), 'YYYYMMDD')
END AS sls_order_dt,
CASE 
    WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8
        THEN NULL
    ELSE TO_DATE(TO_CHAR(sls_ship_dt), 'YYYYMMDD')
END AS sls_ship_dt,
CASE 
    WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) != 8
        THEN NULL
    ELSE TO_DATE(TO_CHAR(sls_due_dt), 'YYYYMMDD')
END AS sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details;



SELECT * 
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_ship_dt;



SELECT sls_sales,sls_quantity,sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity*sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0;

SELECT DISTINCT sls_sales,sls_quantity,sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity*sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0
ORDER BY sls_sales,sls_quantity,sls_price;


SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE 
    WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8
        THEN NULL
    ELSE TO_DATE(TO_CHAR(sls_order_dt), 'YYYYMMDD')
END AS sls_order_dt,
CASE 
    WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8
        THEN NULL
    ELSE TO_DATE(TO_CHAR(sls_ship_dt), 'YYYYMMDD')
END AS sls_ship_dt,
CASE 
    WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) != 8
        THEN NULL
    ELSE TO_DATE(TO_CHAR(sls_due_dt), 'YYYYMMDD')
END AS sls_due_dt,
sls_sales AS old_sls_sales,
CASE 
    WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales
END AS sls_sales,
sls_price as old_sls_price,
CASE 
    WHEN sls_price IS NULL OR sls_price <=0
        THEN sls_sales * NULLIF(sls_quantity,0)
    ELSE sls_price
END AS sls_price,
sls_quantity

FROM bronze.crm_sales_details;

SELECT sls_ord_num,sls_prd_key,sls_cust_id,
CASE 
    WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8
        THEN NULL
    ELSE TO_DATE(TO_CHAR(sls_order_dt), 'YYYYMMDD')
END AS sls_order_dt,
CASE 
    WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8
        THEN NULL
    ELSE TO_DATE(TO_CHAR(sls_ship_dt), 'YYYYMMDD')
END AS sls_ship_dt,
CASE 
    WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) != 8
        THEN NULL
    ELSE TO_DATE(TO_CHAR(sls_due_dt), 'YYYYMMDD')
END AS sls_due_dt,
CASE 
    WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales
END AS sls_sales,
CASE 
    WHEN sls_price IS NULL OR sls_price <=0
        THEN sls_sales * NULLIF(sls_quantity,0)
    ELSE sls_price
END AS sls_price,
sls_quantity
FROM bronze.crm_sales_details;


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

    -- Cleaned sales and price logic
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



