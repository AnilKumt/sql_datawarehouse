SELECT
cid,
cntry
FROM bronze.erp_loc_a101;

SELECT cst_key FROM silver.crm_cust_info;

SELECT
REPLACE(cid,'-','') cid,
cntry FROM bronze.erp_loc_a101 
WHERE REPLACE(cid,'-','') 
NOT IN (
SELECT cst_key FROM silver.crm_cust_info
);

SELECT DISTINCT cntry
FROM bronze.erp_loc_a101
ORDER BY cntry;


SELECT DISTINCT
REPLACE(cid,'-','') AS cid,
CASE WHEN TRIM(cntry) = N'DE' THEN N'Germany'
    WHEN TRIM(cntry) IN (N'US',N'USA') THEN N'United States'
    WHEN TRIM(cntry) =N'' OR cntry IS NULL THEN N'n/a'
    ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101;

SELECT DISTINCT cntry
FROM silver.erp_loc_a101
ORDER BY cntry;

INSERT INTO silver.erp_loc_a101 (cid,cntry)
SELECT 
REPLACE(cid,'-','') AS cid,
CASE WHEN TRIM(cntry) = N'DE' THEN N'Germany'
    WHEN TRIM(cntry) IN (N'US',N'USA') THEN N'United States'
    WHEN TRIM(cntry) =N'' OR cntry IS NULL THEN N'n/a'
    ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101;





