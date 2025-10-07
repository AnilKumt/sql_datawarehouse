SELECT
cid,
bdate,
gen
FROM bronze.erp_cust_a1z12;

SELECT * FROM silver.crm_cust_info;

SELECT
cid,
bdate,
gen
FROM bronze.erp_cust_a1z12
WHERE cid LIKE '%AW00011000%';

SELECT
cid,
CASE 
    WHEN cid LIKE 'NAS%' THEN SUBSTR(cid,4,LENGTH(cid))
    ELSE cid
END AS cid,
bdate,
gen
FROM bronze.erp_cust_a1z12;

SELECT
cid,
CASE 
    WHEN cid LIKE 'NAS%' THEN SUBSTR(cid,4,LENGTH(cid))
    ELSE cid
END AS cid,
bdate,
gen
FROM bronze.erp_cust_a1z12
WHERE CASE WHEN cid LIKE 'NAS%' 
    THEN SUBSTR(cid,4,LENGTH(cid))
    ELSE cid
END NOT IN (SELECT DISTINCT cst_key 
FROM silver.crm_cust_info);

SELECT DISTINCT 
bdate
FROM bronze.erp_cust_a1z12
WHERE bdate < TO_DATE('1924-01-01','YYYY-MM-DD') OR bdate > SYSDATE;


SELECT
CASE 
    WHEN cid LIKE 'NAS%' THEN SUBSTR(cid,4,LENGTH(cid))
    ELSE cid
END AS cid,
bdate,
CASE WHEN bdate > SYSDATE THEN NULL
    ELSE bdate
END AS bdate,
gen
FROM bronze.erp_cust_a1z12;

SELECT DISTINCT gen
FROM bronze.erp_cust_a1z12;

SELECT DISTINCT
gen,
CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
    WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
    ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_a1z12;


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





