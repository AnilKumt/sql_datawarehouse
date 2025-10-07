SELECT cst_id,COUNT(*) FROM silver.crm_cust_info 
GROUP BY cst_id 
HAVING COUNT(*)>1 OR cst_id IS NULL;

SELECT *
FROM (
    SELECT t.*,
           ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM silver.crm_cust_info t
) sub
WHERE flag_last != 1;

SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;


