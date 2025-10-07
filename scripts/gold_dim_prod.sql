SELECT
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_nm,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt,
pn.prd_end_dt
FROM silver.crm_prod_info pn;

--end=null means current

SELECT
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_nm,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt,
pn.prd_end_dt
FROM silver.crm_prod_info pn
WHERE prd_end_dt IS NULL;

SELECT
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_nm,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt
FROM silver.crm_prod_info pn
WHERE prd_end_dt IS NOT NULL;

SELECT
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_nm,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt,
pc.cat,
pc.subcat,
pc.maintenance
FROM silver.crm_prod_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id=pc.id
WHERE prd_end_dt IS NULL;



SELECT prd_key, COUNT(*) FROM(
SELECT
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_nm,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt,
pc.cat,
pc.subcat,
pc.maintenance
FROM silver.crm_prod_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id=pc.id
WHERE prd_end_dt IS NULL)
GROUP BY prd_key
HAVING COUNT(*)>1;

SELECT
    pn.prd_id,
    pn.prd_key,
    pn.prd_nm,
    pn.cat_id,
    pc.cat,
    pc.subcat,
    pc.maintenance,
    pn.prd_cost,
    pn.prd_line,
    pn.prd_start_dt
FROM silver.crm_prod_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id=pc.id
WHERE prd_end_dt IS NULL;


SELECT
    pn.prd_id AS product_id,
    pn.prd_key AS product_number,
    pn.prd_nm AS product_name,
    pn.cat_id As category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintenance,
    pn.prd_cost AS cost,
    pn.prd_line AS product_line,
    pn.prd_start_dt AS start_date
FROM silver.crm_prod_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id=pc.id
WHERE prd_end_dt IS NULL;

SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt,pn.prd_key) AS product_key,
    pn.prd_id AS product_id,
    pn.prd_key AS product_number,
    pn.prd_nm AS product_name,
    pn.cat_id As category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintenance,
    pn.prd_cost AS cost,
    pn.prd_line AS product_line,
    pn.prd_start_dt AS start_date
FROM silver.crm_prod_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id=pc.id
WHERE prd_end_dt IS NULL;


GRANT SELECT ON silver.crm_prod_info TO gold;
GRANT SELECT ON silver.erp_px_cat_g1v2 TO gold;


CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt,pn.prd_key) AS product_key,
    pn.prd_id AS product_id,
    pn.prd_key AS product_number,
    pn.prd_nm AS product_name,
    pn.cat_id As category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintenance,
    pn.prd_cost AS cost,
    pn.prd_line AS product_line,
    pn.prd_start_dt AS start_date
FROM silver.crm_prod_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id=pc.id
WHERE prd_end_dt IS NULL;


SELECT * FROM gold.dim_products;











