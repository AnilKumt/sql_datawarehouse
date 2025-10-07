SELECT 
ci.cst_id,
ci.cst_key,
ci.cst_firstname,
ci.cst_lastname,
ci.cst_material_status,
ci.cst_gndr,
ci.cst_create_date
FROM silver.crm_cust_info ci;

SELECT 
ci.cst_id,
ci.cst_key,
ci.cst_firstname,
ci.cst_lastname,
ci.cst_material_status,
ci.cst_gndr,
ci.cst_create_date,
ca.bdate,
ca.gen
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_a1z12 ca
ON ci.cst_key=ca.cid;


SELECT 
    ci.cst_id,
    ci.cst_key,
    ci.cst_firstname,
    ci.cst_lastname,
    ci.cst_material_status,
    ci.cst_gndr,
    ci.cst_create_date,
    ca.bdate,
    ca.gen,
    la.cntry
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_a1z12 ca
ON ci.cst_key=ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key=la.cid;



SELECT cst_id, COUNT(*) FROM (
SELECT 
    ci.cst_id,
    ci.cst_key,
    ci.cst_firstname,
    ci.cst_lastname,
    ci.cst_material_status,
    ci.cst_gndr,
    ci.cst_create_date,
    ca.bdate,
    ca.gen,
    la.cntry
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_a1z12 ca
ON ci.cst_key=ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key=la.cid)
GROUP BY cst_id
HAVING COUNT(*)>1;


SELECT DISTINCT
    ci.cst_gndr,
    ca.gen
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_a1z12 ca
ON ci.cst_key=ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key=la.cid
ORDER BY 1,2;

-- which data is master source crm or erop


SELECT DISTINCT
    ci.cst_gndr,
    ca.gen,
    CASE WHEN ci.cst_gndr != N'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen,N'n/a')
    END AS new_grn
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_a1z12 ca
ON ci.cst_key=ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key=la.cid
ORDER BY 1,2;



SELECT 
    ci.cst_id,
    ci.cst_key,
    ci.cst_firstname,
    ci.cst_lastname,
    ci.cst_material_status,
    CASE WHEN ci.cst_gndr != N'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen,N'n/a')
    END AS new_gen,
    ci.cst_create_date,
    ca.bdate,
    la.cntry
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_a1z12 ca
ON ci.cst_key=ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key=la.cid;

SELECT 
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    ci.cst_material_status AS marital_status,
    CASE WHEN ci.cst_gndr != N'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen,N'n/a')
    END AS gender,
    ci.cst_create_date As create_date,
    ca.bdate AS Birthdate,
    la.cntry AS Country
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_a1z12 ca
ON ci.cst_key=ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key=la.cid;

-- ordering ased on priority of the column

SELECT 
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.cntry AS Country,
    ci.cst_material_status AS marital_status,
    CASE WHEN ci.cst_gndr != N'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen,N'n/a')
    END AS gender,
    ca.bdate AS Birthdate,
    ci.cst_create_date As create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_a1z12 ca
ON ci.cst_key=ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key=la.cid;




SELECT 
    ROW_NUMBER() OVER (ORDER BY cst_id) customer_key,
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.cntry AS Country,
    ci.cst_material_status AS marital_status,
    CASE WHEN ci.cst_gndr != N'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen,N'n/a')
    END AS gender,
    ca.bdate AS Birthdate,
    ci.cst_create_date As create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_a1z12 ca
ON ci.cst_key=ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key=la.cid;


GRANT SELECT ON silver.crm_cust_info TO gold;
GRANT SELECT ON silver.erp_cust_a1z12 TO gold;
GRANT SELECT ON silver.erp_loc_a101 TO gold;



CREATE VIEW gold.dim_customer AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY cst_id) customer_key,
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.cntry AS Country,
    ci.cst_material_status AS marital_status,
    CASE WHEN ci.cst_gndr != N'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen,N'n/a')
    END AS gender,
    ca.bdate AS Birthdate,
    ci.cst_create_date As create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_a1z12 ca
ON ci.cst_key=ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key=la.cid;


--verification

SELECT * FROM gold.dim_customer;

SELECT DISTINCT gender FROM gold.dim_customer;




