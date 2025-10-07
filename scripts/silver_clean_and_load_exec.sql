GRANT SELECT ON bronze.crm_cust_info TO silver;
GRANT SELECT ON bronze.crm_prod_info TO silver;
GRANT SELECT ON bronze.crm_sales_details TO silver;
GRANT SELECT ON bronze.erp_cust_a1z12 TO silver;
GRANT SELECT ON bronze.erp_loc_a101 TO silver;
GRANT SELECT ON bronze.erp_px_cat_g1v2 TO silver;
GRANT INSERT ON silver.crm_cust_info TO silver;
GRANT INSERT ON silver.crm_prod_info TO silver;
GRANT INSERT ON silver.crm_sales_details TO silver;
GRANT INSERT ON silver.erp_cust_a1z12 TO silver;
GRANT INSERT ON silver.erp_loc_a101 TO silver;
GRANT INSERT ON silver.erp_px_cat_g1v2 TO silver;

BEGIN
    silver.silver_full_load_all_tables;
END;
/

--ALTER PROCEDURE silver.silver_full_load_all_tables COMPILE;