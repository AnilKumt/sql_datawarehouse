CREATE OR REPLACE PROCEDURE bronze.load_bronze AS
-- AUTHID DEFINER is the default and correct for this procedure.
-- It runs with the permissions of its owner, BRONZE.
BEGIN
    DBMS_OUTPUT.PUT_LINE('--- Starting Bronze Layer Load Procedure ---');

    -- Step 1: Truncate existing data for a full refresh
    DBMS_OUTPUT.PUT_LINE('Truncating target tables...');
    EXECUTE IMMEDIATE 'TRUNCATE TABLE bronze.crm_cust_info';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE bronze.crm_prod_info';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE bronze.crm_sales_details';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE bronze.erp_loc_a101';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE bronze.erp_cust_a1z12';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE bronze.erp_px_cat_g1v2';
    DBMS_OUTPUT.PUT_LINE('Truncate complete.');

    -- Step 2: Load data from external tables into target tables
    DBMS_OUTPUT.PUT_LINE('Loading data into bronze.crm_cust_info...');
    INSERT INTO bronze.crm_cust_info SELECT * FROM bronze.crm_cust_info_ext;
    DBMS_OUTPUT.PUT_LINE(SQL%ROWCOUNT || ' rows inserted.');

    DBMS_OUTPUT.PUT_LINE('Loading data into bronze.crm_prod_info...');
    INSERT INTO bronze.crm_prod_info SELECT * FROM bronze.crm_prod_info_ext;
    DBMS_OUTPUT.PUT_LINE(SQL%ROWCOUNT || ' rows inserted.');

    DBMS_OUTPUT.PUT_LINE('Loading data into bronze.crm_sales_details...');
    INSERT INTO bronze.crm_sales_details SELECT * FROM bronze.crm_sales_details_ext;
    DBMS_OUTPUT.PUT_LINE(SQL%ROWCOUNT || ' rows inserted.');

    DBMS_OUTPUT.PUT_LINE('Loading data into bronze.erp_loc_a101...');
    INSERT INTO bronze.erp_loc_a101 SELECT * FROM bronze.erp_loc_a101_ext;
    DBMS_OUTPUT.PUT_LINE(SQL%ROWCOUNT || ' rows inserted.');

    DBMS_OUTPUT.PUT_LINE('Loading data into bronze.erp_cust_a1z12...');
    INSERT INTO bronze.erp_cust_a1z12 SELECT * FROM bronze.erp_cust_a1z12_ext;
    DBMS_OUTPUT.PUT_LINE(SQL%ROWCOUNT || ' rows inserted.');

    DBMS_OUTPUT.PUT_LINE('Loading data into bronze.erp_px_cat_g1v2...');
    INSERT INTO bronze.erp_px_cat_g1v2 SELECT * FROM bronze.erp_px_cat_g1v2_ext;
    DBMS_OUTPUT.PUT_LINE(SQL%ROWCOUNT || ' rows inserted.');

    -- Commit the entire transaction only after all steps succeed
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('--- Load successful. All changes committed. ---');

EXCEPTION
    WHEN OTHERS THEN
        -- If any error occurs during the process, undo all changes
        ROLLBACK;
        -- Output the error message for debugging
        DBMS_OUTPUT.PUT_LINE('--- ERROR! ---');
        DBMS_OUTPUT.PUT_LINE('An error occurred: ' || SQLERRM);
        DBMS_OUTPUT.PUT_LINE('Transaction has been rolled back.');
        -- Re-raise the exception so the calling application knows it failed
        RAISE;
END;
/
