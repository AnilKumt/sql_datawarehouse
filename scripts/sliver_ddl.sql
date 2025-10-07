-- Enable DBMS Output
SET SERVEROUTPUT ON SIZE 1000000;

DECLARE
    v_count NUMBER;

    -- Helper procedure to drop and create a table
    PROCEDURE drop_and_create_table(p_table_name IN VARCHAR2, p_create_sql IN VARCHAR2) IS
    BEGIN
        -- Check if table exists in SILVER schema
        SELECT COUNT(*) INTO v_count
        FROM all_tables
        WHERE owner = 'SILVER' AND table_name = UPPER(p_table_name);

        IF v_count > 0 THEN
            -- Drop the table if it exists
            EXECUTE IMMEDIATE 'DROP TABLE silver.' || p_table_name || ' CASCADE CONSTRAINTS';
            DBMS_OUTPUT.PUT_LINE('Table ' || p_table_name || ' existed and was dropped.');
        END IF;

        -- Create the table
        EXECUTE IMMEDIATE p_create_sql;
        DBMS_OUTPUT.PUT_LINE('Table ' || p_table_name || ' created successfully.');

    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Error processing table ' || p_table_name || ': ' || SQLERRM);
    END;

BEGIN
    -- crm_cust_info
    drop_and_create_table('CRM_CUST_INFO', '
        CREATE TABLE silver.crm_cust_info(
            cst_id NUMBER,
            cst_key NVARCHAR2(50),
            cst_firstname NVARCHAR2(50),
            cst_lastname NVARCHAR2(50),
            cst_material_status NVARCHAR2(50),
            cst_gndr NVARCHAR2(50),
            cst_create_date DATE,
            dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )');

    -- crm_prod_info
    drop_and_create_table('CRM_PROD_INFO', '
        CREATE TABLE silver.crm_prod_info(
            prd_id NUMBER,
            prd_key NVARCHAR2(50),
            prd_nm NVARCHAR2(50),
            prd_cost NUMBER,
            prd_line NVARCHAR2(50),
            prd_start_dt TIMESTAMP,
            prd_end_dt TIMESTAMP,
            dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )');

    -- crm_sales_details
    drop_and_create_table('CRM_SALES_DETAILS', '
        CREATE TABLE silver.crm_sales_details(
            sls_ord_num NVARCHAR2(50),
            sls_prd_key NVARCHAR2(50),
            sls_cust_id NUMBER,
            sls_order_dt DATE,
            sls_ship_dt DATE,
            sls_due_dt DATE,
            sls_sales NUMBER,
            sls_quantity NUMBER,
            sls_price NUMBER,
            dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )');

    -- erp_loc_a101
    drop_and_create_table('ERP_LOC_A101', '
        CREATE TABLE silver.erp_loc_a101(
            cid NVARCHAR2(50),
            cntry NVARCHAR2(50),
            dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )');

    -- erp_cust_a1z12
    drop_and_create_table('ERP_CUST_A1Z12', '
        CREATE TABLE silver.erp_cust_a1z12(
            cid NVARCHAR2(50),
            bdate DATE,
            gen NVARCHAR2(50),
            dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )');

    -- erp_px_cat_g1v2
    drop_and_create_table('ERP_PX_CAT_G1V2', '
        CREATE TABLE silver.erp_px_cat_g1v2(
            id NVARCHAR2(50),
            cat NVARCHAR2(50),
            subcat NVARCHAR2(50),
            maintenance NVARCHAR2(50),
            dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )');

    DBMS_OUTPUT.PUT_LINE('All tables dropped (if existed) and created successfully.');

END;
/
