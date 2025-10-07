ALTER USER bronze QUOTA UNLIMITED ON USERS;

GRANT CREATE ANY DIRECTORY TO bronze;

CREATE OR REPLACE DIRECTORY SOURCE_CRM_DIR AS 'C:\sem five\DBMS\sql-data-warehouse-project\datasets\source_crm';
CREATE OR REPLACE DIRECTORY SOURCE_ERP_DIR AS 'C:\sem five\DBMS\sql-data-warehouse-project\datasets\source_erp';

GRANT READ, WRITE ON DIRECTORY SOURCE_CRM_DIR TO bronze;
GRANT READ, WRITE ON DIRECTORY SOURCE_ERP_DIR TO bronze;


SET SERVEROUTPUT ON;

BEGIN
bronze.load_bronze;
silver.silver_full_load_all_tables;
END;
/