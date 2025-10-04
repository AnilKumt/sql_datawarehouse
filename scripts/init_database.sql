ALTER SESSION SET CONTAINER = XEPDB1;
SET SERVEROUTPUT ON SIZE 1000000;
DECLARE
  v_count NUMBER;
BEGIN
  -- Check and create datawarehouse user
  SELECT COUNT(*) INTO v_count FROM dba_users WHERE username = 'DATAWAREHOUSE';
  IF v_count = 0 THEN
    EXECUTE IMMEDIATE 'CREATE USER datawarehouse IDENTIFIED BY strongpassword';
    EXECUTE IMMEDIATE 'GRANT CONNECT, RESOURCE TO datawarehouse';
    DBMS_OUTPUT.PUT_LINE('User DATAWAREHOUSE created.');
  ELSE
    DBMS_OUTPUT.PUT_LINE('User DATAWAREHOUSE already exists.');
  END IF;

  -- Check and create bronze user
  SELECT COUNT(*) INTO v_count FROM dba_users WHERE username = 'BRONZE';
  IF v_count = 0 THEN
    EXECUTE IMMEDIATE 'CREATE USER bronze IDENTIFIED BY bronze123';
    EXECUTE IMMEDIATE 'GRANT CONNECT, RESOURCE TO bronze';
    DBMS_OUTPUT.PUT_LINE('User BRONZE created.');
  ELSE
    DBMS_OUTPUT.PUT_LINE('User BRONZE already exists.');
  END IF;

  -- Check and create silver user
  SELECT COUNT(*) INTO v_count FROM dba_users WHERE username = 'SILVER';
  IF v_count = 0 THEN
    EXECUTE IMMEDIATE 'CREATE USER silver IDENTIFIED BY silver123';
    EXECUTE IMMEDIATE 'GRANT CONNECT, RESOURCE TO silver';
    DBMS_OUTPUT.PUT_LINE('User SILVER created.');
  ELSE
    DBMS_OUTPUT.PUT_LINE('User SILVER already exists.');
  END IF;

  -- Check and create gold user
  SELECT COUNT(*) INTO v_count FROM dba_users WHERE username = 'GOLD';
  IF v_count = 0 THEN
    EXECUTE IMMEDIATE 'CREATE USER gold IDENTIFIED BY gold123';
    EXECUTE IMMEDIATE 'GRANT CONNECT, RESOURCE TO gold';
    DBMS_OUTPUT.PUT_LINE('User GOLD created.');
  ELSE
    DBMS_OUTPUT.PUT_LINE('User GOLD already exists.');
  END IF;

END;
