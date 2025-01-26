--- Task 1 ---
CREATE TABLE MyTable(id number, val number);

--- Task 2 ---
DECLARE
    i NUMBER;
BEGIN
    FOR i in 1..10000 LOOP
        INSERT INTO MyTable (id, val)
        VALUES (i, DBMS_RANDOM.VALUE(1, 1000000));
        END LOOP;
        COMMIT;
END;

--- Task 3 ---
CREATE OR REPLACE FUNCTION CheckEvenOrOdd RETURN VARCHAR2 IS
    even_count NUMBER := 0;
    odd_count NUMBER := 0;
BEGIN
    SELECT COUNT(CASE WHEN MOD(val, 2) = 0 THEN 1 END),
           COUNT(CASE WHEN MOD(val, 2) <> 0 THEN 1 END)
    INTO even_count, odd_count
    FROM MyTable;

    IF even_count > odd_count THEN
        RETURN 'TRUE';
    ELSIF odd_count > even_count THEN
        RETURN 'FALSE';
    ELSE
        RETURN 'EQUAL';
    END IF;
END;

--- Task 4 ---
CREATE OR REPLACE FUNCTION CreateInsertFunction(f_id IN NUMBER) RETURN VARCHAR2 IS
    v_id MyTable.id%TYPE;
    v_val MyTable.val%TYPE;
    v_command VARCHAR2(4000);
BEGIN
    SELECT id, val
    INTO v_id, v_val
    FROM MyTable
    WHERE id = f_id;
    v_command := 'INSERT INTO MyTable (id, val) VALUES (' || v_id || ', ' || v_val || '):';
    DBMS_OUTPUT.PUT_LINE(v_command);
    RETURN v_command;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE('Error. There is no this ID.');
        RETURN NULL;
END;

BEGIN
    DBMS_OUTPUT.PUT_LINE(CREATEINSERTFUNCTION(5));
END;
