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

--- Task 5 ---
-- INSERT --
CREATE OR REPLACE PROCEDURE InsertIntoMyTable(
    p_id IN MyTable.id%TYPE,
    p_val IN MyTable.val%TYPE
) IS
BEGIN
    INSERT INTO MyTable(id, val)
    VALUES (p_id, p_val);
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Строка успешно добавлена: ID = ' || p_id || ', val = ' || p_val);
END;
/

BEGIN
   InsertIntoMyTable(1, 100);
   InsertIntoMyTable(2,200);
END;
/

-- UPDATE --
CREATE OR REPLACE PROCEDURE UpdateMyTable(
    p_id IN MyTable.id%TYPE,
    p_val in MyTable.val%TYPE
) IS
BEGIN
    UPDATE MyTable
    SET val = p_val
    WHERE id = p_id;

    IF SQL%ROWCOUNT = 0 THEN
        DBMS_OUTPUT.PUT_LINE('Ошибка: строка с ID = ' || p_id || ' не найдена.');
    ELSE
        COMMIT;
        DBMS_OUTPUT.PUT_LINE('Строка успешно обновлена: ID = ' || p_id || ', новое val = ' || p_val);
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Ошибка: ' || SQLERRM);
END;
/

BEGIN
    UpdateMyTable(1, 150);
END;
/

-- DELETE --
CREATE OR REPLACE PROCEDURE DeleteFromMyTable(
    p_id IN MyTable.id%TYPE
) IS
BEGIN
    DELETE FROM MyTable
    WHERE id = p_id;

    IF SQL%ROWCOUNT = 0 THEN
        DBMS_OUTPUT.PUT_LINE('Ошибка: строка с ID = ' || p_id || ' не найдена.');
    ELSE
        COMMIT;
        DBMS_OUTPUT.PUT_LINE('Строка успешно удалена: ID = ' || p_id);
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Ошибка: ' || SQLERRM);
END;
/

BEGIN
    DeleteFromMyTable(10000);
END;
/

--- Task 6 ---
CREATE OR REPLACE FUNCTION CalculateReward(
    f_monthly_salary IN NUMBER,
    f_bonus_percent IN NUMBER
) RETURN NUMBER IS
    r_reward NUMBER;
BEGIN
    IF f_monthly_salary IS NULL OR f_monthly_salary < 0 THEN
        RAISE_APPLICATION_ERROR(-20001, 'Некорректное значение месячной зарплаты.');
    END IF;

    IF f_bonus_percent IS NULL OR f_bonus_percent < 0 THEN
        RAISE_APPLICATION_ERROR(-20002, 'Некорректное значение процента премиальных.');
    END IF;

    r_reward := (1 + f_bonus_percent / 100) * 12 * f_monthly_salary;

    RETURN r_reward;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Ошибка: ' || SQLERRM);
        RETURN NULL;
END;
/

DECLARE
    r_reward NUMBER;
BEGIN
   r_reward := CalculateReward(50000, 10);
   DBMS_OUTPUT.PUT_LINE('Общее вознаграждение за год: ' || r_reward);
END;
/