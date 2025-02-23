CREATE OR REPLACE TYPE dep_rec AS OBJECT (
    table_name VARCHAR2(128),
    depends_on VARCHAR2(128)
);
/

CREATE OR REPLACE TYPE dep_tab AS TABLE OF dep_rec;
/

CREATE OR REPLACE PROCEDURE compare_schemas (
    dev_schema_name IN VARCHAR2,
    prod_schema_name IN VARCHAR2
) AS
    v_count NUMBER;
    v_cycle_detected BOOLEAN := FALSE;
    v_dependencies dep_tab := dep_tab();

    -- Курсор для сравнения объектов (таблицы, процедуры, функции, пакеты, индексы)
    CURSOR object_diff IS
        -- Таблицы
        SELECT 'TABLE' as object_type, t.table_name as object_name
        FROM all_tables t
        WHERE t.owner = UPPER(dev_schema_name)
        MINUS
        SELECT 'TABLE', t2.table_name
        FROM all_tables t2
        WHERE t2.owner = UPPER(prod_schema_name)
        UNION
        -- Таблицы с различающейся структурой
        SELECT 'TABLE', tc1.table_name
        FROM (
            SELECT table_name,
                   COUNT(column_name) as col_count,
                   LISTAGG(column_name || ':' || data_type, ',') WITHIN GROUP (ORDER BY column_name) as structure
            FROM all_tab_columns
            WHERE owner = UPPER(dev_schema_name)
            GROUP BY table_name
            MINUS
            SELECT table_name,
                   COUNT(column_name) as col_count,
                   LISTAGG(column_name || ':' || data_type, ',') WITHIN GROUP (ORDER BY column_name) as structure
            FROM all_tab_columns
            WHERE owner = UPPER(prod_schema_name)
            GROUP BY table_name
        ) tc1
        -- Процедуры
        UNION
        SELECT 'PROCEDURE', o1.object_name
        FROM all_objects o1
        WHERE o1.owner = UPPER(dev_schema_name)
        AND o1.object_type = 'PROCEDURE'
        MINUS
        SELECT 'PROCEDURE', o2.object_name
        FROM all_objects o2
        WHERE o2.owner = UPPER(prod_schema_name)
        AND o2.object_type = 'PROCEDURE'
        -- Функции
        UNION
        SELECT 'FUNCTION', o3.object_name
        FROM all_objects o3
        WHERE o3.owner = UPPER(dev_schema_name)
        AND o3.object_type = 'FUNCTION'
        MINUS
        SELECT 'FUNCTION', o4.object_name
        FROM all_objects o4
        WHERE o4.owner = UPPER(prod_schema_name)
        AND o4.object_type = 'FUNCTION'
        -- Пакеты
        UNION
        SELECT 'PACKAGE', o5.object_name
        FROM all_objects o5
        WHERE o5.owner = UPPER(dev_schema_name)
        AND o5.object_type = 'PACKAGE'
        MINUS
        SELECT 'PACKAGE', o6.object_name
        FROM all_objects o6
        WHERE o6.owner = UPPER(prod_schema_name)
        AND o6.object_type = 'PACKAGE'
        -- Индексы
        UNION
        SELECT 'INDEX', i1.index_name
        FROM all_indexes i1
        WHERE i1.owner = UPPER(dev_schema_name)
        MINUS
        SELECT 'INDEX', i2.index_name
        FROM all_indexes i2
        WHERE i2.owner = UPPER(prod_schema_name)
        UNION
        -- Индексы с различающейся структурой
        SELECT 'INDEX', ic1.index_name
        FROM (
            SELECT i.index_name,
                   i.table_name,
                   i.uniqueness,
                   LISTAGG(ic.column_name, ',') WITHIN GROUP (ORDER BY ic.column_position) as columns
            FROM all_ind_columns ic
            JOIN all_indexes i ON ic.index_name = i.index_name AND ic.index_owner = i.owner
            WHERE i.owner = UPPER(dev_schema_name)
            GROUP BY i.index_name, i.table_name, i.uniqueness
            MINUS
            SELECT i.index_name,
                   i.table_name,
                   i.uniqueness,
                   LISTAGG(ic.column_name, ',') WITHIN GROUP (ORDER BY ic.column_position) as columns
            FROM all_ind_columns ic
            JOIN all_indexes i ON ic.index_name = i.index_name AND ic.index_owner = i.owner
            WHERE i.owner = UPPER(prod_schema_name)
            GROUP BY i.index_name, i.table_name, i.uniqueness
        ) ic1;

BEGIN
    -- Проверка существования схем
    SELECT COUNT(*)
    INTO v_count
    FROM all_users
    WHERE username = UPPER(dev_schema_name);

    IF v_count = 0 THEN
        RAISE_APPLICATION_ERROR(-20001, 'Схема разработки ' || dev_schema_name || ' не существует');
    END IF;

    SELECT COUNT(*)
    INTO v_count
    FROM all_users
    WHERE username = UPPER(prod_schema_name);

    IF v_count = 0 THEN
        RAISE_APPLICATION_ERROR(-20002, 'Промышленная схема ' || prod_schema_name || ' не существует');
    END IF;

    -- Сбор зависимостей только для таблиц
    SELECT dep_rec(table_name, referenced_table_name)
    BULK COLLECT INTO v_dependencies
    FROM (
        SELECT DISTINCT
            ac.table_name,
            ac2.table_name as referenced_table_name
        FROM all_constraints ac
        JOIN all_cons_columns acc ON ac.constraint_name = acc.constraint_name
        JOIN all_constraints ac2 ON ac.r_constraint_name = ac2.constraint_name
        WHERE ac.owner = UPPER(dev_schema_name)
        AND ac.constraint_type = 'R'
        AND ac2.owner = UPPER(dev_schema_name)
    );

    DBMS_OUTPUT.PUT_LINE('Объекты для создания/обновления в ' || prod_schema_name || ':');
    DBMS_OUTPUT.PUT_LINE('----------------------------------------');

    FOR rec IN object_diff LOOP
        DECLARE
            v_object_type VARCHAR2(30) := rec.object_type;
            v_object_name VARCHAR2(128) := rec.object_name;
            v_cycle_found BOOLEAN := FALSE;
        BEGIN
            -- Проверка циклических зависимостей только для таблиц
            IF v_object_type = 'TABLE' THEN
                FOR dep IN (
                    WITH deps (table_name, depends_on, path) AS (
                        SELECT table_name, depends_on, ',' || table_name || ','
                        FROM TABLE(v_dependencies)
                        WHERE table_name = v_object_name
                        UNION ALL
                        SELECT d.table_name, d.depends_on, dd.path || d.table_name || ','
                        FROM TABLE(v_dependencies) d
                        JOIN deps dd ON d.table_name = dd.depends_on
                    ) CYCLE table_name SET is_cycle TO 'Y' DEFAULT 'N'
                    SELECT table_name, path
                    FROM deps
                    WHERE is_cycle = 'Y'
                    AND ',' || path || ',' LIKE '%,' || v_object_name || ',%' || v_object_name || ',%'
                ) LOOP
                    v_cycle_found := TRUE;
                    v_cycle_detected := TRUE;
                    EXIT;
                END LOOP;
            END IF;

            IF v_cycle_found THEN
                DBMS_OUTPUT.PUT_LINE(v_object_type || ': ' || v_object_name || ' (обнаружено закольцованное ограничение)');
            ELSE
                DBMS_OUTPUT.PUT_LINE(v_object_type || ': ' || v_object_name);
            END IF;
        END;
    END LOOP;

    IF v_cycle_detected THEN
        DBMS_OUTPUT.PUT_LINE('----------------------------------------');
        DBMS_OUTPUT.PUT_LINE('Внимание: Обнаружены циклические зависимости в таблицах. Необходима ручная обработка.');
    END IF;

EXCEPTION
    WHEN OTHERS THEN
        RAISE_APPLICATION_ERROR(-20003, 'Ошибка выполнения: ' || SQLERRM);
END compare_schemas;
/

BEGIN
    compare_schemas('C##DEV', 'C##PROD');
END;





