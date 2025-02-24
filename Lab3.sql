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
    v_ddl CLOB;
    v_dependencies dep_tab := dep_tab();

    -- Тип для хранения отсортированных объектов
    TYPE table_rec IS RECORD (
        object_type VARCHAR2(30),
        object_name VARCHAR2(128),
        has_cycle BOOLEAN
    );
    TYPE table_tab IS TABLE OF table_rec;
    v_sorted_tables table_tab := table_tab();

    CURSOR object_diff_to_prod IS
        SELECT 'TABLE' as object_type, t.table_name as object_name
        FROM all_tables t
        WHERE t.owner = UPPER(dev_schema_name)
        MINUS
        SELECT 'TABLE', t2.table_name
        FROM all_tables t2
        WHERE t2.owner = UPPER(prod_schema_name)
        UNION
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
        UNION
        SELECT 'PROCEDURE', o1.object_name
        FROM all_objects o1
        WHERE o1.owner = UPPER(dev_schema_name)
        AND o1.object_type = 'PROCEDURE'
        MINUS
        SELECT 'PROCEDURE', o2.object_name
        FROM all_objects o2
        WHERE o2.owner = UPPER(prod_schema_name)
        AND o2.object_type = 'PROCEDURE';

    CURSOR object_diff_to_drop IS
        SELECT 'TABLE' as object_type, t.table_name as object_name
        FROM all_tables t
        WHERE t.owner = UPPER(prod_schema_name)
        MINUS
        SELECT 'TABLE', t2.table_name
        FROM all_tables t2
        WHERE t2.owner = UPPER(dev_schema_name)
        UNION
        SELECT 'PROCEDURE', o1.object_name
        FROM all_objects o1
        WHERE o1.owner = UPPER(prod_schema_name)
        AND o1.object_type = 'PROCEDURE'
        MINUS
        SELECT 'PROCEDURE', o2.object_name
        FROM all_objects o2
        WHERE o2.owner = UPPER(dev_schema_name)
        AND o2.object_type = 'PROCEDURE';

    -- Функция для топологической сортировки
    PROCEDURE topological_sort IS
        TYPE visited_tab IS TABLE OF BOOLEAN INDEX BY VARCHAR2(128);
        v_visited visited_tab;
        v_temp_mark visited_tab;
        v_tables table_tab := table_tab();

        PROCEDURE visit(p_table_name IN VARCHAR2, p_depth IN NUMBER) IS
            v_rec table_rec;
            v_has_cycle BOOLEAN := FALSE;
            v_dep_table_name VARCHAR2(128);
            v_dep_depends_on VARCHAR2(128);
        BEGIN
            IF v_temp_mark.EXISTS(p_table_name) THEN
                v_cycle_detected := TRUE;
                v_has_cycle := TRUE;
                RETURN;
            END IF;

            IF NOT v_visited.EXISTS(p_table_name) THEN
                v_temp_mark(p_table_name) := TRUE;

                FOR i IN 1..v_dependencies.COUNT LOOP
                    v_dep_table_name := v_dependencies(i).table_name;
                    v_dep_depends_on := v_dependencies(i).depends_on;
                    IF v_dep_table_name = p_table_name THEN
                        visit(v_dep_depends_on, p_depth + 1);
                    END IF;
                END LOOP;

                v_visited(p_table_name) := TRUE;
                v_temp_mark.DELETE(p_table_name);

                v_rec.object_type := 'TABLE';
                v_rec.object_name := p_table_name;
                v_rec.has_cycle := v_has_cycle;
                v_tables.EXTEND;
                v_tables(v_tables.LAST) := v_rec;
            END IF;
        END visit;
    BEGIN
        -- Собираем все таблицы из object_diff_to_prod
        FOR rec IN object_diff_to_prod LOOP
            IF rec.object_type = 'TABLE' AND NOT v_visited.EXISTS(rec.object_name) THEN
                visit(rec.object_name, 1);
            END IF;
        END LOOP;

        -- Копируем отсортированные таблицы
        v_sorted_tables := v_tables;

        -- Добавляем процедуры (они не имеют зависимостей)
        FOR rec IN object_diff_to_prod LOOP
            IF rec.object_type = 'PROCEDURE' THEN
                v_sorted_tables.EXTEND;
                v_sorted_tables(v_sorted_tables.LAST).object_type := rec.object_type;
                v_sorted_tables(v_sorted_tables.LAST).object_name := rec.object_name;
                v_sorted_tables(v_sorted_tables.LAST).has_cycle := FALSE;
            END IF;
        END LOOP;
    END topological_sort;

BEGIN
    DBMS_OUTPUT.PUT_LINE('Текущий контейнер: ' || SYS_CONTEXT('USERENV', 'CON_NAME'));
    DBMS_OUTPUT.PUT_LINE('Текущий пользователь: ' || USER);

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

    -- Сбор зависимостей для таблиц
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

    -- Выполняем топологическую сортировку
    topological_sort;

    DBMS_OUTPUT.PUT_LINE('DDL-скрипты для создания/обновления объектов в ' || prod_schema_name || ':');
    DBMS_OUTPUT.PUT_LINE('----------------------------------------');

    FOR i IN 1..v_sorted_tables.COUNT LOOP
        v_ddl := NULL;
        IF v_sorted_tables(i).object_type = 'TABLE' THEN
            v_ddl := 'CREATE TABLE "' || UPPER(prod_schema_name) || '"."' || v_sorted_tables(i).object_name || '" (' || chr(10);

            FOR col IN (
                SELECT column_name, data_type, data_length, data_precision, data_scale, nullable
                FROM all_tab_columns
                WHERE owner = UPPER(dev_schema_name)
                AND table_name = v_sorted_tables(i).object_name
                ORDER BY column_id
            ) LOOP
                v_ddl := v_ddl || '    "' || col.column_name || '" ' || col.data_type;
                IF col.data_type IN ('VARCHAR2', 'CHAR') THEN
                    v_ddl := v_ddl || '(' || col.data_length || ')';
                ELSIF col.data_type = 'NUMBER' AND col.data_precision IS NOT NULL THEN
                    v_ddl := v_ddl || '(' || col.data_precision || ',' || NVL(col.data_scale, 0) || ')';
                END IF;
                IF col.nullable = 'N' THEN
                    v_ddl := v_ddl || ' NOT NULL';
                END IF;
                v_ddl := v_ddl || ',' || chr(10);
            END LOOP;

            FOR cons IN (
                SELECT constraint_name, constraint_type, r_owner, r_constraint_name
                FROM all_constraints
                WHERE owner = UPPER(dev_schema_name)
                AND table_name = v_sorted_tables(i).object_name
                AND constraint_type IN ('P', 'R')
            ) LOOP
                v_ddl := v_ddl || '    CONSTRAINT "' || cons.constraint_name || '" ';
                IF cons.constraint_type = 'P' THEN
                    v_ddl := v_ddl || 'PRIMARY KEY (';
                    FOR col IN (
                        SELECT column_name
                        FROM all_cons_columns
                        WHERE owner = UPPER(dev_schema_name)
                        AND constraint_name = cons.constraint_name
                        ORDER BY position
                    ) LOOP
                        v_ddl := v_ddl || '"' || col.column_name || '",';
                    END LOOP;
                    v_ddl := RTRIM(v_ddl, ',') || ')';
                ELSIF cons.constraint_type = 'R' THEN
                    v_ddl := v_ddl || 'FOREIGN KEY (';
                    FOR col IN (
                        SELECT column_name
                        FROM all_cons_columns
                        WHERE owner = UPPER(dev_schema_name)
                        AND constraint_name = cons.constraint_name
                        ORDER BY position
                    ) LOOP
                        v_ddl := v_ddl || '"' || col.column_name || '",';
                    END LOOP;
                    v_ddl := RTRIM(v_ddl, ',') || ') REFERENCES "' || cons.r_owner || '"."';
                    DECLARE
                        v_ref_table VARCHAR2(128);
                    BEGIN
                        SELECT table_name
                        INTO v_ref_table
                        FROM all_constraints
                        WHERE owner = cons.r_owner
                        AND constraint_name = cons.r_constraint_name
                        AND ROWNUM = 1;
                        v_ddl := v_ddl || v_ref_table || '" (';
                    END;
                    FOR col IN (
                        SELECT column_name
                        FROM all_cons_columns
                        WHERE owner = cons.r_owner
                        AND constraint_name = cons.r_constraint_name
                        ORDER BY position
                    ) LOOP
                        v_ddl := v_ddl || '"' || col.column_name || '",';
                    END LOOP;
                    v_ddl := RTRIM(v_ddl, ',') || ')';
                END IF;
                v_ddl := v_ddl || ',' || chr(10);
            END LOOP;

            v_ddl := RTRIM(v_ddl, ',' || chr(10)) || chr(10) || ')';

            IF v_sorted_tables(i).has_cycle THEN
                DBMS_OUTPUT.PUT_LINE('-- TABLE: ' || v_sorted_tables(i).object_name || ' (циклическая зависимость)');
            ELSE
                DBMS_OUTPUT.PUT_LINE('-- TABLE: ' || v_sorted_tables(i).object_name);
            END IF;

        ELSIF v_sorted_tables(i).object_type = 'PROCEDURE' THEN
            v_ddl := 'CREATE OR REPLACE PROCEDURE "' || UPPER(prod_schema_name) || '"."' || v_sorted_tables(i).object_name || '" AS' || chr(10);
            FOR src IN (
                SELECT text
                FROM all_source
                WHERE owner = UPPER(dev_schema_name)
                AND name = v_sorted_tables(i).object_name
                AND type = 'PROCEDURE'
                ORDER BY line
            ) LOOP
                IF src.text IS NOT NULL THEN
                    v_ddl := v_ddl || src.text;
                END IF;
            END LOOP;
            IF SUBSTR(TRIM(v_ddl), -1) != ';' THEN
                v_ddl := TRIM(v_ddl) || ';';
            END IF;
            DBMS_OUTPUT.PUT_LINE('-- PROCEDURE: ' || v_sorted_tables(i).object_name);
        END IF;

        DBMS_OUTPUT.PUT_LINE(v_ddl);
        DBMS_OUTPUT.PUT_LINE('');
    END LOOP;

    IF v_cycle_detected THEN
        DBMS_OUTPUT.PUT_LINE('----------------------------------------');
        DBMS_OUTPUT.PUT_LINE('Внимание: Обнаружены циклические зависимости в таблицах. Таблицы с циклами выведены в конце.');
    END IF;

    DBMS_OUTPUT.PUT_LINE('DDL-скрипты для удаления объектов из ' || prod_schema_name || ':');
    DBMS_OUTPUT.PUT_LINE('----------------------------------------');

    FOR rec IN object_diff_to_drop LOOP
        DBMS_OUTPUT.PUT_LINE('DROP ' || rec.object_type || ' "' || UPPER(prod_schema_name) || '"."' || rec.object_name || '";');
    END LOOP;

EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Ошибка: ' || SQLERRM);
        RAISE_APPLICATION_ERROR(-20003, 'Ошибка выполнения: ' || SQLERRM);
END compare_schemas;
/

BEGIN
    compare_schemas('C##DEV', 'C##PROD');
END;
