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
    v_ddl CLOB;

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

BEGIN
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

    DBMS_OUTPUT.PUT_LINE('DDL-скрипты для создания/обновления объектов в ' || prod_schema_name || ':');
    DBMS_OUTPUT.PUT_LINE('----------------------------------------');

    FOR rec IN object_diff_to_prod LOOP
        v_ddl := NULL;
        IF rec.object_type = 'TABLE' THEN
            v_ddl := 'CREATE TABLE "' || UPPER(prod_schema_name) || '"."' || rec.object_name || '" (' || chr(10);

            FOR col IN (
                SELECT column_name, data_type, data_length, data_precision, data_scale, nullable
                FROM all_tab_columns
                WHERE owner = UPPER(dev_schema_name)
                AND table_name = rec.object_name
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
                AND table_name = rec.object_name
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
                    FOR r_table IN (
                        SELECT table_name
                        FROM all_constraints
                        WHERE owner = cons.r_owner
                        AND constraint_name = cons.r_constraint_name
                    ) LOOP
                        v_ddl := v_ddl || r_table.table_name || '" (';
                    END LOOP;
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

            FOR dep IN (
                WITH deps (table_name, depends_on, path) AS (
                    SELECT table_name, depends_on, ',' || table_name || ','
                    FROM TABLE(v_dependencies)
                    WHERE table_name = rec.object_name
                    UNION ALL
                    SELECT d.table_name, d.depends_on, dd.path || d.table_name || ','
                    FROM TABLE(v_dependencies) d
                    JOIN deps dd ON d.table_name = dd.depends_on
                ) CYCLE table_name SET is_cycle TO 'Y' DEFAULT 'N'
                SELECT table_name, path
                FROM deps
                WHERE is_cycle = 'Y'
                AND ',' || path || ',' LIKE '%,' || rec.object_name || ',%' || rec.object_name || ',%'
            ) LOOP
                v_cycle_detected := TRUE;
                DBMS_OUTPUT.PUT_LINE('-- TABLE: ' || rec.object_name || ' (циклическая зависимость)');
                EXIT;
            END LOOP;
            IF NOT v_cycle_detected THEN
                DBMS_OUTPUT.PUT_LINE('-- TABLE: ' || rec.object_name);
            END IF;

        ELSIF rec.object_type = 'PROCEDURE' THEN
            v_ddl := 'CREATE OR REPLACE PROCEDURE "' || UPPER(prod_schema_name) || '"."' || rec.object_name || '" AS' || chr(10);
            FOR src IN (
                SELECT text
                FROM all_source
                WHERE owner = UPPER(dev_schema_name)
                AND name = rec.object_name
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
            DBMS_OUTPUT.PUT_LINE('-- PROCEDURE: ' || rec.object_name);
        END IF;

        DBMS_OUTPUT.PUT_LINE(v_ddl);
        DBMS_OUTPUT.PUT_LINE('');
    END LOOP;

    IF v_cycle_detected THEN
        DBMS_OUTPUT.PUT_LINE('----------------------------------------');
        DBMS_OUTPUT.PUT_LINE('Внимание: Обнаружены циклические зависимости в таблицах.');
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
