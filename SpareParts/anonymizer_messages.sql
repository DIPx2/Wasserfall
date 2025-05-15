/*
-- =======================================================
-- USE LOCAL server
-- =======================================================
-- на каком сервере я работаю и кто я?

SELECT inet_server_addr() AS host, inet_server_port() AS port, current_database() AS db, current_user AS user;

-- =======================================================
-- USE REMOTE server
-- =======================================================

nano /var/lib/postgresql/16/mbss/pg_hba.conf ->> host all robo_read_only 10.94.0.222/32 scram-sha-256
postgres=# SELECT pg_reload_conf();

-- =======================================================
-- USE PostgreSQL
-- =======================================================

CREATE USER robo_read_only WITH PASSWORD 'G7$kBqLpXt9&FZ';
REVOKE ALL ON DATABASE mbss_master FROM robo_read_only;
GRANT CONNECT ON DATABASE mbss_master TO robo_read_only;
GRANT SELECT ON TABLE public.customers TO robo_read_only;

-- =======================================================
-- USE MBSS_STAGE
-- =======================================================

CREATE EXTENSION IF NOT EXISTS postgres_fdw;
CREATE SCHEMA IF NOT EXISTS fdw_mbss;
CREATE SERVER MastersDuplicate FOREIGN DATA WRAPPER postgres_fdw OPTIONS ( host 'prd-chat-pg-02.maxbit.private', dbname 'mbss_master', port '5434' );
CREATE USER MAPPING FOR CURRENT_USER SERVER MastersDuplicate OPTIONS ( user 'robo_read_only', password 'G7$kBqLpXt9&FZ' );
SET search_path TO fdw_mbss;
IMPORT FOREIGN SCHEMA public FROM SERVER MastersDuplicate INTO fdw_mbss;

-- =======================================================
-- USE MBSS_STAGE.PUBLIC
-- =======================================================
-------------Функция преобразования--Если JSON-----------------------------------------------------
-- =======================================================

CREATE OR REPLACE FUNCTION shuffle_json_values(input_text text) RETURNS text AS
$$
DECLARE
    ascii_chars    text[];
    shuffled_chars text[];
    result_text    text;
    ch             text;
    prev_char      text    = '';
    i              int;
    pos            int;
    inside_quotes  boolean = FALSE;
    is_value       boolean = FALSE;
    ascii_count    int     = 0;
BEGIN
    -- Быстрый выход для не-JSON данных
    IF input_text IS NULL OR input_text = '' OR input_text NOT LIKE '{%' THEN RETURN input_text; END IF;
    -- 1. Сбор ASCII-символов в массив (substring вместо [])
    FOR pos IN 1..LENGTH(input_text)
        LOOP
            ch = SUBSTRING(input_text FROM pos FOR 1);
            IF ch = '"' AND (pos = 1 OR SUBSTRING(input_text FROM pos - 1 FOR 1) <> '\') THEN
                inside_quotes = NOT inside_quotes;
                is_value = inside_quotes AND prev_char = ':';
                prev_char = ch;
                CONTINUE;
            END IF;
            IF inside_quotes AND is_value AND ASCII(ch) BETWEEN 32 AND 126 THEN
                ascii_count = ascii_count + 1;
                ascii_chars[ascii_count] = ch;
            END IF;
            prev_char = ch;
        END LOOP;

    -- 2. Алгоритм Фишера-Йетса
    IF ascii_count > 1 THEN -- пропустить при 0 или 1 символе
        FOR i IN REVERSE ascii_count..2
            LOOP
                pos = 1 + (RANDOM() * (i - 1))::int;
                -- Swap элементов
                ch = ascii_chars[i];
                ascii_chars[i] = ascii_chars[pos];
                ascii_chars[pos] = ch;
            END LOOP;
    END IF;

    -- 3. Вставка обратно
    result_text = '';
    i = 1;
    inside_quotes = FALSE;
    is_value = FALSE;

    FOR pos IN 1..LENGTH(input_text)
        LOOP
            ch = SUBSTRING(input_text FROM pos FOR 1);

            IF ch = '"' AND (pos = 1 OR SUBSTRING(input_text FROM pos - 1 FOR 1) <> '\') THEN
                inside_quotes = NOT inside_quotes;
                is_value = inside_quotes AND prev_char = ':';
                result_text = result_text || ch;
                prev_char = ch;
                CONTINUE;
            END IF;

            IF inside_quotes AND is_value AND ASCII(ch) BETWEEN 32 AND 126 THEN
                result_text = result_text || ascii_chars[i];
                i = i + 1;
            ELSE
                result_text = result_text || ch;
            END IF;
        END LOOP;

    RETURN result_text;
END;
$$ LANGUAGE plpgsql PARALLEL SAFE STRICT IMMUTABLE;
*/
-- =======================================================
-------------Наполнения----------------------------------------------------------------
-- =======================================================

BEGIN; ------------- Если нужна большая копия-источник ---- completed in 3 m 10 s 211 ms
	DO $$
	DECLARE
	    src_schema TEXT = 'fdw_mbss';
	    src_table  TEXT = 'messages';
	    dst_schema TEXT = 'public';
	    dst_table  TEXT = 'messages_neo';

	    ddl TEXT;
	    idx RECORD;
	    cons RECORD;
	    trg RECORD;
	BEGIN
	    -- 1. Сформировать CREATE TABLE
	    SELECT 'CREATE TABLE ' || dst_schema || '.' || dst_table || E'\n(\n' || string_agg('    ' || column_expr, E',\n') || E'\n);'
	    INTO ddl
	    FROM ( SELECT column_name || ' ' || data_type ||
	               CASE WHEN character_maximum_length IS NOT NULL
	                    THEN '(' || character_maximum_length || ')'
	                    ELSE '' END ||
	               CASE WHEN is_nullable = 'NO' THEN ' NOT NULL' ELSE '' END ||
	               CASE WHEN column_default IS NOT NULL THEN ' DEFAULT ' || column_default ELSE '' END
	               AS column_expr
	        FROM information_schema.columns
	        WHERE table_schema = src_schema AND table_name = src_table
	        ORDER BY ordinal_position
	    ) sub;
	    -- 2. Создасть таблицу
	    EXECUTE ddl;
	    -- 3. Копировать данные
	    EXECUTE format('INSERT INTO %I.%I SELECT * FROM %I.%I', dst_schema, dst_table, src_schema, src_table);
	    -- 4. Индексы (PK + уникальные + обычные)
	    FOR idx IN
	        SELECT indexname, indexdef
	        FROM pg_indexes
	        WHERE schemaname = src_schema AND tablename = src_table
	    LOOP
	        EXECUTE replace(
	                   replace(idx.indexdef,
	                           src_schema || '.' || src_table,
	                           dst_schema || '.' || dst_table),
	                   idx.indexname, idx.indexname || '_neo'
	               );
	    END LOOP;
	    -- 5. Constraints (PK, FK, CHECK)
	    FOR cons IN
	        SELECT conname, contype, pg_get_constraintdef(c.oid) AS condef
	        FROM pg_constraint c
	        JOIN pg_class t ON t.oid = c.conrelid
	        JOIN pg_namespace n ON n.oid = t.relnamespace
	        WHERE n.nspname = src_schema AND t.relname = src_table
	          AND contype IN ('p', 'f', 'c')  -- primary, foreign, check
	    LOOP
	        EXECUTE format(
	            'ALTER TABLE %I.%I ADD CONSTRAINT %I_%s %s;',
	            dst_schema, dst_table, cons.conname, 'neo', cons.condef
	        );
	    END LOOP;
	    -- 6. Триггеры
	    FOR trg IN
	        SELECT tgname, pg_get_triggerdef(t.oid, true) AS tgdef
	        FROM pg_trigger t
	        JOIN pg_class c ON c.oid = t.tgrelid
	        JOIN pg_namespace n ON n.oid = c.relnamespace
	        WHERE n.nspname = src_schema AND c.relname = src_table
	          AND NOT t.tgisinternal
	    LOOP
	        EXECUTE replace(
	            replace(trg.tgdef, src_schema || '.' || src_table, dst_schema || '.' || dst_table),
	            src_table, dst_table
	        );
	    END LOOP;
	    --RAISE INFO 'Копия % выполнена в %', src_schema || '.' || src_table, dst_schema || '.' || dst_table;
	END
$$ LANGUAGE plpgsql; ------------- <Если нужна большая копия-источник/>

-- =======================================================
-------------Рабочие таблицы-----------------------------------------------------
-- =======================================================

BEGIN; -- 19,317,441 rows affected in 1 m 11 s 959 ms
    SET parallel_setup_cost = 0;
    SET parallel_tuple_cost = 0;
    SET max_parallel_workers_per_gather = 12;
    SET maintenance_work_mem = '256MB';
    SET min_parallel_table_scan_size = 1;
    CREATE TABLE messages_temp_shadow AS SELECT * FROM messages_neo;
COMMIT;

CREATE UNIQUE INDEX unique_id_index ON messages_temp_shadow (id); -- completed in 55 s 668 ms

--ROLLBACK;
-------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS messages_temp_shadow_digit (id uuid, msg text);

DO $$  -- Замена чисел из 3+ цифр на "-0,01" -- completed in 53 s 557 ms
DECLARE
    rec RECORD;
    result_text TEXT;
BEGIN
    FOR rec IN
        SELECT id, msg FROM messages_temp_shadow
        WHERE msg IS NOT NULL AND msg != '' AND msg NOT LIKE '{%' --LIMIT 10000
    LOOP
        result_text = regexp_replace(rec.msg, '\d{3,}', '-0,01', 'g');
        IF result_text != rec.msg THEN
            INSERT INTO messages_temp_shadow_digit (id, msg) VALUES (rec.id, result_text);
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS messages_temp_shadow_emlurl (id uuid, msg text);

DO $$ -- Замена email и URL --  completed in 45 s 252 ms
DECLARE
    rec RECORD;
    result_text TEXT;
    match TEXT;
    rand_digits TEXT;
    local_part TEXT;
    domain_part TEXT;
    pos INT;
    i INT;
BEGIN
    FOR rec IN
        SELECT id, msg FROM messages_temp_shadow
        WHERE msg IS NOT NULL AND msg != '' AND msg NOT LIKE '{%' --limit 100
    LOOP
        result_text = rec.msg;

        -- Замена email-адресов
        FOR match IN SELECT unnest(regexp_matches(result_text, '[\\w\\.\\-]+@[\\w\\.\\-]+\\.\\w+', 'g')) LOOP
            pos = position('@' IN match);
            local_part = substr(match, 1, pos - 1);
            domain_part = substr(match, pos);

            rand_digits = '';
            FOR i IN 1..5 LOOP
                rand_digits = rand_digits || substr('abcdefghijklmnopqrstuvwxyz0123456789', floor(random() * 36 + 1)::int, 1);
            END LOOP;

            result_text = replace(result_text, match, rand_digits || domain_part);
        END LOOP;

        -- Замена URL
        result_text = regexp_replace(result_text, E'https?:\\/\\/[^\\s<>"(),]+', 'https://de.xvideos.com/', 'gi');

        -- Вставляем только если что-то изменилось
        IF result_text IS DISTINCT FROM rec.msg THEN
            INSERT INTO messages_temp_shadow_emlurl (id, msg) VALUES (rec.id, result_text);
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-------------------------------------------------------------------------------
-------------Запуск функции--Если JSON-----------------------------------------------------------
BEGIN; -- 7,076,015 rows affected in 6 m 44 s 735 ms
    SET parallel_setup_cost = 0;
    SET parallel_tuple_cost = 0;
    SET max_parallel_workers_per_gather = 12;
    SET maintenance_work_mem = '256MB';
    SET min_parallel_table_scan_size = 1;
    CREATE TABLE messages_temp_shadow_json AS SELECT id, shuffle_json_values(msg)
    FROM messages_temp_shadow WHERE msg IS NOT NULL AND msg != '' AND msg LIKE '{%';
COMMIT;

-- ROLLBACK;
-- =======================================================
-------------Слияния----------------------------------------------------------------
-- =======================================================
DO $$ -- completed in 53 m 27 s 552 ms
    BEGIN
        MERGE INTO messages_temp_shadow AS target -- 104,228 rows affected in 13 s 888 ms
        USING ( SELECT id, msg FROM messages_temp_shadow_emlurl ) AS source ON target.id = source.id
        WHEN MATCHED THEN UPDATE SET msg = source.msg;

        MERGE INTO messages_temp_shadow AS target -- 7,076,015 rows affected in 17 m 25 s 632 ms
        USING ( SELECT id, shuffle_json_values FROM messages_temp_shadow_json ) AS source ON target.id = source.id
        WHEN MATCHED THEN UPDATE SET msg = source.shuffle_json_values;

        MERGE INTO messages_temp_shadow AS target -- 535,605 rows affected in 1 m 50 s 17 ms
        USING ( SELECT id, msg FROM messages_temp_shadow_digit ) AS source ON target.id = source.id
        WHEN MATCHED THEN UPDATE SET msg = source.msg;
        -- Если в копию-источник
        MERGE INTO messages_neo AS target -- 19,303,394 rows affected in 34 m 47 s 339 ms
        USING ( SELECT id, msg FROM messages_temp_shadow) AS source ON target.id = source.id
        WHEN MATCHED THEN UPDATE SET msg = source.msg;
    END;
$$
-- =======================================================
-------------УБОРКА МУСОРА ЗА СОБОЙ ----------------------------------------
-- =======================================================
-- =======================================================
-------------УБОРКА МУСОРА ЗА СОБОЙ ----------------------------------------
-- =======================================================
--=======================
-- EOF
--=======================