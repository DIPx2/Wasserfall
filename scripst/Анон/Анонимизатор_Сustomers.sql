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
--01------------------
CREATE EXTENSION IF NOT EXISTS postgres_fdw;
CREATE SCHEMA IF NOT EXISTS fdw_mbss;
CREATE SERVER MastersDuplicate FOREIGN DATA WRAPPER postgres_fdw OPTIONS ( host 'prd-chat-pg-02.maxbit.private', dbname 'mbss_master', port '5434' );
CREATE USER MAPPING FOR CURRENT_USER SERVER MastersDuplicate OPTIONS ( user 'robo_read_only', password 'G7$kBqLpXt9&FZ' );
SET search_path TO fdw_mbss;
IMPORT FOREIGN SCHEMA public FROM SERVER MastersDuplicate INTO fdw_mbss;
--02------------------
--ROLLBACK;
DROP TABLE if Exists customers_neo;
DROP TABLE if Exists customers_temp;
--03---------------------------------------------------------------------------------------------------------------------------------------
BEGIN;
	DO $$
	DECLARE
	    src_schema TEXT = 'fdw_mbss';
	    src_table  TEXT = 'customers';
	    dst_schema TEXT = 'public';
	    dst_table  TEXT = 'customers_neo';
	
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
	
	    -- 2. Создать таблицу
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
	
	    RAISE NOTICE 'Копия % выполнена в %', src_schema || '.' || src_table, dst_schema || '.' || dst_table;
	END
	$$ LANGUAGE plpgsql;
COMMIT;
--04---------------------------------------------------------------------------------------------------------------------------------------
BEGIN;
	DO $$
	DECLARE
	    src_schema TEXT = 'fdw_mbss';
	    src_table  TEXT = 'customers'; 
	    dst_schema TEXT = 'public';
	    dst_table  TEXT = 'customers_temp';
	
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
	
	    -- 2. Создать таблицу
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
	
	    RAISE NOTICE 'Копия % выполнена в %', src_schema || '.' || src_table, dst_schema || '.' || dst_table;
	END
	$$ LANGUAGE plpgsql;
COMMIT;
--05---------------------------------------------------------------------------------------------------------------------------------------
UPDATE customers_temp SET email =	CASE 
        								WHEN email IS NULL THEN NULL
        								WHEN email = '' THEN ''
        							ELSE 
										lower(left(email, 1) || '****@' || split_part(email, '@', 2))
    								END;
--06---------------------------------------------------------------------------------------------------------------------------------------
UPDATE customers_temp SET login = 	CASE 
        								WHEN login IS NULL THEN NULL
        								WHEN login = '' THEN ''
        							ELSE 
										lower(left(login, 1) || '****' || right(login, 1))
    								END;
--07---------------------------------------------------------------------------------------------------------------------------------------
UPDATE customers_temp SET "name" =	CASE 
        								WHEN "name" IS NULL THEN NULL
        								WHEN "name" = 'unnamed' THEN 'unnamed'
										WHEN "name" = '' THEN ''
        							ELSE 
										lower(left("name", 1) || '****' || right("name", 1))
    								END;
--08---------------------------------------------------------------------------------------------------------------------------------------
WITH shuffled_chars AS (
    SELECT id, substr(phone, s, 1) AS ch
    FROM customers_temp, generate_series(1, length(phone)) AS s
),
shuffled AS (
    SELECT id, string_agg(ch, '') AS xx_phone
    FROM (
        SELECT * FROM shuffled_chars
        ORDER BY id, random()
    ) AS randomized
    GROUP BY id
)
UPDATE customers_temp
SET phone = shuffled.xx_phone
FROM shuffled
WHERE customers_temp.id = shuffled.id;
--09---------------------------------------------------------------------------------------------------------------------------------------
MERGE INTO customers_neo AS target
USING ( SELECT id, email, login, "name", phone FROM customers_temp ) AS source ON target.id = source.id
WHEN MATCHED THEN
    UPDATE SET
        email = source.email,
        login = source.login,
        "name" = source."name",
        phone = source.phone
;
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------

-- =======================================================
-- USE MBSS_MASTER
-- =======================================================
REVOKE SELECT ON TABLE public.customers FROM robo_read_only;

--XX------------------
-- Удалить всех созданных персонажей, объектов и все записи
