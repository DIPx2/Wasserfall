DO $$
DECLARE
    databases TEXT[] := ARRAY[
        'drip_mbss_master',
        'flagman_mbss_master',
        'fresh_mbss_master',
        'gizbo_mbss_master',
        'irwin_mbss_master',
        'izzi_mbss_master',
        'jet_mbss_master',
        'legzo_mbss_master',
        'lex_mbss_master',
        'monro_mbss_master',
        'rox_mbss_master',
        'sol_mbss_master',
        'starda_mbss_master',
        'volna_mbss_master',
        '1go_mbss_master'  -- добавляем исходную базу, если нужно
    ];
    current_db TEXT;
    sql TEXT;
    r RECORD;
    priv_file TEXT;
BEGIN
    FOREACH current_db IN ARRAY databases
    LOOP
        RAISE NOTICE '========================================';
        RAISE NOTICE 'Processing database: %', current_db;
        RAISE NOTICE '========================================';

        -- 1. MAPPING
        BEGIN
            RAISE NOTICE 'Creating FDW mapping...';
            sql := format('
                CREATE EXTENSION IF NOT EXISTS postgres_fdw;
                DROP SERVER IF EXISTS prd_chat_pg_fdw CASCADE;
                CREATE SERVER prd_chat_pg_fdw FOREIGN DATA WRAPPER postgres_fdw
                    OPTIONS (host ''prd-chat-pg-02.maxbit.private'', port ''5434'', dbname %L);
                CREATE USER MAPPING FOR CURRENT_USER SERVER prd_chat_pg_fdw
                    OPTIONS (user ''robo_sudo'', password ''%%dFgH8!zX4&kLmT2'');
                DROP SCHEMA IF EXISTS fdw_%s CASCADE;
                CREATE SCHEMA fdw_%s;
                IMPORT FOREIGN SCHEMA public FROM SERVER prd_chat_pg_fdw INTO fdw_%s;
                REVOKE INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA fdw_%s FROM PUBLIC;',
                current_db, current_db, current_db, current_db, current_db);
            EXECUTE sql;
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Error creating FDW mapping for %: %', current_db, SQLERRM;
            CONTINUE;
        END;

        -- 2. OLD-COPING
        BEGIN
            RAISE NOTICE 'Creating backup tables...';
            EXECUTE format('CREATE TABLE IF NOT EXISTS public.users_old AS SELECT * FROM public.users');
            EXECUTE format('CREATE TABLE IF NOT EXISTS public.groups_old AS SELECT * FROM public.groups');
            EXECUTE format('CREATE TABLE IF NOT EXISTS public.user_groups_old AS SELECT * FROM public.user_groups');
            EXECUTE format('CREATE TABLE IF NOT EXISTS public.user_project_old AS SELECT * FROM public.user_project');
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Error creating backup tables for %: %', current_db, SQLERRM;
        END;

        -- 3. EXPORT OF PRIVILEGES
        BEGIN
            RAISE NOTICE 'Exporting privileges...';
            priv_file := format('/home/reports/%s_privileges.csv', current_db);

            sql := format('
                COPY (
                    SELECT
                        grantor,
                        grantee,
                        privilege_type,
                        table_schema AS object_schema,
                        table_name AS object_name,
                        ''TABLE'' AS object_type
                    FROM information_schema.table_privileges
                    WHERE table_catalog = %L
                    UNION ALL
                    SELECT
                        pg_get_userbyid(acl.grantor) AS grantor,
                        acl.grantee::regrole::text AS grantee,
                        acl.privilege_type,
                        n.nspname AS object_schema,
                        ''SCHEMA'' AS object_name,
                        ''SCHEMA'' AS object_type
                    FROM pg_namespace n,
                         aclexplode(n.nspacl) AS acl
                    WHERE n.nspname NOT LIKE ''pg_%%''
                      AND n.nspname != ''information_schema''
                    UNION ALL
                    SELECT
                        pg_get_userbyid(acl.grantor) AS grantor,
                        acl.grantee::regrole::text AS grantee,
                        acl.privilege_type,
                        ''DATABASE'' AS object_schema,
                        ''DATABASE'' AS object_name,
                        ''DATABASE'' AS object_type
                    FROM pg_database d,
                         aclexplode(d.datacl) AS acl
                    WHERE d.datname = %L
                ) TO %L WITH CSV HEADER;',
                current_db, current_db, priv_file);

            EXECUTE sql;
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Error exporting privileges for %: %', current_db, SQLERRM;
        END;

        -- 4. DROPPING TABLES
        BEGIN
            RAISE NOTICE 'Dropping tables...';
            FOR r IN
                EXECUTE format('
                    SELECT tablename
                    FROM pg_tables
                    WHERE schemaname = ''public''
                    AND tablename NOT IN (
                        ''users_old'', ''groups_old'', ''user_groups_old'', ''user_project_old'',
                        ''schema_migrations'', ''ar_internal_metadata''  -- исключаем таблицы Rails, если есть
                    )')
            LOOP
                BEGIN
                    EXECUTE format('DROP TABLE IF EXISTS public.%I CASCADE', r.tablename);
                EXCEPTION WHEN OTHERS THEN
                    RAISE WARNING 'Error dropping table %.%: %', current_db, r.tablename, SQLERRM;
                END;
            END LOOP;
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Error during dropping tables for %: %', current_db, SQLERRM;
        END;

        -- 5. DDL + INSERT INTO
        BEGIN
            RAISE NOTICE 'Creating and populating tables from FDW...';
            FOR r IN
                EXECUTE format('
                    SELECT foreign_table_name
                    FROM information_schema.foreign_tables
                    WHERE foreign_table_schema = ''fdw_%s''',
                    current_db)
            LOOP
                BEGIN
                    -- Создание таблицы
                    IF NOT EXISTS (
                        EXECUTE format('
                            SELECT 1
                            FROM pg_tables
                            WHERE schemaname = ''public''
                            AND tablename = %L',
                            r.foreign_table_name)
                    ) THEN
                        EXECUTE format(
                            'CREATE TABLE public.%I (LIKE fdw_%s.%I INCLUDING ALL);',
                            r.foreign_table_name, current_db, r.foreign_table_name
                        );
                    END IF;

                    -- Заполнение данными
                    EXECUTE format(
                        'INSERT INTO public.%I SELECT * FROM fdw_%s.%I;',
                        r.foreign_table_name, current_db, r.foreign_table_name
                    );
                EXCEPTION WHEN OTHERS THEN
                    RAISE WARNING 'Error processing table %.%: %', current_db, r.foreign_table_name, SQLERRM;
                END;
            END LOOP;
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Error during table creation/population for %: %', current_db, SQLERRM;
        END;

        -- 6. RESTORATION OF PRIVILEGES
        BEGIN
            RAISE NOTICE 'Restoring privileges...';
            priv_file := format('/home/reports/%s_privileges.csv', current_db);

            CREATE TEMP TABLE IF NOT EXISTS tmp_privs (
                grantor        TEXT,
                grantee        TEXT,
                privilege_type TEXT,
                object_schema  TEXT,
                object_name    TEXT,
                object_type    TEXT
            ) ON COMMIT DROP;

            EXECUTE format('TRUNCATE TABLE tmp_privs');
            EXECUTE format('COPY tmp_privs FROM %L WITH (FORMAT csv, HEADER true)', priv_file);

            FOR r IN
                SELECT *
                FROM tmp_privs
                WHERE object_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'pg_temp')
                  AND grantee NOT IN ('postgres', 'PUBLIC')
                  AND grantee IS NOT NULL
            LOOP
                BEGIN
                    IF r.object_type IN ('TABLE', 'VIEW', 'SEQUENCE') THEN
                        EXECUTE format(
                            'GRANT %s ON %s %I.%I TO %I;',
                            r.privilege_type,
                            CASE WHEN r.object_type = 'SEQUENCE' THEN 'SEQUENCE' ELSE 'TABLE' END,
                            r.object_schema,
                            r.object_name,
                            r.grantee
                        );
                    ELSIF r.object_type = 'SCHEMA' THEN
                        EXECUTE format(
                            'GRANT %s ON SCHEMA %I TO %I;',
                            r.privilege_type,
                            r.object_schema,
                            r.grantee
                        );
                    ELSIF r.object_type = 'DATABASE' THEN
                        EXECUTE format(
                            'GRANT %s ON DATABASE %I TO %I;',
                            r.privilege_type,
                            current_db,
                            r.grantee
                        );
                    END IF;
                EXCEPTION WHEN OTHERS THEN
                    RAISE WARNING 'Error restoring privilege: % ON % %.% TO %: %',
                        r.privilege_type, r.object_type, r.object_schema, r.object_name, r.grantee, SQLERRM;
                END;
            END LOOP;
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Error during privilege restoration for %: %', current_db, SQLERRM;
        END;

        -- 7. FILLING user_online
        BEGIN
            RAISE NOTICE 'Filling user_online...';
            EXECUTE format('TRUNCATE TABLE public.user_online');
            EXECUTE format('
                INSERT INTO public.user_online ("id", "status", "updated_at")
                SELECT "id", 0, CURRENT_TIMESTAMP
                FROM fdw_%s.users',
                current_db);
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Error filling user_online for %: %', current_db, SQLERRM;
        END;

        -- 8. FILLING WITHOUT DUPLICATES BY EMAIL
        BEGIN
            RAISE NOTICE 'Merging users...';
            EXECUTE format('
                ALTER TABLE public.users ADD CONSTRAINT users_email_unique_%s UNIQUE (email);
                INSERT INTO public.users (id, email, name, avatar_link, role, status, password, created_at, updated_at, ready_after_login, max_active_tickets)
                SELECT id, email, name, avatar_link, role, status, password, created_at, updated_at, ready_after_login, max_active_tickets
                FROM public.users_old
                ON CONFLICT (email) DO UPDATE
                SET
                    id = EXCLUDED.id,
                    name = EXCLUDED.name,
                    avatar_link = EXCLUDED.avatar_link,
                    role = EXCLUDED.role,
                    status = EXCLUDED.status,
                    password = EXCLUDED.password,
                    created_at = EXCLUDED.created_at,
                    updated_at = EXCLUDED.updated_at,
                    ready_after_login = EXCLUDED.ready_after_login,
                    max_active_tickets = EXCLUDED.max_active_tickets;',
                current_db);
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Error merging users for %: %', current_db, SQLERRM;
        END;

        -- 9. MERGE user_project
        BEGIN
            RAISE NOTICE 'Merging user_project...';
            EXECUTE format('
                ALTER TABLE public.user_project ADD CONSTRAINT user_project_user_id_project_id_unique_%s UNIQUE (user_id, project_id);
                INSERT INTO public.user_project (user_id, project_id)
                SELECT user_id, project_id
                FROM public.user_project_old
                ON CONFLICT (user_id, project_id) DO NOTHING;',
                current_db);
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Error merging user_project for %: %', current_db, SQLERRM;
        END;

        -- 10. RESTORATION CONSTRAINTS
        BEGIN
            RAISE NOTICE 'Cleaning up constraints...';
            EXECUTE format('ALTER TABLE public.users DROP CONSTRAINT IF EXISTS users_email_unique_%s', current_db);
            EXECUTE format('ALTER TABLE public.user_project DROP CONSTRAINT IF EXISTS user_project_user_id_project_id_unique_%s', current_db);
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Error cleaning constraints for %: %', current_db, SQLERRM;
        END;

        -- 11. DELETION OF INTERMEDIATE TABLES
        BEGIN
            RAISE NOTICE 'Cleaning up temp tables...';
            EXECUTE format('DROP TABLE IF EXISTS public.users_old');
            EXECUTE format('DROP TABLE IF EXISTS public.groups_old');
            EXECUTE format('DROP TABLE IF EXISTS public.user_groups_old');
            EXECUTE format('DROP TABLE IF EXISTS public.user_project_old');
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Error cleaning temp tables for %: %', current_db, SQLERRM;
        END;

        RAISE NOTICE 'Completed processing database: %', current_db;
    END LOOP;

    RAISE NOTICE '========================================';
    RAISE NOTICE 'All databases processed successfully!';
    RAISE NOTICE '========================================';
END $$;