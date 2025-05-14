--=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=
-- USE postgres
--=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=

CREATE DATABASE zombie;

--=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=
-- USE zombie
--=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=

CREATE EXTENSION IF NOT EXISTS postgres_fdw;
CREATE EXTENSION IF NOT EXISTS dblink;

--============================
-- Make mapping and shemas
--============================

-- ...for master

DO
$$
DECLARE
    arr TEXT[] = ARRAY[
		'volna_mbss_master',
		'starda_mbss_master',
		'sol_mbss_master',
		'rox_mbss_master',
		'monro_mbss_master',
		'mbss_master',
		'lex_mbss_master',
		'legzo_mbss_master',
		'jet_mbss_master',
		'izzi_mbss_master',
		'irwin_mbss_master',
		'gizbo_mbss_master',
		'fresh_mbss_master',
		'flagman_mbss_master',
		'drip_mbss_master',
		'callback_media_master',
		'1go_mbss_master'
    ];
    k TEXT;
    sql TEXT;
BEGIN
    FOREACH k IN ARRAY arr
    LOOP
        sql = format(
            'CREATE SERVER IF NOT EXISTS prd_chat_pg_fdw_%s FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host ''prd-chat-pg-02.maxbit.private'', port ''5434'', dbname ''%s'');
             CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER SERVER prd_chat_pg_fdw_%s OPTIONS (user ''robo_sudo'', password ''%%dFgH8!zX4&kLmT2'');
             CREATE SCHEMA IF NOT EXISTS fdw_%s;
             IMPORT FOREIGN SCHEMA public FROM SERVER prd_chat_pg_fdw_%s INTO fdw_%s;
             REVOKE INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA fdw_%s FROM PUBLIC;',
            k, k, k, k, k, k, k
        );
        EXECUTE sql;
    END LOOP;
END;
$$;

-- ...for stage

DO
$$
DECLARE
    arr TEXT[] = ARRAY[
		'volna_mbss_stage',
		'starda_mbss_stage',
		'sol_mbss_stage',
		'rox_mbss_stage',
		'monro_mbss_stage',
		'mbss_stage',
		'maxmind_stage',
		'lex_mbss_stage',
		'legzo_mbss_stage',
		'jet_mbss_stage',
		'izzi_mbss_stage',
		'irwin_mbss_stage',
		'gizbo_mbss_stage',
		'fresh_mbss_stage',
		'flagman_mbss_stage',
		'drip_mbss_stage',
		'callback_media_stage',
		'callback_media',
		'1go_mbss_stage'
    ];
    k TEXT;
    sql TEXT;
BEGIN
    FOREACH k IN ARRAY arr
    LOOP
        sql = format(
            'CREATE SERVER IF NOT EXISTS stage_chat_pg_fdw_%s FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host ''localhost'', port ''15434'', dbname ''%s'');
             CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER SERVER stage_chat_pg_fdw_%s OPTIONS (user ''robo_sudo'', password ''%%dFgH8!zX4&kLmT2'');
             CREATE SCHEMA IF NOT EXISTS fdw_%s;
             IMPORT FOREIGN SCHEMA public FROM SERVER stage_chat_pg_fdw_%s INTO fdw_%s;',
            k, k, k, k, k, k
        );
        EXECUTE sql;
    END LOOP;
END;
$$;

--=======================
-- EXPORT OF PRIVILEGES
--=======================
DO
$$
DECLARE
    arr_stage        text[] = ARRAY[
        'volna_mbss_stage',
        'starda_mbss_stage',
        'sol_mbss_stage',
        'rox_mbss_stage',
        'monro_mbss_stage',
        'mbss_stage',
        'maxmind_stage',
        'lex_mbss_stage',
        'legzo_mbss_stage',
        'jet_mbss_stage',
        'izzi_mbss_stage',
        'irwin_mbss_stage',
        'gizbo_mbss_stage',
        'fresh_mbss_stage',
        'flagman_mbss_stage',
        'drip_mbss_stage',
        'callback_media_stage',
        'callback_media',
        '1go_mbss_stage'
    ];

    db_name          text;
    db_unquoted      text;
    sanitized_name   text;
    safe_name        text;
    output_path      text;
    conn_name        text;
    conn_str         text;
    dblink_query     text;

BEGIN
    FOREACH db_name IN ARRAY arr_stage
    LOOP
        db_unquoted = trim(both '"' FROM db_name);

        -- Промежуточная очистка имени: заменяем все недопустимые символы на '_'
        sanitized_name = regexp_replace(db_unquoted, '[^a-zA-Z0-9_]', '_', 'g');

        -- Удаляем начальные цифры, если есть
        safe_name = regexp_replace(sanitized_name, '^[0-9]+', '', 'g');

        conn_str = format(
            'host=localhost port=15434 dbname=%s user=robo_sudo password=%L',
            db_unquoted, '%dFgH8!zX4&kLmT2'
        );
        output_path = format('/home/reports/%s_privileges.csv', db_unquoted);
        conn_name = format('conn_%s', safe_name);

        BEGIN
            PERFORM dblink_connect(conn_name, conn_str);

            dblink_query = format($sql$
                -- Table and view privileges
                SELECT grantor, grantee, privilege_type, table_schema, table_name, 'TABLE_OR_VIEW'
                FROM information_schema.table_privileges
                WHERE table_catalog = %L

                UNION ALL

                -- Column privileges
                SELECT grantor, grantee, privilege_type, table_schema, table_name || '.' || column_name, 'COLUMN'
                FROM information_schema.column_privileges
                WHERE table_catalog = %L

                UNION ALL

                -- Schema privileges
                SELECT pg_get_userbyid(acl.grantor), acl.grantee::regrole::text, acl.privilege_type, n.nspname, 'SCHEMA', 'SCHEMA'
                FROM pg_namespace n, aclexplode(n.nspacl) AS acl
                WHERE n.nspname NOT LIKE 'pg_%%' AND n.nspname != 'information_schema'

                UNION ALL

                -- Database privileges
                SELECT pg_get_userbyid(acl.grantor), acl.grantee::regrole::text, acl.privilege_type, 'DATABASE', 'DATABASE', 'DATABASE'
                FROM pg_database d, aclexplode(d.datacl) AS acl
                WHERE d.datname = %L

                UNION ALL

                -- Function privileges
                SELECT pg_get_userbyid(acl.grantor), acl.grantee::regrole::text, acl.privilege_type,
                       n.nspname, p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')', 'FUNCTION'
                FROM pg_proc p
                JOIN pg_namespace n ON p.pronamespace = n.oid,
                     aclexplode(p.proacl) AS acl
                WHERE n.nspname NOT LIKE 'pg_%%' AND n.nspname != 'information_schema'

                UNION ALL

                -- Sequence privileges
                SELECT pg_get_userbyid(acl.grantor), acl.grantee::regrole::text, acl.privilege_type,
                       n.nspname, c.relname, 'SEQUENCE'
                FROM pg_class c
                JOIN pg_namespace n ON c.relnamespace = n.oid,
                     aclexplode(c.relacl) AS acl
                WHERE c.relkind = 'S'
                  AND n.nspname NOT LIKE 'pg_%%'
                  AND n.nspname != 'information_schema'

                UNION ALL

                -- Materialized view privileges
                SELECT pg_get_userbyid(acl.grantor), acl.grantee::regrole::text, acl.privilege_type,
                       n.nspname, c.relname, 'MATERIALIZED_VIEW'
                FROM pg_class c
                JOIN pg_namespace n ON c.relnamespace = n.oid,
                     aclexplode(c.relacl) AS acl
                WHERE c.relkind = 'm'
                  AND n.nspname NOT LIKE 'pg_%%'
                  AND n.nspname != 'information_schema'

                UNION ALL

                -- Type privileges
                SELECT pg_get_userbyid(acl.grantor), acl.grantee::regrole::text, acl.privilege_type,
                       n.nspname, t.typname, 'TYPE'
                FROM pg_type t
                JOIN pg_namespace n ON t.typnamespace = n.oid,
                     aclexplode(t.typacl) AS acl
                WHERE n.nspname NOT LIKE 'pg_%%'
                  AND n.nspname != 'information_schema'
                  AND t.typtype IN ('b', 'd', 'e', 'c')
            $sql$, db_unquoted, db_unquoted, db_unquoted);

            EXECUTE format('DROP TABLE IF EXISTS tmp_privs_%I', safe_name);

            EXECUTE format($f$
                CREATE TEMP TABLE tmp_privs_%1$I AS
                SELECT * FROM dblink(%2$L, %3$L) AS t(
                    grantor text,
                    grantee text,
                    privilege_type text,
                    object_schema text,
                    object_name text,
                    object_type text
                )
            $f$, safe_name, conn_name, dblink_query);

            EXECUTE format('COPY tmp_privs_%I TO %L WITH CSV HEADER', safe_name, output_path);

            RAISE INFO 'Success: %', db_name;

            PERFORM dblink_disconnect(conn_name);

        EXCEPTION
            WHEN OTHERS THEN
                RAISE INFO 'ERROR %: %', db_name, SQLERRM;
                BEGIN
                    PERFORM dblink_disconnect(conn_name);
                EXCEPTION
                    WHEN OTHERS THEN
                        NULL;
                END;
        END;
    END LOOP;
END;
$$;


--=======================
-- 2. OLD-COPING IN STAGE
--=======================


DO
$$
DECLARE fts text;
BEGIN
    FOR fts IN (SELECT DISTINCT foreign_table_schema FROM information_schema.foreign_tables WHERE RIGHT(foreign_table_schema, 5) = 'stage')
    LOOP
        BEGIN
            IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = fts AND table_name = 'users') THEN
                EXECUTE FORMAT('DROP FOREIGN TABLE IF EXISTS %I.users_old', fts);
                --EXECUTE FORMAT('CREATE FOREIGN TABLE %I.users_old AS SELECT * FROM %I.users', fts, fts);
            END IF;

            IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = fts AND table_name = 'groups') THEN
                EXECUTE FORMAT('DROP FOREIGN TABLE IF EXISTS %I.groups_old', fts);
                --EXECUTE FORMAT('CREATE FOREIGN TABLE %I.groups_old AS SELECT * FROM %I.groups', fts, fts);
            END IF;

            IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = fts AND table_name = 'user_groups') THEN
                EXECUTE FORMAT('DROP FOREIGN TABLE IF EXISTS %I.user_groups_old', fts);
                --EXECUTE FORMAT('CREATE FOREIGN TABLE %I.user_groups_old AS SELECT * FROM %I.user_groups', fts, fts);
            END IF;

            IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = fts AND table_name = 'user_project') THEN
                EXECUTE FORMAT('DROP FOREIGN TABLE IF EXISTS %I.user_project_old', fts);
                --EXECUTE FORMAT('CREATE FOREIGN TABLE %I.user_project_old AS SELECT * FROM %I.user_project', fts, fts);
            END IF;
        END;
    END LOOP;
END;
$$;

DO $$
DECLARE
    fts text;
    column_list text;
    create_stmt text;
BEGIN
    FOR fts IN (SELECT DISTINCT foreign_table_schema FROM information_schema.foreign_tables WHERE RIGHT(foreign_table_schema, 5) = 'stage')
    LOOP
        BEGIN
            -- Handle users table
            IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = fts AND table_name = 'users') THEN
                -- Get column definitions for users table
                SELECT string_agg(column_name || ' ' || udt_name ||
                                CASE WHEN character_maximum_length IS NOT NULL
                                     THEN '(' || character_maximum_length || ')'
                                     ELSE '' END, ', ')
                INTO column_list
                FROM information_schema.columns
                WHERE table_schema = fts AND table_name = 'users';

                EXECUTE FORMAT('DROP FOREIGN TABLE IF EXISTS %I.users_old', fts);
                EXECUTE FORMAT('CREATE FOREIGN TABLE %I.users_old (%s) SERVER foreign_server', fts, column_list);
                EXECUTE FORMAT('INSERT INTO %I.users_old SELECT * FROM %I.users', fts, fts);
            END IF;

            -- Handle groups table
            IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = fts AND table_name = 'groups') THEN
                -- Get column definitions for groups table
                SELECT string_agg(column_name || ' ' || udt_name ||
                                CASE WHEN character_maximum_length IS NOT NULL
                                     THEN '(' || character_maximum_length || ')'
                                     ELSE '' END, ', ')
                INTO column_list
                FROM information_schema.columns
                WHERE table_schema = fts AND table_name = 'groups';

                EXECUTE FORMAT('DROP FOREIGN TABLE IF EXISTS %I.groups_old', fts);
                EXECUTE FORMAT('CREATE FOREIGN TABLE %I.groups_old (%s) SERVER foreign_server', fts, column_list);
                EXECUTE FORMAT('INSERT INTO %I.groups_old SELECT * FROM %I.groups', fts, fts);
            END IF;

            -- Handle user_groups table
            IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = fts AND table_name = 'user_groups') THEN
                -- Get column definitions for user_groups table
                SELECT string_agg(column_name || ' ' || udt_name ||
                                CASE WHEN character_maximum_length IS NOT NULL
                                     THEN '(' || character_maximum_length || ')'
                                     ELSE '' END, ', ')
                INTO column_list
                FROM information_schema.columns
                WHERE table_schema = fts AND table_name = 'user_groups';

                EXECUTE FORMAT('DROP FOREIGN TABLE IF EXISTS %I.user_groups_old', fts);
                EXECUTE FORMAT('CREATE FOREIGN TABLE %I.user_groups_old (%s) SERVER foreign_server', fts, column_list);
                EXECUTE FORMAT('INSERT INTO %I.user_groups_old SELECT * FROM %I.user_groups', fts, fts);
            END IF;

            -- Handle user_project table
            IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = fts AND table_name = 'user_project') THEN
                -- Get column definitions for user_project table
                SELECT string_agg(column_name || ' ' || udt_name ||
                                CASE WHEN character_maximum_length IS NOT NULL
                                     THEN '(' || character_maximum_length || ')'
                                     ELSE '' END, ', ')
                INTO column_list
                FROM information_schema.columns
                WHERE table_schema = fts AND table_name = 'user_project';

                EXECUTE FORMAT('DROP FOREIGN TABLE IF EXISTS %I.user_project_old', fts);
                EXECUTE FORMAT('CREATE FOREIGN TABLE %I.user_project_old (%s) SERVER foreign_server', fts, column_list);
                EXECUTE FORMAT('INSERT INTO %I.user_project_old SELECT * FROM %I.user_project', fts, fts);
            END IF;
        END;
    END LOOP;
END;
$$;


-----------------------------------------------------------------------------------------------------------------------------------------------------
--=======================
-- 4. DROPPING TABLES
--=======================
    FOR tbl IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'
        AND tablename NOT IN ('users_old', 'groups_old', 'user_groups_old', 'user_project_old' )
    LOOP
        EXECUTE format('DROP TABLE IF EXISTS %I CASCADE', tbl.tablename);
    END LOOP;
-----------------------------------------------------------------------------------------------------------------------------------------------------
--=======================
-- 5. DDL + INSERT INTO
--=======================
    FOR r IN
        SELECT foreign_table_name
        FROM information_schema.foreign_tables
        WHERE foreign_table_schema = 'fdw_1go_mbss_master'
    LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = r.foreign_table_name
        ) THEN
            EXECUTE format(
                'CREATE TABLE public.%I (LIKE fdw_1go_mbss_master.%I INCLUDING ALL);',
                r.foreign_table_name, r.foreign_table_name
            );
        END IF;
    END LOOP;



    FOR r IN
        SELECT foreign_table_name
        FROM information_schema.foreign_tables
        WHERE foreign_table_schema = 'fdw_1go_mbss_master'
    LOOP
        EXECUTE format(
            'INSERT INTO public.%I SELECT * FROM fdw_1go_mbss_master.%I;',
            r.foreign_table_name, r.foreign_table_name
        );
    END LOOP;

--=======================
-- 6. RESTORATION OF PRIVILEGES
--=======================
DO $$
DECLARE
    rec      RECORD;
    file     TEXT = '/home/reports/1go_mbss_stage_privileges.csv';
    sys_schemas TEXT[] = ARRAY['pg_catalog', 'information_schema', 'pg_toast', 'pg_temp'];
    role_exists BOOLEAN;
BEGIN

    CREATE TEMP TABLE tmp_privs (
        grantor        TEXT,
        grantee        TEXT,
        privilege_type TEXT,
        object_schema  TEXT,
        object_name    TEXT,
        object_type    TEXT
    );

    EXECUTE format('COPY tmp_privs FROM %L WITH (FORMAT csv, HEADER true)', file);

    FOR rec IN
        SELECT *
        FROM tmp_privs
        WHERE
            -- Исключить системные схемы
            object_schema NOT IN (SELECT unnest(sys_schemas))
            -- Исключить системные роли
            AND grantee NOT IN ('postgres', 'PUBLIC')
            -- Исключить пустые или невалидные имена ролей
            AND grantee IS NOT NULL
            AND grantee !~ '^[[:space:]]*$'
            AND grantee !~ '^-+$'
            -- Исключить системные объекты в пользовательских схемах
            AND NOT (object_name LIKE 'pg_%' AND object_type = 'FUNCTION')
    LOOP
        BEGIN
            -- Проверить существование роли
            SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname = rec.grantee) INTO role_exists;

            IF NOT role_exists THEN
                RAISE INFO 'Роль "%" не существует, пропустить привилегию: % ON % %.%',
                    rec.grantee, rec.privilege_type, rec.object_type, rec.object_schema, rec.object_name;
                CONTINUE;
            END IF;

            IF rec.object_type = 'TABLE' OR rec.object_type = 'VIEW' OR rec.object_type = 'SEQUENCE' THEN
                EXECUTE format(
                    'GRANT %s ON %s %I.%I TO %I;',
                    rec.privilege_type,
                    CASE WHEN rec.object_type = 'SEQUENCE' THEN 'SEQUENCE' ELSE 'TABLE' END,
                    rec.object_schema,
                    rec.object_name,
                    rec.grantee
                );
                RAISE INFO 'Восстановлена привилегия: % ON % %.% TO %',
                    rec.privilege_type, rec.object_type, rec.object_schema, rec.object_name, rec.grantee;

            ELSIF rec.object_type = 'SCHEMA' THEN
                EXECUTE format(
                    'GRANT %s ON SCHEMA %I TO %I;',
                    rec.privilege_type,
                    rec.object_schema,
                    rec.grantee
                );
                RAISE INFO 'Восстановлена привилегия: % ON SCHEMA % TO %',
                    rec.privilege_type, rec.object_schema, rec.grantee;

            ELSIF rec.object_type = 'DATABASE' THEN
                EXECUTE format(
                    'GRANT %s ON DATABASE %I TO %I;',
                    rec.privilege_type,
                    current_database(),
                    rec.grantee
                );
                RAISE INFO 'Восстановлена привилегия: % ON DATABASE % TO %',
                    rec.privilege_type, current_database(), rec.grantee;

            ELSIF rec.object_type = 'FUNCTION' THEN
                RAISE INFO 'Пропущена функция %.% (требуется информация о аргументах)',
                    rec.object_schema, rec.object_name;

            ELSE
                RAISE INFO 'Пропущен неизвестный тип объекта: % (%.%.%)',
                    rec.object_type, rec.object_schema, rec.object_name, rec.grantee;
            END IF;
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Ошибка при восстановлении привилегии: % (%.%.%): %',
                rec.object_type, rec.object_schema, rec.object_name, rec.grantee, SQLERRM;
        END;
    END LOOP;

    DROP TABLE tmp_privs;
END $$;
--=======================
-- 7. FILLING user_online
--=======================
TRUNCATE TABLE user_online;
INSERT INTO user_online ("id", "status", "updated_at") SELECT "id", 0, CURRENT_TIMESTAMP FROM fdw_1go_mbss_master.users;
--=======================
-- 8. FILLING WITHOUT DUPLICATES BY EMAIL
--=======================
ALTER TABLE users ADD CONSTRAINT users_email_unique UNIQUE (email);
INSERT INTO users (id, email, name, avatar_link, role, status, password, created_at, updated_at, ready_after_login, max_active_tickets)
SELECT id, email, name, avatar_link, role, status, password, created_at, updated_at, ready_after_login, max_active_tickets
FROM users_old
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
    max_active_tickets = EXCLUDED.max_active_tickets;
--=======================
-- 9. MERGE
--=======================
ALTER TABLE user_project ADD CONSTRAINT user_project_user_id_project_id_unique UNIQUE (user_id, project_id);
INSERT INTO user_project (user_id, project_id) SELECT user_id, project_id FROM user_project_old ON CONFLICT (user_id, project_id) DO NOTHING;
--=======================
-- 10. RESTORATION CONSTRAINTS
--=======================
ALTER TABLE users DROP CONSTRAINT users_email_unique;
ALTER TABLE user_project DROP CONSTRAINT user_project_user_id_project_id_unique;
--=======================
-- 11. DELETION OF INTERMEDIATE TABLES
--=======================
DROP TABLE public.users_old;
DROP TABLE public.groups_old;
DROP TABLE public.user_groups_old;
DROP TABLE public.user_project_old;

$stand$





        PERFORM dblink_disconnect(conn_name);
    END LOOP;
END $$;

