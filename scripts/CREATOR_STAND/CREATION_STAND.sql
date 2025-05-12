DO
$stand$
DECLARE
    arr_master TEXT[] = ARRAY[
		'volna_mbss_master',
		'starda_mbss_master',
		'sol_mbss_master',
		'rox_mbss_master',
		'monro_mbss_master',
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
    arr_stage TEXT[] = ARRAY[
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

SET search_path TO asd;

--=======================
-- 2. OLD-COPING
--=======================
CREATE TABLE users_old AS SELECT * FROM users;
CREATE TABLE groups_old AS SELECT * FROM groups;
CREATE TABLE user_groups_old AS SELECT * FROM user_groups;
CREATE TABLE user_project_old AS SELECT * FROM user_project;
--=======================
-- 3. EXPORT OF PRIVILEGES
--=======================
DO $$
BEGIN
    EXECUTE 'COPY (
        
        SELECT 
            grantor,
            grantee,
            privilege_type,
            table_schema AS object_schema,
            table_name AS object_name,
            ''TABLE'' AS object_type
        FROM information_schema.table_privileges
        WHERE table_catalog = ''1go_mbss_stage''

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
        WHERE n.nspname NOT LIKE ''pg_%'' 
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
        WHERE d.datname = ''1go_mbss_stage''

        UNION ALL

        SELECT
            pg_get_userbyid(acl.grantor) AS grantor,
            acl.grantee::regrole::text AS grantee,
            acl.privilege_type,
            n.nspname AS object_schema,
            p.proname AS object_name,
            ''FUNCTION'' AS object_type
        FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid,
             aclexplode(p.proacl) AS acl
        WHERE n.nspname NOT LIKE ''pg_%''
          AND n.nspname != ''information_schema''
    ) TO ''/home/reports/1go_mbss_stage_privileges.csv'' WITH CSV HEADER;';
END $$;
--=======================
-- 4. DROPPING TABLES
--=======================
DO $$ 
DECLARE 
    tbl RECORD;
BEGIN
    FOR tbl IN 
        SELECT tablename 
        FROM pg_tables 
        WHERE schemaname = 'public' 
        AND tablename NOT IN ('users_old', 'groups_old', 'user_groups_old', 'user_project_old' )
    LOOP
        EXECUTE format('DROP TABLE IF EXISTS %I CASCADE', tbl.tablename);
    END LOOP;
END $$;
--=======================
-- 5. DDL + INSERT INTO
--=======================
DO $$
DECLARE
    r RECORD;
BEGIN
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
END $$;

DO $$
DECLARE
    r RECORD;
BEGIN
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
END $$;
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
--=======================
-- EOF
--=======================
