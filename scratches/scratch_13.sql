--////////////////////////////////////////////////////////
-- USE postgres.public
--////////////////////////////////////////////////////////

CREATE DATABASE zombie;

--////////////////////////////////////////////////////////
-- USE zombie.public
--////////////////////////////////////////////////////////

CREATE EXTENSION IF NOT EXISTS dblink;

--=======================
-- EXPORT OF PRIVILEGES
--=======================

DO
$$
DECLARE
    arr_stage text[] = ARRAY[
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
        sanitized_name = regexp_replace(db_unquoted, '[^a-zA-Z0-9_]', '_', 'g');
        safe_name = regexp_replace(sanitized_name, '^[0-9]+', '', 'g');

        conn_str = format( 'host=localhost port=15434 dbname=%s user=robo_sudo password=%L', db_unquoted, '%dFgH8!zX4&kLmT2' );
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
-- 2. OLD-COPING
--=======================

DO
$$
DECLARE
    arr_stage text[] = ARRAY[
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

    db_name text;
    db_unquoted text;
    sanitized_name text;
    safe_name text;
    conn_name text;
    conn_str text;

BEGIN
    FOREACH db_name IN ARRAY arr_stage
    LOOP
        db_unquoted = trim(both '"' FROM db_name);
        sanitized_name = regexp_replace(db_unquoted, '[^a-zA-Z0-9_]', '_', 'g');
        safe_name = regexp_replace(sanitized_name, '^[0-9]+', '', 'g');
        conn_str = format('host=localhost port=15434 dbname=%s user=robo_sudo password=%s', db_unquoted, '%dFgH8!zX4&kLmT2');
        conn_name = format('conn_%s', safe_name);

        BEGIN
            PERFORM dblink_connect(conn_name, conn_str);

            IF EXISTS (SELECT 1 FROM dblink(conn_name, $a$SELECT 1 FROM information_schema.tables WHERE table_name = 'users'$a$) AS t(x int)) THEN
                --PERFORM dblink_exec(conn_name, 'DROP TABLE users_old;');
                PERFORM dblink_exec(conn_name, 'CREATE TABLE users_old (LIKE users INCLUDING ALL); INSERT INTO users_old SELECT * FROM users;');
            END IF;

            IF EXISTS (SELECT 1 FROM dblink(conn_name, $b$SELECT 1 FROM information_schema.tables WHERE table_name = 'user_project'$b$) AS t(x int)) THEN
                --PERFORM dblink_exec(conn_name, 'DROP TABLE user_project_old;');
                PERFORM dblink_exec(conn_name, 'CREATE TABLE user_project_old (LIKE user_project INCLUDING ALL); INSERT INTO user_project_old SELECT * FROM user_project;');
            END IF;

            IF EXISTS (SELECT 1 FROM dblink(conn_name, $c$SELECT 1 FROM information_schema.tables WHERE table_name = 'user_groups'$c$) AS t(x int)) THEN
                --PERFORM dblink_exec(conn_name, 'DROP TABLE user_groups_old;');
                PERFORM dblink_exec(conn_name, 'CREATE TABLE user_groups_old (LIKE user_groups INCLUDING ALL); INSERT INTO user_groups_old SELECT * FROM user_groups;');
            END IF;

            IF EXISTS (SELECT 1 FROM dblink(conn_name, $d$SELECT 1 FROM information_schema.tables WHERE table_name = 'groups'$d$) AS t(x int)) THEN
                --PERFORM dblink_exec(conn_name, 'DROP TABLE groups_old;');
                PERFORM dblink_exec(conn_name, 'CREATE TABLE groups_old (LIKE groups INCLUDING ALL); INSERT INTO groups_old SELECT * FROM groups;');
            END IF;

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
-- 4. DROPPING TABLES
--=======================
DO
$$
DECLARE
    arr_stage text[] = ARRAY[
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

    db_name text;
    db_unquoted text;
    sanitized_name text;
    safe_name text;
    conn_name text;
    conn_str text;
    drop_sql text;
    tab_rec record;

BEGIN
    FOREACH db_name IN ARRAY arr_stage LOOP

        db_unquoted = trim(both '"' FROM db_name);
        sanitized_name = regexp_replace(db_unquoted, '[^a-zA-Z0-9_]', '_', 'g');
        safe_name = regexp_replace(sanitized_name, '^[0-9]+', '', 'g');
        conn_str = format('host=localhost port=15434 dbname=%I user=robo_sudo password=%s', db_unquoted, '%dFgH8!zX4&kLmT2');
        conn_name = format('conn_%s', safe_name);

        BEGIN
            PERFORM dblink_connect(conn_name, conn_str);

            FOR tab_rec IN
                SELECT tablename FROM dblink(conn_name, $f$SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename NOT IN ('maxmind_stage', 'users_old', 'groups_old', 'user_groups_old', 'user_project_old')$f$) AS t (tablename text)
            LOOP
                drop_sql = format('DROP TABLE IF EXISTS %I CASCADE', tab_rec.tablename);
                PERFORM dblink_exec(conn_name, drop_sql);
            END LOOP;

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
-- 5. DDL + INSERT INTO
--=======================
DO $$
DECLARE
    arr_stage text[] = ARRAY[
        'volna_mbss_stage', 'starda_mbss_stage', 'sol_mbss_stage', 'rox_mbss_stage',
        'monro_mbss_stage', 'mbss_stage', 'lex_mbss_stage', 'legzo_mbss_stage',
        'jet_mbss_stage', 'izzi_mbss_stage', 'irwin_mbss_stage', 'gizbo_mbss_stage',
        'fresh_mbss_stage', 'flagman_mbss_stage', 'drip_mbss_stage',
        'callback_media_stage', '1go_mbss_stage'
    ];

    arr_master text[] = ARRAY[
        'volna_mbss_master', 'starda_mbss_master', 'sol_mbss_master', 'rox_mbss_master',
        'monro_mbss_master', 'mbss_master', 'lex_mbss_master', 'legzo_mbss_master',
        'jet_mbss_master', 'izzi_mbss_master', 'irwin_mbss_master', 'gizbo_mbss_master',
        'fresh_mbss_master', 'flagman_mbss_master', 'drip_mbss_master',
        'callback_media_master', '1go_mbss_master'
    ];

    idx int;
    master_db text;
    stage_db text;
    master_conn text;
    stage_conn text;
    master_conn_str text;
    stage_conn_str text;
    tab_rec record;
    create_sql text;
    insert_sql text;
BEGIN
    FOR idx IN 1..array_length(arr_stage, 1)
    LOOP
        master_db = arr_master[idx];
        stage_db = arr_stage[idx];

        master_conn = format( 'conn_%s', regexp_replace( regexp_replace( regexp_replace(trim(both '"' FROM master_db), '[^a-zA-Z0-9_]', '_', 'g'), '^[0-9]+', '', 'g'), '_+', '_', 'g') );
        stage_conn = format( 'conn_%s', regexp_replace( regexp_replace( regexp_replace(trim(both '"' FROM stage_db), '[^a-zA-Z0-9_]', '_', 'g'), '^[0-9]+', '', 'g'), '_+', '_', 'g') );

        master_conn_str = format('host=localhost port=15434 dbname=%s user=robo_sudo password=%s', trim(both '"' FROM master_db), '%dFgH8!zX4&kLmT2');
        stage_conn_str = format('host=localhost port=15434 dbname=%s user=robo_sudo password=%s', trim(both '"' FROM stage_db), '%dFgH8!zX4&kLmT2');

        PERFORM dblink_connect(master_conn, master_conn_str);
        PERFORM dblink_connect(stage_conn, stage_conn_str);

        FOR tab_rec IN
            SELECT tablename FROM dblink(master_conn, $y$SELECT tablename FROM pg_tables WHERE schemaname = 'public'$y$) AS t(tablename text)
        LOOP
            create_sql = format( 'CREATE TABLE IF NOT EXISTS %I (LIKE dblink(''%s'', $r$SELECT * FROM %I LIMIT 0$r$) AS t INCLUDING ALL)', tab_rec.tablename, master_conn, tab_rec.tablename );
            EXECUTE create_sql;
            insert_sql = format( 'INSERT INTO %I SELECT * FROM dblink(''%s'', $w$SELECT * FROM %I$w$) AS t(*)', tab_rec.tablename, master_conn, tab_rec.tablename );
            PERFORM dblink_exec(stage_conn, insert_sql);
        END LOOP;

        PERFORM dblink_disconnect(master_conn);
        PERFORM dblink_disconnect(stage_conn);
        RAISE INFO 'Copied from % to %', master_db, stage_db;
    END LOOP;
END;
$$;

--=======================
-- RESTORE ALL PRIVILEGES FROM CSV DUMP
--=======================

DO $$
DECLARE
    arr_stage text[] = ARRAY[
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

    db_name        text;
    sanitized_name text;
    safe_name      text;
    file_path      text;
    rec            record;
    role_exists    boolean;
    sys_schemas    text[] = ARRAY['pg_catalog', 'information_schema', 'pg_toast', 'pg_temp'];

BEGIN
    FOREACH db_name IN ARRAY arr_stage LOOP
        sanitized_name = regexp_replace(trim(both '"' FROM db_name), '[^a-zA-Z0-9_]', '_', 'g');
        safe_name = regexp_replace(sanitized_name, '^[0-9]+', '', 'g');
        file_path = format('/home/reports/%s_privileges.csv', db_name);

        BEGIN
            RAISE INFO 'Восстановление привилегий для: %', db_name;

            CREATE TEMP TABLE tmp_privs (
                grantor        TEXT,
                grantee        TEXT,
                privilege_type TEXT,
                object_schema  TEXT,
                object_name    TEXT,
                object_type    TEXT
            ) ON COMMIT DROP;

            EXECUTE format('COPY tmp_privs FROM %L WITH (FORMAT csv, HEADER true)', file_path);

            FOR rec IN
                SELECT *
                FROM tmp_privs
                WHERE
                    object_schema NOT IN (SELECT unnest(sys_schemas))
                    AND grantee NOT IN ('postgres', 'PUBLIC')
                    AND grantee IS NOT NULL
                    AND grantee !~ '^[[:space:]]*$'
                    AND grantee !~ '^-+$'
                    AND NOT (object_name LIKE 'pg_%' AND object_type = 'FUNCTION')
            LOOP
                BEGIN
                    SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname = rec.grantee) INTO role_exists;

                    IF NOT role_exists THEN
                        RAISE INFO 'Роль "%" не существует, пропущено: % ON % %.%',
                            rec.grantee, rec.privilege_type, rec.object_type, rec.object_schema, rec.object_name;
                        CONTINUE;
                    END IF;

                    IF rec.object_type IN ('TABLE', 'VIEW', 'SEQUENCE') THEN
                        EXECUTE format(
                            'GRANT %s ON %s %I.%I TO %I;',
                            rec.privilege_type,
                            CASE WHEN rec.object_type = 'SEQUENCE' THEN 'SEQUENCE' ELSE 'TABLE' END,
                            rec.object_schema,
                            rec.object_name,
                            rec.grantee
                        );

                    ELSIF rec.object_type = 'SCHEMA' THEN
                        EXECUTE format(
                            'GRANT %s ON SCHEMA %I TO %I;',
                            rec.privilege_type,
                            rec.object_schema,
                            rec.grantee
                        );

                    ELSIF rec.object_type = 'DATABASE' THEN
                        EXECUTE format(
                            'GRANT %s ON DATABASE %I TO %I;',
                            rec.privilege_type,
                            current_database(),
                            rec.grantee
                        );

                    ELSIF rec.object_type = 'FUNCTION' THEN
                        RAISE INFO 'Пропущена функция %.% (нужны аргументы)', rec.object_schema, rec.object_name;

                    ELSIF rec.object_type = 'MATERIALIZED_VIEW' THEN
                        EXECUTE format(
                            'GRANT %s ON MATERIALIZED VIEW %I.%I TO %I;',
                            rec.privilege_type,
                            rec.object_schema,
                            rec.object_name,
                            rec.grantee
                        );

                    ELSIF rec.object_type = 'TYPE' THEN
                        EXECUTE format(
                            'GRANT %s ON TYPE %I.%I TO %I;',
                            rec.privilege_type,
                            rec.object_schema,
                            rec.object_name,
                            rec.grantee
                        );

                    ELSE
                        RAISE INFO 'Пропущен неизвестный тип объекта: % (%.%.%)',
                            rec.object_type, rec.object_schema, rec.object_name, rec.grantee;
                    END IF;

                    RAISE INFO 'Привилегия восстановлена: % ON % %.% TO %',
                        rec.privilege_type, rec.object_type, rec.object_schema, rec.object_name, rec.grantee;

                EXCEPTION WHEN OTHERS THEN
                    RAISE WARNING 'Ошибка при восстановлении: % ON % %.% TO % — %',
                        rec.privilege_type, rec.object_type, rec.object_schema, rec.object_name, rec.grantee, SQLERRM;
                END;
            END LOOP;

        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Ошибка при обработке %: %', db_name, SQLERRM;
        END;
    END LOOP;
END $$;

--=======================
-- 7. FILLING user_online
--=======================

DO $$
DECLARE
    arr_stage text[] = ARRAY[
        'volna_mbss_stage', 'starda_mbss_stage', 'sol_mbss_stage', 'rox_mbss_stage',
        'monro_mbss_stage', 'mbss_stage', 'lex_mbss_stage', 'legzo_mbss_stage',
        'jet_mbss_stage', 'izzi_mbss_stage', 'irwin_mbss_stage', 'gizbo_mbss_stage',
        'fresh_mbss_stage', 'flagman_mbss_stage', 'drip_mbss_stage',
        'callback_media_stage', '1go_mbss_stage'
    ];

    arr_master text[] = ARRAY[
        'volna_mbss_master', 'starda_mbss_master', 'sol_mbss_master', 'rox_mbss_master',
        'monro_mbss_master', 'mbss_master', 'lex_mbss_master', 'legzo_mbss_master',
        'jet_mbss_master', 'izzi_mbss_master', 'irwin_mbss_master', 'gizbo_mbss_master',
        'fresh_mbss_master', 'flagman_mbss_master', 'drip_mbss_master',
        'callback_media_master', '1go_mbss_master'
    ];

    idx int;
    master_db text;
    stage_db text;
    master_conn text;
    stage_conn text;
    master_conn_str text;
    stage_conn_str text;
    insert_sql text;

BEGIN
    FOR idx IN 1..array_length(arr_stage, 1) LOOP
        master_db = arr_master[idx];
        stage_db = arr_stage[idx];

        master_conn = format('conn_%s', regexp_replace(master_db, '[^a-zA-Z0-9_]', '_', 'g'));
        stage_conn = format('conn_%s', regexp_replace(stage_db, '[^a-zA-Z0-9_]', '_', 'g'));

        master_conn_str = format('host=localhost port=15434 dbname=%s user=robo_sudo password=%s', master_db, '%dFgH8!zX4&kLmT2');
        stage_conn_str = format('host=localhost port=15434 dbname=%s user=robo_sudo password=%s', stage_db, '%dFgH8!zX4&kLmT2');

        PERFORM dblink_connect(master_conn, master_conn_str);
        PERFORM dblink_connect(stage_conn, stage_conn_str);

        PERFORM dblink_exec(stage_conn, 'TRUNCATE TABLE users');

        insert_sql = format( 'INSERT INTO users ("id", "status", "updated_at") SELECT "id", 0, CURRENT_TIMESTAMP FROM dblink(''%s'', $g$SELECT id FROM users$g$) AS t("id" int)', master_conn );

        PERFORM dblink_exec(stage_conn, insert_sql);

        PERFORM dblink_disconnect(master_conn);
        PERFORM dblink_disconnect(stage_conn);

        RAISE INFO 'TRUNCATE + INSERT done for % → %', master_db, stage_db;
    END LOOP;
END;
$$;

--=======================
-- 8. FILLING WITHOUT DUPLICATES BY EMAIL
--=======================

DO $$
DECLARE
    arr_stage text[] = ARRAY[
        'volna_mbss_stage', 'starda_mbss_stage', 'sol_mbss_stage', 'rox_mbss_stage',
        'monro_mbss_stage', 'mbss_stage', 'lex_mbss_stage', 'legzo_mbss_stage',
        'jet_mbss_stage', 'izzi_mbss_stage', 'irwin_mbss_stage', 'gizbo_mbss_stage',
        'fresh_mbss_stage', 'flagman_mbss_stage', 'drip_mbss_stage',
        'callback_media_stage', '1go_mbss_stage'
    ];

    arr_master text[] = ARRAY[
        'volna_mbss_master', 'starda_mbss_master', 'sol_mbss_master', 'rox_mbss_master',
        'monro_mbss_master', 'mbss_master', 'lex_mbss_master', 'legzo_mbss_master',
        'jet_mbss_master', 'izzi_mbss_master', 'irwin_mbss_master', 'gizbo_mbss_master',
        'fresh_mbss_master', 'flagman_mbss_master', 'drip_mbss_master',
        'callback_media_master', '1go_mbss_master'
    ];

    idx int;
    master_db text;
    stage_db text;
    master_conn text;
    stage_conn text;
    master_conn_str text;
    stage_conn_str text;
    insert_sql text;

BEGIN
    FOR idx IN 1..array_length(arr_stage, 1) LOOP
        master_db = arr_master[idx];
        stage_db = arr_stage[idx];

        master_conn = format('conn_%s', regexp_replace(master_db, '[^a-zA-Z0-9_]', '_', 'g'));
        stage_conn = format('conn_%s', regexp_replace(stage_db, '[^a-zA-Z0-9_]', '_', 'g'));

        master_conn_str = format('host=localhost port=15434 dbname=%s user=robo_sudo password=%s', master_db, '%dFgH8!zX4&kLmT2');
        stage_conn_str = format('host=localhost port=15434 dbname=%s user=robo_sudo password=%s', stage_db, '%dFgH8!zX4&kLmT2');

        -- Подключения
        PERFORM dblink_connect(master_conn, master_conn_str);
        PERFORM dblink_connect(stage_conn, stage_conn_str);

        -- Добавить ограничение (если не существует)
        PERFORM dblink_exec(stage_conn, '
            DO $do$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.table_constraints
                    WHERE table_name = ''users''
                    AND constraint_type = ''UNIQUE''
                    AND constraint_name = ''users_email_unique''
                ) THEN
                    ALTER TABLE users ADD CONSTRAINT users_email_unique UNIQUE (email);
                END IF;
            END;
            $do$;
        ');

insert_sql = format($sql$
    INSERT INTO users (id, email, name, avatar_link, role, status, password, created_at, updated_at, ready_after_login, max_active_tickets)
    SELECT id, email, name, avatar_link, role, status, password, created_at, updated_at, ready_after_login, max_active_tickets
    FROM dblink('%s', $q$SELECT id, email, name, avatar_link, role, status, password, created_at, updated_at, ready_after_login, max_active_tickets FROM users_old$q$)
    AS t(id int, email text, name text, avatar_link text, role text, status int, password text, created_at timestamp, updated_at timestamp, ready_after_login boolean, max_active_tickets int)
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
$sql$, master_conn); -- tagged-квотинг


        PERFORM dblink_exec(stage_conn, insert_sql);

        PERFORM dblink_disconnect(master_conn);
        PERFORM dblink_disconnect(stage_conn);

        RAISE INFO 'UPSERT done for % → %', master_db, stage_db;
    END LOOP;
END;
$$;


DO $$
DECLARE
    arr_stage text[] = ARRAY[
        'volna_mbss_stage', 'starda_mbss_stage', 'sol_mbss_stage', 'rox_mbss_stage',
        'monro_mbss_stage', 'mbss_stage', 'lex_mbss_stage', 'legzo_mbss_stage',
        'jet_mbss_stage', 'izzi_mbss_stage', 'irwin_mbss_stage', 'gizbo_mbss_stage',
        'fresh_mbss_stage', 'flagman_mbss_stage', 'drip_mbss_stage',
        'callback_media_stage', '1go_mbss_stage'
    ];

    idx int;
    stage_db text;
    stage_conn text;
    stage_conn_str text;

BEGIN
    FOR idx IN 1..array_length(arr_stage, 1) LOOP
        stage_db = arr_stage[idx];
        stage_conn = format('conn_%s', regexp_replace(stage_db, '[^a-zA-Z0-9_]', '_', 'g'));
        stage_conn_str = format('host=localhost port=15434 dbname=%s user=robo_sudo password=%s', stage_db, '%dFgH8!zX4&kLmT2');

        -- Подключение
        PERFORM dblink_connect(stage_conn, stage_conn_str);

        -- 9. MERGE
        PERFORM dblink_exec(stage_conn, '
            ALTER TABLE user_project ADD CONSTRAINT user_project_user_id_project_id_unique UNIQUE (user_id, project_id);
            INSERT INTO user_project (user_id, project_id)
            SELECT user_id, project_id FROM user_project_old
            ON CONFLICT (user_id, project_id) DO NOTHING;
            ALTER TABLE user_project DROP CONSTRAINT user_project_user_id_project_id_unique;
        ');

        -- 10. RESTORATION CONSTRAINTS
        PERFORM dblink_exec(stage_conn, '
            ALTER TABLE users DROP CONSTRAINT IF EXISTS users_email_unique;
        ');

        -- 11. DELETION OF INTERMEDIATE TABLES
        PERFORM dblink_exec(stage_conn, '
            DROP TABLE IF EXISTS public.users_old;
            DROP TABLE IF EXISTS public.groups_old;
            DROP TABLE IF EXISTS public.user_groups_old;
            DROP TABLE IF EXISTS public.user_project_old;
        ');

        -- Отключение
        PERFORM dblink_disconnect(stage_conn);

        RAISE INFO 'MERGE and cleanup done for %', stage_db;
    END LOOP;
END;
$$;

--////////////////////////////////////////////////////////
-- DROP zombie.public
--////////////////////////////////////////////////////////