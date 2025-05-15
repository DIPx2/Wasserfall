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
        'maxmind_stage', -- maxmind
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
-- OLD-COPING
--=======================

DO -- С учетом того, что структура разнится
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
                SELECT tablename FROM dblink(conn_name, $f$SELECT tablename FROM pg_tables WHERE schemaname = 'public'
                AND tablename NOT IN ('maxmind_stage', 'users_old', 'groups_old', 'user_groups_old', 'user_project_old')$f$) AS t (tablename text)
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
        'volna_mbss_stage',
        'starda_mbss_stage',
        'sol_mbss_stage',
        'rox_mbss_stage',
        'monro_mbss_stage',
        'mbss_stage',
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
        '1go_mbss_stage'
    ];

    arr_master text[] = ARRAY[
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
    FOREACH db_name IN ARRAY arr_stage
        LOOP
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
        'volna_mbss_stage',
        'starda_mbss_stage',
        'sol_mbss_stage',
        'rox_mbss_stage',
        'monro_mbss_stage',
        'mbss_stage',
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
        '1go_mbss_stage'
    ];

    arr_master text[] = ARRAY[
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

    idx int;
    master_db text;
    stage_db text;
    master_conn text;
    stage_conn text;
    master_conn_str text;
    stage_conn_str text;
    insert_sql text;

BEGIN
    FOR idx IN 1..array_length(arr_stage, 1)
        LOOP
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
        'volna_mbss_stage',
        'starda_mbss_stage',
        'sol_mbss_stage',
        'rox_mbss_stage',
        'monro_mbss_stage',
        'mbss_stage',
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
        '1go_mbss_stage'
    ];

    arr_master text[] = ARRAY[
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

--=======================
-- MERGE, RESTORATION CONSTRAINTS, DELETION OF INTERMEDIATE TABLES
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
        '1go_mbss_stage'
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

        PERFORM dblink_connect(stage_conn, stage_conn_str);

        -- MERGE
        PERFORM dblink_exec(stage_conn, '
            ALTER TABLE user_project ADD CONSTRAINT user_project_user_id_project_id_unique UNIQUE (user_id, project_id);
            INSERT INTO user_project (user_id, project_id)
            SELECT user_id, project_id FROM user_project_old
            ON CONFLICT (user_id, project_id) DO NOTHING;
            ALTER TABLE user_project DROP CONSTRAINT user_project_user_id_project_id_unique;
        ');
        -- RESTORATION CONSTRAINTS
        PERFORM dblink_exec(stage_conn, '
            ALTER TABLE users DROP CONSTRAINT IF EXISTS users_email_unique;
        ');

        -- DELETION OF INTERMEDIATE TABLES
        PERFORM dblink_exec(stage_conn, '
            DROP TABLE IF EXISTS public.users_old;
            DROP TABLE IF EXISTS public.groups_old;
            DROP TABLE IF EXISTS public.user_groups_old;
            DROP TABLE IF EXISTS public.user_project_old;
        ');

        PERFORM dblink_disconnect(stage_conn);

        RAISE INFO 'MERGE and cleanup done for %', stage_db;
    END LOOP;
END;
$$;

--////////////////////////////////////////////////////////
-- DROPING zombie.public
--////////////////////////////////////////////////////////

--==========================
-- Генератор случайного адреса электронной почты в заданном поле указанной базы данных
--==========================

DO $$
DECLARE
    domains TEXT[] = ARRAY[
        'gmail.com', 'yahoo.com', 'outlook.com', 'protonmail.com', 'zoho.com',
        'mail.com', 'aol.com', 'icloud.com', 'fastmail.com', 'tutanota.com',
        'mail.ru', 'yandex.ru', 'rambler.ru', 'bk.ru', 'list.ru',
        'inbox.ru', 'ya.ru', 'gmail.by', 'mail.ua', 'meta.ua',
        'почта.рф', 'яндекс.рф', 'маил.рф', 'бел.бел', 'укр.укр',
        'католик.рф', 'домен.рус', 'сайт.бел', 'онлайн.укр', 'кремль.рф',
        'i-love-you.org', 'hacker.me', 'artists.world', 'musician.net',
        'writers.club', 'developers.team', 'engineer.space', 'scientist.tech'
    ];

    first_names TEXT[] = ARRAY[
        'leonardo', 'vincent', 'claude', 'rembrandt', 'johann',
        'wolfgang', 'franz', 'ludwig', 'frederic', 'antonio',
        'william', 'alexander', 'homer', 'john', 'dante',
        'alexei', 'ivan', 'sergey', 'nikolai', 'boris',
        'pyotr', 'dmitry', 'mikhail', 'andrei', 'vladimir',
        'ekaterina', 'olga', 'anna', 'maria', 'natalia',
        'oleksandr', 'volodymyr', 'vasyl', 'mykola', 'taras',
        'yaroslav', 'bohdan', 'sviatoslav', 'nazar', 'rostislav',
        'sophia', 'victoria', 'isabella', 'emma', 'olivia',
        'anastasia', 'irina', 'svetlana', 'lyudmila', 'tatyana'
    ];

    last_names TEXT[] = ARRAY[
        'da_vinci', 'vangogh', 'monet', 'harmenszoon', 'bach',
        'mozart', 'schubert', 'beethoven', 'chopin', 'vivaldi',
        'shakespeare', 'pushkin', 'simpson', 'milton', 'alighieri',
        'romanov', 'petrov', 'sidorov', 'lebedev', 'smirnov',
        'chekhov', 'dostoevsky', 'turgenev', 'gogol', 'bulgakov',
        'tolstoy', 'nabokov', 'solzhenitsyn', 'pasternak', 'akunin',
        'shevchenko', 'franko', 'lesya', 'skovoroda', 'kostenko',
        'bykov', 'korotkevich', 'bogdanovich', 'kupala', 'kolas',
        'coder', 'developer', 'engineer', 'scientist', 'artist',
        'musician', 'writer', 'philosopher', 'explorer', 'inventor'
    ];

    prefixes TEXT[] = ARRAY['', 'the_', 'real_', 'official_', 'my_', 'super_', 'best_'];
    suffixes TEXT[] = ARRAY['', '123', '2023', '007', '88', '42', '99', 'x', 'z', 'jr', 'sr'];
    separators TEXT[] = ARRAY['.', '-', '_', ''];

    rec RECORD;
    e_mail TEXT;
    local_part TEXT;
    domain TEXT;
    use_prefix BOOLEAN;
    use_suffix BOOLEAN;
    use_digits BOOLEAN;
    name_format INT;

BEGIN

    --FOR rec IN SELECT id FROM users WHERE email IS NOT NULL LOOP
    --FOR rec IN SELECT id FROM customers WHERE email IS NOT NULL LOOP
        domain = domains[1 + floor(random() * array_length(domains, 1))];

        use_prefix = random() > 0.7;
        use_suffix = random() > 0.6;
        use_digits = random() > 0.5;
        name_format = floor(random() * 4);

        local_part = first_names[1 + floor(random() * array_length(first_names, 1))];

        CASE name_format
            WHEN 0 THEN -- имя.фамилия
                local_part = local_part || separators[1 + floor(random() * array_length(separators, 1))] ||
                             last_names[1 + floor(random() * array_length(last_names, 1))];
            WHEN 1 THEN -- и.фамилия
                local_part = substring(local_part from 1 for 1) || separators[1 + floor(random() * array_length(separators, 1))] ||
                             last_names[1 + floor(random() * array_length(last_names, 1))];
            WHEN 2 THEN -- фамилия.и
                local_part = last_names[1 + floor(random() * array_length(last_names, 1))] ||
                             separators[1 + floor(random() * array_length(separators, 1))] ||
                             substring(local_part from 1 for 1);
            ELSE -- имя+цифры
                local_part = local_part || floor(random() * 1000)::text;
        END CASE;

        IF use_prefix THEN
            local_part = prefixes[1 + floor(random() * array_length(prefixes, 1))] || local_part;
        END IF;

        IF use_suffix THEN
            local_part = local_part || suffixes[1 + floor(random() * array_length(suffixes, 1))];
        END IF;

        IF use_digits AND name_format != 2 THEN
            IF random() > 0.5 THEN
                local_part = local_part || floor(random() * 100)::text;
            ELSE
                local_part = floor(random() * 100)::text || local_part;
            END IF;
        END IF;

        e_mail = lower(local_part) || '@' || domain;

--==========================
        -- UPDATE users SET email = e_mail WHERE id = rec.id;
		-- UPDATE customers SET email = e_mail WHERE id = rec.id;
--==========================

    END LOOP;
END $$;

--------------------------------------------------------------------------------------------------------------------------------------------------

--==========================
-- Генератор случайного номера телефона для указанных стран
--==========================

DO $$
DECLARE
    country_codes TEXT[] = ARRAY[
        '+49',  -- Германия
        '+7',   -- Россия
        '+375', -- Беларусь
        '+48',  -- Польша
        '+43',  -- Австрия
        '+31'   -- Нидерланды
    ];

    german_operators TEXT[] = ARRAY['151', '160', '170', '175', '176', '177', '178', '179'];
    russian_operators TEXT[] = ARRAY['901', '902', '903', '904', '905', '906', '909', '910', '911', '912', '913'];
    belarus_operators TEXT[] = ARRAY['25', '29', '33', '44'];
    polish_operators TEXT[] = ARRAY['501', '502', '503', '504', '505', '506', '507', '508', '509'];
    austrian_operators TEXT[] = ARRAY['650', '660', '664', '676', '699'];
    dutch_operators TEXT[] = ARRAY['6'];

    rec RECORD;
    phone TEXT;
    code TEXT;
    operator TEXT;
    number_part TEXT;

BEGIN

    FOR rec IN SELECT id FROM customers WHERE phone IS NOT NULL LOOP
        code = country_codes[1 + floor(random() * array_length(country_codes, 1))];

        IF code = '+49' THEN
            operator = german_operators[1 + floor(random() * array_length(german_operators, 1))];
            number_part = lpad(floor(random() * 10000000)::text, 7, '0');

        ELSIF code = '+7' THEN
            operator = russian_operators[1 + floor(random() * array_length(russian_operators, 1))];
            number_part = lpad(floor(random() * 1000000)::text, 7, '0');

        ELSIF code = '+375' THEN
            operator = belarus_operators[1 + floor(random() * array_length(belarus_operators, 1))];
            number_part = lpad(floor(random() * 1000000)::text, 7, '0');

        ELSIF code = '+48' THEN
            operator = polish_operators[1 + floor(random() * array_length(polish_operators, 1))];
            number_part = lpad(floor(random() * 1000000)::text, 6, '0');

        ELSIF code = '+43' THEN
            operator = austrian_operators[1 + floor(random() * array_length(austrian_operators, 1))];
            number_part = lpad(floor(random() * 1000000)::text, 6, '0');

        ELSE -- '+31'
            operator = dutch_operators[1 + floor(random() * array_length(dutch_operators, 1))];
            number_part = lpad(floor(random() * 10000000)::text, 8, '0');
        END IF;

        phone = code || operator || number_part;

--==========================
        UPDATE customers SET phone = phone WHERE id = rec.id;
--==========================
    END LOOP;
END $$;

--------------------------------------------------------------------------------------------------------------------------------------------------

--==========================
-- Генератор случайных имён и фамилий на кириллице с разным регистром
--==========================

DO $$
DECLARE
    first_names TEXT[] = ARRAY[
        'Александр', 'Михаил', 'Дмитрий', 'Сергей', 'Андрей',
        'Иван', 'Николай', 'Павел', 'Юрий', 'Владимир',
        'Екатерина', 'Анна', 'Ольга', 'Мария', 'Наталья',
        'Ирина', 'Светлана', 'Татьяна', 'Любовь', 'Людмила',
        'Анастасия', 'Елена', 'Зоя', 'Галина', 'Виктория'
    ];

    last_names TEXT[] = ARRAY[
        'Иванов', 'Петров', 'Сидоров', 'Смирнов', 'Кузнецов',
        'Попов', 'Соколов', 'Лебедев', 'Козлов', 'Новиков',
        'Фёдоров', 'Морозов', 'Волков', 'Алексеев', 'Лебедева',
        'Михайлова', 'Киселёва', 'Орлова', 'Тарасова', 'Семенова'
    ];

    rec RECORD;
    raw_first TEXT;
    raw_last TEXT;
    new_name TEXT;

    FUNCTION random_case(input TEXT) RETURNS TEXT AS $f$
    DECLARE
        i INT;
        out TEXT = '';
        ch TEXT;
    BEGIN
        FOR i IN 1..length(input) LOOP
            ch = substr(input, i, 1);
            IF random() > 0.5 THEN
                out = out || upper(ch);
            ELSE
                out = out || lower(ch);
            END IF;
        END LOOP;
        RETURN out;
    END;
    $f$ LANGUAGE plpgsql;
BEGIN
    --FOR rec IN SELECT id FROM users WHERE name IS NOT NULL LOOP
    --FOR rec IN SELECT id FROM customers WHERE name IS NOT NULL LOOP
        raw_first = first_names[1 + floor(random() * array_length(first_names, 1))];
        raw_last = last_names[1 + floor(random() * array_length(last_names, 1))];
        new_name = random_case(raw_first) || ' ' || random_case(raw_last);
--==========================
        --UPDATE users SET name = new_name WHERE id = rec.id;
        --UPDATE customers SET name = new_name WHERE id = rec.id;
--==========================
    END LOOP;
END $$;




--------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------
-- Depersonalization ChatSupport:Development:mbss_master.messages --------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------

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