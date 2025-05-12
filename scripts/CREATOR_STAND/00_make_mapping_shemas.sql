/*

--=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=
-- USE postgres
--=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=


CREATE DATABASE zombie;


--=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=
-- USE zombie
--=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=



--============================
-- Make mapping and shemas
--============================

CREATE EXTENSION IF NOT EXISTS postgres_fdw;

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
DO $$
DECLARE
    arr_stage TEXT[] := ARRAY[
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
    db_name TEXT;
    output_path TEXT;
    conn_name TEXT;
    conn_str TEXT;
    dblink_query TEXT;
BEGIN
    CREATE EXTENSION IF NOT EXISTS dblink;

    FOREACH db_name IN ARRAY arr_stage LOOP
        output_path := format('/home/reports/%s_privileges.csv', db_name);
        conn_name := format('conn_%s', db_name);

        conn_str := format(
            'dbname=%s user=robo_sudo password=%s host=localhost port=15434',
            db_name, '%dFgH8!zX4&kLmT2'
        );

        BEGIN
            PERFORM dblink_connect(conn_name, conn_str);

            dblink_query := format($sql$
                SELECT grantor,
                       grantee,
                       privilege_type,
                       table_schema AS object_schema,
                       table_name AS object_name,
                       'TABLE' AS object_type
                FROM information_schema.table_privileges
                WHERE table_catalog = %L

                UNION ALL

                SELECT pg_get_userbyid(acl.grantor),
                       acl.grantee::regrole::text,
                       acl.privilege_type,
                       n.nspname,
                       'SCHEMA',
                       'SCHEMA'
                FROM pg_namespace n,
                     aclexplode(n.nspacl) AS acl
                WHERE n.nspname NOT LIKE 'pg_%%'
                  AND n.nspname != 'information_schema'

                UNION ALL

                SELECT pg_get_userbyid(acl.grantor),
                       acl.grantee::regrole::text,
                       acl.privilege_type,
                       'DATABASE',
                       'DATABASE',
                       'DATABASE'
                FROM pg_database d,
                     aclexplode(d.datacl) AS acl
                WHERE d.datname = %L

                UNION ALL

                SELECT pg_get_userbyid(acl.grantor),
                       acl.grantee::regrole::text,
                       acl.privilege_type,
                       n.nspname,
                       p.proname,
                       'FUNCTION'
                FROM pg_proc p
                JOIN pg_namespace n ON p.pronamespace = n.oid,
                     aclexplode(p.proacl) AS acl
                WHERE n.nspname NOT LIKE 'pg_%%'
                  AND n.nspname != 'information_schema'
            $sql$, db_name, db_name);

            -- создаём временную таблицу
            EXECUTE format('DROP TABLE IF EXISTS tmp_privs_%I', db_name);
            EXECUTE format(
                'CREATE TEMP TABLE tmp_privs_%1$I AS
                 SELECT * FROM dblink(%2$L, %3$L) AS t(
                    grantor text,
                    grantee text,
                    privilege_type text,
                    object_schema text,
                    object_name text,
                    object_type text
                 )',
                db_name, conn_name, dblink_query
            );

            -- сохраняем результат в файл
            EXECUTE format('COPY tmp_privs_%I TO %L WITH CSV HEADER', db_name, output_path);

            RAISE NOTICE 'Успешно обработана база данных: %', db_name;

        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Ошибка при обработке базы %: %', db_name, SQLERRM;
        END;

        PERFORM dblink_disconnect(conn_name);
    END LOOP;
END $$;




*/