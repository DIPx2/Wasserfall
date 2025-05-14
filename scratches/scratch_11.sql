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
