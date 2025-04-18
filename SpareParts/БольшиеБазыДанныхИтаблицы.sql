DO $$
DECLARE
    tbl RECORD;
    size_bytes BIGINT;
BEGIN
    FOR tbl IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'robo_dbaas_core'
    LOOP
        EXECUTE format(
            'SELECT pg_total_relation_size(''%I.%I''::regclass)',
            'robo_dbaas_core',
            tbl.tablename
        ) INTO size_bytes;

        RAISE INFO '% => % bytes', 'robo_dbaas_core.' || tbl.tablename, size_bytes;
    END LOOP;
END $$;



/*
DO $$
DECLARE
    СписокСерверов TEXT[] = ARRAY[]::TEXT[];
    line RECORD;
    line2  RECORD;
BEGIN
    СписокСерверов := array_append(СписокСерверов, 'prd-chat-pg-02.maxbit.private:5434');
    --EXECUTE FORMAT( 'CREATE SERVER IF NOT EXISTS robo_mashine FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host %L, port %L, dbname %L);', (string_to_array(СписокСерверов[1], ':'))[1], (string_to_array(СписокСерверов[1], ':'))[2], 'fresh_mbss_master' );
    --EXECUTE FORMAT( 'CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER SERVER robo_mashine OPTIONS (user %L, password %L);', 'robo_sudo', '%dFgH8!zX4&kLmT2' );
    --EXECUTE 'IMPORT FOREIGN SCHEMA public FROM SERVER robo_mashine INTO robo_dbaas_core;';

    CREATE TEMP TABLE tmp_foreign_table_sizes ( table_name TEXT, total_size TEXT ) ON COMMIT DROP;

    FOR line IN SELECT 'robo_dbaas_core.' || quote_ident(tablename) AS table_name, pg_size_pretty(pg_total_relation_size('robo_dbaas_core.' || quote_ident(tablename))) AS total_size
        FROM pg_tables
        WHERE schemaname = 'robo_dbaas_core'
        ORDER BY pg_total_relation_size('robo_dbaas_core.' || quote_ident(tablename)) DESC
    LOOP
        INSERT INTO tmp_foreign_table_sizes(table_name, total_size)
        VALUES (line.table_name, line.total_size);
    END LOOP;

    FOR line2 IN SELECT * FROM tmp_foreign_table_sizes
        LOOP
            RAISE INFO '%',  line2;
        END LOOP;

END $$;
 */