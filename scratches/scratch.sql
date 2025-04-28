DO $$
DECLARE
    conn_str TEXT;
    sql TEXT;
    server_port TEXT = 'host=prd-bi-01.maxbit.private port=5432';
    dblink_conn TEXT;
    current_db TEXT;
    db_name TEXT;
BEGIN
    CREATE TABLE IF NOT EXISTS public.robo_statistics (...);

    TRUNCATE TABLE public.robo_statistics;

    SELECT current_database() INTO current_db;

    FOR conn_str IN
        SELECT 'dbname=' || datname || ' user=robo_sudo password=%dFgH8!zX4&kLmT2 ' || server_port
        FROM pg_database
        WHERE datname NOT IN ('template0', 'template1', 'postgres')
    LOOP
        BEGIN
            db_name := substring(conn_str from 'dbname=([^ ]+)');
            RAISE NOTICE 'Processing database: %', db_name;

            dblink_conn := dblink_connect('myconn', conn_str);

            sql := format($sql$
                INSERT INTO public.robo_statistics
                SELECT
                    %L,
                    s.schemaname,
                    s.relname,
                    pg_table_size(s.schemaname || '.' || s.relname),
                    pg_total_relation_size(s.schemaname || '.' || s.relname),
                    pg_indexes_size(s.schemaname || '.' || s.relname),
                    c.reltuples::bigint,
                    s.seq_scan,
                    s.idx_scan,
                    s.n_tup_ins,
                    s.n_tup_upd,
                    s.n_tup_del,
                    s.n_live_tup,
                    s.n_dead_tup,
                    s.last_vacuum,
                    s.last_autovacuum,
                    s.last_analyze,
                    s.last_autoanalyze,
                    now()
                FROM pg_stat_user_tables s
                JOIN pg_class c ON c.relname = s.relname
                    AND c.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = s.schemaname)
            $sql$, db_name);

            PERFORM dblink_exec('myconn', sql);

            PERFORM dblink_disconnect('myconn');
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Error processing database %: %', db_name, SQLERRM;
            PERFORM dblink_disconnect('myconn');
        END;
    END LOOP;
END $$;
