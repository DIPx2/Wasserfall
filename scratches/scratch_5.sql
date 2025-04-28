/*
SELECT
    pg_class.relname AS table_name,
    pg_attribute.attname AS column_name,
    seq.relname AS sequence_name
FROM pg_class AS seq
JOIN pg_namespace ON seq.relnamespace = pg_namespace.oid
JOIN pg_depend ON pg_depend.objid = seq.oid
JOIN pg_class ON pg_depend.refobjid = pg_class.oid
JOIN pg_attribute ON pg_depend.refobjid = pg_attribute.attrelid AND pg_depend.refobjsubid = pg_attribute.attnum
WHERE seq.relkind = 'S'
AND pg_namespace.nspname = 'public';
*/

DO $$
DECLARE
    rec RECORD;
    sql TEXT;
BEGIN

    CREATE TEMP TABLE table_extended_stats (
        table_name TEXT,
        schema_name TEXT,
        owner TEXT,
        row_count BIGINT,
        total_size TEXT,
        index_size TEXT,
        fragmentation_ratio NUMERIC,
        seq_scans BIGINT,
        idx_scans BIGINT,
        inserts BIGINT,
        updates BIGINT,
        deletes BIGINT,
        triggers TEXT,
        foreign_keys TEXT,
        indexes TEXT
    );


    FOR rec IN
        SELECT schemaname, tablename, tableowner
        FROM pg_catalog.pg_tables
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
    LOOP

        sql = format( -- Делаем форматирование ВНУТРИ цикла
            $x0$ INSERT INTO table_extended_stats
               SELECT %L AS table_name,
                      %L AS schema_name,
                      %L AS owner,
                      (SELECT reltuples::BIGINT FROM pg_class WHERE relname = %L) AS row_count,
                      pg_size_pretty(pg_relation_size(%L)) AS total_size,
                      pg_size_pretty(pg_indexes_size(%L)) AS index_size,
                      (SELECT CASE WHEN pg_relation_size(%L) > 0
                                   THEN pg_indexes_size(%L)::NUMERIC / pg_relation_size(%L)
                                   ELSE 0 END) AS fragmentation_ratio,
                      (SELECT coalesce(seq_scan, 0) FROM pg_stat_user_tables WHERE relname = %L) AS seq_scans,
                      (SELECT coalesce(idx_scan, 0) FROM pg_stat_user_tables WHERE relname = %L) AS idx_scans,
                      (SELECT coalesce(n_tup_ins, 0) FROM pg_stat_user_tables WHERE relname = %L) AS inserts,
                      (SELECT coalesce(n_tup_upd, 0) FROM pg_stat_user_tables WHERE relname = %L) AS updates,
                      (SELECT coalesce(n_tup_del, 0) FROM pg_stat_user_tables WHERE relname = %L) AS deletes,
                      (SELECT string_agg(trigger_name, ', ') FROM information_schema.triggers WHERE event_object_table = %L) AS triggers,
                      (SELECT string_agg(constraint_name, ', ') FROM information_schema.table_constraints WHERE table_name = %L AND constraint_type = 'FOREIGN KEY') AS foreign_keys,
                      (SELECT string_agg(indexname, ', ') FROM pg_indexes WHERE tablename = %L) AS indexes; $x0$,
            rec.tablename, rec.schemaname, rec.tableowner,
            rec.tablename, rec.tablename, rec.tablename, rec.tablename,
            rec.tablename, rec.tablename, rec.tablename, rec.tablename,
            rec.tablename, rec.tablename, rec.tablename, rec.tablename,
            rec.tablename, rec.tablename
        );

        EXECUTE sql;
    END LOOP;

    FOR rec in SELECT * FROM table_extended_stats
        LOOP
            RAISE INFO '%', rec;
        END LOOP;

    DROP TABLE table_extended_stats;

END $$;
