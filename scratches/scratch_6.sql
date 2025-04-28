SELECT
    relname AS table_name,
    n_tup_ins AS inserts,
    n_tup_upd AS updates,
    n_tup_del AS deletes,
    seq_scan + idx_scan AS selects
FROM
    pg_stat_user_tables
WHERE
    schemaname = 'public'
    AND relname = 'messages';

SELECT count(*) FROM messages;

SELECT
    MAX(length(trigger_uuid)) AS max_length,
    MIN(length(trigger_uuid)) AS min_length,
    AVG(length(trigger_uuid)) AS avg_length
FROM
    public.messages;

 SELECT count(*) from public.messages WHERE length(trigger_uuid) = 0;

SELECT
    pg_class.relname AS index_name,
    pg_relation_size(pg_class.oid) AS index_size_bytes,
    pg_relation_size(table_class.oid) AS table_size_bytes,
    ROUND(
        (pg_relation_size(pg_class.oid)::numeric / NULLIF(pg_relation_size(table_class.oid), 0)) * 100,
        2
    ) AS index_size_percent_of_table,
    pg_stat_user_indexes.idx_scan AS index_scans,
    pg_stat_user_indexes.idx_tup_read AS tuples_read,
    pg_stat_user_indexes.idx_tup_fetch AS tuples_fetched
FROM
    pg_class
    JOIN pg_index ON pg_class.oid = pg_index.indexrelid
    JOIN pg_class AS table_class ON table_class.oid = pg_index.indrelid
    LEFT JOIN pg_stat_user_indexes ON pg_stat_user_indexes.indexrelid = pg_class.oid
WHERE
    table_class.relname = 'messages'
    AND pg_class.relname = 'messages_pkey';


SELECT
    pg_class.relname AS index_name,
    pg_relation_size(pg_class.oid) AS index_size_bytes,
    pg_relation_size(table_class.oid) AS table_size_bytes,
    ROUND(
        (pg_relation_size(pg_class.oid)::numeric / NULLIF(pg_relation_size(table_class.oid), 0)) * 100,
        2
    ) AS index_size_percent_of_table,

    pg_stat_user_indexes.idx_scan AS index_scans,
    ROUND(
        (pg_stat_user_indexes.idx_scan::numeric / NULLIF(pg_stat_user_tables.seq_tup_read + pg_stat_user_tables.idx_tup_fetch, 0)) * 100,
        2
    ) AS index_scans_percent,

    pg_stat_user_indexes.idx_tup_read AS tuples_read,
    ROUND(
        (pg_stat_user_indexes.idx_tup_read::numeric / NULLIF(pg_stat_user_tables.n_tup_ins + pg_stat_user_tables.n_live_tup, 0)) * 100,
        2
    ) AS tuples_read_percent,

    pg_stat_user_indexes.idx_tup_fetch AS tuples_fetched,
    ROUND(
        (pg_stat_user_indexes.idx_tup_fetch::numeric / NULLIF(pg_stat_user_tables.n_tup_ins + pg_stat_user_tables.n_live_tup, 0)) * 100,
        2
    ) AS tuples_fetched_percent

FROM
    pg_class
    JOIN pg_index ON pg_class.oid = pg_index.indexrelid
    JOIN pg_class AS table_class ON table_class.oid = pg_index.indrelid
    LEFT JOIN pg_stat_user_indexes ON pg_stat_user_indexes.indexrelid = pg_class.oid
    LEFT JOIN pg_stat_user_tables ON pg_stat_user_tables.relid = table_class.oid
WHERE
    table_class.relname = 'messages'
    AND pg_class.relname = 'messages_pkey';


SELECT
    pg_class.relname AS index_name,
    pg_relation_size(pg_class.oid) AS index_size_bytes,
    pg_relation_size(table_class.oid) AS table_size_bytes,
    ROUND(
        (pg_relation_size(pg_class.oid)::numeric / NULLIF(pg_relation_size(table_class.oid), 0)) * 100,
        2
    ) AS index_size_percent_of_table,

    pg_stat_user_indexes.idx_scan AS index_scans,
    ROUND(
        (pg_stat_user_indexes.idx_scan::numeric / NULLIF(pg_stat_user_tables.seq_scan + pg_stat_user_tables.idx_scan, 0)) * 100,
        2
    ) AS index_scans_percent,

    pg_stat_user_indexes.idx_tup_read AS tuples_read,
    ROUND(
        (pg_stat_user_indexes.idx_tup_read::numeric / NULLIF(pg_stat_user_tables.n_tup_ins + pg_stat_user_tables.n_tup_upd + pg_stat_user_tables.n_tup_del, 0)) * 100,
        2
    ) AS tuples_read_percent,

    pg_stat_user_indexes.idx_tup_fetch AS tuples_fetched,
    ROUND(
        (pg_stat_user_indexes.idx_tup_fetch::numeric / NULLIF(pg_stat_user_tables.n_tup_ins + pg_stat_user_tables.n_tup_upd + pg_stat_user_tables.n_tup_del, 0)) * 100,
        2
    ) AS tuples_fetched_percent,

    pg_stat_user_tables.n_live_tup AS live_rows,
    pg_stat_user_tables.n_dead_tup AS dead_rows,
    ROUND(
        (pg_stat_user_tables.n_dead_tup::numeric / NULLIF(pg_stat_user_tables.n_live_tup + pg_stat_user_tables.n_dead_tup, 0)) * 100,
        2
    ) AS dead_rows_percent,

    pg_stat_user_tables.n_tup_ins AS total_inserts,
    pg_stat_user_tables.n_tup_upd AS total_updates,
    pg_stat_user_tables.n_tup_del AS total_deletes

FROM
    pg_class
    JOIN pg_index ON pg_class.oid = pg_index.indexrelid
    JOIN pg_class AS table_class ON table_class.oid = pg_index.indrelid
    LEFT JOIN pg_stat_user_indexes ON pg_stat_user_indexes.indexrelid = pg_class.oid
    LEFT JOIN pg_stat_user_tables ON pg_stat_user_tables.relid = table_class.oid
WHERE
    table_class.relname = 'messages'
    AND pg_class.relname = 'messages_pkey';


SELECT
    pg_class.relname AS index_name,
    pg_relation_size(pg_class.oid) AS index_size_bytes,
    pg_relation_size(table_class.oid) AS table_size_bytes,
    ROUND(
        (pg_relation_size(pg_class.oid)::numeric / NULLIF(pg_relation_size(table_class.oid), 0)) * 100,
        2
    ) AS index_size_percent_of_table,

    pg_stat_user_indexes.idx_scan AS index_scans,
    ROUND(
        (pg_stat_user_indexes.idx_scan::numeric / NULLIF(pg_stat_user_tables.seq_scan + pg_stat_user_tables.idx_scan, 0)) * 100,
        2
    ) AS index_scans_percent,

    pg_stat_user_indexes.idx_tup_read AS tuples_read,
    ROUND(
        (pg_stat_user_indexes.idx_tup_read::numeric / NULLIF(pg_stat_user_tables.n_tup_ins + pg_stat_user_tables.n_tup_upd + pg_stat_user_tables.n_tup_del, 0)) * 100,
        2
    ) AS tuples_read_percent,

    pg_stat_user_indexes.idx_tup_fetch AS tuples_fetched,
    ROUND(
        (pg_stat_user_indexes.idx_tup_fetch::numeric / NULLIF(pg_stat_user_tables.n_tup_ins + pg_stat_user_tables.n_tup_upd + pg_stat_user_tables.n_tup_del, 0)) * 100,
        2
    ) AS tuples_fetched_percent,

    pg_stat_user_tables.n_live_tup AS live_rows,
    pg_stat_user_tables.n_dead_tup AS dead_rows,
    ROUND(
        (pg_stat_user_tables.n_dead_tup::numeric / NULLIF(pg_stat_user_tables.n_live_tup + pg_stat_user_tables.n_dead_tup, 0)) * 100,
        2
    ) AS dead_rows_percent,

    pg_stat_user_tables.n_tup_ins AS total_inserts,
    pg_stat_user_tables.n_tup_upd AS total_updates,
    pg_stat_user_tables.n_tup_del AS total_deletes

FROM
    pg_class
    JOIN pg_index ON pg_class.oid = pg_index.indexrelid
    JOIN pg_class AS table_class ON table_class.oid = pg_index.indrelid
    LEFT JOIN pg_stat_user_indexes ON pg_stat_user_indexes.indexrelid = pg_class.oid
    LEFT JOIN pg_stat_user_tables ON pg_stat_user_tables.relid = table_class.oid
WHERE
    table_class.relname = 'messages'
ORDER BY
    index_size_bytes DESC;



SELECT
    pg_class.relname AS index_name,
    CASE
        WHEN pg_index.indisprimary THEN 'PRIMARY KEY'
        WHEN pg_index.indisunique THEN 'UNIQUE'
        ELSE 'INDEX'
    END AS index_type,
    pg_am.amname AS access_method,
    pg_relation_size(pg_class.oid) AS index_size_bytes,
    pg_relation_size(table_class.oid) AS table_size_bytes,
    ROUND(
        (pg_relation_size(pg_class.oid)::numeric / NULLIF(pg_relation_size(table_class.oid), 0)) * 100,
        2
    ) AS index_size_percent_of_table,

    pg_stat_user_indexes.idx_scan AS index_scans,
    ROUND(
        (pg_stat_user_indexes.idx_scan::numeric / NULLIF(pg_stat_user_tables.seq_scan + pg_stat_user_tables.idx_scan, 0)) * 100,
        2
    ) AS index_scans_percent,

    pg_stat_user_indexes.idx_tup_read AS tuples_read,
    ROUND(
        (pg_stat_user_indexes.idx_tup_read::numeric / NULLIF(pg_stat_user_tables.n_tup_ins + pg_stat_user_tables.n_tup_upd + pg_stat_user_tables.n_tup_del, 0)) * 100,
        2
    ) AS tuples_read_percent,

    pg_stat_user_indexes.idx_tup_fetch AS tuples_fetched,
    ROUND(
        (pg_stat_user_indexes.idx_tup_fetch::numeric / NULLIF(pg_stat_user_tables.n_tup_ins + pg_stat_user_tables.n_tup_upd + pg_stat_user_tables.n_tup_del, 0)) * 100,
        2
    ) AS tuples_fetched_percent,

    pg_stat_user_tables.n_live_tup AS live_rows,
    pg_stat_user_tables.n_dead_tup AS dead_rows,
    ROUND(
        (pg_stat_user_tables.n_dead_tup::numeric / NULLIF(pg_stat_user_tables.n_live_tup + pg_stat_user_tables.n_dead_tup, 0)) * 100,
        2
    ) AS dead_rows_percent,

    pg_stat_user_tables.n_tup_ins AS total_inserts,
    pg_stat_user_tables.n_tup_upd AS total_updates,
    pg_stat_user_tables.n_tup_del AS total_deletes,

    to_char(pg_stat_user_tables.last_vacuum, 'YYYY-MM-DD HH24:MI:SS') AS last_vacuum,
    to_char(pg_stat_user_tables.last_autovacuum, 'YYYY-MM-DD HH24:MI:SS') AS last_autovacuum,
    to_char(pg_stat_user_tables.last_analyze, 'YYYY-MM-DD HH24:MI:SS') AS last_analyze,
    to_char(pg_stat_user_tables.last_autoanalyze, 'YYYY-MM-DD HH24:MI:SS') AS last_autoanalyze,

    pg_stat_user_tables.vacuum_count,
    pg_stat_user_tables.autovacuum_count,
    pg_stat_user_tables.analyze_count,
    pg_stat_user_tables.autoanalyze_count,

    pg_stat_user_tables.n_mod_since_analyze AS rows_modified_since_last_analyze

FROM
    pg_class
    JOIN pg_index ON pg_class.oid = pg_index.indexrelid
    JOIN pg_class AS table_class ON table_class.oid = pg_index.indrelid
    JOIN pg_am ON pg_class.relam = pg_am.oid
    LEFT JOIN pg_stat_user_indexes ON pg_stat_user_indexes.indexrelid = pg_class.oid
    LEFT JOIN pg_stat_user_tables ON pg_stat_user_tables.relid = table_class.oid
WHERE
    table_class.relname = 'messages'
ORDER BY
    index_size_bytes DESC;


/*

 Что эта функция делает:
Собирает всю статистику по индексам заданной таблицы.

Показвает размеры индексов в абсолютных числах и в процентах от таблицы.

Показывает нагрузку на индексы (сканирования, чтения, извлечения).

Показывает сколько живых/мертвых строк.

Даты последнего VACUUM/ANALYZE.

Счётчики сколько раз вакуумировали/анализировали таблицу.
 */

CREATE OR REPLACE FUNCTION audit_table_indexes(tablename text)
RETURNS TABLE (
    index_name text,
    index_type text,
    access_method text,
    index_size_bytes bigint,
    table_size_bytes bigint,
    index_size_percent_of_table numeric(10,2),
    index_scans bigint,
    index_scans_percent numeric(10,2),
    tuples_read bigint,
    tuples_read_percent numeric(10,2),
    tuples_fetched bigint,
    tuples_fetched_percent numeric(10,2),
    live_rows bigint,
    dead_rows bigint,
    dead_rows_percent numeric(10,2),
    total_inserts bigint,
    total_updates bigint,
    total_deletes bigint,
    last_vacuum text,
    last_autovacuum text,
    last_analyze text,
    last_autoanalyze text,
    vacuum_count bigint,
    autovacuum_count bigint,
    analyze_count bigint,
    autoanalyze_count bigint,
    rows_modified_since_last_analyze bigint
) AS
$$
BEGIN
    RETURN QUERY
    SELECT
        idx.relname AS index_name,
        CASE
            WHEN i.indisprimary THEN 'PRIMARY KEY'
            WHEN i.indisunique THEN 'UNIQUE'
            ELSE 'INDEX'
        END AS index_type,
        am.amname AS access_method,
        pg_relation_size(idx.oid) AS index_size_bytes,
        pg_relation_size(tbl.oid) AS table_size_bytes,
        ROUND(
            (pg_relation_size(idx.oid)::numeric / NULLIF(pg_relation_size(tbl.oid), 0)) * 100,
            2
        ) AS index_size_percent_of_table,

        si.idx_scan,
        ROUND(
            (si.idx_scan::numeric / NULLIF(st.seq_scan + st.idx_scan, 0)) * 100,
            2
        ) AS index_scans_percent,

        si.idx_tup_read,
        ROUND(
            (si.idx_tup_read::numeric / NULLIF(st.n_tup_ins + st.n_tup_upd + st.n_tup_del, 0)) * 100,
            2
        ) AS tuples_read_percent,

        si.idx_tup_fetch,
        ROUND(
            (si.idx_tup_fetch::numeric / NULLIF(st.n_tup_ins + st.n_tup_upd + st.n_tup_del, 0)) * 100,
            2
        ) AS tuples_fetched_percent,

        st.n_live_tup AS live_rows,
        st.n_dead_tup AS dead_rows,
        ROUND(
            (st.n_dead_tup::numeric / NULLIF(st.n_live_tup + st.n_dead_tup, 0)) * 100,
            2
        ) AS dead_rows_percent,

        st.n_tup_ins AS total_inserts,
        st.n_tup_upd AS total_updates,
        st.n_tup_del AS total_deletes,

        to_char(st.last_vacuum, 'YYYY-MM-DD HH24:MI:SS') AS last_vacuum,
        to_char(st.last_autovacuum, 'YYYY-MM-DD HH24:MI:SS') AS last_autovacuum,
        to_char(st.last_analyze, 'YYYY-MM-DD HH24:MI:SS') AS last_analyze,
        to_char(st.last_autoanalyze, 'YYYY-MM-DD HH24:MI:SS') AS last_autoanalyze,

        st.vacuum_count,
        st.autovacuum_count,
        st.analyze_count,
        st.autoanalyze_count,
        st.n_mod_since_analyze AS rows_modified_since_last_analyze

    FROM
        pg_class idx
        JOIN pg_index i ON idx.oid = i.indexrelid
        JOIN pg_class tbl ON tbl.oid = i.indrelid
        JOIN pg_am am ON idx.relam = am.oid
        LEFT JOIN pg_stat_user_indexes si ON si.indexrelid = idx.oid
        LEFT JOIN pg_stat_user_tables st ON st.relid = tbl.oid
    WHERE
        tbl.relname = tablename
    ORDER BY
        index_size_bytes DESC;
END;
$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION audit_table_indexes(tablename text)
RETURNS TABLE (
    index_name text,
    index_type text,
    access_method text,
    index_size_bytes bigint,
    table_size_bytes bigint,
    index_size_percent_of_table numeric(10,2),
    index_scans bigint,
    index_scans_percent numeric(10,2),
    tuples_read bigint,
    tuples_read_percent numeric(10,2),
    tuples_fetched bigint,
    tuples_fetched_percent numeric(10,2),
    live_rows bigint,
    dead_rows bigint,
    dead_rows_percent numeric(10,2),
    total_inserts bigint,
    total_updates bigint,
    total_deletes bigint,
    last_vacuum text,
    last_autovacuum text,
    last_analyze text,
    last_autoanalyze text,
    vacuum_count bigint,
    autovacuum_count bigint,
    analyze_count bigint,
    autoanalyze_count bigint,
    rows_modified_since_last_analyze bigint,
    recommendation text
) AS
$$
BEGIN
    RETURN QUERY
    SELECT
        idx.relname AS index_name,
        CASE
            WHEN i.indisprimary THEN 'PRIMARY KEY'
            WHEN i.indisunique THEN 'UNIQUE'
            ELSE 'INDEX'
        END AS index_type,
        am.amname AS access_method,
        pg_relation_size(idx.oid) AS index_size_bytes,
        pg_relation_size(tbl.oid) AS table_size_bytes,
        ROUND(
            (pg_relation_size(idx.oid)::numeric / NULLIF(pg_relation_size(tbl.oid), 0)) * 100,
            2
        ) AS index_size_percent_of_table,

        COALESCE(si.idx_scan, 0),
        ROUND(
            (COALESCE(si.idx_scan, 0)::numeric / NULLIF(st.seq_scan + st.idx_scan, 0)) * 100,
            2
        ) AS index_scans_percent,

        COALESCE(si.idx_tup_read, 0),
        ROUND(
            (COALESCE(si.idx_tup_read, 0)::numeric / NULLIF(st.n_tup_ins + st.n_tup_upd + st.n_tup_del, 0)) * 100,
            2
        ) AS tuples_read_percent,

        COALESCE(si.idx_tup_fetch, 0),
        ROUND(
            (COALESCE(si.idx_tup_fetch, 0)::numeric / NULLIF(st.n_tup_ins + st.n_tup_upd + st.n_tup_del, 0)) * 100,
            2
        ) AS tuples_fetched_percent,

        st.n_live_tup,
        st.n_dead_tup,
        ROUND(
            (st.n_dead_tup::numeric / NULLIF(st.n_live_tup + st.n_dead_tup, 0)) * 100,
            2
        ) AS dead_rows_percent,

        st.n_tup_ins,
        st.n_tup_upd,
        st.n_tup_del,

        to_char(st.last_vacuum, 'YYYY-MM-DD HH24:MI:SS'),
        to_char(st.last_autovacuum, 'YYYY-MM-DD HH24:MI:SS'),
        to_char(st.last_analyze, 'YYYY-MM-DD HH24:MI:SS'),
        to_char(st.last_autoanalyze, 'YYYY-MM-DD HH24:MI:SS'),

        st.vacuum_count,
        st.autovacuum_count,
        st.analyze_count,
        st.autoanalyze_count,
        st.n_mod_since_analyze,

        -- Генерация рекомендации
        CASE
            WHEN st.n_dead_tup > 10000 AND (st.n_dead_tup::numeric / NULLIF(st.n_live_tup + st.n_dead_tup, 1)) > 0.2 THEN 'ВНИМАНИЕ: Много мёртвых строк — требуется VACUUM!'
            WHEN st.n_mod_since_analyze > (st.n_live_tup / 5) THEN 'ТРЕБУЕТСЯ: Переанализировать таблицу (ANALYZE)'
            WHEN COALESCE(si.idx_scan, 0) = 0 THEN 'Индекс не используется — можно рассмотреть удаление'
            WHEN (pg_relation_size(idx.oid)::numeric / NULLIF(pg_relation_size(tbl.oid), 1)) > 0.5 THEN 'Индекс очень большой относительно таблицы'
            ELSE 'OK'
        END AS recommendation

    FROM
        pg_class idx
        JOIN pg_index i ON idx.oid = i.indexrelid
        JOIN pg_class tbl ON tbl.oid = i.indrelid
        JOIN pg_am am ON idx.relam = am.oid
        LEFT JOIN pg_stat_user_indexes si ON si.indexrelid = idx.oid
        LEFT JOIN pg_stat_user_tables st ON st.relid = tbl.oid
    WHERE
        tbl.relname = tablename
    ORDER BY
        index_size_bytes DESC;
END;
$$ LANGUAGE plpgsql;
