WITH main_table AS (
    SELECT
        c.oid AS table_oid,
        c.relname AS table_name,
        n.nspname AS schema_name,
        c.reltoastrelid AS toast_table_oid
    FROM
        pg_class c
    JOIN
        pg_namespace n ON n.oid = c.relnamespace
    WHERE
        n.nspname = 'public' AND c.relname = 'messages'
),
toast_info AS (
    SELECT
        mt.table_oid AS main_table_oid,
        mt.table_name AS main_table_name,
        mt.schema_name AS main_schema_name,
        tt.oid AS toast_table_oid,
        tt.relname AS toast_table_name,
        ni.oid AS toast_index_oid,
        ni.relname AS toast_index_name
    FROM
        main_table mt
    LEFT JOIN
        pg_class tt ON mt.toast_table_oid = tt.oid
    LEFT JOIN
        pg_index ix ON ix.indrelid = tt.oid
    LEFT JOIN
        pg_class ni ON ni.oid = ix.indexrelid
    WHERE tt.oid IS NOT NULL -- Убедимся, что TOAST таблица существует
)
SELECT
    ti.main_schema_name || '.' || ti.main_table_name AS main_table,
    ti.toast_table_name,
    pg_size_pretty(pg_table_size(ti.toast_table_oid)) AS toast_table_data_size,
    ti.toast_index_name,
    pg_size_pretty(pg_relation_size(ti.toast_index_oid)) AS toast_index_size,
    pg_size_pretty(pg_total_relation_size(ti.toast_table_oid)) AS toast_table_total_size_inc_index
FROM
    toast_info ti;


SELECT chunk_id, chunk_seq, length(chunk_data) AS chunk_data_length, chunk_data FROM pg_toast.pg_toast_17017342 LIMIT 10;

-- для поиска таблиц с самыми большими TOAST-данными:
SELECT
    c.relname AS table_name,
    t.relname AS toast_table_name,
    pg_size_pretty(pg_total_relation_size(t.oid)) AS toast_table_size_pretty,
    pg_total_relation_size(t.oid) AS toast_table_size_bytes
FROM
    pg_class c
JOIN
    pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN
    pg_class t ON c.reltoastrelid = t.oid
WHERE
    c.relkind IN ('r', 'm') -- 'r' для обычных таблиц, 'm' для материализованных представлений
    AND t.oid IS NOT NULL -- Убедимся, что TOAST-таблица существует
    AND n.nspname NOT IN ('pg_catalog', 'information_schema') -- Исключаем системные схемы
    AND n.nspname NOT LIKE 'pg_toast%' -- Исключаем сами TOAST-схемы
ORDER BY
    toast_table_size_bytes DESC;


------------------------

SELECT oid::regclass AS table_name
FROM pg_class
WHERE oid = 17017342;