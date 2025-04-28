WITH
-- 1. CSV-сводка по базам
csv_summary AS (
    SELECT
        1 AS section_order,
        '=== CSV Summary by DB ===' AS section,
        NULL::text AS dbname,
        NULL::text AS schema_name,
        NULL::text AS table_name,
        NULL::numeric AS size_gb,
        NULL::numeric AS max_gb,
        NULL::numeric AS min_gb,
        NULL::numeric AS diff_gb,
        NULL::text AS relative_diff,
        NULL::integer AS table_count,
        'Название БД;Количество таблиц;Общий размер таблиц (ГБ);Средний размер таблицы (ГБ);Максимальный размер таблицы (ГБ);Минимальный размер таблицы (ГБ);Разница между max и min (ГБ);% разницы между max и min' AS csv_line,
        NULL::bigint AS seq_scan,
        NULL::bigint AS idx_scan,
        NULL::bigint AS n_tup_ins,
        NULL::bigint AS n_tup_upd,
        NULL::bigint AS n_tup_del
    UNION ALL
    SELECT
        1 AS section_order,
        NULL::text AS section,
        NULL::text AS dbname,
        NULL::text AS schema_name,
        NULL::text AS table_name,
        NULL::numeric AS size_gb,
        NULL::numeric AS max_gb,
        NULL::numeric AS min_gb,
        NULL::numeric AS diff_gb,
        NULL::text AS relative_diff,
        NULL::integer AS table_count,
        dbname || ';' ||
        COUNT(*)::text || ';' ||
        ROUND(SUM(table_size_bytes)/1073741824.0, 3)::text || ';' ||
        ROUND(AVG(table_size_bytes)/1073741824.0, 3)::text || ';' ||
        ROUND(MAX(table_size_bytes)/1073741824.0, 3)::text || ';' ||
        ROUND(MIN(table_size_bytes)/1073741824.0, 3)::text || ';' ||
        ROUND((MAX(table_size_bytes)-MIN(table_size_bytes))/1073741824.0, 3)::text || ';' ||
        CASE
            WHEN MIN(table_size_bytes) = 0 THEN 'N/A'
            ELSE ROUND(((MAX(table_size_bytes)-MIN(table_size_bytes))*100.0/NULLIF(MIN(table_size_bytes), 0)), 2)::text
        END AS csv_line,
        NULL::bigint AS seq_scan,
        NULL::bigint AS idx_scan,
        NULL::bigint AS n_tup_ins,
        NULL::bigint AS n_tup_upd,
        NULL::bigint AS n_tup_del
    FROM
        robo_db_table_sizes
    WHERE
        dbname NOT IN ('messenger_martin', 'messenger_flagman', '')
    GROUP BY
        dbname
),

-- 2. Топ-10 самых тяжёлых таблиц
top_tables AS (
    SELECT
        2 AS section_order,
        '=== TOP 10 Largest Tables ===' AS section,
        NULL::text AS dbname,
        NULL::text AS schema_name,
        NULL::text AS table_name,
        NULL::numeric AS size_gb,
        NULL::numeric AS max_gb,
        NULL::numeric AS min_gb,
        NULL::numeric AS diff_gb,
        NULL::text AS relative_diff,
        NULL::integer AS table_count,
        NULL::text AS csv_line,
        NULL::bigint AS seq_scan,
        NULL::bigint AS idx_scan,
        NULL::bigint AS n_tup_ins,
        NULL::bigint AS n_tup_upd,
        NULL::bigint AS n_tup_del
    UNION ALL
    SELECT
        2 AS section_order,
        NULL::text AS section,
        dbname,
        schema_name,
        table_name,
        ROUND(table_size_bytes / 1073741824.0, 3) AS size_gb,
        NULL::numeric AS max_gb,
        NULL::numeric AS min_gb,
        NULL::numeric AS diff_gb,
        NULL::text AS relative_diff,
        NULL::integer AS table_count,
        NULL::text AS csv_line,
        NULL::bigint AS seq_scan,
        NULL::bigint AS idx_scan,
        NULL::bigint AS n_tup_ins,
        NULL::bigint AS n_tup_upd,
        NULL::bigint AS n_tup_del
    FROM
        robo_db_table_sizes
    LIMIT 10
),

-- 3. БД с наибольшим разбросом в размерах таблиц
size_spread AS (
    SELECT
        3 AS section_order,
        '=== Databases with Most Table Size Spread ===' AS section,
        NULL::text AS dbname,
        NULL::text AS schema_name,
        NULL::text AS table_name,
        NULL::numeric AS size_gb,
        NULL::numeric AS max_gb,
        NULL::numeric AS min_gb,
        NULL::numeric AS diff_gb,
        NULL::text AS relative_diff,
        NULL::integer AS table_count,
        NULL::text AS csv_line,
        NULL::bigint AS seq_scan,
        NULL::bigint AS idx_scan,
        NULL::bigint AS n_tup_ins,
        NULL::bigint AS n_tup_upd,
        NULL::bigint AS n_tup_del
    UNION ALL
    SELECT
        3 AS section_order,
        NULL::text AS section,
        dbname,
        NULL::text AS schema_name,
        NULL::text AS table_name,
        NULL::numeric AS size_gb,
        ROUND(MAX(table_size_bytes)/1073741824.0, 3) AS max_gb,
        ROUND(MIN(table_size_bytes)/1073741824.0, 3) AS min_gb,
        ROUND((MAX(table_size_bytes) - MIN(table_size_bytes)) / 1073741824.0, 3) AS diff_gb,
        CASE
            WHEN MIN(table_size_bytes) = 0 THEN '∞'
            ELSE ROUND(((MAX(table_size_bytes)-MIN(table_size_bytes))*100.0/MIN(table_size_bytes)), 2)::text || '%'
        END AS relative_diff,
        NULL::integer AS table_count,
        NULL::text AS csv_line,
        NULL::bigint AS seq_scan,
        NULL::bigint AS idx_scan,
        NULL::bigint AS n_tup_ins,
        NULL::bigint AS n_tup_upd,
        NULL::bigint AS n_tup_del
    FROM
        robo_db_table_sizes
    GROUP BY
        dbname
    LIMIT 10
),

-- 4. Неиспользуемые таблицы (без сканов и изменений)
unused_tables AS (
    SELECT
        4 AS section_order,
        '=== Unused / Dead Tables (0 scans and changes) ===' AS section,
        NULL::text AS dbname,
        NULL::text AS schema_name,
        NULL::text AS table_name,
        NULL::numeric AS size_gb,
        NULL::numeric AS max_gb,
        NULL::numeric AS min_gb,
        NULL::numeric AS diff_gb,
        NULL::text AS relative_diff,
        NULL::integer AS table_count,
        NULL::text AS csv_line,
        NULL::bigint AS seq_scan,
        NULL::bigint AS idx_scan,
        NULL::bigint AS n_tup_ins,
        NULL::bigint AS n_tup_upd,
        NULL::bigint AS n_tup_del
    UNION ALL
    SELECT
        4 AS section_order,
        NULL::text AS section,
        dbname,
        schema_name,
        table_name,
        ROUND(table_size_bytes / 1073741824.0, 3) AS size_gb,
        NULL::numeric AS max_gb,
        NULL::numeric AS min_gb,
        NULL::numeric AS diff_gb,
        NULL::text AS relative_diff,
        NULL::integer AS table_count,
        NULL::text AS csv_line,
        seq_scan,
        idx_scan,
        n_tup_ins,
        n_tup_upd,
        n_tup_del
    FROM
        robo_db_table_sizes
    WHERE
        seq_scan = 0 AND idx_scan = 0 AND
        n_tup_ins = 0 AND n_tup_upd = 0 AND n_tup_del = 0
),

-- 5. Размеры схем по БД
schema_usage AS (
    SELECT
        5 AS section_order,
        '=== Schema Usage Summary ===' AS section,
        NULL::text AS dbname,
        NULL::text AS schema_name,
        NULL::text AS table_name,
        NULL::numeric AS size_gb,
        NULL::numeric AS max_gb,
        NULL::numeric AS min_gb,
        NULL::numeric AS diff_gb,
        NULL::text AS relative_diff,
        NULL::integer AS table_count,
        NULL::text AS csv_line,
        NULL::bigint AS seq_scan,
        NULL::bigint AS idx_scan,
        NULL::bigint AS n_tup_ins,
        NULL::bigint AS n_tup_upd,
        NULL::bigint AS n_tup_del
    UNION ALL
    SELECT
        5 AS section_order,
        NULL::text AS section,
        dbname,
        schema_name,
        NULL::text AS table_name,
        ROUND(SUM(table_size_bytes)/1073741824.0, 3) AS size_gb,
        NULL::numeric AS max_gb,
        NULL::numeric AS min_gb,
        NULL::numeric AS diff_gb,
        NULL::text AS relative_diff,
        COUNT(*)::integer AS table_count,
        NULL::text AS csv_line,
        NULL::bigint AS seq_scan,
        NULL::bigint AS idx_scan,
        NULL::bigint AS n_tup_ins,
        NULL::bigint AS n_tup_upd,
        NULL::bigint AS n_tup_del
    FROM
        robo_db_table_sizes
    GROUP BY
        dbname, schema_name
)

-- Объединяем все результаты
SELECT
    section_order,
    section,
    dbname,
    schema_name,
    table_name,
    size_gb,
    max_gb,
    min_gb,
    diff_gb,
    relative_diff,
    table_count,
    csv_line,
    seq_scan,
    idx_scan,
    n_tup_ins,
    n_tup_upd,
    n_tup_del
FROM (
    SELECT * FROM csv_summary
    UNION ALL
    SELECT * FROM top_tables
    UNION ALL
    SELECT * FROM size_spread
    UNION ALL
    SELECT * FROM unused_tables
    UNION ALL
    SELECT * FROM schema_usage
) AS combined_results
ORDER BY
    section_order,
    CASE
        WHEN section IS NOT NULL THEN 0
        ELSE 1
    END;