--==========================
-- USE AutomationServer.RoboHub.Robo_statistics
--==========================

/*
	-- Use the public schema on a remote server
	SELECT '''dbname=' || datname || ' user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-chat-pg-02.maxbit.private port=5434 options=-csearch_path=''' || ','
	FROM pg_database WHERE datname NOT IN ('postgres', 'template1', 'template0');
*/
/*
DO $$ ------------ Наполнение тыблицы

DECLARE
    conn_str text;
    tbl record;
    db record;
    db_list text[] = ARRAY[

'dbname=mbss_master user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-chat-pg-02.maxbit.private port=5434 options=-csearch_path=',
'dbname=flagman_mbss_master user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-chat-pg-02.maxbit.private port=5434 options=-csearch_path=',
'dbname=rox_mbss_master user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-chat-pg-02.maxbit.private port=5434 options=-csearch_path=',
'dbname=volna_mbss_master user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-chat-pg-02.maxbit.private port=5434 options=-csearch_path=',
'dbname=callback_media_master user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-chat-pg-02.maxbit.private port=5434 options=-csearch_path=',
'dbname=legzo_mbss_master user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-chat-pg-02.maxbit.private port=5434 options=-csearch_path=',
'dbname=jet_mbss_master user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-chat-pg-02.maxbit.private port=5434 options=-csearch_path=',
'dbname=fresh_mbss_master user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-chat-pg-02.maxbit.private port=5434 options=-csearch_path=',
'dbname=sol_mbss_master user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-chat-pg-02.maxbit.private port=5434 options=-csearch_path=',
'dbname=izzi_mbss_master user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-chat-pg-02.maxbit.private port=5434 options=-csearch_path=',
'dbname=starda_mbss_master user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-chat-pg-02.maxbit.private port=5434 options=-csearch_path=',
'dbname=drip_mbss_master user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-chat-pg-02.maxbit.private port=5434 options=-csearch_path=',
'dbname=1go_mbss_master user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-chat-pg-02.maxbit.private port=5434 options=-csearch_path=',
'dbname=lex_mbss_master user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-chat-pg-02.maxbit.private port=5434 options=-csearch_path=',
'dbname=irwin_mbss_master user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-chat-pg-02.maxbit.private port=5434 options=-csearch_path=',
'dbname=monro_mbss_master user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-chat-pg-02.maxbit.private port=5434 options=-csearch_path=',
'dbname=gizbo_mbss_master user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-chat-pg-02.maxbit.private port=5434 options=-csearch_path='

/*

    'dbname=messenger_volna user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-msg-pg-03.maxbit.private port=5432 options=-csearch_path=',
    'dbname=messenger_drip user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-msg-pg-03.maxbit.private port=5432 options=-csearch_path=',
    'dbname=messenger_fresh user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-msg-pg-03.maxbit.private port=5432 options=-csearch_path=',
    'dbname=messenger_jet user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-msg-pg-03.maxbit.private port=5432 options=-csearch_path=',
    'dbname=messenger_izzi user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-msg-pg-03.maxbit.private port=5432 options=-csearch_path=',
    'dbname=messenger_legzo user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-msg-pg-03.maxbit.private port=5432 options=-csearch_path=',
    'dbname=messenger_starda user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-msg-pg-03.maxbit.private port=5432 options=-csearch_path=',
    'dbname=messenger_sol user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-msg-pg-03.maxbit.private port=5432 options=-csearch_path=',
    'dbname=messenger_admin user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-msg-pg-03.maxbit.private port=5432 options=-csearch_path=',
    'dbname=messenger_monro user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-msg-pg-03.maxbit.private port=5432 options=-csearch_path=',
    'dbname=messenger_1go user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-msg-pg-03.maxbit.private port=5432 options=-csearch_path=',
    'dbname=messenger_gizbo user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-msg-pg-03.maxbit.private port=5432 options=-csearch_path=',
    'dbname=messenger_lex user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-msg-pg-03.maxbit.private port=5432 options=-csearch_path=',
    'dbname=messenger_irwin user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-msg-pg-03.maxbit.private port=5432 options=-csearch_path=',
    'dbname=messenger_flagman user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-msg-pg-03.maxbit.private port=5432 options=-csearch_path=',
    'dbname=messenger_martin user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-msg-pg-03.maxbit.private port=5432 options=-csearch_path='
*/
    ];
BEGIN



    CREATE TABLE IF NOT EXISTS robo_db_table_sizes (
        dbname text,
        table_name text,
        table_size_bytes bigint,
        db_size_bytes bigint
    );

    TRUNCATE TABLE robo_db_table_sizes;

    FOREACH conn_str IN ARRAY db_list
    LOOP

        SELECT * INTO db FROM public.dblink(conn_str, 'SELECT current_database(), pg_database_size(current_database())')
            AS t(dbname text, db_size_bytes bigint);

        FOR tbl IN SELECT * FROM public.dblink(conn_str,
                $sql$
                    SELECT relname, pg_total_relation_size(format('%I.%I', schemaname, relname)) AS size_bytes
                    FROM pg_stat_user_tables
                $sql$)
                AS t(relname text, size_bytes bigint)
        LOOP
            INSERT INTO robo_db_table_sizes
            VALUES (db.dbname, tbl.relname, tbl.size_bytes, db.db_size_bytes);
        END LOOP;
    END LOOP;

   -- FOR tbl IN SELECT * FROM robo_db_table_sizes
   -- LOOP
   --     RAISE INFO '%;%;%;%', tbl.dbname, tbl.db_size_bytes, tbl.table_name, tbl.table_size_bytes;
   -- END LOOP;
END;
$$
 */
-----------------------------------------------------------------------------
/*
/*
Строки вида:
"audiences;messenger_drip;0.00559;messenger_irwin;0.00064;0.00495;0.00246;772.62;127.53;73.93"
"workflows;messenger_sol;0.00554;messenger_irwin;0.00172;0.00381;0.00303;221.24;82.52;43.18"
"trigger_profile_audiences;messenger_1go;0.00123;messenger_monro;0.00001;0.00122;0.00011;16000.00;1069.27;92.74"
"trigger_profile_audiences;messenger_1go;0.00123;messenger_gizbo;0.00001;0.00122;0.00011;16000.00;1069.27;92.74"
"trigger_profile_audiences;messenger_1go;0.00123;messenger_jet;0.00001;0.00122;0.00011;16000.00;1069.27;92.74"
*/

SELECT 'Название таблицы;БД с наибольшей таблицей;Размер наибольшей таблицы (ГБ);БД с наименьшей таблицей;Размер наименьшей таблицы (ГБ);Разница в размерах таблиц (ГБ);Средний размер таблицы (ГБ);Разница в процентах между max и min;На сколько % max больше среднего;На сколько % min меньше среднего' AS csv_line

UNION ALL

SELECT
    table_name || ';' ||
    max_db || ';' ||
    ROUND(max_size_gb, 5)::text || ';' ||
    min_db || ';' ||
    ROUND(min_size_gb, 5)::text || ';' ||
    ROUND(size_diff_gb, 5)::text || ';' ||
    ROUND(avg_size_gb, 5)::text || ';' ||
    ROUND(pct_diff_max_min, 2)::text || ';' ||
    ROUND(pct_above_avg, 2)::text || ';' ||
    ROUND(pct_below_avg, 2)::text
FROM (
    WITH table_stats AS (
        SELECT
            table_name,
            dbname,
            table_size_bytes,
            table_size_bytes / 1073741824.0 AS size_gb
        FROM robo_db_table_sizes
        WHERE dbname NOT IN ('messenger_martin', 'messenger_flagman', '')
    ),
    aggregated AS (
        SELECT
            table_name,
            MAX(size_gb) AS max_size_gb,
            MIN(size_gb) AS min_size_gb,
            AVG(size_gb) AS avg_size_gb,
            MAX(size_gb) - MIN(size_gb) AS size_diff_gb,
            (MAX(size_gb) - MIN(size_gb)) * 100.0 / NULLIF(MIN(size_gb), 0) AS pct_diff_max_min,
            (MAX(size_gb) - AVG(size_gb)) * 100.0 / NULLIF(AVG(size_gb), 0) AS pct_above_avg,
            (AVG(size_gb) - MIN(size_gb)) * 100.0 / NULLIF(AVG(size_gb), 0) AS pct_below_avg
        FROM table_stats
        GROUP BY table_name
        HAVING COUNT(DISTINCT dbname) > 1
    ),
    max_dbs AS (
        SELECT
            t.table_name,
            t.dbname AS max_db
        FROM table_stats t
        JOIN (
            SELECT table_name, MAX(size_gb) AS max_size_gb
            FROM table_stats
            GROUP BY table_name
        ) m ON t.table_name = m.table_name AND t.size_gb = m.max_size_gb
    ),
    min_dbs AS (
        SELECT
            t.table_name,
            t.dbname AS min_db
        FROM table_stats t
        JOIN (
            SELECT table_name, MIN(size_gb) AS min_size_gb
            FROM table_stats
            GROUP BY table_name
        ) m ON t.table_name = m.table_name AND t.size_gb = m.min_size_gb
    )
    SELECT
        a.table_name,
        md.max_db,
        a.max_size_gb,
        mid.min_db,
        a.min_size_gb,
        a.size_diff_gb,
        a.avg_size_gb,
        a.pct_diff_max_min,
        a.pct_above_avg,
        a.pct_below_avg
    FROM aggregated a
    LEFT JOIN max_dbs md ON a.table_name = md.table_name
    LEFT JOIN min_dbs mid ON a.table_name = mid.table_name
    WHERE a.size_diff_gb > 0
    ORDER BY a.size_diff_gb DESC
) AS result;
 */
-----------------------------------------------------------------------------
 /*

/*
"Название БД;Количество таблиц;Общий размер таблиц (ГБ);Средний размер таблицы (ГБ);Максимальный размер таблицы (ГБ);Минимальный размер таблицы (ГБ);Разница между max и min (ГБ);% разницы между max и min"
"messenger_izzi;30;41.652;1.388;34.641;0.000;34.641;454042100.00"
"messenger_fresh;30;36.793;1.226;25.778;0.000;25.778;337877000.00"
 */
SELECT 'Название БД;Количество таблиц;Общий размер таблиц (ГБ);Средний размер таблицы (ГБ);Максимальный размер таблицы (ГБ);Минимальный размер таблицы (ГБ);Разница между max и min (ГБ);% разницы между max и min' AS csv_line

UNION ALL

SELECT
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
    END
FROM
    robo_db_table_sizes
WHERE
    dbname NOT IN ('messenger_martin', 'messenger_flagman', '')
GROUP BY
    dbname;
*/
-----------------------------------------------------------------------------
/*
WITH ------------------- сводная ведомость
-- Основной CTE для получения данных о размерах таблиц
table_stats AS (
    SELECT
        REPLACE(table_name, '"', '') AS table_name,
        REPLACE(COALESCE(largest_db, ''), '"', '') AS largest_db,
        CAST(largest_size_gb AS numeric(10,5)) AS max_size_gb,
        REPLACE(COALESCE(smallest_db, ''), '"', '') AS smallest_db,
        CAST(smallest_size_gb AS numeric(10,5)) AS min_size_gb,
        CAST(raw_size_difference_gb AS numeric(10,5)) AS size_diff_gb,
        CAST(raw_avg_size_gb AS numeric(10,5)) AS avg_size_gb,
        ROUND((max_size - min_size) * 100.0 / NULLIF(min_size, 0), 2) AS pct_diff_max_min,
        ROUND((max_size - avg_size) * 100.0 / NULLIF(avg_size, 0), 2) AS pct_above_avg,
        ROUND((avg_size - min_size) * 100.0 / NULLIF(avg_size, 0), 2) AS pct_below_avg
    FROM (
        WITH ranked_sizes AS (
            SELECT
                table_name,
                dbname,
                table_size_bytes,
                table_size_bytes / 1073741824.0 AS size_gb,
                rank() OVER (PARTITION BY table_name ORDER BY table_size_bytes DESC) AS size_rank,
                max(table_size_bytes) OVER (PARTITION BY table_name) AS max_size,
                min(table_size_bytes) OVER (PARTITION BY table_name) AS min_size,
                avg(table_size_bytes) OVER (PARTITION BY table_name) AS avg_size,
                count(*) OVER (PARTITION BY table_name) AS table_count
            FROM
                robo_db_table_sizes
            WHERE dbname NOT IN ('messenger_martin', 'messenger_flagman', '')
        ),
        size_comparison AS (
            SELECT
                table_name,
                max(CASE WHEN size_rank = 1 THEN dbname END) AS largest_db,
                max(CASE WHEN size_rank = 1 THEN size_gb END) AS largest_size_gb,
                max(CASE WHEN size_rank = table_count THEN dbname END) AS smallest_db,
                max(CASE WHEN size_rank = table_count THEN size_gb END) AS smallest_size_gb,
                max_size,
                min_size,
                avg_size,
                (max_size - min_size) / 1073741824.0 AS raw_size_difference_gb,
                avg_size / 1073741824.0 AS raw_avg_size_gb
            FROM
                ranked_sizes
            GROUP BY
                table_name, max_size, min_size, avg_size, table_count
        )
        SELECT * FROM size_comparison
        WHERE smallest_db IS NOT NULL AND raw_size_difference_gb > 0
    ) AS final_data
),

-- Агрегированная статистика
overall_stats AS (
    SELECT
        COUNT(*) AS total_tables,
        MIN(min_size_gb) AS absolute_min_size,
        MAX(max_size_gb) AS absolute_max_size,
        AVG(avg_size_gb) AS mean_size,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY avg_size_gb) AS median_size,
        STDDEV(avg_size_gb) AS size_stddev,
        SUM(max_size_gb) AS total_max_size,
        SUM(min_size_gb) AS total_min_size,
        SUM(avg_size_gb) AS total_avg_size
    FROM table_stats
),

-- Распределение по размерам
size_distribution AS (
    SELECT
        CASE
            WHEN max_size_gb < 0.01 THEN '0-0.01 ГБ'
            WHEN max_size_gb < 0.1 THEN '0.01-0.1 ГБ'
            WHEN max_size_gb < 1 THEN '0.1-1 ГБ'
            WHEN max_size_gb < 10 THEN '1-10 ГБ'
            ELSE '>10 ГБ'
        END AS size_group,
        COUNT(*) AS tables_count,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM table_stats), 2) AS pct_of_total
    FROM table_stats
    GROUP BY size_group
),

-- Аномалии по размерам
size_anomalies AS (
    SELECT
        table_name,
        largest_db,
        max_size_gb,
        avg_size_gb,
        max_size_gb - avg_size_gb AS deviation_from_avg,
        ROUND((max_size_gb - avg_size_gb) * 100.0 / NULLIF(avg_size_gb, 0), 2) AS pct_deviation
    FROM table_stats
    WHERE max_size_gb > (SELECT AVG(avg_size_gb) + 3 * STDDEV(avg_size_gb) FROM table_stats)
    ORDER BY deviation_from_avg DESC
)

-- Форматированный отчет
SELECT * FROM (
    -- Общая статистика
    SELECT 1 AS sort_order, '=== ОБЩАЯ СТАТИСТИКА РАЗМЕРОВ ТАБЛИЦ ===' AS report_section, NULL AS metric, NULL AS value, NULL AS comment
    UNION ALL
    SELECT 2, 'Всего таблиц', total_tables::text, NULL, 'Общее количество анализируемых таблиц' FROM overall_stats
    UNION ALL
    SELECT 3, 'Минимальный размер', absolute_min_size::text, 'ГБ', 'Размер самой маленькой таблицы' FROM overall_stats
    UNION ALL
    SELECT 4, 'Максимальный размер', absolute_max_size::text, 'ГБ', 'Размер самой большой таблицы' FROM overall_stats
    UNION ALL
    SELECT 5, 'Средний размер', ROUND(mean_size::numeric, 5)::text, 'ГБ', 'Среднее арифметическое размеров' FROM overall_stats
    UNION ALL
    SELECT 6, 'Медианный размер', ROUND(median_size::numeric, 5)::text, 'ГБ', 'Медиана распределения' FROM overall_stats
    UNION ALL
    SELECT 7, 'Стандартное отклонение', ROUND(size_stddev::numeric, 5)::text, 'ГБ', 'Разброс размеров' FROM overall_stats

    UNION ALL
    SELECT 8, NULL, NULL, NULL, NULL
    UNION ALL
    SELECT 9, '=== РАСПРЕДЕЛЕНИЕ ТАБЛИЦ ПО РАЗМЕРАМ ===', NULL, NULL, NULL
    UNION ALL
    SELECT 10, size_group, tables_count::text, pct_of_total::text || '%', 'Доля таблиц в диапазоне'
    FROM size_distribution

    UNION ALL
    SELECT 11, NULL, NULL, NULL, NULL
    UNION ALL
    SELECT 12, '=== ТАБЛИЦЫ С АНОМАЛЬНЫМИ РАЗМЕРАМИ ===', NULL, NULL, 'Размер сильно превышает средний'
    UNION ALL
    SELECT 13, 'Имя таблицы', 'БД', 'Отклонение от среднего', 'Процент отклонения'
    UNION ALL
    SELECT 14, table_name, largest_db, ROUND(deviation_from_avg::numeric, 3)::text || ' ГБ', pct_deviation::text || '%'
    FROM size_anomalies

    UNION ALL
    SELECT 15, NULL, NULL, NULL, NULL
    UNION ALL
    SELECT 16, '=== ТОП-10 САМЫХ БОЛЬШИХ РАЗЛИЧИЙ МЕЖДУ КОПИЯМИ ТАБЛИЦ ===', NULL, NULL, NULL
    UNION ALL
    SELECT 17, 'Имя таблицы', 'Макс.размер (ГБ)', 'Мин.размер (ГБ)', 'Разница (%)'
    UNION ALL
    SELECT 18, table_name, max_size_gb::text, min_size_gb::text, pct_diff_max_min::text || '%'
    FROM table_stats
) AS report
ORDER BY sort_order;
 */


