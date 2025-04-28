-------------------------------
-- 1. Создание схемы и таблицы для хранения статистики
-------------------------------
DO $$
BEGIN
    CREATE SCHEMA IF NOT EXISTS robo_statistics;
    CREATE TABLE IF NOT EXISTS robo_statistics.tables_stats (
        dbname TEXT,
        schemaname TEXT,
        relname TEXT,
        table_size_bytes BIGINT,
        total_size_bytes BIGINT,
        index_size_bytes BIGINT,
        seq_scan BIGINT,
        idx_scan BIGINT,
        n_tup_ins BIGINT,
        n_tup_upd BIGINT,
        n_tup_del BIGINT,
        n_live_tup BIGINT,
        n_dead_tup BIGINT,
        collected_at TIMESTAMP DEFAULT now(),
        error_message TEXT
    );
    RAISE NOTICE 'Таблица robo_statistics.tables_stats создана/проверена';
EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'Ошибка при создании таблицы: %', SQLERRM;
END $$;


-------------------------------
-- 2. Установка расширения postgres_fdw
-------------------------------
DO $$
BEGIN
    CREATE EXTENSION IF NOT EXISTS postgres_fdw WITH SCHEMA public;
    RAISE NOTICE 'Расширение postgres_fdw установлено';
EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'Ошибка установки расширения: %', SQLERRM;
END $$;


-------------------------------
-- 3. Очистка ранее собранных данных из таблицы статистики
-------------------------------
DO $$
BEGIN
    TRUNCATE TABLE robo_statistics.tables_stats;
    RAISE NOTICE 'Старые данные очищены';
EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'Ошибка при очистке данных: %', SQLERRM;
END $$;


-------------------------------
-- 4.1. Динамическое создание FDW-серверов и пользовательских маппингов
-------------------------------
DO $$
DECLARE
    rec RECORD;
    fdw_server TEXT;
    db_password TEXT := '%dFgH8!zX4&kLmT2';
    db_host TEXT := 'prd-bi-01.maxbit.private';
    connection_success BOOLEAN;
BEGIN
    db_password := replace(db_password, '%', '%%');

    -- Удаление старых серверов и маппингов перед созданием новых
    FOR rec IN
        SELECT srvname FROM pg_foreign_server WHERE srvname LIKE 'robo_fdw_%'
    LOOP
        BEGIN
            EXECUTE format('DROP SERVER IF EXISTS %I CASCADE', rec.srvname);
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Ошибка при удалении сервера %: %', rec.srvname, SQLERRM;
        END;
    END LOOP;

    -- Создание новых серверов и маппингов
    FOR rec IN
        SELECT datname
        FROM pg_database
        WHERE datistemplate = false
        AND datname NOT IN ('postgres', 'template0', 'template1')
    LOOP
        BEGIN
            fdw_server := 'robo_fdw_' || rec.datname;

            -- Проверка доступности базы перед созданием сервера (с указанием схемы public)
            BEGIN
                PERFORM public.dblink_connect(format('host=%s port=5432 dbname=%s user=robo_sudo password=%s',
                                           db_host, rec.datname, db_password));
                connection_success := true;
                PERFORM public.dblink_disconnect();
            EXCEPTION WHEN OTHERS THEN
                connection_success := false;
                RAISE WARNING 'Не удалось подключиться к БД %: %', rec.datname, SQLERRM;
            END;

            IF connection_success THEN
                -- Создание сервера и маппинга
                EXECUTE format(
                    'CREATE SERVER %I FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host %L, port %L, dbname %L)',
                    fdw_server, db_host, '5432', rec.datname
                );

                EXECUTE format(
                    'CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER SERVER %I OPTIONS (user %L, password %L)',
                    fdw_server, 'robo_sudo', db_password
                );

                RAISE NOTICE 'Создан сервер и маппинг для БД %', rec.datname;
            ELSE
                RAISE WARNING 'Пропуск создания сервера для недоступной БД %', rec.datname;
                INSERT INTO robo_statistics.tables_stats
                VALUES (
                    rec.datname, NULL, NULL, NULL, NULL, NULL,
                    NULL, NULL, NULL, NULL, NULL,
                    NULL, NULL, now(), 'Не удалось подключиться к БД'
                );
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE WARNING 'Ошибка при создании сервера/маппинга для БД %: %', rec.datname, SQLERRM;
                INSERT INTO robo_statistics.tables_stats
                VALUES (
                    rec.datname, NULL, NULL, NULL, NULL, NULL,
                    NULL, NULL, NULL, NULL, NULL,
                    NULL, NULL, now(), SQLERRM
                );
        END;
    END LOOP;
END $$;

-------------------------------
-- 4.2. Загрузка статистики через созданные FDW-серверы
-------------------------------
DO $$
DECLARE
    rec RECORD;
    fdw_server TEXT;
    foreign_table TEXT;
    db_list TEXT[];
BEGIN
    -- Получаем список доступных баз данных из созданных серверов
    SELECT array_agg(replace(srvname, 'robo_fdw_', ''))
    INTO db_list
    FROM pg_foreign_server
    WHERE srvname LIKE 'robo_fdw_%';

    -- Обрабатываем только доступные базы
    FOREACH rec.datname IN ARRAY db_list
    LOOP
        BEGIN
            fdw_server := 'robo_fdw_' || rec.datname;
            foreign_table := 'robo_ft_' || rec.datname;

            -- Создание временной foreign table
            EXECUTE format(
                'CREATE FOREIGN TABLE IF NOT EXISTS robo_statistics.%I (
                    schemaname TEXT,
                    relname TEXT,
                    seq_scan BIGINT,
                    idx_scan BIGINT,
                    n_tup_ins BIGINT,
                    n_tup_upd BIGINT,
                    n_tup_del BIGINT,
                    n_live_tup BIGINT,
                    n_dead_tup BIGINT
                ) SERVER %I OPTIONS (schema_name ''public'', table_name ''pg_stat_user_tables'')',
                foreign_table, fdw_server
            );

            -- Загрузка данных
            EXECUTE format(
                'INSERT INTO robo_statistics.tables_stats
                SELECT
                    %L as dbname,
                    schemaname,
                    relname,
                    pg_table_size(quote_ident(schemaname)||''.''||quote_ident(relname)) as table_size_bytes,
                    pg_total_relation_size(quote_ident(schemaname)||''.''||quote_ident(relname)) as total_size_bytes,
                    pg_indexes_size(quote_ident(schemaname)||''.''||quote_ident(relname)) as index_size_bytes,
                    seq_scan,
                    idx_scan,
                    n_tup_ins,
                    n_tup_upd,
                    n_tup_del,
                    n_live_tup,
                    n_dead_tup,
                    now(),
                    NULL
                FROM robo_statistics.%I',
                rec.datname, foreign_table
            );

            -- Удаление временной таблицы
            EXECUTE format('DROP FOREIGN TABLE IF EXISTS robo_statistics.%I', foreign_table);

            RAISE NOTICE 'Данные загружены для БД %', rec.datname;
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Ошибка при загрузке данных из БД %: %', rec.datname, SQLERRM;
            INSERT INTO robo_statistics.tables_stats
            VALUES (
                rec.datname, NULL, NULL, NULL, NULL, NULL,
                NULL, NULL, NULL, NULL, NULL,
                NULL, NULL, now(), SQLERRM
            );
        END;
    END LOOP;
END $$;

-------------------------------
-- 5. Очистка временных foreign tables и (опционально) FDW-серверов
-------------------------------
DO $$
DECLARE
    rec RECORD;
BEGIN
    -- Foreign tables
    FOR rec IN
        SELECT foreign_table_name
        FROM information_schema.foreign_tables
        WHERE foreign_table_schema = 'robo_statistics'
          AND foreign_table_name LIKE 'robo_ft_%'
    LOOP
        EXECUTE format('DROP FOREIGN TABLE IF EXISTS robo_statistics.%I', rec.foreign_table_name);
    END LOOP;

    -- FDW servers (опционально)
    /*
    FOR rec IN
        SELECT srvname
        FROM pg_foreign_server
        WHERE srvname LIKE 'robo_fdw_%'
    LOOP
        EXECUTE format('DROP SERVER IF EXISTS %I CASCADE', rec.srvname);
    END LOOP;
    */
    RAISE NOTICE 'Временные объекты очищены';
END $$;


-------------------------------
-- 6. CSV-отчет: агрегированная статистика по базам данных
-------------------------------
SELECT 'Название БД;Количество таблиц;Общий размер таблиц (ГБ);Средний размер таблицы (ГБ);Максимальный размер таблицы (ГБ);Минимальный размер таблицы (ГБ);Разница между max и min (ГБ);% разницы между max и min' AS csv_line
UNION ALL
SELECT
    dbname || ';' ||
    COUNT(*)::text || ';' ||
    COALESCE(ROUND(SUM(table_size_bytes)/1073741824.0, 3)::text, '0') || ';' ||
    COALESCE(ROUND(AVG(table_size_bytes)/1073741824.0, 3)::text, '0') || ';' ||
    COALESCE(ROUND(MAX(table_size_bytes)/1073741824.0, 3)::text, '0') || ';' ||
    COALESCE(ROUND(MIN(table_size_bytes)/1073741824.0, 3)::text, '0') || ';' ||
    COALESCE(ROUND((MAX(table_size_bytes)-MIN(table_size_bytes))/1073741824.0, 3)::text, '0') || ';' ||
    (CASE WHEN MIN(table_size_bytes) = 0 THEN 'N/A'
        ELSE ROUND(((MAX(table_size_bytes)-MIN(table_size_bytes))*100.0/NULLIF(MIN(table_size_bytes), 0)), 2)::text END)
FROM robo_statistics.tables_stats
GROUP BY dbname;


-------------------------------
-- 7. CSV-отчет: различия между копиями одних и тех же таблиц
-------------------------------
SELECT 'Название таблицы;БД с наибольшей таблицей;Размер наибольшей таблицы (ГБ);БД с наименьшей таблицей;Размер наименьшей таблицы (ГБ);Разница в размерах таблиц (ГБ);Средний размер таблицы (ГБ);Разница в процентах между max и min;На сколько % max больше среднего;На сколько % min меньше среднего' AS csv_line
UNION ALL
SELECT
    tname || ';' ||
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
            relname AS tname,
            dbname,
            table_size_bytes / 1073741824.0 AS size_gb
        FROM robo_statistics.tables_stats
        WHERE dbname IS NOT NULL AND relname IS NOT NULL
    ),
    aggregated AS (
        SELECT
            tname,
            MAX(size_gb) AS max_size_gb,
            MIN(size_gb) AS min_size_gb,
            AVG(size_gb) AS avg_size_gb,
            MAX(size_gb) - MIN(size_gb) AS size_diff_gb,
            (MAX(size_gb) - MIN(size_gb)) * 100.0 / NULLIF(MIN(size_gb),0) AS pct_diff_max_min,
            (MAX(size_gb) - AVG(size_gb)) * 100.0 / NULLIF(AVG(size_gb),0) AS pct_above_avg,
            (AVG(size_gb) - MIN(size_gb)) * 100.0 / NULLIF(AVG(size_gb),0) AS pct_below_avg
        FROM table_stats
        GROUP BY tname
        HAVING COUNT(DISTINCT dbname) > 1
    ),
    max_dbs AS (
        SELECT tname, dbname AS max_db
        FROM table_stats t
        JOIN (
            SELECT tname, MAX(size_gb) AS max_size_gb FROM table_stats GROUP BY tname
        ) m ON t.tname = m.tname AND t.size_gb = m.max_size_gb
    ),
    min_dbs AS (
        SELECT tname, dbname AS min_db
        FROM table_stats t
        JOIN (
            SELECT tname, MIN(size_gb) AS min_size_gb FROM table_stats GROUP BY tname
        ) m ON t.tname = m.tname AND t.size_gb = m.min_size_gb
    )
    SELECT
        a.tname,
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
    LEFT JOIN max_dbs md ON a.tname = md.tname
    LEFT JOIN min_dbs mid ON a.tname = mid.tname
    WHERE a.size_diff_gb > 0
    ORDER BY a.size_diff_gb DESC
) t;


-------------------------------
-- 8. Сводная ведомость по аномалиям и распределению таблиц
-------------------------------
WITH table_stats AS (
    SELECT
        relname AS table_name,
        dbname,
        table_size_bytes / 1073741824.0 AS size_gb
    FROM robo_statistics.tables_stats
    WHERE dbname IS NOT NULL AND relname IS NOT NULL
),
agg AS (
    SELECT
        table_name,
        MAX(size_gb) AS max_size_gb,
        MIN(size_gb) AS min_size_gb,
        AVG(size_gb) AS avg_size_gb,
        MAX(size_gb) - MIN(size_gb) AS size_diff_gb,
        (MAX(size_gb) - MIN(size_gb)) * 100.0 / NULLIF(MIN(size_gb),0) AS pct_diff_max_min,
        (MAX(size_gb) - AVG(size_gb)) * 100.0 / NULLIF(AVG(size_gb),0) AS pct_above_avg,
        (AVG(size_gb) - MIN(size_gb)) * 100.0 / NULLIF(AVG(size_gb),0) AS pct_below_avg
    FROM table_stats
    GROUP BY table_name
    HAVING COUNT(DISTINCT dbname) > 1
),
max_dbs AS (
    SELECT t.table_name, t.dbname AS max_db
    FROM table_stats t
    JOIN (
        SELECT table_name, MAX(size_gb) AS max_size FROM table_stats GROUP BY table_name
    ) m ON t.table_name = m.table_name AND t.size_gb = m.max_size
),
min_dbs AS (
    SELECT t.table_name, t.dbname AS min_db
    FROM table_stats t
    JOIN (
        SELECT table_name, MIN(size_gb) AS min_size FROM table_stats GROUP BY table_name
    ) m ON t.table_name = m.table_name AND t.size_gb = m.min_size
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
FROM agg a
LEFT JOIN max_dbs md ON a.table_name = md.table_name
LEFT JOIN min_dbs mid ON a.table_name = mid.table_name
WHERE a.size_diff_gb > 0
ORDER BY a.size_diff_gb DESC
LIMIT 100;