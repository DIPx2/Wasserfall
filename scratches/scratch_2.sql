-------------------------------
-- 1. СОЗДАНИЕ ТАБЛИЦЫ СТАТИСТИКИ
-------------------------------
DO $$
BEGIN
    -- Создаем схему если не существует
    CREATE SCHEMA IF NOT EXISTS robo_statistics;

    -- Создаем основную таблицу для хранения статистики
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

    RAISE NOTICE 'Таблица statistics.tables_stats создана/проверена';
EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'Ошибка при создании таблицы: %', SQLERRM;
END $$;

-------------------------------
-- 2. УСТАНОВКА РАСШИРЕНИЯ
-------------------------------
DO $$
BEGIN
    -- Устанавливаем расширение в схеме public
    CREATE EXTENSION IF NOT EXISTS postgres_fdw WITH SCHEMA public;
    RAISE NOTICE 'Расширение postgres_fdw установлено';
EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'Ошибка установки расширения: %', SQLERRM;
END $$;

-------------------------------
-- 3. ОЧИСТКА СТАРЫХ ДАННЫХ
-------------------------------
DO $$
BEGIN
    TRUNCATE TABLE robo_statistics.tables_stats;
    RAISE NOTICE 'Старые данные очищены';
EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'Ошибка при очистке данных: %', SQLERRM;
END $$;

-------------------------------
-- 4. СОЗДАНИЕ FDW СЕРВЕРОВ И ЗАГРУЗКА ДАННЫХ
-------------------------------
DO $$
DECLARE
    rec RECORD;
    fdw_server TEXT;
    foreign_table TEXT;
    db_password TEXT := '%dFgH8!zX4&kLmT2';
BEGIN
    -- Экранирование символов % в пароле
    db_password := replace(db_password, '%', '%%');

    -- Получаем список баз данных
    FOR rec IN
        SELECT datname
        FROM pg_database
        WHERE datistemplate = false
        AND datname NOT IN ('postgres', 'template0', 'template1')
    LOOP
        BEGIN
            -- Создаем FDW сервер с префиксом robo_fdw_
            fdw_server := 'robo_fdw_' || rec.datname;

            EXECUTE format('
                CREATE SERVER IF NOT EXISTS %I
                FOREIGN DATA WRAPPER postgres_fdw
                OPTIONS (
                    host ''prd-bi-01.maxbit.private'',
                    port ''5432'',
                    dbname %L
                )',
                fdw_server,
                rec.datname
            );

            -- Создаем маппинг пользователя
            EXECUTE format('
                CREATE USER MAPPING IF NOT EXISTS
                FOR CURRENT_USER
                SERVER %I
                OPTIONS (
                    user ''robo_sudo'',
                    password %L
                )',
                fdw_server,
                db_password
            );

            -- Создаем foreign table с префиксом robo_ft_
            foreign_table := 'robo_ft_' || rec.datname;

            EXECUTE format('
                CREATE FOREIGN TABLE IF NOT EXISTS robo_statistics.%I (
                    schemaname TEXT,
                    relname TEXT,
                    seq_scan BIGINT,
                    idx_scan BIGINT,
                    n_tup_ins BIGINT,
                    n_tup_upd BIGINT,
                    n_tup_del BIGINT,
                    n_live_tup BIGINT,
                    n_dead_tup BIGINT
                )
                SERVER %I
                OPTIONS (
                    schema_name ''public'',
                    table_name ''pg_stat_user_tables''
                )',
                foreign_table,
                fdw_server
            );

            -- Загружаем данные в основную таблицу
            EXECUTE format('
                INSERT INTO robo_statistics.tables_stats
                SELECT
                    %L as dbname,
                    schemaname,
                    relname,
                    pg_table_size(schemaname||''.''||relname) as table_size_bytes,
                    pg_total_relation_size(schemaname||''.''||relname) as total_size_bytes,
                    pg_indexes_size(schemaname||''.''||relname) as index_size_bytes,
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
                rec.datname,
                foreign_table
            );

            -- Удаляем временную foreign table
            EXECUTE format('DROP FOREIGN TABLE IF EXISTS robo_statistics.%I', foreign_table);

        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Ошибка при обработке БД %: %', rec.datname, SQLERRM;
            INSERT INTO robo_statistics.tables_stats
            VALUES (
                rec.datname,
                NULL, NULL, NULL, NULL, NULL,
                NULL, NULL, NULL, NULL, NULL,
                NULL, NULL, now(),
                SQLERRM
            );
        END;
    END LOOP;
END $$;

-------------------------------
-- 5. ОЧИСТКА ВРЕМЕННЫХ ОБЪЕКТОВ
-------------------------------
DO $$
DECLARE
    rec RECORD;
BEGIN
    -- Удаляем все оставшиеся foreign tables
    FOR rec IN
        SELECT foreign_table_name
        FROM information_schema.foreign_tables
        WHERE foreign_table_schema = 'robo_statistics'
        AND foreign_table_name LIKE 'robo_ft_%'
    LOOP
        EXECUTE format('DROP FOREIGN TABLE IF EXISTS robo_statistics.%I', rec.foreign_table_name);
    END LOOP;

    -- Удаляем все созданные серверы (опционально)
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
-- 6. ФИНАЛЬНЫЙ ОТЧЕТ (CSV)
-------------------------------
SELECT
    dbname || ';' ||
    COALESCE(COUNT(*)::text, '0') || ';' ||
    CASE WHEN SUM(table_size_bytes) IS NULL THEN 'N/A'
         ELSE ROUND(SUM(table_size_bytes)/1073741824.0, 3)::text END || ';' ||
    CASE WHEN AVG(table_size_bytes) IS NULL THEN 'N/A'
         ELSE ROUND(AVG(table_size_bytes)/1073741824.0, 3)::text END || ';' ||
    CASE WHEN MAX(table_size_bytes) IS NULL THEN 'N/A'
         ELSE ROUND(MAX(table_size_bytes)/1073741824.0, 3)::text END || ';' ||
    CASE WHEN MIN(table_size_bytes) IS NULL THEN 'N/A'
         ELSE ROUND(MIN(table_size_bytes)/1073741824.0, 3)::text END || ';' ||
    CASE WHEN MAX(table_size_bytes) IS NULL OR MIN(table_size_bytes) IS NULL THEN 'N/A'
         ELSE ROUND((MAX(table_size_bytes) - MIN(table_size_bytes))/1073741824.0, 3)::text END || ';' ||
    CASE WHEN MIN(table_size_bytes) IS NULL OR MIN(table_size_bytes) = 0 THEN 'N/A'
         ELSE ROUND(100.0 * (MAX(table_size_bytes) - MIN(table_size_bytes)) / MIN(table_size_bytes), 2)::text END
FROM robo_statistics.tables_stats
GROUP BY dbname;