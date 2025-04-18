DO $$
DECLARE
    conn_str text;
    tbl record;
    db record;
    db_list text[] := ARRAY[
'dbname=messenger_volna user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-msg-pg-03.maxbit.private port=5432 options=-csearch_path=',
'dbname=messenger_starda user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-msg-pg-03.maxbit.private port=5432 options=-csearch_path=',
'dbname=messenger_sol user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-msg-pg-03.maxbit.private port=5432 options=-csearch_path=',
'dbname=messenger_rox user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-msg-pg-03.maxbit.private port=5432 options=-csearch_path=',
'dbname=messenger_monro user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-msg-pg-03.maxbit.private port=5432 options=-csearch_path=',
'dbname=messenger_martin user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-msg-pg-03.maxbit.private port=5432 options=-csearch_path=',
'dbname=messenger_lex user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-msg-pg-03.maxbit.private port=5432 options=-csearch_path=',
'dbname=messenger_legzo user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-msg-pg-03.maxbit.private port=5432 options=-csearch_path=',
'dbname=messenger_jet user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-msg-pg-03.maxbit.private port=5432 options=-csearch_path=',
'dbname=messenger_izzi user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-msg-pg-03.maxbit.private port=5432 options=-csearch_path=',
'dbname=messenger_irwin user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-msg-pg-03.maxbit.private port=5432 options=-csearch_path=',
'dbname=messenger_gizbo user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-msg-pg-03.maxbit.private port=5432 options=-csearch_path=',
'dbname=messenger_fresh user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-msg-pg-03.maxbit.private port=5432 options=-csearch_path=',
'dbname=messenger_flagman user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-msg-pg-03.maxbit.private port=5432 options=-csearch_path=',
'dbname=messenger_drip user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-msg-pg-03.maxbit.private port=5432 options=-csearch_path=',
'dbname=messenger_admin user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-msg-pg-03.maxbit.private port=5432 options=-csearch_path=',
'dbname=messenger_1go user=robo_sudo password=%dFgH8!zX4&kLmT2 host=prd-msg-pg-03.maxbit.private port=5432 options=-csearch_path='
    ];
BEGIN
    -- Создаём временную таблицу без схемы
    CREATE TEMP TABLE temp_db_table_sizes (
        dbname text,
        table_name text,
        table_size_bytes bigint,
        db_size_bytes bigint
    ) ON COMMIT DROP;

    -- Обходим каждую базу
    FOREACH conn_str IN ARRAY db_list
    LOOP
        -- Получаем имя базы и её размер
        SELECT *
        INTO db
        FROM dblink(conn_str,
                    'SELECT current_database(), pg_database_size(current_database())')
            AS t(dbname text, db_size_bytes bigint);

        -- Получаем размеры всех пользовательских таблиц (без схемы)
        FOR tbl IN
            SELECT *
            FROM dblink(conn_str,
                $sql$
                    SELECT
                        relname,
                        pg_total_relation_size(format('%I.%I', schemaname, relname)) AS size_bytes
                    FROM pg_stat_user_tables
                $sql$)
                AS t(relname text, size_bytes bigint)
        LOOP
            INSERT INTO temp_db_table_sizes
            VALUES (db.dbname, tbl.relname, tbl.size_bytes, db.db_size_bytes);
        END LOOP;
    END LOOP;

    -- Выводим содержимое временной таблицы
    RAISE NOTICE 'Результаты:';
    FOR tbl IN SELECT * FROM temp_db_table_sizes
    LOOP
        RAISE NOTICE '%;%;%;%',
            tbl.dbname, tbl.db_size_bytes, tbl.table_name, tbl.table_size_bytes;
    END LOOP;
END$$;
