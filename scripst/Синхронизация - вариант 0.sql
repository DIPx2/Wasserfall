-- Генерация команд UPDATE и INSERT
WITH local_data AS (
    SELECT id, status, updated_at
    FROM public.user_online
),
remote_data AS (
    -- Здесь замените на аналог удалённой таблицы, если она доступна локально для сравнения
    SELECT id, status, updated_at
    FROM public.user_online
)
SELECT 
    CASE
        WHEN remote.id IS NOT NULL THEN 
            -- Генерация команды UPDATE для существующих записей
            'UPDATE user_online ' ||
            'SET status = ' || local.status || 
            ', updated_at = ''' || COALESCE(local.updated_at::TEXT, 'NULL') || ''' ' ||
            'WHERE id = ''' || local.id || ''';'
        WHEN remote.id IS NULL THEN 
            -- Генерация команды INSERT для новых записей
            'INSERT INTO user_online (id, status, updated_at) ' ||
            'VALUES (''' || local.id || ''', ' || local.status || 
            ', ''' || COALESCE(local.updated_at::TEXT, 'NULL') || ''');'
    END AS sync_command
FROM local_data local
LEFT JOIN remote_data remote
ON local.id = remote.id;
--------------------------------------------------------------

DO $$
DECLARE
    rec RECORD;
    update_command TEXT;
    insert_command TEXT;
    exists_in_remote BOOLEAN;
BEGIN
    -- Перебор записей в локальной таблице
    FOR rec IN
        SELECT id, status, updated_at
        FROM public.user_online
    LOOP
        -- Проверка, существует ли запись с таким же id в удалённой таблице
        SELECT EXISTS (
            SELECT 1
            FROM public.user_online AS remote -- Замените на локальную аналогичную таблицу, если она используется для проверки
            WHERE remote.id = rec.id
        )
        INTO exists_in_remote;

        IF exists_in_remote THEN
            -- Генерация команды UPDATE для обновления всех полей
            update_command := 
                'UPDATE user_online ' ||
                'SET status = ' || rec.status || 
                ', updated_at = ''' || COALESCE(rec.updated_at::TEXT, 'NULL') || ''' ' ||
                'WHERE id = ''' || rec.id || ''';';
            RAISE NOTICE '%', update_command; -- Вывод команды UPDATE
        ELSE
            -- Генерация команды INSERT для вставки новой записи
            insert_command := 
                'INSERT INTO user_online (id, status, updated_at) ' ||
                'VALUES (''' || rec.id || ''', ' || rec.status || 
                ', ''' || COALESCE(rec.updated_at::TEXT, 'NULL') || ''');';
            --RAISE NOTICE '%', insert_command; -- Вывод команды INSERT
			SELECT insert_command;
        END IF;
    END LOOP;
END $$;