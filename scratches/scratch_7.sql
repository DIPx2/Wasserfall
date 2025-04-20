-- 1. Создаем функцию для обработки JSON с исправлением ошибки
CREATE OR REPLACE FUNCTION shuffle_json_values(input_text TEXT)
RETURNS TEXT AS $$
DECLARE
    ascii_chars TEXT[];
    shuffled_chars TEXT[];
    result_text TEXT;
    ch TEXT;
    prev_char TEXT := '';
    i INT;
    pos INT;
    inside_quotes BOOLEAN := false;
    is_value BOOLEAN := false;
    ascii_count INT := 0;
BEGIN
    -- Быстрый выход для не-JSON данных
    IF input_text IS NULL OR input_text = '' OR input_text NOT LIKE '{%' THEN
        RETURN input_text;
    END IF;

    -- 1. Сбор ASCII-символов в массив (используем substring вместо [])
    FOR pos IN 1..length(input_text) LOOP
        ch := substring(input_text FROM pos FOR 1);

        IF ch = '"' AND (pos = 1 OR substring(input_text FROM pos-1 FOR 1) <> '\') THEN
            inside_quotes := NOT inside_quotes;
            is_value := inside_quotes AND prev_char = ':';
            prev_char := ch;
            CONTINUE;
        END IF;

        IF inside_quotes AND is_value AND ascii(ch) BETWEEN 32 AND 126 THEN
            ascii_count := ascii_count + 1;
            ascii_chars[ascii_count] := ch;
        END IF;

        prev_char := ch;
    END LOOP;

    -- 2. Алгоритм Фишера-Йетса (in-place перемешивание)
    IF ascii_count > 1 THEN  -- Оптимизация: пропускаем при 0 или 1 символе
        FOR i IN REVERSE ascii_count..2 LOOP
            pos := 1 + (random() * (i-1))::INT;
            -- Swap элементов
            ch := ascii_chars[i];
            ascii_chars[i] := ascii_chars[pos];
            ascii_chars[pos] := ch;
        END LOOP;
    END IF;

    -- 3. Вставка обратно (исправленный вариант)
    result_text := '';
    i := 1;
    inside_quotes := false;
    is_value := false;

    FOR pos IN 1..length(input_text) LOOP
        ch := substring(input_text FROM pos FOR 1);

        IF ch = '"' AND (pos = 1 OR substring(input_text FROM pos-1 FOR 1) <> '\') THEN
            inside_quotes := NOT inside_quotes;
            is_value := inside_quotes AND prev_char = ':';
            result_text := result_text || ch;
            prev_char := ch;
            CONTINUE;
        END IF;

        IF inside_quotes AND is_value AND ascii(ch) BETWEEN 32 AND 126 THEN
            result_text := result_text || ascii_chars[i];
            i := i + 1;
        ELSE
            result_text := result_text || ch;
        END IF;
    END LOOP;

    RETURN result_text;
END;
$$ LANGUAGE plpgsql PARALLEL SAFE STRICT IMMUTABLE;

-- 2. Создаем таблицу для результатов
CREATE TABLE IF NOT EXISTS messages_temp_shadow_json_parallele (
    id UUID,
    msg TEXT
);

-- 3. Очищаем целевую таблицу (если нужно)
TRUNCATE messages_temp_shadow_json_parallele;


BEGIN;
-- Увеличиваем количество воркеров
SET LOCAL max_parallel_workers_per_gather = 4;
-- Уменьшаем стоимость параллелизации
SET LOCAL parallel_setup_cost = 1;
SET LOCAL parallel_tuple_cost = 0.001;

-- Запрос с явным указанием параллельного сканирования
INSERT INTO messages_temp_shadow_json_parallele (id, msg)
SELECT id, shuffle_json_values(msg)
FROM messages_temp_shadow
WHERE msg IS NOT NULL AND msg != '' AND msg LIKE '{%'
-- Подсказка оптимизатору (необязательно)
OFFSET 0;  -- Обходной трюк для активации параллелизма

COMMIT;






/*
BEGIN;
-- Современный аналог force_parallel_mode:
SET debug_parallel_query = on;  -- Только для PostgreSQL 16+
SET local max_parallel_workers_per_gather = 4;

-- Ваш запрос
INSERT INTO messages_temp_shadow_json_parallele (id, msg)
SELECT id, shuffle_json_values(msg)
FROM messages_temp_shadow
WHERE msg IS NOT NULL AND msg != '' AND msg LIKE '{%';

COMMIT;





BEGIN;

SET LOCAL force_parallel_mode = on;
SET LOCAL max_parallel_workers_per_gather = 6;
SET LOCAL maintenance_work_mem = '256MB';

INSERT INTO messages_temp_shadow_json_parallele (id, msg)
SELECT id, shuffle_json_values(msg)
FROM messages_temp_shadow
WHERE msg IS NOT NULL AND msg != '' AND msg LIKE '{%';

COMMIT;

ROLLBACK;

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF)
INSERT INTO messages_temp_shadow_json_parallele (id, msg)
SELECT id, shuffle_json_values(msg)
FROM messages_temp_shadow
WHERE msg IS NOT NULL AND msg != '' AND msg LIKE '{%';

SELECT name, setting, short_desc
FROM pg_settings
WHERE name LIKE '%parallel%'
AND name IN ('max_parallel_workers', 'max_parallel_workers_per_gather', 'parallel_leader_participation');



-- 4. Настраиваем параллельное выполнение
SET max_parallel_workers_per_gather = 8;
SET maintenance_work_mem = '256MB';
-- 5. Основной запрос
INSERT INTO messages_temp_shadow_json_parallele (id, msg)
SELECT id, shuffle_json_values(msg)
FROM messages_temp_shadow
WHERE msg IS NOT NULL AND msg != '' AND msg LIKE '{%';
 */