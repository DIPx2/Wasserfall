/*
SELECT name, setting, short_desc
FROM pg_settings
WHERE name LIKE '%parallel%'
AND name IN ('max_parallel_workers', 'max_parallel_workers_per_gather', 'parallel_leader_participation');

SELECT pid, query_start, state, query
FROM pg_stat_activity
WHERE backend_type = 'parallel worker';

SHOW parallel_setup_cost;  -- Должно быть < 1000 (лучше ~10)
SHOW parallel_tuple_cost;  -- Должно быть < 0.1 (лучше ~0.001)

SELECT proname, proparallel FROM pg_proc
WHERE proname = 'shuffle_json_values';
*/

Исходя из ваших результатов, вот анализ и рекомендации:

Текущая ситуация:
Настройки параллелизма:
max_parallel_workers = 8 (достаточно)
max_parallel_workers_per_gather = 2 (можно увеличить)
parallel_leader_participation = on (норма)

Активные воркеры:
Пустой результат означает, что параллельные воркеры не запускаются

Почему нет параллельного выполнения:
Недостаточно данных:
PostgreSQL не использует параллелизм для маленьких таблиц (< ~1000 строк)
Параметры стоимости:

sql
SHOW parallel_setup_cost;  -- Должно быть < 1000 (лучше ~10)
SHOW parallel_tuple_cost;  -- Должно быть < 0.1 (лучше ~0.001)

Проверка размера таблицы:
sql
SELECT count(*) FROM messages_temp_shadow
WHERE msg IS NOT NULL AND msg != '' AND msg LIKE '{%';

Решения:
1. Принудительное включение параллелизма:
sql
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

2. Проверка через EXPLAIN:
sql
EXPLAIN (ANALYZE, COSTS OFF)
INSERT INTO messages_temp_shadow_json_parallele (id, msg)
SELECT id, shuffle_json_values(msg)
FROM messages_temp_shadow
WHERE msg IS NOT NULL AND msg != '' AND msg LIKE '{%';

3. Альтернативный метод (гарантированный параллелизм):
sql
-- Создаем временную таблицу с параллельным заполнением
CREATE TEMP TABLE tmp_parallel_result AS
SELECT id, shuffle_json_values(msg) as msg
FROM messages_temp_shadow
WHERE msg IS NOT NULL AND msg != '' AND msg LIKE '{%'
ORDER BY id;  -- ORDER BY часто активирует параллелизм

-- Переносим результаты
INSERT INTO messages_temp_shadow_json_parallele
SELECT * FROM tmp_parallel_result;

DROP TABLE tmp_parallel_result;
Дополнительные проверки:
Убедитесь, что функция действительно параллельно-безопасна:

sql
SELECT proname, proparallel FROM pg_proc
WHERE proname = 'shuffle_json_values';
Должно возвращать s (PARALLEL SAFE)

Проверьте статистику таблицы:

sql
ANALYZE messages_temp_shadow;
Если после всех этих действий параллельные воркеры все равно не запускаются, вероятно:

Таблица слишком маленькая

Сервер настроен на минимальное использование параллелизма

Ограничения вашей версии PostgreSQL

--------------------------------------------------------------------------------------------------------------------------

DO $$ -- GPT-4.1
DECLARE
    rec RECORD;
    ascii_chars TEXT;
    shuffled_chars TEXT;
    ch TEXT;
    i INT;
    pos INT;
    inside_quotes BOOLEAN;
    is_value BOOLEAN;
    prev_char TEXT;
    original_text TEXT;
    result_text TEXT;
    arr TEXT[];
    n INT;
    j INT;
    tmp TEXT;
BEGIN
    FOR rec IN
        SELECT id, msg FROM messages_temp_shadow
        WHERE msg IS NOT NULL AND msg != '' AND msg LIKE '{%'
        ORDER BY id
    LOOP
        original_text := rec.msg;
        ascii_chars := '';
        shuffled_chars := '';
        result_text := '';
        inside_quotes := false;
        is_value := false;
        prev_char := '';

        -- 1. Сбор ASCII-символов внутри значений
        FOR pos IN 1..char_length(original_text) LOOP
            ch := substr(original_text, pos, 1);

            IF ch = '"' THEN
                IF pos = 1 OR substr(original_text, pos - 1, 1) <> '\' THEN
                    inside_quotes := NOT inside_quotes;
                    is_value := inside_quotes AND prev_char = ':';
                END IF;
                prev_char := ch;
                CONTINUE;
            END IF;

            IF inside_quotes AND is_value AND ascii(ch) >= 32 AND ascii(ch) <= 126 THEN
                ascii_chars := ascii_chars || ch;
            END IF;

            prev_char := ch;
        END LOOP;

        -- 2. Перемешивание с помощью массива
        IF length(ascii_chars) > 1 THEN
            arr := regexp_split_to_array(ascii_chars, '');
            n := array_length(arr, 1);
            -- Фишер-Йетс перемешивание
            FOR i IN REVERSE n..2 LOOP
                j := floor(random() * i + 1)::int;
                tmp := arr[i];
                arr[i] := arr[j];
                arr[j] := tmp;
            END LOOP;
            shuffled_chars := array_to_string(arr, '');
        ELSE
            shuffled_chars := ascii_chars;
        END IF;

        -- 3. Вставка обратно
        i := 1;
        inside_quotes := false;
        is_value := false;
        prev_char := '';
        FOR pos IN 1..char_length(original_text) LOOP
            ch := substr(original_text, pos, 1);

            IF ch = '"' THEN
                IF pos = 1 OR substr(original_text, pos - 1, 1) <> '\' THEN
                    inside_quotes := NOT inside_quotes;
                    is_value := inside_quotes AND prev_char = ':';
                END IF;
                result_text := result_text || ch;
                prev_char := ch;
                CONTINUE;
            END IF;

            IF inside_quotes AND is_value AND ascii(ch) >= 32 AND ascii(ch) <= 126 THEN
                result_text := result_text || substr(shuffled_chars, i, 1);
                i := i + 1;
            ELSE
                result_text := result_text || ch;
            END IF;

            prev_char := ch;
        END LOOP;

        -- Только если результат отличается от оригинала
        IF result_text IS DISTINCT FROM original_text THEN
            INSERT INTO messages_temp_shadow_json (id, msg) VALUES (rec.id, result_text);
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;