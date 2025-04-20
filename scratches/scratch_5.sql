/*
Ключевые оптимизации:
Использование массивов вместо строк для операций с символами - массивы работают быстрее при случайном доступе
Алгоритм Фишера-Йетса для перемешивания - более эффективен чем посимвольное удаление
Упрощенные условия с использованием BETWEEN для проверки ASCII-кодов
Оптимизированные строковые операции с использованием substring() вместо substr()
Счетчик элементов вместо постоянного пересчета длины массива
Этот вариант должен работать значительно быстрее оригинального, особенно для больших JSON-строк.
Почему этот вариант быстрый и точный:
Массивы вместо строк – O(1) доступ к элементам, быстрое перемешивание.
Алгоритм Фишера-Йетса – оптимальный способ перемешивания за O(n).
Минимум аллокаций – работаем с уже выделенными массивами, а не пересоздаём строки.
Чистый PL/pgSQL – процедурный подход здесь быстрее, чем попытки эмулировать логику в SQL.
 */

DO $$ --  DEEPSEEK
DECLARE
    rec RECORD;
    original_text TEXT;
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
    FOR rec IN
        SELECT id, msg FROM messages_temp_shadow
        WHERE msg IS NOT NULL AND msg != '' AND msg LIKE '{%' --LIMIT 100000
    LOOP
        original_text := rec.msg;
        ascii_chars := '{}';
        ascii_count := 0;
        inside_quotes := false;
        is_value := false;
        prev_char := '';

        -- 1. Сбор ASCII-символов внутри значений в массив
        FOR pos IN 1..length(original_text) LOOP
            ch := substring(original_text FROM pos FOR 1);

            IF ch = '"' AND (pos = 1 OR substring(original_text FROM pos-1 FOR 1) <> '\') THEN
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

        -- 2. Перемешивание массива (алгоритм Фишера-Йетса)
        IF ascii_count > 0 THEN
            shuffled_chars := ascii_chars;
            FOR i IN REVERSE ascii_count..2 LOOP
                pos := floor(random() * i + 1)::INT;
                -- Обмен значениями
                ch := shuffled_chars[i];
                shuffled_chars[i] := shuffled_chars[pos];
                shuffled_chars[pos] := ch;
            END LOOP;
        END IF;

        -- 3. Вставка обратно
        result_text := '';
        inside_quotes := false;
        is_value := false;
        prev_char := '';
        i := 1;

        FOR pos IN 1..length(original_text) LOOP
            ch := substring(original_text FROM pos FOR 1);

            IF ch = '"' AND (pos = 1 OR substring(original_text FROM pos-1 FOR 1) <> '\') THEN
                inside_quotes := NOT inside_quotes;
                is_value := inside_quotes AND prev_char = ':';
                result_text := result_text || ch;
                prev_char := ch;
                CONTINUE;
            END IF;

            IF inside_quotes AND is_value AND ascii(ch) BETWEEN 32 AND 126 THEN
                result_text := result_text || shuffled_chars[i];
                i := i + 1;
            ELSE
                result_text := result_text || ch;
            END IF;

            prev_char := ch;
        END LOOP;

        -- Только если результат отличается от оригинала
        --IF result_text IS DISTINCT FROM original_text THEN
            INSERT INTO messages_temp_shadow_json (id, msg) VALUES (rec.id, result_text);
        --END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;