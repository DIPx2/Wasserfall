----------------------------------------------------------------------------------------------
-- -------------------------------------------------------------------------------------------
-- <ОТДЕЛЬНЫЕ БЛОКИ РЕГУЛЯРКИ>
-- -------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------
DO $$ -- Замена email и URL, только если были изменения
DECLARE
    rec RECORD;
    result_text TEXT;
    match TEXT;
    rand_digits TEXT;
    local_part TEXT;
    domain_part TEXT;
    pos INT;
    i INT;
BEGIN
    FOR rec IN
        SELECT id, msg FROM messages_temp_shadow
        WHERE msg IS NOT NULL AND msg != '' AND msg NOT LIKE '{%' --limit 100
    LOOP
        result_text := rec.msg;

        -- Замена email-адресов
        FOR match IN SELECT unnest(regexp_matches(result_text, '[\\w\\.\\-]+@[\\w\\.\\-]+\\.\\w+', 'g')) LOOP
            pos := position('@' IN match);
            local_part := substr(match, 1, pos - 1);
            domain_part := substr(match, pos);

            rand_digits := '';
            FOR i IN 1..5 LOOP
                rand_digits := rand_digits || substr('abcdefghijklmnopqrstuvwxyz0123456789', floor(random() * 36 + 1)::int, 1);
            END LOOP;

            result_text := replace(result_text, match, rand_digits || domain_part);
        END LOOP;

        -- Замена URL
        result_text := regexp_replace(result_text, E'https?:\\/\\/[^\\s<>"(),]+', 'https://de.xvideos.com/', 'gi');

        -- Вставляем только если что-то изменилось
        IF result_text IS DISTINCT FROM rec.msg THEN
            INSERT INTO messages_temp_shadow_emlurl (id, msg) VALUES (rec.id, result_text);
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

----------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------
DO $$  -- Замена чисел из 3+ цифр на "-0,01", но только если были замены
DECLARE
    rec RECORD;
    result_text TEXT;
BEGIN
    FOR rec IN
        SELECT id, msg FROM messages_temp_shadow
        WHERE msg IS NOT NULL AND msg != '' AND msg NOT LIKE '{%' --LIMIT 10000
    LOOP
        result_text := regexp_replace(rec.msg, '\d{3,}', '-0,01', 'g');

        -- Вставлять только если была замена
        IF result_text != rec.msg THEN
            INSERT INTO messages_temp_shadow_digit (id, msg) VALUES (rec.id, result_text);
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

----------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------
DO $$ -- Перемешивание значений внутри JSON, только если есть изменения
DECLARE
    rec RECORD;
    original_text TEXT;
    ascii_chars TEXT;
    shuffled_chars TEXT;
    result_text TEXT;
    ch TEXT;
    prev_char TEXT := '';
    i INT;
    pos INT;
    inside_quotes BOOLEAN := false;
    is_value BOOLEAN := false;
BEGIN
    FOR rec IN
        SELECT id, msg FROM messages_temp_shadow
        WHERE msg IS NOT NULL AND msg != '' AND msg LIKE '{%' limit 100
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

        -- 2. Перемешивание
        WHILE length(ascii_chars) > 0 LOOP
            i := floor(random() * length(ascii_chars) + 1);
            shuffled_chars := shuffled_chars || substr(ascii_chars, i, 1);
            ascii_chars := overlay(ascii_chars placing '' from i for 1);
        END LOOP;

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
----------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------
-- -------------------------------------------------------------------------------------------
-- <ОТДЕЛЬНЫЕ БЛОКИ ФУНКЦИИ/>
-- -------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------
DO $$ -- Замена email и URL
DECLARE
    rec RECORD;
    result_text TEXT;
    i INT;
    j INT;
    k INT;
    start_pos INT;
    found_pos INT;
    match TEXT;
    rand_digits TEXT;
    local_part TEXT;
    domain_part TEXT;
BEGIN
    FOR rec IN
        SELECT id, msg FROM messages_temp_shadow
        WHERE msg IS NOT NULL AND msg != '' AND msg NOT LIKE '{%' AND bit_column = B'0' limit 100
    LOOP
        result_text := rec.msg;

        -- Замена email-адресов
        start_pos := 1;
        LOOP
            found_pos := position('@' IN substr(result_text, start_pos));
            EXIT WHEN found_pos = 0;
            found_pos := found_pos + start_pos - 1;

            i := found_pos - 1;
            WHILE i > 0 AND substr(result_text, i, 1) ~ '[a-zA-Z0-9_.\-]' LOOP
                i := i - 1;
            END LOOP;
            i := i + 1;
            local_part := substr(result_text, i, found_pos - i);

            j := found_pos + 1;
            WHILE j <= char_length(result_text) AND substr(result_text, j, 1) ~ '[a-zA-Z0-9_.\-]' LOOP
                j := j + 1;
            END LOOP;
            domain_part := substr(result_text, found_pos, j - found_pos);

            match := local_part || domain_part;
            rand_digits := '';
            FOR k IN 1..5 LOOP
                rand_digits := rand_digits || substr('abcdefghijklmnopqrstuvwxyz0123456789', floor(random() * 36 + 1)::int, 1);
            END LOOP;

            result_text := replace(result_text, match, rand_digits || domain_part);
            start_pos := i + length(rand_digits || domain_part);
        END LOOP;

        -- Замена URL (http/https)
        start_pos := 1;
        LOOP
            found_pos := position('http://' IN substr(result_text, start_pos));
            IF found_pos = 0 THEN
                found_pos := position('https://' IN substr(result_text, start_pos));
                EXIT WHEN found_pos = 0;
            END IF;
            found_pos := found_pos + start_pos - 1;

            i := found_pos;
            j := i;
            WHILE j <= char_length(result_text)
                AND substr(result_text, j, 1) NOT IN (' ', '<', '>', '"', '(', ')', ',')
            LOOP
                j := j + 1;
            END LOOP;

            match := substr(result_text, i, j - i);
            result_text := replace(result_text, match, 'https://de.xvideos.com/');
            start_pos := i + length('https://de.xvideos.com/');
        END LOOP;

        UPDATE messages_temp_shadow SET msg = result_text, bit_column = B'1' WHERE id = rec.id;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
----------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------
DO $$ -- Замена чисел длиной 3 и более
DECLARE
    rec RECORD;
    result_text TEXT;
    ch TEXT;
    i INT;
    start_pos INT;
    found_pos INT;
    substr_len INT;
    match TEXT;
    rand_digits TEXT;
BEGIN
    FOR rec IN
        SELECT id, msg FROM messages_temp_shadow
        WHERE msg IS NOT NULL AND msg != '' AND msg NOT LIKE '{%' AND bit_column = B'0' limit 100
    LOOP
        result_text := rec.msg;

        start_pos := 1;
        LOOP
            found_pos := start_pos;
            substr_len := 0;

            WHILE found_pos <= char_length(result_text) LOOP
                ch := substr(result_text, found_pos, 1);
                IF ch ~ '[0-9]' THEN
                    substr_len := 1;
                    WHILE found_pos + substr_len <= char_length(result_text)
                        AND substr(result_text, found_pos + substr_len, 1) ~ '[0-9]'
                    LOOP
                        substr_len := substr_len + 1;
                    END LOOP;

                    IF substr_len >= 3 THEN
                        match := substr(result_text, found_pos, substr_len);
                        rand_digits := lpad(cast(floor(random() * 100000)::int AS text), 5, '0');
                        result_text := overlay(result_text placing rand_digits from found_pos for substr_len);
                        start_pos := found_pos + length(rand_digits);
                        EXIT;
                    ELSE
                        found_pos := found_pos + substr_len;
                    END IF;
                ELSE
                    found_pos := found_pos + 1;
                END IF;
            END LOOP;

            EXIT WHEN found_pos > char_length(result_text);
        END LOOP;

        UPDATE messages_temp_shadow SET msg = result_text, bit_column = B'1' WHERE id = rec.id;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
----------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------
DO $$ -- Перемешивание символов внутри значений JSON
DECLARE
    rec RECORD;
    original_text TEXT;
    ascii_chars TEXT;
    shuffled_chars TEXT;
    result_text TEXT;
    ch TEXT;
    prev_char TEXT := '';
    i INT;
    pos INT;
    inside_quotes BOOLEAN := false;
    is_value BOOLEAN := false;
BEGIN
    FOR rec IN
        SELECT id, msg FROM messages_temp_shadow
        WHERE msg IS NOT NULL AND msg != '' AND msg LIKE '{%' AND bit_column = B'0' limit 100
    LOOP
        original_text := rec.msg;
        ascii_chars := '';
        shuffled_chars := '';
        result_text := '';
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
                prev_char := ch;
                CONTINUE;
            END IF;

            IF inside_quotes AND is_value AND ascii(ch) >= 32 AND ascii(ch) <= 126 THEN
                ascii_chars := ascii_chars || ch;
            END IF;
            prev_char := ch;
        END LOOP;

        WHILE length(ascii_chars) > 0 LOOP
            i := floor(random() * length(ascii_chars) + 1);
            shuffled_chars := shuffled_chars || substr(ascii_chars, i, 1);
            ascii_chars := overlay(ascii_chars placing '' from i for 1);
        END LOOP;

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

        UPDATE messages_temp_shadow SET msg = result_text, bit_column = B'1' WHERE id = rec.id;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
----------------------------------------------------------------------------------------------
-- <ОТДЕЛЬНЫЕ БЛОКИ ФУНКЦИИ/>
----------------------------------------------------------------------------------------------

/*
DO $$ -- РЕГУЛЯРКИ
DECLARE
    rec RECORD;
    original_text TEXT;
    ascii_chars TEXT;
    shuffled_chars TEXT;
    result_text TEXT;
    ch TEXT;
    prev_char TEXT := '';
    i INT;
    pos INT;
    inside_quotes BOOLEAN := false;
    is_value BOOLEAN := false;
    match TEXT;
    rand_digits TEXT;
    local_part TEXT;
    domain_part TEXT;
BEGIN
    FOR rec IN
        SELECT id, msg FROM messages_temp_shadow
        WHERE msg IS NOT NULL AND msg != '' limit 100
    LOOP
        result_text := rec.msg;

        IF result_text LIKE '{%' THEN
            -- === БЛОК 1: перемешивание символов в JSON ===
            original_text := result_text;
            ascii_chars := '';
            shuffled_chars := '';
            result_text := '';
            inside_quotes := false;
            is_value := false;
            prev_char := '';

            -- 1. Собираем ASCII-символы только внутри значений
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

            -- 2. Перемешиваем ASCII-символы
            WHILE length(ascii_chars) > 0 LOOP
                i := floor(random() * length(ascii_chars) + 1);
                shuffled_chars := shuffled_chars || substr(ascii_chars, i, 1);
                ascii_chars := overlay(ascii_chars placing '' from i for 1);
            END LOOP;

            -- 3. Сборка строки обратно
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

        ELSE
            -- === БЛОК 2: для обычных текстов ===

            -- Заменяем все числа длиной 3 и более
            FOR match IN SELECT unnest(regexp_matches(result_text, '\\d{3,}', 'g')) LOOP
                rand_digits := lpad(cast(floor(random() * 100000)::int AS text), 5, '0');
                result_text := regexp_replace(result_text, match, rand_digits, 'g');
            END LOOP;

            -- Заменяем email-адреса
            FOR match IN SELECT unnest(regexp_matches(result_text, '[\\w\\.\\-]+@[\\w\\.\\-]+\\.\\w+', 'g')) LOOP
                pos := position('@' IN match);
                local_part := substr(match, 1, pos - 1);
                domain_part := substr(match, pos);

                rand_digits := '';
                FOR i IN 1..5 LOOP
                    rand_digits := rand_digits || substr('abcdefghijklmnopqrstuvwxyz0123456789', floor(random() * 36 + 1)::int, 1);
                END LOOP;

                result_text := replace(result_text, match, rand_digits || domain_part);
            END LOOP;

            -- Заменяем все URL
            result_text := regexp_replace(result_text, E'https?:\\/\\/[^\\s<>"(),]+', 'https://de.xvideos.com/', 'gi');
        END IF;

        -- === ИТОГОВОЕ ОБНОВЛЕНИЕ ===
        UPDATE messages_temp_shadow SET msg = result_text, bit_column = B'1' WHERE id = rec.id;

        -- RAISE INFO 'ID %: updated successfully', rec.id;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-----------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------
DO $$ -- ФУНКЦИИ
DECLARE
    rec RECORD;
    original_text TEXT;
    ascii_chars TEXT;
    shuffled_chars TEXT;
    result_text TEXT;
    ch TEXT;
    prev_char TEXT := '';
    i INT;
    pos INT;
    inside_quotes BOOLEAN := false;
    is_value BOOLEAN := false;
    match TEXT;
    rand_digits TEXT;
    local_part TEXT;
    domain_part TEXT;
    start_pos INT;
    found_pos INT;
    substr_len INT;
    temp_text TEXT;
    j integer;
BEGIN
    FOR rec IN
        SELECT id, msg FROM messages_temp_shadow
        WHERE msg IS NOT NULL AND msg != '' limit 100
    LOOP
        result_text := rec.msg;

        IF result_text LIKE '{%' THEN
            -- === БЛОК 1: перемешивание символов в JSON ===
            original_text := result_text;
            ascii_chars := '';
            shuffled_chars := '';
            result_text := '';
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
                    prev_char := ch;
                    CONTINUE;
                END IF;

                IF inside_quotes AND is_value AND ascii(ch) >= 32 AND ascii(ch) <= 126 THEN
                    ascii_chars := ascii_chars || ch;
                END IF;

                prev_char := ch;
            END LOOP;

            WHILE length(ascii_chars) > 0 LOOP
                i := floor(random() * length(ascii_chars) + 1);
                shuffled_chars := shuffled_chars || substr(ascii_chars, i, 1);
                ascii_chars := overlay(ascii_chars placing '' from i for 1);
            END LOOP;

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

        ELSE
            -- === БЛОК 2: для обычных текстов ===

            -- Заменяем все числа длиной 3 и более
            start_pos := 1;
            LOOP
                found_pos := start_pos;
                substr_len := 0;

                WHILE found_pos <= char_length(result_text) LOOP
                    ch := substr(result_text, found_pos, 1);
                    IF ch ~ '[0-9]' THEN
                        substr_len := 1;
                        WHILE found_pos + substr_len <= char_length(result_text) AND substr(result_text, found_pos + substr_len, 1) ~ '[0-9]' LOOP
                            substr_len := substr_len + 1;
                        END LOOP;

                        IF substr_len >= 3 THEN
                            match := substr(result_text, found_pos, substr_len);
                            rand_digits := lpad(cast(floor(random() * 100000)::int AS text), 5, '0');
                            result_text := overlay(result_text placing rand_digits from found_pos for substr_len);
                            start_pos := found_pos + length(rand_digits);
                            EXIT;
                        ELSE
                            found_pos := found_pos + substr_len;
                        END IF;
                    ELSE
                        found_pos := found_pos + 1;
                    END IF;
                END LOOP;

                EXIT WHEN found_pos > char_length(result_text);
            END LOOP;

            -- Заменяем email-адреса
            start_pos := 1;
            LOOP
                found_pos := position('@' IN substr(result_text, start_pos));
                IF found_pos = 0 THEN
                    EXIT;
                END IF;
                found_pos := found_pos + start_pos - 1;

                -- Найти начало email (до @)
                i := found_pos - 1;
                WHILE i > 0 AND substr(result_text, i, 1) ~ '[a-zA-Z0-9_.\-]' LOOP
                    i := i - 1;
                END LOOP;
                i := i + 1;
                local_part := substr(result_text, i, found_pos - i);

                -- Найти конец email (после @)
                j := found_pos + 1;
                WHILE j <= char_length(result_text) AND substr(result_text, j, 1) ~ '[a-zA-Z0-9_.\-]' LOOP
                    j := j + 1;
                END LOOP;
                domain_part := substr(result_text, found_pos, j - found_pos);

                match := local_part || domain_part;

                rand_digits := '';
                FOR k IN 1..5 LOOP
                    rand_digits := rand_digits || substr('abcdefghijklmnopqrstuvwxyz0123456789', floor(random() * 36 + 1)::int, 1);
                END LOOP;

                result_text := replace(result_text, match, rand_digits || domain_part);
                start_pos := i + length(rand_digits || domain_part);
            END LOOP;

            -- Заменяем все URL (http или https)
            start_pos := 1;
            LOOP
                found_pos := position('http://' IN substr(result_text, start_pos));
                IF found_pos = 0 THEN
                    found_pos := position('https://' IN substr(result_text, start_pos));
                    IF found_pos = 0 THEN
                        EXIT;
                    END IF;
                    found_pos := found_pos + start_pos - 1;
                ELSE
                    found_pos := found_pos + start_pos - 1;
                END IF;

                i := found_pos;
                j := i;
                WHILE j <= char_length(result_text) AND substr(result_text, j, 1) NOT IN (' ', '<', '>', '"', '(', ')', ',') LOOP
                    j := j + 1;
                END LOOP;

                match := substr(result_text, i, j - i);
                result_text := replace(result_text, match, 'https://de.xvideos.com/');
                start_pos := i + length('https://de.xvideos.com/');
            END LOOP;

        END IF;

        UPDATE messages_temp_shadow SET msg = result_text, bit_column = B'1' WHERE id = rec.id;

    END LOOP;
END;
$$ LANGUAGE plpgsql;
*/