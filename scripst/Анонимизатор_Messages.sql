DO $$
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
        SELECT id, msg FROM messages_temp
        WHERE msg IS NOT NULL AND msg != '' -- LIMIT 300
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
        UPDATE messages_temp
        SET msg = result_text
        WHERE id = rec.id;

        -- RAISE INFO 'ID %: updated successfully', rec.id;
    END LOOP;
END;
$$ LANGUAGE plpgsql;


--======================
---- messages
--=====================
/*
DO $$
DECLARE
    rec RECORD;
    original_text text;
    ascii_chars text;
    shuffled_chars text;
    result_text text;
    ch text;
    prev_char text := '';
    i int;
    pos int;
    inside_quotes boolean := false;
    is_value boolean := false;
BEGIN
    FOR rec IN SELECT id, msg FROM public.messages_temp WHERE msg LIKE '{%' LIMIT 300 LOOP
        original_text := rec.msg;
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

                    -- определяем, значение ли это
                    IF inside_quotes AND prev_char = ':' THEN
                        is_value := true;
                    ELSE
                        is_value := false;
                    END IF;
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

                    -- определяем, значение ли это
                    IF inside_quotes AND prev_char = ':' THEN
                        is_value := true;
                    ELSE
                        is_value := false;
                    END IF;
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

        -- 4. Вывод результата
        RAISE INFO 'ID: %, SHUFFLED: %', rec.id, result_text;
    END LOOP;
END;
$$ LANGUAGE plpgsql;


DO $$
DECLARE
    rec RECORD;
    msg_text text;
    result_text text;
    match text;
    rand_digits text;
    local_part text;
    domain_part text;
    pos int;
BEGIN
    FOR rec IN
        SELECT id, msg FROM messages_temp
        WHERE msg NOT LIKE '{%' AND msg NOT LIKE 'curl%' AND msg IS NOT NULL AND msg != '' LIMIT 300
    LOOP
        msg_text := rec.msg;
        result_text := msg_text;

        -- 1. Заменяем все числа длиной 3 и более
        FOR match IN SELECT unnest(regexp_matches(result_text, '\\d{3,}', 'g')) LOOP
            rand_digits := lpad(cast(floor(random() * 100000)::int AS text), 5, '0');
            result_text := regexp_replace(result_text, match, rand_digits, 'g');
        END LOOP;

        -- 2. Заменяем email-адреса (оставляем только часть после "@")
        FOR match IN SELECT unnest(regexp_matches(result_text, '[\\w\\.\\-]+@[\\w\\.\\-]+\\.\\w+', 'g')) LOOP
            pos := position('@' IN match);
            local_part := substr(match, 1, pos - 1);
            domain_part := substr(match, pos);

            -- Генерация 5-символьной абракадабры
            rand_digits := '';
            FOR i IN 1..5 LOOP
                rand_digits := rand_digits || substr('abcdefghijklmnopqrstuvwxyz0123456789', floor(random() * 36 + 1)::int, 1);
            END LOOP;

            result_text := replace(result_text, match, rand_digits || domain_part);
        END LOOP;

        -- 3. Заменяем все URL на https://de.xvideos.com/
        result_text := regexp_replace( result_text, E'https?:\\/\\/[^\\s<>"(),]+', 'https://de.xvideos.com/', 'gi' );

        -- 4. Вывод результата
        RAISE INFO 'ID: %, SHUFFLED: %', rec.id, result_text;
    END LOOP;
END;
$$ LANGUAGE plpgsql;


/*
SELECT id,  MAX(LENGTH(msg)) from messages where msg is not null and msg not like '%{%' and msg != '' group by id limit 1000

SELECT msg from messages where msg is not null and msg not like '%{%' and msg != '' and length(msg) between 1000 and 12597 -- 483 записи
SELECT msg from messages where msg is not null and msg not like '%{%' and msg != '' and length(msg) between 2000 and 12597 -- 37 записей
SELECT msg from messages where msg is not null and msg not like '%{%' and msg != '' and length(msg) between 3000 and 12597 -- 9 записей
SELECT msg from messages where msg is not null and msg not like '%{%' and msg != '' and length(msg) between 4000 and 12597 -- 4 записи
SELECT msg from messages where msg is not null and msg not like '%{%' and msg != '' and length(msg) = 12597 -- 1 запись

SELECT 
    information_schema.columns.table_name,
    information_schema.columns.column_name
FROM 
    information_schema.columns
JOIN 
    information_schema.tables 
    USING (table_schema, table_name)
WHERE 
    information_schema.columns.data_type = 'character varying'AND
	information_schema.tables.table_type = 'BASE TABLE' AND 
	information_schema.columns.table_schema = 'public' AND
	information_schema.columns.table_catalog = 'mbss_master'
ORDER BY 
    information_schema.columns.table_name, 
    information_schema.columns.column_name;
	
SELECT * FROM messages_temp WHERE msg !~ '^\s*{.*}$'; -- Regex to detect invalid JSON-like structure
 */