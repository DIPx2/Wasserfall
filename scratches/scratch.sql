DO $$ -- Перемешивание значений внутри JSON
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
        WHERE msg IS NOT NULL AND msg != '' AND msg LIKE '{%' --limit 100
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

        INSERT INTO messages_temp_shadow_json (id, msg) VALUES (rec.id, result_text);
    END LOOP;
END;
$$ LANGUAGE plpgsql;