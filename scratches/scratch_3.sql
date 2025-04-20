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


--TRUNCATE TABLE messages_temp_shadow_digit;

--SELECT count(*) FROM messages_temp_shadow_digit