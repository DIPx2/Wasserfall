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


--TRUNCATE TABLE messages_temp_shadow_emlurl;
SELECT * FROM messages_temp_shadow_emlurl
--SELECT count(*) FROM messages_temp_shadow_emlurl