/*
CREATE OR REPLACE PROCEDURE AutomateDatabaseCreation(db_param JSONB)
    LANGUAGE Plpgsql
AS
$$
BEGIN
    NULL;
END;
$$;
*/
DO
$$
DECLARE
    УказанияДляСозданияБазыДанных JSONB DEFAULT '{}'::jsonb;

    СерверМодель             CONSTANT TEXT = 'localhost';
    ПортМодель               CONSTANT TEXT = '5432';
    СхемаМодель              CONSTANT TEXT = 'public';
    БазыДанныхМодель         CONSTANT TEXT = 'database_model';

    СерверРеализация         CONSTANT TEXT = 'production_server';
    ПортРеализация           CONSTANT TEXT = '5432';
    СхемаРеализация          CONSTANT TEXT = 'real_schema';
    БазаДанныхРеализация     CONSTANT TEXT = 'real_database';

    ВладелецБазыПоУмолчанию  CONSTANT TEXT = 'default_owner';
    КаталогИсполняемыхФайлов CONSTANT TEXT = '/usr/pgsql-16/bin';

    DefaultPrivilegesGrantee TEXT = 'mbss_stage';
    DefaultPrivilegesGrantor TEXT = 'mbss_stage';

    setting RECORD;
BEGIN
    FOR setting IN
        SELECT * FROM (VALUES
            ('server', СерверМодель),
            ('port',   ПортМодель),
            ('schema', СхемаМодель),
            ('database', БазыДанныхМодель)
        ) AS kv(jsonb_key, value)
    LOOP
        УказанияДляСозданияБазыДанных =
            jsonb_set(
                УказанияДляСозданияБазыДанных,
                ARRAY[setting.jsonb_key],
                to_jsonb(setting.value),
                true
            );
    END LOOP;

    RAISE NOTICE 'JSON: %', УказанияДляСозданияБазыДанных;
END
$$;
