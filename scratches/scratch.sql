CREATE OR REPLACE FUNCTION robo.pgbadger_report_slicer(
    input_json JSONB)
    RETURNS JSONB
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS
$BODY$
DECLARE

    file_size BIGINT;
    semaphore INTEGER;
BEGIN

    CREATE TEMPORARY TABLE temp_0
    (
        ident           SERIAL PRIMARY KEY,
        full_log_name   TEXT                     DEFAULT 'nihil'           NOT NULL,
        full_log_size   BIGINT                   DEFAULT 0                 NOT NULL,
        full_log_modf   TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
        slice_file_size INTEGER                  DEFAULT -1                NOT NULL,
        report          INTEGER                  DEFAULT 0                 NOT NULL
    ) ON COMMIT DROP;


    BEGIN

        INSERT INTO temp_0 (full_log_name, full_log_size, full_log_modf)
        SELECT "name", "size", "modification"
        FROM Pg_Ls_Logdir()
        WHERE "modification"::DATE = CURRENT_DATE
          AND RIGHT("name", 4) = '.log'
        RETURNING ident INTO semaphore;

        EXECUTE FORMAT('COPY (SELECT ''NULL'') TO PROGRAM %L', FORMAT(
                'pgbadger --begin "%s" --end "%s" --outdir %s --outfile %s %s',
                input_json ->> 'begin_dattime',
                input_json ->> 'end_dattime',
                input_json ->> 'out_dir_slice',
                input_json ->> 'out_file_slice',
                input_json ->> 'log_file'
                                                               ));

        file_size =
                (PG_STAT_FILE(input_json ->> 'out_dir_slice' || '/' || input_json ->> 'out_file_slice')).size::BIGINT;

        -- RAISE INFO 'Команда успешно выполнена: %', str_complete;
        -- RAISE INFO 'Размер файла: %', file_size;

    EXCEPTION
        WHEN OTHERS THEN
            -- RAISE INFO 'Ошибка при выполнении команды: %', SQLERRM;
            file_size = -1; -- Размер файла -1 в случае ошибки
    END;

    RETURN JSONB_BUILD_OBJECT('logs', COALESCE(Output_Json, '[]'::JSONB), 'pgbadger_command', Str_Complete, 'file_size',
                              file_size);
END;
$BODY$;
