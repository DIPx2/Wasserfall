CREATE FUNCTION Robo.Pgbadger_Report_Slicer(input_json jsonb) RETURNS jsonb
    SECURITY DEFINER
    LANGUAGE Plpgsql
AS
$$
DECLARE
    semaphore INTEGER;
    verbose   BOOLEAN := COALESCE(input_json ->> 'verbose', 'false')::BOOLEAN;
BEGIN

    CREATE TEMPORARY TABLE temp_0 (
        ident           SERIAL PRIMARY KEY,
        full_log_name   TEXT                     DEFAULT 'nihil'           NOT NULL,
        full_log_size   BIGINT                   DEFAULT 0                 NOT NULL,
        full_log_modf   TIMESTAMPTZ              DEFAULT CURRENT_TIMESTAMP NOT NULL,
        slice_file_size INTEGER                  DEFAULT -1                NOT NULL,
        report          INTEGER                  DEFAULT 0                 NOT NULL,
        status_text     TEXT                     DEFAULT 'init'            NOT NULL
    ) ON COMMIT DROP;

    BEGIN
  
        INSERT INTO temp_0 (full_log_name, full_log_size, full_log_modf)
        SELECT "name", "size", "modification"
        FROM Pg_Ls_Logdir()
        WHERE "modification"::DATE = CURRENT_DATE AND RIGHT("name", 4) = '.log'
        RETURNING ident INTO semaphore;


        EXECUTE FORMAT(
            'COPY (SELECT ''NULL'') TO PROGRAM %L',
            FORMAT(
                '%s --begin "%s" --end "%s" --outdir %s --outfile %s %s',
                input_json ->> 'pgbg_path',
                input_json ->> 'begin_dattime',
                input_json ->> 'end_dattime',
                input_json ->> 'out_dir_slice',
                input_json ->> 'out_file_slice',
                input_json ->> 'log_file'
            )
        );


        UPDATE temp_0
        SET report = 1,
            status_text = 'report_created'
        WHERE ident = semaphore;


        BEGIN
            UPDATE temp_0
            SET report = 2,
                status_text = 'report_ready',
                slice_file_size = (
                    SELECT (PG_STAT_FILE(
                        (input_json ->> 'out_dir_slice') || '/' || (input_json ->> 'out_file_slice')
                    )).size
                )::BIGINT
            WHERE ident = semaphore;
        EXCEPTION
            WHEN OTHERS THEN
                UPDATE temp_0
                SET report = -1,
                    status_text = 'report_file_missing'
                WHERE ident = semaphore;
        END;

    EXCEPTION
        WHEN OTHERS THEN
            RETURN JSONB_BUILD_OBJECT(
                'status', 'error',
                'status_text', 'exception',
                'message', SQLERRM,
                'sqlstate', SQLSTATE
            );
    END;

/*
    IF verbose THEN 
        RETURN (
            SELECT JSONB_BUILD_OBJECT(
                'ident', ident,
                'report', report,
                'full_log_name', full_log_name,
                'full_log_size', full_log_size,
                'full_log_modf', full_log_modf,
                'slice_file_size', slice_file_size,
                'status_text', status_text
            )
            FROM temp_0
            WHERE ident = semaphore
        );
    ELSE
	*/
        RETURN (
            SELECT JSONB_BUILD_OBJECT(
                'full_log_name', full_log_name,
                'full_log_size', full_log_size,
                'full_log_modf', full_log_modf,
                'slice_file_size', slice_file_size,
                'status_text', status_text
            )
            FROM temp_0
            WHERE ident = semaphore
        );
    --END IF;

END;
$$;

ALTER FUNCTION Robo.Pgbadger_Report_Slicer(JSONB) OWNER TO Robo_Sudo;

