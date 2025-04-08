/*
CREATE OR REPLACE FUNCTION Robohub.Robo_Slicer.log_slicer()
    RETURNS VOID
    LANGUAGE Plpgsql
    VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS
$SLICER$
DECLARE
    id_barn_flag         INTEGER;
    time_start           BIGINT;
    omega                RECORD;
    jsonb_departure      JSONB;
    jsonb_arrival        JSONB;
    err_mess             TEXT;
    err_det              TEXT;
    err_cd               TEXT;
    log_synthesized_name TEXT;
    ts_current           TIMESTAMP WITH TIME ZONE := CURRENT_TIMESTAMP;
    start_time           TIMESTAMP;
    end_time             TIMESTAMP;

BEGIN
    IF NOW()::TIME BETWEEN TIME '00:00:00' AND TIME '00:10:00' THEN
        log_synthesized_name = FORMAT('postgresql-%s.log', TO_CHAR((CURRENT_DATE - INTERVAL '1 day'), 'Dy'));
        start_time = TO_TIMESTAMP(
                TO_CHAR((CURRENT_DATE - INTERVAL '1 day')::TIMESTAMP + TIME '22:00:00', 'YYYY-MM-DD HH24:MI:SS'),
                'YYYY-MM-DD HH24:MI:SS');
        end_time = TO_TIMESTAMP(
                TO_CHAR((CURRENT_DATE - INTERVAL '1 day')::TIMESTAMP + TIME '23:59:59', 'YYYY-MM-DD HH24:MI:SS'),
                'YYYY-MM-DD HH24:MI:SS');
    ELSE
        log_synthesized_name = FORMAT('postgresql-%s.log', TO_CHAR(CURRENT_DATE, 'Dy'));
    END IF;

    IF (ts_current - DATE_TRUNC('hour', ts_current) BETWEEN INTERVAL '-5 minutes' AND INTERVAL '5 minutes' OR
        start_time IS NULL) THEN
        start_time = DATE_TRUNC('hour', ts_current) - INTERVAL '2 hours';
        end_time = DATE_TRUNC('hour', ts_current);
    END IF;

    CREATE TEMP TABLE tmp_info_table
    (
        conn_name            TEXT,
        serv_id              INTEGER,
        serv_port            INTEGER,
        serv_host            TEXT,
        compl_name           TEXT,
        out_dir              TEXT,
        out_file             TEXT,
        pgbg_path            TEXT,
        log_synthesized_name TEXT,
        start_time           TIMESTAMP,
        end_time             TIMESTAMP
    ) ON COMMIT DROP;

    INSERT INTO tmp_info_table
    SELECT 'y_connect'                                 AS conn_name,
           Robohub.Robo_Reference."Servers".Pk_Id_Conn AS serv_id,
           Robohub.Robo_Reference."Servers".Conn_Port  AS serv_port,
           Robohub.Robo_Reference."Servers".Conn_Host  AS serv_host,
           REGEXP_REPLACE(
                   FORMAT('%s--%s', Robohub.Robo_Reference."Servers".Conn_Host,
                          Robohub.Robo_Reference."Servers".Conn_Port),
                   '\.maxbit\.private', '', 'g'
           ) || '--' || TO_CHAR(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD--HH24-MI') || '.html',
           pi.Path_Out_Dir                             AS out_dir,
           pi.Path_Out_Log_File                        AS out_file,
           pi.Path_Pgbg                                AS pgbg_path,
           log_synthesized_name,
           start_time AT TIME ZONE 'UTC',
           end_time AT TIME ZONE 'UTC'
    FROM Robohub.Robo_Reference."Servers"
             JOIN Pg_Ini pi ON pi.Fk_Pk_Id_Conn = Robohub.Robo_Reference."Servers".Pk_Id_Conn
    WHERE (Robohub.Robo_Reference."Servers".Switch_Serv & B'00100000') = B'00100000';

    FOR omega IN SELECT * FROM tmp_info_table
        LOOP

            BEGIN
                IF omega.conn_name IN (SELECT UNNEST(Robohub.Public.Dblink_Get_Connections())) THEN
                    PERFORM Robohub.Public.Dblink_Disconnect(omega.conn_name);
                END IF;
            EXCEPTION
                WHEN OTHERS THEN
                    GET STACKED DIAGNOSTICS
                        err_mess = MESSAGE_TEXT,
                        err_det = PG_EXCEPTION_DETAIL,
                        err_cd = RETURNED_SQLSTATE;
                    INSERT INTO Robohub.Robo_Slicer."Errors" (Fk_Pk_Id_Conn,
                                                              Slice_Err_Code,
                                                              Slice_Err_Detail,
                                                              Slice_Err_Mess,
                                                              Slice_Now_Ins,
                                                              Work_Scheme)
                    VALUES (omega.serv_id,
                            err_cd,
                            err_det,
                            err_mess,
                            NOW(),
                            'robo_slicer');
                    CONTINUE;

            END;

            BEGIN
                INSERT INTO Robo_Slicer.Pg_Barn (Fk_Pk_Id_Conn, Log_Slice_Name)
                VALUES (omega.serv_id, omega.compl_name)
                RETURNING Pk_Id_Barn INTO id_barn_flag;
            END;

            jsonb_departure = JSONB_BUILD_OBJECT(
                    'pgbg_path', omega.pgbg_path,
                    'log_file', omega.out_file || log_synthesized_name,
                    'out_dir_slice', omega.out_dir,
                    'out_file_slice', omega.compl_name,
                    'begin_dattime', TO_CHAR(omega.start_time, 'YYYY-MM-DD HH24:MI:SS'),
                    'end_dattime', TO_CHAR(omega.end_time, 'YYYY-MM-DD HH24:MI:SS')
                              );

            BEGIN
                PERFORM Robohub.Public.Dblink_Connect(
                        omega.conn_name,
                        FORMAT(
                                'dbname=%s user=%s password=%s host=%s port=%s application_name=%s options=-csearch_path=',
                                'postgres',
                                'robo_sudo',
                                '%dFgH8!zX4&kLmT2',
                                omega.serv_host,
                                omega.serv_port::INT,
                                'robo_slicer'
                        )
                        );
            EXCEPTION
                WHEN OTHERS THEN
                    GET STACKED DIAGNOSTICS
                        err_mess = MESSAGE_TEXT,
                        err_det = PG_EXCEPTION_DETAIL,
                        err_cd = RETURNED_SQLSTATE;
                    INSERT INTO Robohub.Robo_Slicer."Errors" (Fk_Pk_Id_Conn,
                                                              Slice_Err_Code,
                                                              Slice_Err_Detail,
                                                              Slice_Err_Mess,
                                                              Slice_Now_Ins,
                                                              Work_Scheme)
                    VALUES (omega.serv_id,
                            err_cd,
                            err_det,
                            err_mess,
                            NOW(),
                            'robo_slicer');
                    RETURN;
            END;

            time_start = EXTRACT(EPOCH FROM CLOCK_TIMESTAMP())::BIGINT;

            SELECT INTO jsonb_arrival result
            FROM Robohub.Public.Dblink(omega.conn_name,
                                       FORMAT('SELECT robo.pgbadger_report_slicer(%L)', jsonb_departure)) AS (result JSONB);


            IF jsonb_arrival ->> 'status' = 'error' THEN

                INSERT INTO Robo_Slicer."Errors" (Fk_Pk_Id_Conn, Slice_Err_Mess, Slice_Err_Detail, Slice_Err_Code,
                                                  Work_Scheme)
                VALUES (omega.serv_id,
                        jsonb_arrival ->> 'message',
                        jsonb_arrival ->> 'status',
                        jsonb_arrival ->> 'sqlstate',
                        jsonb_arrival ->> 'status_text');

            END IF;

            BEGIN
                -- возможен прилет одного из значений типа null - в журнал не запишется: всё IS NOT NULL
                IF jsonb_arrival ->> 'status_text' = 'report_ready' THEN
                    UPDATE Robo_Slicer.Pg_Barn
                    SET Log_Mod           = EXTRACT(EPOCH FROM (jsonb_arrival ->> 'full_log_modf')::TIMESTAMP)::BIGINT,
                        Log_Timer_Slicing = EXTRACT(EPOCH FROM CLOCK_TIMESTAMP())::BIGINT - time_start,
                        Log_Size          = (jsonb_arrival ->> 'full_log_size')::BIGINT,
                        Log_Slice_Size    = (jsonb_arrival ->> 'slice_file_size')::INTEGER,
                        Log_Name          = jsonb_arrival ->> 'full_log_name'
                    WHERE Pk_Id_Barn = id_barn_flag;
                END IF;
            EXCEPTION
                WHEN OTHERS THEN
                    GET STACKED DIAGNOSTICS
                        err_mess = MESSAGE_TEXT,
                        err_det = PG_EXCEPTION_DETAIL,
                        err_cd = RETURNED_SQLSTATE;
                    INSERT INTO Robohub.Robo_Slicer."Errors" (Fk_Pk_Id_Conn,
                                                              Slice_Err_Code,
                                                              Slice_Err_Detail,
                                                              Slice_Err_Mess,
                                                              Slice_Now_Ins,
                                                              Work_Scheme)
                    VALUES (omega.serv_id,
                            err_cd,
                            err_det,
                            err_mess,
                            NOW(),
                            'robo_slicer');
                    CONTINUE;
            END;

            IF omega.conn_name IN (SELECT UNNEST(Robohub.Public.Dblink_Get_Connections())) THEN
                PERFORM Robohub.Public.Dblink_Disconnect(omega.conn_name);
            END IF;

        END LOOP;
END;
$SLICER$;
*/

DO $ZOMBIE$
BEGIN
    PERFORM robohub.robo_slicer.log_slicer();
END
$ZOMBIE$;


/*
CREATE OR REPLACE FUNCTION robo.pgbadger_report_slicer(input_json JSONB)
RETURNS JSONB
LANGUAGE plpgsql
COST 100
VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS
$BODY$
DECLARE
    semaphore INTEGER;
    --verbose   BOOLEAN = COALESCE(input_json ->> 'verbose', 'false')::BOOLEAN;
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
$BODY$;

*/