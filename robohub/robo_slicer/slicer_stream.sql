CREATE FUNCTION robo_slicer.slicer_stream() RETURNS void
    SECURITY DEFINER
    LANGUAGE plpgsql AS
$$

DECLARE
    id_barn_flag         integer; -- Идентификатор записи в таблице Pg_Barn для текущей обработки
    time_start           bigint; -- Временная метка начала работы удалённой функции (в секундах с эпохи)
    omega                record; -- Переменная цикла для прохода по временной таблице tmp_info_table
    jsonb_departure      jsonb; -- JSONB-объект с параметрами, передаваемыми в удалённую функцию pgbadger_report_slicer
    jsonb_arrival        jsonb; -- JSONB-объект с результатами, возвращёнными удалённой функцией
    err_mess             text; -- Сообщение об ошибке при обработке исключений
    err_det              text; -- Детали ошибки (PG_EXCEPTION_DETAIL)
    err_cd               text; -- Код SQLSTATE ошибки
    log_synthesized_name text; -- Имя лог-файла, определённое по дате (например, 'postgresql-Mon.log')
    ts_current           timestamp with time zone = CURRENT_TIMESTAMP AT TIME ZONE 'UTC'; -- Текущее UTC-время
    start_time           timestamp; -- Начальное время интервала логов (UTC)
    end_time             timestamp; -- Конечное время интервала логов (UTC)
BEGIN
    IF (NOW() AT TIME ZONE 'UTC')::time BETWEEN time '00:00:00' AND time '00:10:00' THEN
        log_synthesized_name = FORMAT('postgresql-%s.log', TO_CHAR((CURRENT_TIMESTAMP AT TIME ZONE 'UTC' - INTERVAL '1 day')::date, 'Dy'));
        start_time = TO_TIMESTAMP(TO_CHAR((CURRENT_TIMESTAMP AT TIME ZONE 'UTC' - INTERVAL '1 day')::date + time '22:00:00', 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS');
        end_time = TO_TIMESTAMP(TO_CHAR((CURRENT_TIMESTAMP AT TIME ZONE 'UTC' - INTERVAL '1 day')::date + time '23:59:59', 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS');
    ELSE
        log_synthesized_name = FORMAT('postgresql-%s.log', TO_CHAR((CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date, 'Dy'));
    END IF;

    IF (ts_current - DATE_TRUNC('hour', ts_current) BETWEEN INTERVAL '-5 minutes' AND INTERVAL '5 minutes' OR start_time IS NULL) THEN
        start_time = DATE_TRUNC('hour', ts_current) - INTERVAL '2 hours';
        end_time = DATE_TRUNC('hour', ts_current);
    END IF;

    CREATE TEMP TABLE tmp_info_table
    (
        conn_name text, serv_id integer, serv_port integer, serv_host text, compl_name text, out_dir text, out_file text, pgbg_path text, log_synthesized_name text, start_time timestamp, end_time timestamp) ON COMMIT DROP;

    INSERT INTO tmp_info_table
    SELECT 'y_connect'                                                                                                                                                                                                                                                    AS conn_name,
           robohub.robo_reference."Servers".pk_id_conn                                                                                                                                                                                                                    AS serv_id,
           robohub.robo_reference."Servers".conn_port                                                                                                                                                                                                                     AS serv_port,
           robohub.robo_reference."Servers".conn_host                                                                                                                                                                                                                     AS serv_host,
           REGEXP_REPLACE(FORMAT('%s--%s', robohub.robo_reference."Servers".conn_host, robohub.robo_reference."Servers".conn_port), '\.maxbit\.private', '', 'g') || '--' || TO_CHAR(DATE_TRUNC('hour', start_time AT TIME ZONE 'UTC'), 'YYYY-MM-DD--HH24-MI') || '.html' AS compl_name,
           pi.path_out_dir                                                                                                                                                                                                                                                AS out_dir,
           pi.path_out_log_file                                                                                                                                                                                                                                           AS out_file,
           pi.path_pgbg                                                                                                                                                                                                                                                   AS pgbg_path,
           log_synthesized_name,
           start_time AT TIME ZONE 'UTC',
           end_time AT TIME ZONE 'UTC'
    FROM robohub.robo_reference."Servers"
             JOIN robohub.robo_slicer.pg_ini pi ON pi.fk_pk_id_conn = robohub.robo_reference."Servers".pk_id_conn
    WHERE (robohub.robo_reference."Servers".switch_serv & B'00100000') = B'00100000';


    FOR omega IN SELECT * FROM tmp_info_table
        LOOP

            BEGIN
                IF omega.conn_name IN (SELECT UNNEST(robohub.public.dblink_get_connections())) THEN PERFORM robohub.public.dblink_disconnect(omega.conn_name); END IF;
            EXCEPTION
                WHEN OTHERS THEN GET STACKED DIAGNOSTICS err_mess = MESSAGE_TEXT, err_det = PG_EXCEPTION_DETAIL, err_cd = RETURNED_SQLSTATE;
                INSERT INTO robohub.robo_slicer."Errors" (fk_pk_id_conn,
                                                          slice_err_code,
                                                          slice_err_detail,
                                                          slice_err_mess,
                                                          slice_now_ins,
                                                          work_scheme)
                VALUES (omega.serv_id,
                        err_cd,
                        err_det,
                        err_mess,
                        CURRENT_TIMESTAMP AT TIME ZONE 'UTC',
                        'robo_slicer');
                CONTINUE;
            END;

            BEGIN
                INSERT INTO robohub.robo_slicer.pg_barn (fk_pk_id_conn, log_slice_name) VALUES (omega.serv_id, omega.compl_name) RETURNING pk_id_barn INTO id_barn_flag;
            END;

            jsonb_departure = JSONB_BUILD_OBJECT('pgbg_path', omega.pgbg_path, 'log_file', omega.out_file || log_synthesized_name, 'out_dir_slice', omega.out_dir, 'out_file_slice', omega.compl_name, 'begin_dattime', TO_CHAR(omega.start_time, 'YYYY-MM-DD HH24:MI:SS'), 'end_dattime',
                                                 TO_CHAR(omega.end_time, 'YYYY-MM-DD HH24:MI:SS'));

            BEGIN
                PERFORM robohub.public.dblink_connect(omega.conn_name, FORMAT('dbname=%s user=%s password=%s host=%s port=%s application_name=%s options=-csearch_path=', 'postgres', 'robo_sudo', '%dFgH8!zX4&kLmT2', omega.serv_host, omega.serv_port::int, 'robo_slicer'));
            EXCEPTION
                WHEN OTHERS THEN GET STACKED DIAGNOSTICS err_mess = MESSAGE_TEXT, err_det = PG_EXCEPTION_DETAIL, err_cd = RETURNED_SQLSTATE;
                INSERT INTO robohub.robo_slicer."Errors" (fk_pk_id_conn,
                                                          slice_err_code,
                                                          slice_err_detail,
                                                          slice_err_mess,
                                                          slice_now_ins,
                                                          work_scheme)
                VALUES (omega.serv_id,
                        err_cd,
                        err_det,
                        err_mess,
                        CURRENT_TIMESTAMP AT TIME ZONE 'UTC',
                        'robo_slicer');
                RETURN;
            END;

            time_start = EXTRACT(EPOCH FROM CLOCK_TIMESTAMP())::bigint;

            SELECT INTO jsonb_arrival result FROM robohub.public.dblink(omega.conn_name, FORMAT('SELECT robo.pgbadger_report_slicer(%L)', jsonb_departure)) AS (result jsonb);

            IF jsonb_arrival ->> 'status' = 'error' THEN
                INSERT INTO robohub.robo_slicer."Errors" (fk_pk_id_conn,
                                                          slice_err_mess,
                                                          slice_err_detail,
                                                          slice_err_code,
                                                          work_scheme)
                VALUES (omega.serv_id,
                        jsonb_arrival ->> 'message',
                        jsonb_arrival ->> 'status',
                        jsonb_arrival ->> 'sqlstate',
                        jsonb_arrival ->> 'status_text');
            END IF;

            BEGIN
                IF jsonb_arrival ->> 'status_text' = 'report_ready' THEN
                    UPDATE robohub.robo_slicer.pg_barn
                    SET log_mod           = EXTRACT(EPOCH FROM (jsonb_arrival ->> 'full_log_modf')::timestamp)::bigint,
                        log_timer_slicing = EXTRACT(EPOCH FROM CLOCK_TIMESTAMP())::bigint - time_start,
                        log_size          = (jsonb_arrival ->> 'full_log_size')::bigint,
                        log_slice_size    = (jsonb_arrival ->> 'slice_file_size')::integer,
                        log_name          = jsonb_arrival ->> 'full_log_name'
                    WHERE pk_id_barn = id_barn_flag;
                END IF;
            EXCEPTION
                WHEN OTHERS THEN GET STACKED DIAGNOSTICS err_mess = MESSAGE_TEXT, err_det = PG_EXCEPTION_DETAIL, err_cd = RETURNED_SQLSTATE;
                INSERT INTO robohub.robo_slicer."Errors" (fk_pk_id_conn,
                                                          slice_err_code,
                                                          slice_err_detail,
                                                          slice_err_mess,
                                                          slice_now_ins,
                                                          work_scheme)
                VALUES (omega.serv_id,
                        err_cd,
                        err_det,
                        err_mess,
                        CURRENT_TIMESTAMP AT TIME ZONE 'UTC',
                        'robo_slicer');
                CONTINUE;
            END;

            IF omega.conn_name IN (SELECT UNNEST(robohub.public.dblink_get_connections())) THEN PERFORM robohub.public.dblink_disconnect(omega.conn_name); END IF;

        END LOOP;
END;
$$;

ALTER FUNCTION robo_slicer.slicer_stream() OWNER TO gtimofeyev;

