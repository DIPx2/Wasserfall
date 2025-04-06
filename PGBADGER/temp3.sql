/*
DO $$
DECLARE
    cmd TEXT;
    file_size BIGINT;
    result_text TEXT;
BEGIN
    -- Формируем строку с командой для pgBadger
    cmd := FORMAT(
            'COPY (SELECT ''NULL'') TO PROGRAM %L',
            'pgbadger --begin "2025-04-06 12:00:00" --end "2025-04-06 14:00:00" --outdir /home/reports/ --outfile prd-chat-pg-02--5434--2025-04-06--14-126.html /var/lib/postgresql/16/mbss/log/postgresql-Sun.log'
    );

    -- Выводим команду для проверки
    RAISE NOTICE 'Сформированная команда: %', cmd;

    BEGIN
        -- Выполняем команду с помощью EXECUTE
        EXECUTE cmd;

        -- Получаем размер файла, если команда выполнена успешно
        file_size := (pg_stat_file('/home/reports/prd-chat-pg-02--5434--2025-04-06--14-126.html')).size;

        -- Проверка на успешное выполнение
        RAISE NOTICE 'Команда успешно выполнена. Размер файла: % bytes', file_size;

        -- Формируем строку с результатом (исправлен спецификатор)
        result_text := FORMAT('Команда выполнена успешно. Размер файла: %s bytes', file_size);
    EXCEPTION
        WHEN OTHERS THEN
            -- В случае ошибки выводим сообщение об ошибке (исправлен спецификатор)
            RAISE NOTICE 'Ошибка при выполнении команды: %', SQLERRM;
            result_text := FORMAT('Ошибка: %s', SQLERRM);
    END;

    -- Выводим окончательный результат
    RAISE NOTICE 'Результат: %', result_text;
END $$;
*/



CREATE OR REPLACE FUNCTION Pgbadger_Report_Slicer(input_json JSONB)
    RETURNS JSONB AS
$$
DECLARE
    output_json  JSONB;
    str_complete TEXT;
    cmd          TEXT;
    file_size    BIGINT;
BEGIN
    -- Формируем JSON с логами
    SELECT JSONB_AGG(
               JSONB_BUILD_OBJECT(
                       'name', "name",
                       'size', "size",
                       'modification', "modification"
               )
           )
    INTO output_json
    FROM Pg_Ls_Logdir()
    WHERE "modification"::DATE = CURRENT_DATE
      AND RIGHT("name", 4) = '.log';

    -- Формируем строку команды для pgBadger
    str_complete := FORMAT(
            'pgbadger --begin "%s" --end "%s" --outdir %s --outfile %s %s',
            input_json ->> 'begin_dattime',
            input_json ->> 'end_dattime',
            input_json ->> 'out_dir_slice',
            input_json ->> 'out_file_slice',
            input_json ->> 'log_file'
    );

    -- Формируем команду для выполнения
    cmd := FORMAT('COPY (SELECT ''NULL'') TO PROGRAM %L', str_complete);

    -- Выполнение команды с проверкой на успех
    BEGIN
        EXECUTE cmd;

        -- Получаем размер файла после выполнения команды
        file_size := (pg_stat_file(input_json ->> 'out_dir_slice' || '/' || input_json ->> 'out_file_slice')).size::BIGINT;

        -- Выводим команду и размер файла
        RAISE NOTICE 'Команда успешно выполнена: %', str_complete;
        RAISE NOTICE 'Размер файла: %', file_size;

    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Ошибка при выполнении команды: %', SQLERRM;
            file_size := -1; -- Устанавливаем размер файла в -1 в случае ошибки
    END;

    -- Возвращаем результат в формате JSON
    RETURN JSONB_BUILD_OBJECT(
            'logs', COALESCE(output_json, '[]'::JSONB),
            'pgbadger_command', str_complete,
            'file_size', file_size
           );
END;
$$ LANGUAGE Plpgsql;




DO
$$
    DECLARE
        id_barn_flag         INTEGER;
        time_start           BIGINT;
        omega                RECORD;
        json_flight          JSONB;
        err_mess             TEXT;
        err_det              TEXT;
        err_cd               TEXT;
        log_synthesized_name TEXT;
        ts_current           TIMESTAMP := CURRENT_TIMESTAMP;
        start_time           TIMESTAMP;
        end_time             TIMESTAMP;
        time_diff            INTERVAL;
        result_json          JSONB;
    BEGIN
        IF NOW()::TIME BETWEEN TIME '00:00:00' AND TIME '00:10:00' THEN
            log_synthesized_name := FORMAT('postgresql-%s.log', TO_CHAR((CURRENT_DATE - INTERVAL '1 day'), 'Dy'));
            start_time := TO_TIMESTAMP(
                    TO_CHAR((CURRENT_DATE - INTERVAL '1 day')::TIMESTAMP + TIME '22:00:00', 'YYYY-MM-DD HH24:MI:SS'),
                    'YYYY-MM-DD HH24:MI:SS');
            end_time := TO_TIMESTAMP(
                    TO_CHAR((CURRENT_DATE - INTERVAL '1 day')::TIMESTAMP + TIME '23:59:59', 'YYYY-MM-DD HH24:MI:SS'),
                    'YYYY-MM-DD HH24:MI:SS');
        ELSE
            log_synthesized_name := FORMAT('postgresql-%s.log', TO_CHAR(CURRENT_DATE, 'Dy'));
        END IF;

        ts_current := (ts_current AT TIME ZONE 'UTC')::TIMESTAMP;
        time_diff := ts_current - DATE_TRUNC('hour', ts_current);

        IF (time_diff BETWEEN INTERVAL '-5 minutes' AND INTERVAL '5 minutes') THEN
            start_time := DATE_TRUNC('hour', ts_current) - INTERVAL '2 hours';
            end_time := DATE_TRUNC('hour', ts_current);
            RAISE NOTICE 'Interval: % - %', start_time, end_time;
        ELSE
            NULL;
        END IF;

        IF start_time IS NULL THEN
            start_time := DATE_TRUNC('hour', ts_current) - INTERVAL '2 hours';
            end_time := DATE_TRUNC('hour', ts_current);
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
        SELECT 'y_connect'          AS conn_name,
               s.Pk_Id_Conn         AS serv_id,
               s.Conn_Port          AS serv_port,
               s.Conn_Host          AS serv_host,
               REGEXP_REPLACE(
                       FORMAT('%s--%s', s.Conn_Host, s.Conn_Port),
                       '\.maxbit\.private', '', 'g'
               ) || '--' || TO_CHAR(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD--HH24-MI') || '.html',
               pi.Path_Out_Dir      AS out_dir,
               pi.Path_Out_Log_File AS out_file,
               pi.Path_Pgbg         AS pgbg_path,
               log_synthesized_name,
               start_time AT TIME ZONE 'UTC',
               end_time AT TIME ZONE 'UTC'
        FROM Robohub.Reference."Servers" s
                 JOIN Pg_Ini pi ON pi.Fk_Pk_Id_Conn = s.Pk_Id_Conn
        WHERE (s.Switch_Serv & B'00100000') = B'00100000';

        FOR omega IN SELECT * FROM tmp_info_table LOOP
            BEGIN
                IF omega.conn_name IN (SELECT UNNEST(Robohub.Public.Dblink_Get_Connections())) THEN
                    PERFORM Robohub.Public.Dblink_Disconnect(omega.conn_name);
                END IF;

                INSERT INTO Pgbadger_Repo_Slicer.Pg_Barn (Fk_Pk_Id_Conn, Log_Name)
                VALUES (omega.serv_id, omega.compl_name)
                RETURNING Pk_Id_Barn INTO id_barn_flag;

                json_flight := JSONB_BUILD_OBJECT(
                        'pgbg_path', omega.pgbg_path,
                        'log_file', omega.out_file || log_synthesized_name,
                        'out_dir_slice', omega.out_dir,
                        'out_file_slice', omega.compl_name,
                        'begin_dattime', TO_CHAR(omega.start_time, 'YYYY-MM-DD HH24:MI:SS'),
                        'end_dattime', TO_CHAR(omega.end_time, 'YYYY-MM-DD HH24:MI:SS')
                );

                time_start := EXTRACT(EPOCH FROM CLOCK_TIMESTAMP());

                PERFORM Robohub.Public.Dblink_Connect(
                        omega.conn_name,
                        FORMAT(
                                'dbname=%s user=%s password=%s host=%s port=%s',
                                'postgres', 'robo_sudo', '%dFgH8!zX4&kLmT2',
                                omega.serv_host,
                                omega.serv_port::INT
                        )
                );

                result_json := Pgbadger_Report_Slicer(json_flight);

                RAISE NOTICE 'Результат: %', result_json ->> 'pgbadger_command';

                UPDATE Pgbadger_Repo_Slicer.Pg_Barn
                SET Log_Timer_Slicing = EXTRACT(EPOCH FROM CLOCK_TIMESTAMP()) - time_start
                WHERE Pk_Id_Barn = id_barn_flag;

                IF omega.conn_name IN (SELECT UNNEST(Robohub.Public.Dblink_Get_Connections())) THEN
                    PERFORM Robohub.Public.Dblink_Disconnect(omega.conn_name);
                END IF;
            EXCEPTION
                WHEN OTHERS THEN
                    GET STACKED DIAGNOSTICS
                        err_mess = MESSAGE_TEXT,
                        err_det = PG_EXCEPTION_DETAIL,
                        err_cd = RETURNED_SQLSTATE;

                    INSERT INTO Robohub.Pgbadger_Repo_Slicer."Errors" (Fk_Pk_Id_Conn, Slice_Err_Code,
                                                                       Slice_Err_Detail, Slice_Err_Mess,
                                                                       Slice_Now_Ins)
                    VALUES (omega.serv_id, err_cd, err_det, err_mess, NOW());
                    RETURN;
            END;
        END LOOP;

        result_json := Pgbadger_Report_Slicer(json_flight);

        RAISE NOTICE 'Результат: %', result_json ->> 'pgbadger_command';

        UPDATE Pgbadger_Repo_Slicer.Pg_Barn
        SET Log_Timer_Slicing = EXTRACT(EPOCH FROM CLOCK_TIMESTAMP()) - time_start
        WHERE Pk_Id_Barn = id_barn_flag;

        IF omega.conn_name IN (SELECT UNNEST(Robohub.Public.Dblink_Get_Connections())) THEN
            PERFORM Robohub.Public.Dblink_Disconnect(omega.conn_name);
        END IF;
    END;
$$;



/*
PERFORM Robohub.Public.Dblink_Connect(
omega.conn_name,
FORMAT(
'dbname=%s user=%s password=%s host=%s port=%s',
'postgres', 'robo_sudo', '%dFgH8!zX4&kLmT2',
omega.serv_host,
omega.serv_port::INT
)
);
EXCEPTION
WHEN OTHERS THEN
GET STACKED DIAGNOSTICS
err_mess = MESSAGE_TEXT,
err_det = PG_EXCEPTION_DETAIL,
err_cd = RETURNED_SQLSTATE;

INSERT INTO Robohub.Pgbadger_Repo_Slicer."Errors" (Fk_Pk_Id_Conn, Slice_Err_Code,
Slice_Err_Detail, Slice_Err_Mess,
Slice_Now_Ins)
VALUES (omega.serv_id, err_cd,
err_det, err_mess, NOW());
RETURN;
END;
*/


/*
                RAISE NOTICE '%', (SELECT json_agg(t) FROM tmp_info_table t);
               {
                "conn_name":"y_connect",
                "serv_id":3,
                "serv_port":5434,
                "serv_host":"prd-chat-pg-02.maxbit.private",
                "compl_name":"prd-chat-pg-02--5434--2025-04-06--09-27.html",
                "out_dir":"/home/reports/",
                "out_file":"/var/lib/postgresql/16/mbss/log/",
                "pgbg_path":"/usr/bin/pgbadger"
                }
*/


/*

                     INSERT INTO Robohub.Pgbadger_Repo_Slicer."Errors" (Fk_Pk_Id_Conn,
                                                                       Slice_Err_Code,
                                                                       Slice_Err_Detail,
                                                                       Slice_Err_Mess,
                                                                       Slice_Now_Ins)
                    VALUES (omega.serv_id,
                            0,
                            'No log files found',
                            '',
                            NOW());
 */