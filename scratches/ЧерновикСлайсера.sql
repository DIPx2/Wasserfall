как на plpgsql написать такую функцию FN: один раз каждые два часа, примерно на начало каждого временного интервала в течении суток будет вызываться функция FN. В ней  проверить текущее время, если текущее время равно времени кратному 2 в интервале плюс-минус 5 минут, то переменной start_time присвоить значение начала этого часового интервала - два часа на начало часа, а переменной end_time присвоить значение кратное 2 этого часового интервала? Например, в 02:00:01 вызsвается функция FN, переменной start_time присвоить 00:00:00 а переменной end_time присвоить 02:00:00


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
    begin_dattime        TEXT;
    end_dattime          TEXT;

BEGIN
    CREATE TEMP TABLE tmp_info_table ON COMMIT DROP AS
    SELECT
        'y_connect' AS conn_name,
        Robohub.Reference."Servers".Pk_Id_Conn AS serv_id,
        Robohub.Reference."Servers".Conn_Port AS serv_port,
        Robohub.Reference."Servers".Conn_Host AS serv_host,
        REGEXP_REPLACE(
            FORMAT('%s--%s', Robohub.Reference."Servers".Conn_Host, Robohub.Reference."Servers".Conn_Port),
            '\.maxbit\.private', '', 'g'
        ) || '--' || TO_CHAR(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD--HH24-MI') || '.html' AS compl_name,
        Path_Out_Dir AS out_dir,
        Path_Out_Log_File AS out_file,
        Path_Pgbg AS pgbg_path
    FROM Robohub.Reference."Servers"
    JOIN Pg_Ini
        ON Pg_Ini.Fk_Pk_Id_Conn = Robohub.Reference."Servers".Pk_Id_Conn
    WHERE (Robohub.Reference."Servers".Switch_Serv & B'00100000') = B'00100000';


    IF NOW()::TIME BETWEEN TIME '00:00:00' AND TIME '00:10:00' THEN
        log_synthesized_name := FORMAT('postgresql-%s.log', TO_CHAR((CURRENT_DATE - INTERVAL '1 day'), 'Dy'));
        begin_dattime := FORMAT('%s 22:00:00', (CURRENT_DATE - INTERVAL '1 day')::DATE);
        end_dattime := FORMAT('%s 23:59:59', (CURRENT_DATE - INTERVAL '1 day')::DATE);
    ELSE
        log_synthesized_name := FORMAT('postgresql-%s.log', TO_CHAR(CURRENT_DATE, 'Dy'));
    END IF;



    -- Обработка временной таблицы
    FOR omega IN SELECT * FROM tmp_info_table
    LOOP
        -- Проверка существующего подключения
        IF omega.conn_name IN (SELECT UNNEST(Robohub.Public.Dblink_Get_Connections())) THEN
            PERFORM Robohub.Public.Dblink_Disconnect(omega.conn_name);
        END IF;

        -- Вставка данных в основную таблицу
        INSERT INTO Pgbadger_Repo_Slicer.Pg_Barn (Fk_Pk_Id_Conn, Log_Name)
        VALUES (omega.serv_id, omega.compl_name)
        RETURNING Pk_Id_Barn INTO id_barn_flag;

        -- Создание JSON-объекта
        SELECT JSONB_BUILD_OBJECT(
                   'pgbg_path', omega.pgbg_path,
                   'log_file', omega.out_file,
                   'out_dir_slice', omega.out_dir || omega.compl_name
               )
        INTO json_flight;

        -- Засечение времени
        time_start := EXTRACT(EPOCH FROM CLOCK_TIMESTAMP());

        -- Вызов процедуры
        PERFORM Pgbadger_Report_Slicer(json_flight);

        -- Обновление информации
        UPDATE Pgbadger_Repo_Slicer.Pg_Barn
        SET Log_Timer_Slicing = EXTRACT(EPOCH FROM CLOCK_TIMESTAMP()) - time_start
        WHERE Pk_Id_Barn = id_barn_flag;

        -- Отключение соединения
        IF omega.conn_name IN (SELECT UNNEST(Robohub.Public.Dblink_Get_Connections())) THEN
            PERFORM Robohub.Public.Dblink_Disconnect(omega.conn_name);
        END IF;
    END LOOP;
END;
$$;


/*
DO
$$
    DECLARE
        conn_name TEXT DEFAULT 'y_connect';

    BEGIN
        IF conn_name IN (SELECT UNNEST(Robohub.Public.Dblink_Get_Connections())) THEN
            PERFORM Robohub.Public.Dblink_Disconnect(conn_name);
            PERFORM Robohub.Public.Dblink_Connect(conn_name, FORMAT('dbname=%s user=%s password=%s host=%s port=%s', 'postgres', 'robo_sudo', '%dFgH8!zX4&kLmT2', 'prd-chat-pg-02.maxbit.private', 5434));

            PERFORM dblink_exec( conn_name, $sql$ COPY (SELECT 'NULL') TO PROGRAM '/usr/bin/pgbadger --version >> /home/robo_sudo/pg_ver.txt'; $sql$ );


            PERFORM Robohub.Public.Dblink_Disconnect(conn_name);
        END IF;
    END;

$$
*/
/*
DO
$$
    DECLARE
        conn_name         TEXT DEFAULT 'y_connect';
        Err_Mess          TEXT;
        Err_Det           TEXT;
        Err_Cd            TEXT;
        n_Pk_Id_Conn      INTEGER;
        n_Conn_Host       TEXT;
        n_Conn_Port       INTEGER;
        Connection_String TEXT;
        v_name            TEXT;
        v_size            INTEGER;
        v_modification    TIMESTAMPTZ;
        compl_name        TEXT;
        flag              INTEGER;
        time_start        TIMESTAMPTZ;
        stmt TEXT;


    BEGIN
        FOR n_Pk_Id_Conn, n_Conn_Host, n_Conn_Port IN
            SELECT Pk_Id_Conn, Conn_Host, Conn_Port
            FROM Robohub.Reference."Servers"
            WHERE (Switch_Serv & B'00100000') = B'00100000'
            LOOP

                IF conn_name IN (SELECT UNNEST(Robohub.Public.Dblink_Get_Connections())) THEN
                    PERFORM Robohub.Public.Dblink_Disconnect(conn_name);
                END IF;

                BEGIN
                    SELECT INTO Connection_String FORMAT('dbname=%s user=%s password=%s host=%s port=%s', 'postgres',
                                                         'robo_sudo', '%dFgH8!zX4&kLmT2', n_Conn_Host, n_Conn_Port);

                    PERFORM Robohub.Public.Dblink_Connect(conn_name, Connection_String);

                EXCEPTION
                    WHEN OTHERS THEN
                        GET STACKED DIAGNOSTICS Err_Mess = MESSAGE_TEXT, Err_Det = PG_EXCEPTION_DETAIL, Err_Cd = RETURNED_SQLSTATE;
                        INSERT INTO Robohub.Pgbadger_Repo_Slicer."Errors" (Pk_Id_Slice_Err,
                                                                           Fk_Pk_Id_Conn,
                                                                           Slice_Err_Code,
                                                                           Slice_Err_Detail,
                                                                           Slice_Err_Mess,
                                                                           Slice_Now_Ins)
                        VALUES (DEFAULT, n_Pk_Id_Conn, Err_Cd, Err_Det, Err_Mess, DEFAULT);
                        RETURN;
                END;
                SELECT rept.NAME, rept.size, rept.modification
                INTO v_name, v_size, v_modification
                FROM Robohub.Public.Dblink(conn_name,
                                           'SELECT "name", "size", "modification"
                                           FROM Pg_Ls_Logdir()
                                           WHERE "modification"::DATE = CURRENT_DATE
                                               AND RIGHT("name", 4) = ''.log''')
                         AS rept (NAME TEXT, size INTEGER, modification TIMESTAMPTZ);


                IF v_name IS NULL THEN
                    RAISE NOTICE 'No data found.';
                    INSERT INTO Robohub.Pgbadger_Repo_Slicer."Errors" (Pk_Id_Slice_Err,
                                                                       Fk_Pk_Id_Conn,
                                                                       Slice_Err_Code,
                                                                       Slice_Err_Detail,
                                                                       Slice_Err_Mess,
                                                                       Slice_Now_Ins)
                    VALUES (DEFAULT, n_Pk_Id_Conn, DEFAULT, 'No log files found', DEFAULT, DEFAULT);
                    RETURN;
                END IF;


                compl_name = REGEXP_REPLACE(FORMAT('%s--%s', n_Conn_Host, n_Conn_Port), '\.maxbit\.private', '', 'g') ||
                             '--' || TO_CHAR(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD--HH24-MI') || '.html';

                INSERT INTO Pgbadger_Repo_Slicer.Pg_Barn (Pk_Id_Barn, Fk_Pk_Id_Conn, Ins_Date, Log_Mod, Log_Name,
                                                          Log_Size, Log_Slice_Name, Log_Slice_Size)
                VALUES (DEFAULT, n_Pk_Id_Conn, DEFAULT, v_modification, v_name, v_size, compl_name, DEFAULT)
                RETURNING Pk_Id_Barn INTO flag;

                time_start = CLOCK_TIMESTAMP();

             --EXECUTE $y$ COPY (SELECT NULL) TO PROGRAM '/bin/bash -c "sudo -u robo_sudo /usr/bin/pgbadger --begin "2025-04-03 08:00:00" --end "2025-04-03 08:30:00" --outdir /home/reports/ --outfile report-experiment-xyz.html /home/robo_sudo/postgresql-Thu.log" >> /home/robo_sudo/pgbadger.log 2>&1 '$y$;


--EXECUTE $y$ COPY (SELECT NULL) TO PROGRAM '/usr/bin/pgbadger --begin "2025-04-03 08:00:00" --end "2025-04-03 08:30:00" --outdir /home/reports/ --outfile report-experiment-xyz.html /home/robo_sudo/postgresql-Thu.log >> /home/robo_sudo/pgbadger.log $y$;


COPY (SELECT 'NULL') TO PROGRAM '/usr/bin/pgbadger --version >> /home/robo_sudo/pg_ver.txt';



                UPDATE Pgbadger_Repo_Slicer.Pg_Barn
                SET Log_Slicing = CLOCK_TIMESTAMP() - time_start
                WHERE Pk_Id_Barn = flag;

                PERFORM Robohub.Public.Dblink_Disconnect(conn_name);

            END LOOP; -- SERVERS
    END;
$$
*/


--su - robo_sudo
--/usr/bin/pgbadger --begin "2025-04-03 08:00:00" --end "2025-04-03 08:30:00" --outdir /mnt/pgbadger_reports/ --outfile report-experiment-xyz.html /var/lib/postgresql/16/mbss/log/postgresql-Thu.log 2>&1 >> /home/robo_sudo/pgbadger_error.log
/*
 DO
$$
    DECLARE
        Err_Mess       TEXT;
        Err_Det        TEXT;
        Err_Cd         TEXT;
        n_Pk_Id_Conn   INTEGER;
        n_Conn_Host    TEXT;
        n_Conn_Port    INTEGER;
        v_name         TEXT;
        v_size         INTEGER;
        v_modification TIMESTAMPTZ;
        compl_name     TEXT;
        flag           INTEGER;
        time_start     TIMESTAMPTZ;

    BEGIN
        FOR n_Pk_Id_Conn, n_Conn_Host, n_Conn_Port IN
            SELECT Pk_Id_Conn, Conn_Host, Conn_Port
            FROM Robohub.Reference."Servers"
            WHERE (Switch_Serv & B'00100000') = B'00100000'
            LOOP
                BEGIN

                    EXECUTE FORMAT($f$
                SELECT name, size, modification
                FROM %I
                WHERE modification::DATE = CURRENT_DATE
                  AND RIGHT(name, 4) = '.log'
            $f$, n_Conn_Host, n_Conn_Port)
                        INTO v_name, v_size, v_modification;

                EXCEPTION
                    WHEN OTHERS THEN
                        GET STACKED DIAGNOSTICS Err_Mess = MESSAGE_TEXT, Err_Det = PG_EXCEPTION_DETAIL, Err_Cd = RETURNED_SQLSTATE;
                        INSERT INTO Robohub.Pgbadger_Repo_Slicer."Errors" (Pk_Id_Slice_Err,
                                                                           Fk_Pk_Id_Conn,
                                                                           Slice_Err_Code,
                                                                           Slice_Err_Detail,
                                                                           Slice_Err_Mess,
                                                                           Slice_Now_Ins)
                        VALUES (DEFAULT, n_Pk_Id_Conn, Err_Cd, Err_Det, Err_Mess, DEFAULT);
                        RETURN;
                END;

                IF v_name IS NULL THEN
                    INSERT INTO Robohub.Pgbadger_Repo_Slicer."Errors" (Pk_Id_Slice_Err,
                                                                       Fk_Pk_Id_Conn,
                                                                       Slice_Err_Code,
                                                                       Slice_Err_Detail,
                                                                       Slice_Err_Mess,
                                                                       Slice_Now_Ins)
                    VALUES (DEFAULT, n_Pk_Id_Conn, DEFAULT, 'No log files found', DEFAULT, DEFAULT);
                    RETURN;
                END IF;

                compl_name = REGEXP_REPLACE(
                                     FORMAT('%s--%s', n_Conn_Host, n_Conn_Port),
                                     '\.maxbit\.private',
                                     '',
                                     'g') || '--' || TO_CHAR(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD--HH24-MI') ||
                             '.html';

                INSERT INTO Pgbadger_Repo_Slicer.Pg_Barn (Pk_Id_Barn,
                                                          Fk_Pk_Id_Conn,
                                                          Ins_Date,
                                                          Log_Mod,
                                                          Log_Name,
                                                          Log_Size,
                                                          Log_Slice_Name,
                                                          Log_Slice_Size)
                VALUES (DEFAULT, n_Pk_Id_Conn, DEFAULT, v_modification, v_name, v_size, compl_name, DEFAULT)
                RETURNING Pk_Id_Barn INTO flag;

                time_start = CLOCK_TIMESTAMP();


                UPDATE Pgbadger_Repo_Slicer.Pg_Barn
                SET Log_Slicing = CLOCK_TIMESTAMP() - time_start
                WHERE Pk_Id_Barn = flag;

            END LOOP;
    END;
$$;

*/

/*
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

CREATE SERVER prd_chat_pg_02
    FOREIGN DATA WRAPPER Postgres_Fdw
    OPTIONS (host 'prd-chat-pg-02.maxbit.private', port '5434', dbname 'postgres', application_name 'robo_slicer');

CREATE USER MAPPING FOR CURRENT_USER
    SERVER prd_chat_pg_02
    OPTIONS (user 'robo_sudo', password '%dFgH8!zX4&kLmT2');

CREATE USER MAPPING FOR robo_sudo
    SERVER prd_chat_pg_02
    OPTIONS (user 'robo_sudo', password '%dFgH8!zX4&kLmT2');
*/


/*
DO
$$
    DECLARE
        conn_name         TEXT DEFAULT 'y_connect';
        Err_Mess          TEXT;
        Err_Det           TEXT;
        Err_Cd            TEXT;
        n_Pk_Id_Conn      INTEGER;
        n_Conn_Host       TEXT;
        n_Conn_Port       INTEGER;
        Connection_String TEXT;
        v_name            TEXT;
        v_size            INTEGER;
        v_modification    TIMESTAMPTZ;
        compl_name        TEXT;
        flag              INTEGER;
        time_start        TIMESTAMPTZ;


    BEGIN
        FOR n_Pk_Id_Conn, n_Conn_Host, n_Conn_Port IN
            SELECT Pk_Id_Conn, Conn_Host, Conn_Port
            FROM Robohub.Reference."Servers"
            WHERE (Switch_Serv & B'00100000') = B'00100000'
            LOOP

                IF conn_name IN (SELECT UNNEST(Robohub.Public.Dblink_Get_Connections())) THEN
                    PERFORM Robohub.Public.Dblink_Disconnect(conn_name);
                END IF;

                BEGIN
                    SELECT INTO Connection_String FORMAT('dbname=%s user=%s password=%s host=%s port=%s', 'postgres',
                                                         'robo_sudo', '%dFgH8!zX4&kLmT2', n_Conn_Host, n_Conn_Port);

                    PERFORM Robohub.Public.Dblink_Connect(conn_name, Connection_String);

                EXCEPTION
                    WHEN OTHERS THEN
                        GET STACKED DIAGNOSTICS Err_Mess = MESSAGE_TEXT, Err_Det = PG_EXCEPTION_DETAIL, Err_Cd = RETURNED_SQLSTATE;
                        INSERT INTO Robohub.Pgbadger_Repo_Slicer."Errors" (Pk_Id_Slice_Err,
                                                                           Fk_Pk_Id_Conn,
                                                                           Slice_Err_Code,
                                                                           Slice_Err_Detail,
                                                                           Slice_Err_Mess,
                                                                           Slice_Now_Ins)
                        VALUES (DEFAULT, n_Pk_Id_Conn, Err_Cd, Err_Det, Err_Mess, DEFAULT);
                        RETURN;
                END;
                SELECT rept.NAME, rept.size, rept.modification
                INTO v_name, v_size, v_modification
                FROM Robohub.Public.Dblink(conn_name,
                                           'SELECT "name", "size", "modification"
                                           FROM Pg_Ls_Logdir()
                                           WHERE "modification"::DATE = CURRENT_DATE
                                               AND RIGHT("name", 4) = ''.log''')
                         AS rept (NAME TEXT, size INTEGER, modification TIMESTAMPTZ);


                IF v_name IS NULL THEN
                    RAISE NOTICE 'No data found.';
                    INSERT INTO Robohub.Pgbadger_Repo_Slicer."Errors" (Pk_Id_Slice_Err,
                                                                       Fk_Pk_Id_Conn,
                                                                       Slice_Err_Code,
                                                                       Slice_Err_Detail,
                                                                       Slice_Err_Mess,
                                                                       Slice_Now_Ins)
                    VALUES (DEFAULT, n_Pk_Id_Conn, DEFAULT, 'No log files found', DEFAULT, DEFAULT);
                    RETURN;
                END IF;


                compl_name = REGEXP_REPLACE(FORMAT('%s--%s', n_Conn_Host, n_Conn_Port), '\.maxbit\.private', '', 'g') ||
                             '--' || TO_CHAR(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD--HH24-MI') || '.html';

                INSERT INTO Pgbadger_Repo_Slicer.Pg_Barn (Pk_Id_Barn, Fk_Pk_Id_Conn, Ins_Date, Log_Mod, Log_Name,
                                                          Log_Size, Log_Slice_Name, Log_Slice_Size)
                VALUES (DEFAULT, n_Pk_Id_Conn, DEFAULT, v_modification, v_name, v_size, compl_name, DEFAULT)
                RETURNING Pk_Id_Barn INTO flag;

                time_start = CLOCK_TIMESTAMP();


                --PERFORM Robohub.Public.Dblink_Exec( conn_name, E'COPY (SELECT NULL) TO PROGRAM \'/bin/bash -c "sudo -u robo_sudo /usr/bin/pgbadger --begin "2025-04-03 08:00:00" --end "2025-04-03 08:30:00" --outdir /mnt/pgbadger_reports/ --outfile report-experiment-xyz.html /var/lib/postgresql/16/mbss/log/postgresql-Thu.log 2>&1 >> /home/robo_sudo/pgbadger_error.log"\'' );

                UPDATE Pgbadger_Repo_Slicer.Pg_Barn
                SET Log_Slicing = CLOCK_TIMESTAMP() - time_start
                WHERE Pk_Id_Barn = flag;

                PERFORM Robohub.Public.Dblink_Disconnect(conn_name);

            END LOOP; -- SERVERS
    END;
$$
*/


/*

create table pgbadger_repo_slicer.pg_barn
(
    pk_id_barn     serial
        primary key,
    fk_pk_id_conn  integer  not null     references reference."Servers"            on delete cascade,
    ins_date       bigint  default (EXTRACT(epoch FROM now()))::bigint not null,
    log_slicing     INTERVAL NOT NULL DEFAULT '1970-05-11 00:00:00'::TIMESTAMP - '1970-01-01 00:00:00'::TIMESTAMP,
    log_size       integer                                             not null,
    log_slice_size integer default 0                                   not null,
    log_mod        timestamp with time zone                            not null,
    log_name       text                                                not null,
    log_slice_name text    default 'nihil'::text                       not null

);

create table pgbadger_repo_slicer."Errors"
(
    pk_id_slice_err  integer generated always as identity primary key,
    fk_pk_id_conn   integer not null references robohub.reference."Servers" on delete cascade,
    slice_now_ins    bigint       default EXTRACT(epoch FROM now()),
    slice_err_label  varchar(250) default '*'::bpchar,
    slice_err_detail text         default '*'::text,
    slice_err_code   text         default '*'::text
);

alter table pgbadger_repo_slicer."Errors"
    owner to gtimofeyev;


DO
$$
    DECLARE
        compl_name           TEXT;
        connection_string    TEXT;
        command_job          TEXT;
        begin_dattime        TEXT;
        end_dattime          TEXT;
        log_synthesized_name TEXT;
        n_Pk_Id_Conn         INTEGER;
        n_Conn_Host          VARCHAR(100);
        n_Conn_Port          INTEGER;
        i                    TIME(0) = CAST(NOW() AS TIME(0));
    BEGIN
        -- Для каждого сервера с битом "pgbadger slicer" = ON
        FOR n_Pk_Id_Conn, n_Conn_Host, n_Conn_Port IN
            SELECT Pk_Id_Conn, Conn_Host, Conn_Port
            FROM Robohub.Reference."Servers"
            WHERE (Switch_Serv & B'00100000') = B'00100000'
            LOOP
                -- Заполнить переменные
                SELECT INTO compl_name
                    REGEXP_REPLACE(FORMAT('%s--%s', n_Conn_Host, n_Conn_Port), '\.maxbit\.private', '', 'g') || '--' ||
                    TO_CHAR(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD--HH24-MI') || '.html';

                SELECT INTO connection_string FORMAT('dbname=%s user=%s password=%s host=%s port=%s', 'postgres',
                                                     'robo_sudo', '%dFgH8!zX4&kLmT2', n_Conn_Host, n_Conn_Port);

                log_synthesized_name = FORMAT('postgresql-%s.log', TO_CHAR(CURRENT_DATE, 'Dy'));

                IF i BETWEEN TIME '00:00:00' AND TIME '00:10:00' THEN
                    log_synthesized_name =
                            FORMAT('postgresql-%s.log', TO_CHAR((CURRENT_DATE - INTERVAL '1 day'), 'Dy'));
                    begin_dattime = FORMAT('%s 22:00:00', (CURRENT_DATE - INTERVAL '1 day')::DATE);
                    end_dattime = FORMAT('%s 23:59:59', (CURRENT_DATE - INTERVAL '1 day')::DATE);

                ELSIF i BETWEEN TIME '22:00:00' AND TIME '22:10:00' THEN
                    begin_dattime = FORMAT('%s 20:00:00', CURRENT_DATE::DATE);
                    end_dattime = FORMAT('%s 22:00:00', CURRENT_DATE::DATE);

                ELSIF i BETWEEN TIME '20:00:00' AND TIME '20:10:00' THEN
                    begin_dattime = FORMAT('%s 18:00:00', CURRENT_DATE::DATE);
                    end_dattime = FORMAT('%s 20:00:00', CURRENT_DATE::DATE);

                ELSIF i BETWEEN TIME '18:00:00' AND TIME '18:10:00' THEN
                    begin_dattime = FORMAT('%s 16:00:00', CURRENT_DATE::DATE);
                    end_dattime = FORMAT('%s 18:00:00', CURRENT_DATE::DATE);

                ELSIF i BETWEEN TIME '16:00:00' AND TIME '16:10:00' THEN
                    begin_dattime = FORMAT('%s 14:00:00', CURRENT_DATE::DATE);
                    end_dattime = FORMAT('%s 16:00:00', CURRENT_DATE::DATE);

                ELSIF i BETWEEN TIME '14:00:00' AND TIME '14:10:00' THEN
                    begin_dattime = FORMAT('%s 12:00:00', CURRENT_DATE::DATE);
                    end_dattime = FORMAT('%s 14:00:00', CURRENT_DATE::DATE);

                ELSIF i BETWEEN TIME '12:00:00' AND TIME '12:10:00' THEN
                    begin_dattime = FORMAT('%s 10:00:00', CURRENT_DATE::DATE);
                    end_dattime = FORMAT('%s 12:00:00', CURRENT_DATE::DATE);

                ELSIF i BETWEEN TIME '10:00:00' AND TIME '10:10:00' THEN
                    begin_dattime = FORMAT('%s 08:00:00', CURRENT_DATE::DATE);
                    end_dattime = FORMAT('%s 10:00:00', CURRENT_DATE::DATE);

                ELSIF i BETWEEN TIME '08:00:00' AND TIME '08:10:00' THEN
                    begin_dattime = FORMAT('%s 06:00:00', CURRENT_DATE::DATE);
                    end_dattime = FORMAT('%s 08:00:00', CURRENT_DATE::DATE);

                ELSIF i BETWEEN TIME '06:00:00' AND TIME '06:10:00' THEN
                    begin_dattime = FORMAT('%s 04:00:00', CURRENT_DATE::DATE);
                    end_dattime = FORMAT('%s 06:00:00', CURRENT_DATE::DATE);

                ELSIF i BETWEEN TIME '04:00:00' AND TIME '04:10:00' THEN
                    begin_dattime = FORMAT('%s 02:00:00', CURRENT_DATE::DATE);
                    end_dattime = FORMAT('%s 04:00:00', CURRENT_DATE::DATE);

                ELSIF i BETWEEN TIME '02:00:00' AND TIME '02:10:00' THEN
                    begin_dattime = FORMAT('%s 00:00:00', CURRENT_DATE::DATE);
                    end_dattime = FORMAT('%s 02:00:00', CURRENT_DATE::DATE);
                ELSE
                    NULL;
                    begin_dattime = FORMAT('%s 00:00:00', CURRENT_DATE::DATE);
                    end_dattime = FORMAT('%s 02:00:00', CURRENT_DATE::DATE);
                    NULL;
                END IF;

                SELECT INTO command_job FORMAT('PERFORM pg_catalog.pg_copy_to_program ( E''' || Path_Pgbg ||
                                               ' --begin "' || begin_dattime || '" --end "' || end_dattime ||
                                               '" --outdir ' ||
                                               Path_Out_Dir || ' --outfile ' || compl_name || ' ' ||
                                               Path_Out_Log_File || log_synthesized_name || ' 2>&1'' )')
                FROM Robohub.Pgbadger_Repo_Slicer.Pg_Ini
                WHERE Fk_Pk_Id_Conn = n_Pk_Id_Conn;

                <<PROCESSING>>
                    DECLARE
                    conn_name        TEXT DEFAULT 'y_connect';
                    Err_Mess         TEXT;
                    Err_Det          TEXT;
                    Err_Cd           TEXT;
                    log_real_name    TEXT;
                    log_size         BIGINT;
                    Log_modification TIMESTAMP;
                BEGIN
                    IF conn_name IN (SELECT UNNEST(Robohub.Public.Dblink_Get_Connections())) THEN
                        PERFORM Robohub.Public.Dblink_Disconnect(conn_name);
                    END IF;
                    PERFORM Robohub.Public.Dblink_Connect(conn_name, connection_string);
                EXCEPTION
                    WHEN OTHERS THEN
                        GET STACKED DIAGNOSTICS Err_Mess = MESSAGE_TEXT, Err_Det = PG_EXCEPTION_DETAIL, Err_Cd = RETURNED_SQLSTATE;
                        INSERT INTO Robohub.Pgbadger_Repo_Slicer."Errors" (Fk_Pk_Id_Pgini, Pk_Id_Slice_Err,
                                                                           Slice_Err_Code, Slice_Err_Detail,
                                                                           Slice_Err_Label, Slice_Err_Message,
                                                                           Slice_Now_Ins)
                        VALUES (DEFAULT, n_Pk_Id_Conn, Err_Cd, Err_Det, Err_Mess, DEFAULT);
                        RETURN;

                        FOR log_real_name, log_size, Log_modification IN SELECT "name", "size", "modification"
                                                                         FROM Pg_Ls_Logdir()
                                                                         WHERE "name" = log_synthesized_name
                            LOOP
                                NULL;
                            END LOOP;


                END; -- <<PROCESSING>>
            END LOOP; -- SERVERS
    END; -- SCRIPT
$$;




create table pgbadger_repo_slicer."Errors"
(
    pk_id_slice_err integer generated always as identity primary key,
    fk_pk_id_pgini integer not null references pgbadger_repo_slicer."pg_ini" on delete cascade,
    slice_now_ins BIGINT DEFAULT EXTRACT(EPOCH FROM now()),
    slice_err_label     varchar(250) default '*'::bpchar,
    slice_err_message   text         default '*'::text,
    slice_err_detail    text         default '*'::text,
    slice_err_code      text         default '*'::text
);

SELECT * FROM pg_ls_logdir();

create table pgbadger_reports.pg_barn
(
    pk_id_barn serial primary key,
    fk_pk_id_conn integer not null references reference."Servers" on delete cascade,
    ins_date bigint DEFAULT EXTRACT(EPOCH FROM now())::bigint NOT NULL,
    log_piece_size integer DEFAULT 0 not null,
    log_size integer DEFAULT 0 not null,
    report_name   text  DEFAULT 'nihil' not null

);

DO
$$
    DECLARE
        compl_name          TEXT;
        connection_string TEXT;
        command_job TEXT;
        begin_dattime                  TEXT = '2025-03-24 07:45:00';
        end_dattime                  TEXT = '2025-03-24 08:30:00';
        log_synthesized_name                  TEXT = 'postgresql-Mon.log';
    BEGIN

        SELECT INTO compl_name REGEXP_REPLACE(FORMAT('%s--%s', Conn_Host, Conn_Port), '\.maxbit\.private', '', 'g') || '--' || TO_CHAR(NOW(), 'YYYY-MM-DD--HH24-MI') || '.html'
        FROM Robohub.Reference."Servers" WHERE (Switch_Serv & B'00100000') = B'00100000';

        FOR connection_string, command_job IN
            SELECT FORMAT('dbname=%s user=%s password=%s host=%s port=%s', 'postgres', 'robo_sudo', '%dFgH8!zX4&kLmT2', Conn_Host, Conn_Port),
                   FORMAT('PERFORM pg_catalog.pg_copy_to_program ( E''' || Path_Pgbg || ' --begin "' || begin_dattime ||
                          '" --end "' || end_dattime || '" --outdir ' || Path_Out_Dir || ' --outfile ' || compl_name || ' ' || Path_Out_Log_File || log_synthesized_name || ' 2>&1'' )')
            FROM Robohub.Reference."Servers"
                     JOIN Pgbadger_Reports.Pg_Ini Pi ON "Servers".Pk_Id_Conn = pi.Fk_Pk_Id_Conn
            WHERE (Switch_Serv & B'00100000') = B'00100000'
            LOOP
                RAISE NOTICE '%', compl_name;
            END LOOP;
    END;
$$;
*/
/*DO
$$
    DECLARE
        compl_name          TEXT DEFAULT 'мама_мыла_раму';
        connection_string TEXT;
        command_job TEXT;

        begin_dattime TEXT = '2025-03-24 07:45:00';
        end_dattime TEXT = '2025-03-24 08:30:00';

        log_synthesized_name TEXT = 'postgresql-Mon.log';

    BEGIN
        FOR compl_name, connection_string, command_job IN
            SELECT REGEXP_REPLACE(FORMAT('%s--%s', Conn_Host, Conn_Port), '\.maxbit\.private', '', 'g') || '--' || TO_CHAR(NOW(), 'YYYY-MM-DD--HH24-MI') || '.html',
                   FORMAT('dbname=%s user=%s password=%s host=%s port=%s', 'postgres', 'robo_sudo', '%dFgH8!zX4&kLmT2', Conn_Host, Conn_Port),

                   FORMAT('PERFORM pg_catalog.pg_copy_to_program(E''' || path_pgbg || ' --begin "' || begin_dattime || '" --end "' || end_dattime || '" --outdir ' || path_out_dir || ' --outfile ' || compl_name || ' ' || path_out_log_file || log_synthesized_name || ' 2>&1'' )')

            FROM Robohub.Reference."Servers"
            JOIN  Pgbadger_Reports.Pg_Ini Pi ON "Servers".Pk_Id_Conn = Pi.Fk_Pk_Id_Conn
            WHERE (Switch_Serv & B'00100000') = B'00100000'
            LOOP
                RAISE NOTICE '%', command_job;
            END LOOP;
    END;
$$;
*/

--INSERT INTO Pgbadger_Reports.Pg_Ini (Pk_Id_Pgini, Fk_Pk_Id_Conn, Path_Pgbg, Path_Out_Dir, Path_Out_Log_File)
--VALUES (DEFAULT, 3, $$/usr/bin/pgbadger$$, $$/home/reports/$$, $$/var/lib/postgresql/16/mbss/log/$$);
--PERFORM pg_catalog.pg_copy_to_program(E'/usr/bin/pgbadger --begin "2025-03-24 07:45:00" --end "2025-03-24 08:30:00" --outdir /home/reports/ --outfile /tmp/report-with_timer-Mon.html /var/lib/postgresql/16/mbss/log/postgresql-Mon.log 2>&1')
--PERFORM pg_catalog.pg_copy_to_program(E'/usr/bin/pgbadger --begin "2025-03-24 07:45:00" --end "2025-03-24 08:30:00" --outdir /home/reports/ --outfile /tmp/report-with_timer-Mon.html /var/lib/postgresql/16/mbss/log/postgresql-Mon.log 2>&1')

/*
INSERT INTO Pgbadger_Reports.pg_ini (Pk_Id_Pgini, Fk_Pk_Id_Conn, Connect_Str,Exec_Str_Fl,Exec_Str_Db)
VALUES ( DEFAULT, 3,  $$FORMAT('dbname=%s user=%s password=%s host=%s port=%s', 'postgres', 'robo_sudo', '%dFgH8!zX4&kLmT2', 'prd-chat-pg-02.maxbit.private', 5434)$$,
        $$E'COPY (SELECT \'Запуск pgBadger...\') TO PROGRAM \'/usr/bin/pgbadger --begin "2025-03-24 07:45:00" --end "2025-03-24 08:30:00" --outdir /home/reports/ --outfile /tmp/report-with_timer-Mon.html /var/lib/postgresql/16/mbss/log/postgresql-Mon.log 2>&1\'' $$,
        $$E'INSERT INTO pgbadger_reports (report_name, report_content) VALUES (\'pgBadger Report\', pg_catalog.pg_read_file(\'/tmp/report-with_timer-Mon.html\'))'$$
        );



SELECT * FROM Pgbadger_Reports.pg_ini
*/


/*

CREATE TABLE IF NOT EXISTS pgbadger_reports.pg_rpt
(
    Pk_Id_PgRpt SERIAL PRIMARY KEY,
    Fk_Pk_Id_Conn INTEGER NOT NULL REFERENCES Reference."Servers" ON DELETE CASCADE,
    NowDate TimeStamp DEFAULT (NOW() AT TIME ZONE 'UTC'),
    Rpt TEXT
)

CREATE TABLE Pgbadger_Reports.pg_ini
(
    Pk_Id_PgIni   SERIAL PRIMARY KEY,
    Fk_Pk_Id_Conn INTEGER NOT NULL REFERENCES Reference."Servers" ON DELETE CASCADE,
    path_pgbg   TEXT    NOT NULL,
    path_out_dir   TEXT    NOT NULL,
    path_out_log_file   TEXT    NOT NULL
);
COMMENT ON TABLE Pgbadger_Reports.pg_ini IS 'Родитель - Reference."Servers"';




alter table pgbadger_reports.pg_barn
    owner to gtimofeyev;




COMMENT ON TABLE Pgbadger_Reports.pg_ini IS 'Родитель - Reference."Servers"';
*/

/*
DO
$$
BEGIN
    PERFORM DBLINK_CONNECT('xyz_connect', FORMAT('dbname=%s user=%s password=%s host=%s port=%s', 'postgres', 'robo_sudo', '%dFgH8!zX4&kLmT2', 'prd-chat-pg-02.maxbit.private', 5434));
    PERFORM DBLINK_EXEC( 'xyz_connect', E'COPY (SELECT \'Запуск pgBadger...\') TO PROGRAM \'/usr/bin/pgbadger --begin "2025-03-24 07:45:00" --end "2025-03-24 08:30:00" --outdir /home/reports/ --outfile report-with_timer-Mon.html /var/lib/postgresql/16/mbss/log/postgresql-Mon.log 2>&1\'' );
    PERFORM DBLINK_DISCONNECT('xyz_connect');
END;
$$
LANGUAGE plpgsql;


DO $$

BEGIN
    PERFORM DBLINK_CONNECT( 'xyz_connect', FORMAT('dbname=%s user=%s password=%s host=%s port=%s', 'postgres', 'robo_sudo', '%dFgH8!zX4&kLmT2', 'prd-chat-pg-02.maxbit.private', 5434 ));
    PERFORM DBLINK_EXEC( 'xyz_connect', E'COPY (SELECT \'Запуск pgBadger...\') TO PROGRAM \'/usr/bin/pgbadger --begin "2025-03-24 07:45:00" --end "2025-03-24 08:30:00" --outdir /home/reports/ --outfile /tmp/report-with_timer-Mon.html /var/lib/postgresql/16/mbss/log/postgresql-Mon.log 2>&1\'' );
    PERFORM DBLINK_EXEC( 'xyz_connect', E'INSERT INTO pgbadger_reports (report_name, report_content) VALUES (\'pgBadger Report\', pg_catalog.pg_read_file(\'/tmp/report-with_timer-Mon.html\'))' );
    PERFORM DBLINK_DISCONNECT('xyz_connect');
END;

$$ LANGUAGE plpgsql;


DO $$
DECLARE
    pgbadger_output TEXT;
BEGIN
    PERFORM DBLINK_CONNECT(
        'xyz_connect',
        FORMAT('dbname=%s user=%s password=%s host=%s port=%s',
            'postgres', 'robo_sudo', '%dFgH8!zX4&kLmT2',
            'prd-chat-pg-02.maxbit.private', 5434)
    );

    EXECUTE 'COPY (SELECT pg_catalog.pg_read_file(''/var/lib/postgresql/16/mbss/log/postgresql-Mon.log'', 0, 1000000)) TO STDOUT' INTO pgbadger_output;
    INSERT INTO pgbadger_reports (report_name, report_content) VALUES ('pgBadger Report', pgbadger_output);
    PERFORM DBLINK_DISCONNECT('xyz_connect');

END;
$$ LANGUAGE plpgsql;
*/