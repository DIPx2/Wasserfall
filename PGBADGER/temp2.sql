CREATE OR REPLACE FUNCTION Pgbadger_Report_Slicer(input_json JSONB)
    RETURNS JSONB AS
$$
DECLARE
    output_json JSONB;
BEGIN

NULL;
    --/usr/bin/pgbadger --begin "2025-04-03 08:00:00" --end "2025-04-03 08:30:00" --outdir /mnt/pgbadger_reports/ --outfile report-experiment-xyz.html /var/lib/postgresql/16/mbss/log/postgresql-Thu.log 2>&1 >> /home/robo_sudo/pgbadger_error.log


    RETURN output_json;
END;
$$ LANGUAGE Plpgsql;



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
        flag              INTEGER;
        time_start        TIMESTAMPTZ;
        json_flight       JSONB;
        func_reference    JSONB;

    BEGIN
        FOR n_Pk_Id_Conn, n_Conn_Host, n_Conn_Port IN SELECT Pk_Id_Conn, Conn_Host, Conn_Port
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


                WITH json_data AS (SELECT Path_Out_Dir, Path_Out_Log_File, Path_Pgbg
                                   FROM Pg_Ini
                                   WHERE Fk_Pk_Id_Conn = n_Pk_Id_Conn)
                SELECT JSON_BUILD_OBJECT(
                               'Path_Out_Dir', Path_Out_Dir,
                               'Path_Out_Log_File', Path_Out_Log_File,
                               'Path_Pgbg', Path_Pgbg,
                               'compl_name',
                               REGEXP_REPLACE(FORMAT('%s--%s', n_Conn_Host, n_Conn_Port), '\.maxbit\.private', '',
                                              'g') ||
                               '--' || TO_CHAR(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD--HH24-MI') || '.html'
                       )
                INTO func_reference
                FROM json_data;


                INSERT INTO Pgbadger_Repo_Slicer.Pg_Barn (Pk_Id_Barn, Fk_Pk_Id_Conn, Ins_Date, Log_Mod, Log_Name,
                                                          Log_Size, Log_Slice_Name, Log_Slice_Size)
                VALUES (DEFAULT, n_Pk_Id_Conn, DEFAULT, v_modification, v_name, v_size, func_reference ->> 'compl_name', DEFAULT)
                RETURNING Pk_Id_Barn INTO flag;


                SELECT JSONB_BUILD_OBJECT('file_slice', func_reference ->> 'compl_name' ) INTO json_flight;

                time_start = CLOCK_TIMESTAMP();

                UPDATE Pgbadger_Repo_Slicer.Pg_Barn
                SET Log_Slicing = CLOCK_TIMESTAMP() - time_start
                WHERE Pk_Id_Barn = flag;

                PERFORM Robohub.Public.Dblink_Disconnect(conn_name);

            END LOOP; -- SERVERS
    END;
$$
