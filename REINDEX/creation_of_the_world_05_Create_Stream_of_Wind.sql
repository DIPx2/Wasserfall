-- To use the the database robohub and the scheme reindex

CREATE OR REPLACE PROCEDURE Robohub.Reindex."reindexing_stream"(IN bloat_ratio_search DOUBLE PRECISION DEFAULT 24.99) -- Входной параметр: пороговое значение в % для поиска "раздутых" индексов
    LANGUAGE 'plpgsql'
AS
$BODY$ -- VERSION 1-00-001
DECLARE
    conn_name             TEXT DEFAULT 'x_connect'; -- Имя соединения для DBLINK
    Record_Number_Details INTEGER; -- Переменная для хранения id операции
    err_mess              TEXT; -- Переменная для хранения текста ошибки
    err_det               TEXT; -- Переменная для хранения деталей ошибки
    err_cd                TEXT; -- Переменная для хранения кода ошибки
BEGIN
    <<FLOW>>
        DECLARE
            x_user     TEXT DEFAULT 'robo_sudo';
            x_password TEXT DEFAULT '%dFgH8!zX4&kLmT2';
            --x_user     TEXT DEFAULT 'Wszczęsimierz_Szczęśnowszczyk';
            --x_password TEXT DEFAULT 'qwerty';
            server     JSON; -- Переменная для хранения информации о серверах
            database   JSON; -- Переменная для хранения информации о базах данных
    BEGIN
        -- Цикл по всем серверам из таблицы "Servers"
        FOR server IN SELECT JSON_BUILD_OBJECT('Id_Conn', Pk_Id_Conn, 'port', Conn_Port, 'host',
                                               Conn_Host) AS server
                      FROM "robohub".Reindex."Servers"
            LOOP
                -- Цикл по всем базам данных, связанным с текущим сервером
                FOR database IN SELECT JSON_BUILD_OBJECT('Id_Db', Pk_Id_Db, 'Id_Conn', Fk_Pk_Id_Conn::TEXT,
                                                         'Scheme',
                                                         Db_Scheme, 'Name', Db_Name) AS database
                                FROM Robohub.Reindex."DataBases"
                                WHERE Fk_Pk_Id_Conn = (server ->> 'Id_Conn')::INTEGER
                    LOOP
                        -- Вставка записи для логирования операции
                        INSERT INTO Robohub.Reindex."Details" (Pk_Id_Det,
                                                               Fk_Pk_Id_Db_J,
                                                               Det_Date,
                                                               Det_Clocking,
                                                               Det_Perc_Bloat,
                                                               Det_Perc_Bloat_After,
                                                               Det_Index_Name,
                                                               Det_Table_Name)
                        VALUES (DEFAULT,
                                (database ->> 'Id_Db')::INTEGER,
                                DEFAULT,
                                DEFAULT,
                                DEFAULT,
                                DEFAULT,
                                DEFAULT,
                                DEFAULT)
                        RETURNING Pk_Id_Det INTO Record_Number_Details;

                        BEGIN
                            -- Проверка, существует ли уже соединение с именем conn_name
                            IF conn_name IN (SELECT UNNEST(DBLINK_GET_CONNECTIONS())) THEN
                                -- Если соединение существует, отключить его
                                PERFORM DBLINK_DISCONNECT(conn_name);
                            END IF;

                            -- Установить соединение с базой данных
                            PERFORM DBLINK_CONNECT(conn_name,
                                                   FORMAT('dbname=%s user=%s password=%s host=%s port=%s',
                                                          database ->> 'Name', x_user, x_password,
                                                          server ->> 'host', server ->> 'port'));
                        EXCEPTION
                            -- Обработка ошибок при подключении
                            WHEN OTHERS THEN
                                GET STACKED DIAGNOSTICS err_mess = MESSAGE_TEXT, err_det = PG_EXCEPTION_DETAIL, err_cd = RETURNED_SQLSTATE;
                                INSERT INTO Robohub.Reindex."Errors" (Pk_Id_Err, Fk_Pk_Id_Db_K, Err_Label, Err_Message,
                                                                      Err_Detail, Err_Code)
                                VALUES (DEFAULT, Record_Number_Details, 'DBLINK_CONNECT', err_mess, err_det, err_cd);
                                RETURN;
                        END;

                        <<PROCESSING>>
                            DECLARE
                            sql_query_0                TEXT; -- Переменная для хранения SQL-запроса
                            sql_query_1                TEXT; -- Переменная для хранения SQL-запроса
                            start_time                 BIGINT; -- Переменная для хранения времени начала операции
                            j_temp_id                  BIGINT; -- Временная переменная для хранения ID
                            j_temp_bloat_ratio_percent DOUBLE PRECISION; -- Временная переменная для хранения процента "раздутости"
                            j_temp_schema_name         TEXT; -- Временная переменная для хранения имени схемы
                            j_temp_table_name          TEXT; -- Временная переменная для хранения имени таблицы
                            j_temp_index_name          TEXT; -- Временная переменная для хранения имени индекса
                            updated_bloat_ratio        DOUBLE PRECISION; -- Переменная для хранения обновленного процента "раздутости"
                        BEGIN
                            -- Удалить временную таблицу, если она существует
                            IF EXISTS (SELECT 1
                                       FROM Information_Schema.Tables
                                       WHERE Table_Schema = 'public'
                                         AND Table_Name = 'bloats_tmp') THEN
                                EXECUTE 'DROP TABLE bloats_tmp';
                            END IF;

                            -- Создать временную таблицу для хранения информации о "раздутых" индексах
                            CREATE TEMP TABLE Bloats_Tmp
                            (
                                temp_id                  SERIAL PRIMARY KEY,
                                temp_bloat_ratio_percent DOUBLE PRECISION,
                                temp_schema_name         TEXT,
                                temp_table_name          TEXT,
                                temp_index_name          TEXT
                            );

                            -- Формировать SQL-запрос для поиска "раздутых" индексов
                            sql_query_0 := FORMAT(
                                    'SELECT bloat_ratio_percent_, schema_name_, table_name_, index_name_ FROM Public.Get_Bloated_Indexes() WHERE bloat_ratio_percent_ > %s',
                                    bloat_ratio_search);

                            -- Вставить данные о "раздутых" индексах во временную таблицу
                            INSERT INTO Bloats_Tmp (temp_bloat_ratio_percent, temp_schema_name, temp_table_name,
                                                    temp_index_name)
                            SELECT j1, j2, j3, j4
                            FROM DBLINK(conn_name, sql_query_0) AS result(j1 DOUBLE PRECISION, j2 TEXT, j3 TEXT, j4 TEXT);

                            -- Проверить, есть ли "раздутые" индексы
                            IF (SELECT COUNT(temp_id) FROM Bloats_Tmp) != 0 THEN

                                -- Цикл по всем "раздутым" индексам
                                FOR j_temp_id, j_temp_bloat_ratio_percent, j_temp_schema_name, j_temp_table_name, j_temp_index_name IN SELECT temp_id,
                                                                                                                                              temp_bloat_ratio_percent,
                                                                                                                                              temp_schema_name,
                                                                                                                                              temp_table_name,
                                                                                                                                              temp_index_name
                                                                                                                                       FROM Bloats_Tmp
                                                                                                                                       ORDER BY temp_bloat_ratio_percent
                                    LOOP
                                        -- Зафиксировать время начала операции
                                        start_time = EXTRACT(EPOCH FROM CLOCK_TIMESTAMP())::BIGINT;

                                        BEGIN
                                            -- Выполнить реиндексацию индекса
                                            PERFORM DBLINK_EXEC(conn_name,
                                                                'REINDEX INDEX CONCURRENTLY ' || j_temp_index_name ||
                                                                ';');
                                        EXCEPTION
                                            -- Обработка ошибок при реиндексации
                                            WHEN OTHERS THEN
                                                PERFORM DBLINK_DISCONNECT(conn_name);
                                                GET STACKED DIAGNOSTICS err_mess = MESSAGE_TEXT, err_det = PG_EXCEPTION_DETAIL, err_cd = RETURNED_SQLSTATE;
                                                INSERT INTO Robohub.Reindex."Errors" (Pk_Id_Err, Fk_Pk_Id_Db_K,
                                                                                      Err_Label, Err_Message,
                                                                                      Err_Detail, Err_Code)
                                                VALUES (DEFAULT, Record_Number_Details, 'DBLINK_CONNECT_REINDEX',
                                                        err_mess, err_det, err_cd);
                                                RETURN;
                                        END;

                                        -- Обновить журнал
                                        UPDATE Robohub.Reindex."Details"
                                        SET Det_Perc_Bloat = j_temp_bloat_ratio_percent,
                                            Det_Index_Name = j_temp_index_name,
                                            Det_Table_Name = j_temp_table_name,
                                            Det_Clocking   = EXTRACT(EPOCH FROM CLOCK_TIMESTAMP())::BIGINT - start_time
                                        WHERE Pk_Id_Det = Record_Number_Details;

                                        -- Формировать SQL-запрос для получения обновленного процента "раздутости"
                                        sql_query_1 := FORMAT(
                                                'SELECT bloat_ratio_percent_ FROM Public.Get_Bloated_Indexes() WHERE schema_name_ = %L AND table_name_ = %L AND index_name_ = %L',
                                                j_temp_schema_name, j_temp_table_name, j_temp_index_name);

                                        -- Получить обновленный процент "раздутости"
                                        SELECT i1
                                        INTO updated_bloat_ratio
                                        FROM DBLINK(conn_name, sql_query_1) AS result(i1 DOUBLE PRECISION);

                                        -- NULL может приходить из функции
                                        IF updated_bloat_ratio IS NULL THEN
                                            updated_bloat_ratio = -0,0;
                                        END IF;

                                        -- Обновить запись в логе с новым процентом "раздутости"
                                        UPDATE Robohub.Reindex."Details"
                                        SET Det_Perc_Bloat_After = updated_bloat_ratio
                                        WHERE Pk_Id_Det = Record_Number_Details;

                                    END LOOP;
                            END IF;
                            DROP TABLE IF EXISTS Bloats_Tmp; -- Удалить временную таблицу
                        END PROCESSING;
                    END LOOP; -- Конец цикла по всем базам данных
                PERFORM DBLINK_DISCONNECT(conn_name); -- Отключить соединение с базой данных
            END LOOP; -- Конец цикла по всем серверам
    END FLOW;
END;
$BODY$;