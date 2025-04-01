
-- To use the the database robohub and the scheme reindex

DO
$BODY$
    DECLARE
        conn_name        TEXT DEFAULT 'x_connect';
    BEGIN
        <<FLOW>>
            DECLARE

        x_user     TEXT DEFAULT 'robo_sudo';
        x_password TEXT DEFAULT '%dFgH8!zX4&kLmT2';


            server     JSON;
            database   JSON;
        BEGIN
            FOR server IN SELECT JSON_BUILD_OBJECT('Id_Conn', Pk_Id_Conn, 'port', Conn_Port, 'host', Conn_Host) AS server FROM "Servers" WHERE "Servers".Toggle_Switch IS TRUE
                LOOP
                    FOR database IN SELECT JSON_BUILD_OBJECT('Id_Db', Pk_Id_Db, 'Id_Conn', Fk_Pk_Id_Conn::TEXT, 'Scheme', Db_Scheme, 'Name', Db_Name) AS database FROM "DataBases" WHERE Fk_Pk_Id_Conn = (server ->> 'Id_Conn')::INTEGER AND "DataBases".Toggle_Switch IS TRUE
                        LOOP
                            IF conn_name IN (SELECT UNNEST(robohub.public.DBLINK_GET_CONNECTIONS())) THEN PERFORM robohub.public.DBLINK_DISCONNECT(conn_name); END IF;
								PERFORM robohub.public.DBLINK_CONNECT(conn_name, FORMAT('dbname=%s user=%s password=%s host=%s port=%s', database ->> 'Name', x_user, x_password, server ->> 'host', server ->> 'port'));
								PERFORM robohub.public.DBLINK_EXEC(conn_name, $$ ALTER DEFAULT PRIVILEGES FOR ROLE Gtimofeyev GRANT ALL ON TABLES TO Robo_Sudo;  $$);
                        END LOOP;
                    PERFORM robohub.public.DBLINK_DISCONNECT(conn_name);
                END LOOP;
        END FLOW;
    END;
$BODY$;