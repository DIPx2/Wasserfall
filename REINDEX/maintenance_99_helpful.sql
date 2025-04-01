INSERT INTO reindex."Servers"(pk_id_conn, toggle_switch, conn_port, conn_host)
	VALUES (DEFAULT, DEFAULT, 5432, 'prd-bi-04.maxbit.private') RETURNING pk_id_conn;



INSERT INTO reindex."DataBases" (pk_id_db, fk_pk_id_conn, toggle_switch, db_scheme, db_name) VALUES (DEFAULT, 8, DEFAULT, DEFAULT, 'web_analytics');
INSERT INTO reindex."DataBases" (pk_id_db, fk_pk_id_conn, toggle_switch, db_scheme, db_name) VALUES (DEFAULT, 8, DEFAULT, DEFAULT, 'tg_rand_dev');
INSERT INTO reindex."DataBases" (pk_id_db, fk_pk_id_conn, toggle_switch, db_scheme, db_name) VALUES (DEFAULT, 8, DEFAULT, DEFAULT, 'tg_rand');
INSERT INTO reindex."DataBases" (pk_id_db, fk_pk_id_conn, toggle_switch, db_scheme, db_name) VALUES (DEFAULT, 8, DEFAULT, DEFAULT, 'rp_banners_bot_master');
INSERT INTO reindex."DataBases" (pk_id_db, fk_pk_id_conn, toggle_switch, db_scheme, db_name) VALUES (DEFAULT, 8, DEFAULT, DEFAULT, 'rp_banners_bot_dev');
INSERT INTO reindex."DataBases" (pk_id_db, fk_pk_id_conn, toggle_switch, db_scheme, db_name) VALUES (DEFAULT, 8, DEFAULT, DEFAULT, 'recommendations');
INSERT INTO reindex."DataBases" (pk_id_db, fk_pk_id_conn, toggle_switch, db_scheme, db_name) VALUES (DEFAULT, 8, DEFAULT, DEFAULT, 'msgr_bot_master');
INSERT INTO reindex."DataBases" (pk_id_db, fk_pk_id_conn, toggle_switch, db_scheme, db_name) VALUES (DEFAULT, 8, DEFAULT, DEFAULT, 'msgr_bot_dev');
INSERT INTO reindex."DataBases" (pk_id_db, fk_pk_id_conn, toggle_switch, db_scheme, db_name) VALUES (DEFAULT, 8, DEFAULT, DEFAULT, 'kometa_clone_bot_master');
INSERT INTO reindex."DataBases" (pk_id_db, fk_pk_id_conn, toggle_switch, db_scheme, db_name) VALUES (DEFAULT, 8, DEFAULT, DEFAULT, 'kometa_clone_bot_dev');
INSERT INTO reindex."DataBases" (pk_id_db, fk_pk_id_conn, toggle_switch, db_scheme, db_name) VALUES (DEFAULT, 8, DEFAULT, DEFAULT, 'influence_tournament_bot_master');
INSERT INTO reindex."DataBases" (pk_id_db, fk_pk_id_conn, toggle_switch, db_scheme, db_name) VALUES (DEFAULT, 8, DEFAULT, DEFAULT, 'influence_tournament_bot_dev');
INSERT INTO reindex."DataBases" (pk_id_db, fk_pk_id_conn, toggle_switch, db_scheme, db_name) VALUES (DEFAULT, 8, DEFAULT, DEFAULT, 'casino_bot_master');
INSERT INTO reindex."DataBases" (pk_id_db, fk_pk_id_conn, toggle_switch, db_scheme, db_name) VALUES (DEFAULT, 8, DEFAULT, DEFAULT, 'casino_bot_dev');


-------------------------------------------------------------------------------------------------------------------
--Create a set of insertion rows-----------------------------------------------------------------------------------

-- Use database and use scheme the server that needs to be connected to reindexing management

DO
$$
DECLARE
    Tuple_Number INTEGER = 6;
BEGIN

CREATE TEMP TABLE temp_table ( result_text TEXT );

INSERT INTO temp_table(result_text)
SELECT 'INSERT INTO reindex."DataBases" (pk_id_db, fk_pk_id_conn, toggle_switch, db_scheme, db_name) VALUES (DEFAULT, ' || Tuple_Number || ', DEFAULT, DEFAULT, ''' || datname || ''');'
FROM pg_database
WHERE datname NOT IN ('postgres', 'template1', 'template0')
ORDER BY datname DESC;

END;
$$;

SELECT * FROM temp_table;

-- DROP TABLE temp_table;

-------------------------------------------------------------------------------------------------------------------
--Installation of switches-----------------------------------------------------------------------------------------

-- To use the the database robohub and the scheme reindex

DO
$$
DECLARE
    Tuple_Number INTEGER = 8;
BEGIN

--UPDATE reindex."Servers" SET toggle_switch = TRUE WHERE Pk_Id_Conn = Tuple_Number;
--UPDATE reindex."DataBases" SET toggle_switch = TRUE WHERE Fk_Pk_Id_Conn = Tuple_Number;

--UPDATE reindex."Servers" SET toggle_switch = FALSE WHERE Pk_Id_Conn = Tuple_Number;
--UPDATE reindex."DataBases" SET toggle_switch = FALSE WHERE Fk_Pk_Id_Conn = Tuple_Number;

END;
$$;
-------------------------------------------------------------------------------------------------------------------

