
-- To use the the database robohub and the scheme reindex

INSERT INTO Reindex."Servers" (Pk_Id_Conn, Toggle_Switch, Conn_Port, Conn_Host) VALUES (DEFAULT, FALSE, 15434, 'prd-chat-pg-03.maxbit.private');
INSERT INTO Reindex."Servers" (Pk_Id_Conn, Toggle_Switch, Conn_Port, Conn_Host) VALUES (DEFAULT, FALSE, 5432, 'dev-msg-pg-01.maxbit.private');

INSERT INTO Reindex."DataBases" (Pk_Id_Db, Fk_Pk_Id_Conn, Toggle_Switch, Db_Scheme, Db_Name)
VALUES (DEFAULT, 1, FALSE, 'public', 'starda_mbss_stage');
INSERT INTO Reindex."DataBases" (Pk_Id_Db, Fk_Pk_Id_Conn, Toggle_Switch, Db_Scheme, Db_Name)
VALUES (DEFAULT, 1, FALSE, 'public', 'sol_mbss_stage');
INSERT INTO Reindex."DataBases" (Pk_Id_Db, Fk_Pk_Id_Conn, Toggle_Switch, Db_Scheme, Db_Name)
VALUES (DEFAULT, 1, FALSE, 'public', 'monro_mbss_stage');
INSERT INTO Reindex."DataBases" (Pk_Id_Db, Fk_Pk_Id_Conn, Toggle_Switch, Db_Scheme, Db_Name)
VALUES (DEFAULT, 1, FALSE, 'public', 'mbss_stage');
INSERT INTO Reindex."DataBases" (Pk_Id_Db, Fk_Pk_Id_Conn, Toggle_Switch, Db_Scheme, Db_Name)
VALUES (DEFAULT, 1, FALSE, 'public', 'maxmind_stage');
INSERT INTO Reindex."DataBases" (Pk_Id_Db, Fk_Pk_Id_Conn, Toggle_Switch, Db_Scheme, Db_Name)
VALUES (DEFAULT, 1, FALSE, 'public', 'lex_mbss_stage');
INSERT INTO Reindex."DataBases" (Pk_Id_Db, Fk_Pk_Id_Conn, Toggle_Switch, Db_Scheme, Db_Name)
VALUES (DEFAULT, 1, FALSE, 'public', 'legzo_mbss_stage');
INSERT INTO Reindex."DataBases" (Pk_Id_Db, Fk_Pk_Id_Conn, Toggle_Switch, Db_Scheme, Db_Name)
VALUES (DEFAULT, 1, FALSE, 'public', 'jet_mbss_stage');
INSERT INTO Reindex."DataBases" (Pk_Id_Db, Fk_Pk_Id_Conn, Toggle_Switch, Db_Scheme, Db_Name)
VALUES (DEFAULT, 1, FALSE, 'public', 'izzi_mbss_stage');
INSERT INTO Reindex."DataBases" (Pk_Id_Db, Fk_Pk_Id_Conn, Toggle_Switch, Db_Scheme, Db_Name)
VALUES (DEFAULT, 1, FALSE, 'public', 'irwin_mbss_stage');
INSERT INTO Reindex."DataBases" (Pk_Id_Db, Fk_Pk_Id_Conn, Toggle_Switch, Db_Scheme, Db_Name)
VALUES (DEFAULT, 1, FALSE, 'public', 'gizbo_mbss_stage');
INSERT INTO Reindex."DataBases" (Pk_Id_Db, Fk_Pk_Id_Conn, Toggle_Switch, Db_Scheme, Db_Name)
VALUES (DEFAULT, 1, FALSE, 'public', 'fresh_mbss_stage');
INSERT INTO Reindex."DataBases" (Pk_Id_Db, Fk_Pk_Id_Conn, Toggle_Switch, Db_Scheme, Db_Name)
VALUES (DEFAULT, 1, FALSE, 'public', 'drip_mbss_stage');
INSERT INTO Reindex."DataBases" (Pk_Id_Db, Fk_Pk_Id_Conn, Toggle_Switch, Db_Scheme, Db_Name)
VALUES (DEFAULT, 1, FALSE, 'public', 'callback_media_stage');
INSERT INTO Reindex."DataBases" (Pk_Id_Db, Fk_Pk_Id_Conn, Toggle_Switch, Db_Scheme, Db_Name)
VALUES (DEFAULT, 1, FALSE, 'public', 'callback_media');
INSERT INTO Reindex."DataBases" (Pk_Id_Db, Fk_Pk_Id_Conn, Toggle_Switch, Db_Scheme, Db_Name)
VALUES (DEFAULT, 1, FALSE, 'public', '1go_mbss_stage');

INSERT INTO Reindex."DataBases" (Pk_Id_Db, Fk_Pk_Id_Conn, Toggle_Switch, Db_Scheme, Db_Name)
VALUES (DEFAULT, 2, DEFAULT, DEFAULT, 'test');
INSERT INTO Reindex."DataBases" (Pk_Id_Db, Fk_Pk_Id_Conn, Toggle_Switch, Db_Scheme, Db_Name)
VALUES (DEFAULT, 2, DEFAULT, DEFAULT, 'messenger_volna');
INSERT INTO Reindex."DataBases" (Pk_Id_Db, Fk_Pk_Id_Conn, Toggle_Switch, Db_Scheme, Db_Name)
VALUES (DEFAULT, 2, DEFAULT, DEFAULT, 'messenger_starda');
INSERT INTO Reindex."DataBases" (Pk_Id_Db, Fk_Pk_Id_Conn, Toggle_Switch, Db_Scheme, Db_Name)
VALUES (DEFAULT, 2, DEFAULT, DEFAULT, 'messenger_sol');
INSERT INTO Reindex."DataBases" (Pk_Id_Db, Fk_Pk_Id_Conn, Toggle_Switch, Db_Scheme, Db_Name)
VALUES (DEFAULT, 2, DEFAULT, DEFAULT, 'messenger_rox');
INSERT INTO Reindex."DataBases" (Pk_Id_Db, Fk_Pk_Id_Conn, Toggle_Switch, Db_Scheme, Db_Name)
VALUES (DEFAULT, 2, DEFAULT, DEFAULT, 'messenger_monro');
INSERT INTO Reindex."DataBases" (Pk_Id_Db, Fk_Pk_Id_Conn, Toggle_Switch, Db_Scheme, Db_Name)
VALUES (DEFAULT, 2, DEFAULT, DEFAULT, 'messenger_lex');
INSERT INTO Reindex."DataBases" (Pk_Id_Db, Fk_Pk_Id_Conn, Toggle_Switch, Db_Scheme, Db_Name)
VALUES (DEFAULT, 2, DEFAULT, DEFAULT, 'messenger_legzo');
INSERT INTO Reindex."DataBases" (Pk_Id_Db, Fk_Pk_Id_Conn, Toggle_Switch, Db_Scheme, Db_Name)
VALUES (DEFAULT, 2, DEFAULT, DEFAULT, 'messenger_jet');
INSERT INTO Reindex."DataBases" (Pk_Id_Db, Fk_Pk_Id_Conn, Toggle_Switch, Db_Scheme, Db_Name)
VALUES (DEFAULT, 2, DEFAULT, DEFAULT, 'messenger_izzi');
INSERT INTO Reindex."DataBases" (Pk_Id_Db, Fk_Pk_Id_Conn, Toggle_Switch, Db_Scheme, Db_Name)
VALUES (DEFAULT, 2, DEFAULT, DEFAULT, 'messenger_irwin');
INSERT INTO Reindex."DataBases" (Pk_Id_Db, Fk_Pk_Id_Conn, Toggle_Switch, Db_Scheme, Db_Name)
VALUES (DEFAULT, 2, DEFAULT, DEFAULT, 'messenger_gizbo');
INSERT INTO Reindex."DataBases" (Pk_Id_Db, Fk_Pk_Id_Conn, Toggle_Switch, Db_Scheme, Db_Name)
VALUES (DEFAULT, 2, DEFAULT, DEFAULT, 'messenger_fresh');
INSERT INTO Reindex."DataBases" (Pk_Id_Db, Fk_Pk_Id_Conn, Toggle_Switch, Db_Scheme, Db_Name)
VALUES (DEFAULT, 2, DEFAULT, DEFAULT, 'messenger_flagman');
INSERT INTO Reindex."DataBases" (Pk_Id_Db, Fk_Pk_Id_Conn, Toggle_Switch, Db_Scheme, Db_Name)
VALUES (DEFAULT, 2, DEFAULT, DEFAULT, 'messenger_drip');
INSERT INTO Reindex."DataBases" (Pk_Id_Db, Fk_Pk_Id_Conn, Toggle_Switch, Db_Scheme, Db_Name)
VALUES (DEFAULT, 2, DEFAULT, DEFAULT, 'messenger_admin');
INSERT INTO Reindex."DataBases" (Pk_Id_Db, Fk_Pk_Id_Conn, Toggle_Switch, Db_Scheme, Db_Name)
VALUES (DEFAULT, 2, DEFAULT, DEFAULT, 'messenger_1go');
INSERT INTO Reindex."DataBases" (Pk_Id_Db, Fk_Pk_Id_Conn, Toggle_Switch, Db_Scheme, Db_Name)
VALUES (DEFAULT, 2, DEFAULT, DEFAULT, 'demo_test');
INSERT INTO Reindex."DataBases" (Pk_Id_Db, Fk_Pk_Id_Conn, Toggle_Switch, Db_Scheme, Db_Name)
VALUES (DEFAULT, 2, DEFAULT, DEFAULT, 'demo');
