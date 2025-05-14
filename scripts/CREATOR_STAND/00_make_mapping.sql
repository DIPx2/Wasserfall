--============================
-- USE postgres.public
--============================

CREATE EXTENSION IF NOT EXISTS postgres_fdw;

DO
$$
DECLARE
    arr TEXT[] = ARRAY[
		'1go_mbss_master',
		'drip_mbss_master',
		'flagman_mbss_master',
		'fresh_mbss_master',
		'gizbo_mbss_master',
		'irwin_mbss_master',
		'izzi_mbss_master',
		'jet_mbss_master',
		'legzo_mbss_master',
		'lex_mbss_master',
		'mbss_master',
		'monro_mbss_master',
		'rox_mbss_master',
		'sol_mbss_master',
		'starda_mbss_master',
		'volna_mbss_master'
    ];
    k TEXT;
    sql TEXT;
BEGIN
    FOREACH k IN ARRAY arr
    LOOP
        sql = format(
            'CREATE SERVER IF NOT EXISTS prd_chat_pg_fdw_%s FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host ''prd-chat-pg-02.maxbit.private'', port ''5434'', dbname ''%s'');
             CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER SERVER prd_chat_pg_fdw_%s OPTIONS (user ''robo_sudo'', password ''%%dFgH8!zX4&kLmT2'');
             CREATE SCHEMA IF NOT EXISTS fdw_%s;
             IMPORT FOREIGN SCHEMA public FROM SERVER prd_chat_pg_fdw_%s INTO fdw_%s;
             REVOKE INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA fdw_%s FROM PUBLIC;',
            k, k, k, k, k, k, k
        );
        EXECUTE sql;
    END LOOP;
END;
$$;

--=======================
-- EOF
--=======================