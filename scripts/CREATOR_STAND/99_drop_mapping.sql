--============================
-- USE postgres.public
--============================

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
BEGIN
    FOREACH k IN ARRAY arr
    LOOP
        EXECUTE format('DROP SERVER IF EXISTS prd_chat_pg_fdw_%s CASCADE;', k);
        EXECUTE format('DROP SCHEMA IF EXISTS fdw_%s CASCADE;', k);
    END LOOP;
END;
$$;