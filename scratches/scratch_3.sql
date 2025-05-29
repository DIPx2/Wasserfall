DO $$
DECLARE
    db_list_to text[] = ARRAY[	'volna_mbss_master',
								'starda_mbss_master',
								'sol_mbss_master',
								'rox_mbss_master',
								'monro_mbss_master',
								'martin_mbss_master',
								'lex_mbss_master',
								'legzo_mbss_master',
								'jet_mbss_master',
								'izzi_mbss_master',
								'irwin_mbss_master',
								'gizbo_mbss_master',
								'fresh_mbss_master',
								'flagman_mbss_master',
								'drip_mbss_master',
								'1go_mbss_master'
							 ];
    db_source text = 'mbss_master';
    table_name text = 'user_groups';
    db text;
    query text;
BEGIN
    CREATE EXTENSION IF NOT EXISTS dblink;
    PERFORM dblink_connect('conn_x', 'dbname=' || db_source || ' user=robo_sudo password=%dFgH8!zX4&kLmT2 options=-csearch_path=');
    PERFORM dblink_disconnect('conn_x');
    DROP TABLE IF EXISTS temp_user_groups;
    CREATE TEMP TABLE temp_user_groups AS SELECT * FROM dblink('dbname=' || db_source || ' user=robo_sudo password=%dFgH8!zX4&kLmT2 options=-csearch_path=', 'SELECT user_id, group_id FROM public.' || table_name) AS t(user_id uuid, group_id bigint);
    FOREACH db IN ARRAY db_list_to LOOP
        BEGIN
            PERFORM dblink_connect('conn_x', 'dbname=' || db || ' user=robo_sudo password=%dFgH8!zX4&kLmT2 options=-csearch_path=');
            query = 'INSERT INTO public.' || table_name || ' (user_id, group_id) SELECT user_id, group_id FROM temp_user_groups';
            -- PERFORM dblink_exec('conn_x', 'TRUNCATE TABLE public.' || table_name);
            -- PERFORM dblink_exec('conn_x', query);
            PERFORM dblink_disconnect('conn_x');
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Error processing %: %', db, SQLERRM;
            PERFORM dblink_disconnect('conn_x');
        END;
    END LOOP;
    DROP TABLE IF EXISTS temp_user_groups;
    DROP EXTENSION dblink;
END;
$$ LANGUAGE plpgsql;