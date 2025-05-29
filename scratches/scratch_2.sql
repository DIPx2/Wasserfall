CREATE OR REPLACE FUNCTION remote_vacuum_messages()
RETURNS void AS $$
DECLARE
  db_list1 text[] = ARRAY[
    'messenger_martin'
  ];

db_list text[] = ARRAY[
    'messenger_izzi',
    'messenger_monro',
    'messenger_fresh',
    'messenger_legzo',
    'messenger_starda',
    'messenger_sol',
    'messenger_jet',
    'messenger_1go',
    'messenger_rox',
    'messenger_drip',
    'messenger_volna',
    'messenger_flagman',
    'messenger_martin'
  ];
  db text;
BEGIN
  FOREACH db IN ARRAY db_list1
  LOOP
    PERFORM dblink_exec( 'host=prd-msg-pg-03.maxbit.private port=5432 dbname=' || db || ' user=gtimofeyev password=FoPD9Sow4DD2kLnQ', 'VACUUM FULL public.messages' );
  END LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT  remote_vacuum_messages();