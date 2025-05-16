--//////////////////////////////////
-- Сформированный список каманд выполнить вручную --->>> prd-chat-pg-02.maxbit.private
--//////////////////////////////////
DO
$$
    DECLARE
        r record;
    BEGIN
        FOR r IN (SELECT pk_id_barn, path_out_dir, log_slice_name
                  FROM pg_barn JOIN robo_slicer.pg_ini ON pg_barn.fk_pk_id_conn = pg_ini.fk_pk_id_conn
                  WHERE TO_TIMESTAMP(ins_date) <= (NOW() - INTERVAL '31 days')::timestamp AND pk_id_pgini = 1)
            LOOP
                RAISE INFO '%', 'rm -f' || ' ' || r.path_out_dir || r.log_slice_name;
                DELETE FROM pg_barn WHERE pk_id_barn = r.pk_id_barn;
            END LOOP;
    END;
$$