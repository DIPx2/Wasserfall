CREATE TABLE robo_slicer.pg_ini
(
    pk_id_pgini serial PRIMARY KEY, fk_pk_id_conn integer NOT NULL REFERENCES robo_reference."Servers" ON DELETE CASCADE, path_pgbg text NOT NULL, path_out_dir text NOT NULL, path_out_log_file text NOT NULL);

COMMENT ON TABLE robo_slicer.pg_ini IS 'Родитель - Reference."Servers"';

ALTER TABLE robo_slicer.pg_ini
    OWNER TO gtimofeyev;

