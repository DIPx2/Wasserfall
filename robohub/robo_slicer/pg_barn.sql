CREATE TABLE robo_slicer.pg_barn
(
    pk_id_barn        serial PRIMARY KEY,
    fk_pk_id_conn     integer                                             NOT NULL REFERENCES robo_reference."Servers" ON DELETE CASCADE,
    ins_date          bigint  DEFAULT (EXTRACT(EPOCH FROM NOW()))::bigint NOT NULL,
    log_mod           bigint  DEFAULT 0                                   NOT NULL,
    log_timer_slicing bigint  DEFAULT 0                                   NOT NULL,
    log_size          bigint  DEFAULT 0                                   NOT NULL,
    log_slice_size    integer DEFAULT 0                                   NOT NULL,
    log_name          text    DEFAULT ';'::text                           NOT NULL,
    log_slice_name    text    DEFAULT 'nihil'::text                       NOT NULL
);

ALTER TABLE robo_slicer.pg_barn
    OWNER TO gtimofeyev;

