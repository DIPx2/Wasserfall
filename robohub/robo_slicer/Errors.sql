CREATE TABLE robo_slicer."Errors"
(
    pk_id_slice_err  integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fk_pk_id_conn    integer                                    NOT NULL REFERENCES robo_reference."Servers" ON DELETE CASCADE,
    slice_now_ins    timestamp with time zone DEFAULT NOW()     NOT NULL,
    slice_err_mess   text                     DEFAULT ';'::text NOT NULL,
    slice_err_detail text                     DEFAULT ';'::text NOT NULL,
    slice_err_code   text                     DEFAULT ';'::text NOT NULL,
    work_scheme      text                     DEFAULT ';'::text NOT NULL
);

ALTER TABLE robo_slicer."Errors"
    OWNER TO gtimofeyev;

