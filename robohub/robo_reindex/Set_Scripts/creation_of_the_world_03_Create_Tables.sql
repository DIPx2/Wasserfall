
-- To use the the database robohub and the scheme reindex

--01----------------------------------------------------------------------------------
CREATE TABLE Reindex."Servers"
(
    Pk_Id_Conn    SERIAL PRIMARY KEY,
    Toggle_Switch BOOLEAN      NOT NULL DEFAULT 'FALSE',
    Conn_Port     INTEGER      NOT NULL,
    Conn_Host     VARCHAR(100) NOT NULL
);
COMMENT ON TABLE Reindex."Servers" IS 'Родительская таблица списков подключений - сервера';
COMMENT ON COLUMN Reindex."Servers".Toggle_Switch IS 'Для обслуживающих работ';

--02----------------------------------------------------------------------------------
CREATE TABLE Reindex."DataBases"
(
    Pk_Id_Db      SERIAL PRIMARY KEY,
    Fk_Pk_Id_Conn INTEGER      NOT NULL REFERENCES Reindex."Servers" ON DELETE CASCADE,
    Toggle_Switch BOOLEAN      NOT NULL DEFAULT 'FALSE',
    Db_Scheme     VARCHAR(100)          DEFAULT 'Public' NOT NULL,
    Db_Name       VARCHAR(100) NOT NULL
);
COMMENT ON TABLE Reindex."DataBases" IS 'Родитель - Reindex."Servers"';
COMMENT ON COLUMN Reindex."DataBases".Toggle_Switch IS 'Для обслуживающих работ';

--03----------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS Reindex."Details"
(
    pk_id_det            INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fk_Pk_Id_Db_j        INTEGER      NOT NULL REFERENCES Reindex."DataBases" (Pk_Id_Db) ON DELETE CASCADE,
    det_date             BIGINT       NOT NULL DEFAULT (EXTRACT(EPOCH FROM ((NOW() AT TIME ZONE 'UTC') AT TIME ZONE '+03'))),
    det_clocking         BIGINT       NOT NULL DEFAULT 11232000,
    det_perc_bloat       REAL         NOT NULL DEFAULT 0,
    det_perc_bloat_after REAL         NOT NULL DEFAULT '-999'::REAL,
    det_table_name       VARCHAR(350) NOT NULL DEFAULT 'NIHIL',
    det_index_name       VARCHAR(350) NOT NULL DEFAULT 'NIHIL'
);
COMMENT ON TABLE Reindex."Details" IS 'Родитель - Reindex."DataBases"';
COMMENT ON COLUMN Reindex."Details".Det_Clocking IS 'Unix-формат времени, количество времени на обработку одного индекса';
COMMENT ON COLUMN Reindex."Details".Det_Index_Name IS 'NIHIL - у базы данных отсутствуют раздутые индексы';
COMMENT ON COLUMN Reindex."Details".Det_Date IS 'Unix-формат даты, временная метка операции.';

--04--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS Reindex."Errors"
(
    pk_id_err     INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fk_Pk_Id_Db_k INTEGER NOT NULL REFERENCES Reindex."Details" (Pk_Id_Det) ON DELETE CASCADE,
    err_label     VARCHAR(250) DEFAULT '*'::BPCHAR,
    err_message   TEXT         DEFAULT '*'::TEXT,
    err_detail    TEXT         DEFAULT '*'::TEXT,
    err_code      TEXT         DEFAULT '*'::TEXT
);
COMMENT ON TABLE Reindex."Errors" IS 'Родитель - Reindex."Details"';

------------------------------------------------------------------------------------