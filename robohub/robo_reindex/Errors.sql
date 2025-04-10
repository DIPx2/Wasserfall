CREATE TABLE Robo_Reindex."Errors"
(
    Pk_Id_Err     INTEGER GENERATED ALWAYS AS IDENTITY,
    Fk_Pk_Id_Db_K INTEGER NOT NULL,
    Err_Label     VARCHAR(250) DEFAULT '*'::BPCHAR,
    Err_Message   TEXT         DEFAULT '*'::TEXT,
    Err_Detail    TEXT         DEFAULT '*'::TEXT,
    Err_Code      TEXT         DEFAULT '*'::TEXT,
    PRIMARY KEY (Pk_Id_Err),
    FOREIGN KEY (Fk_Pk_Id_Db_K) REFERENCES Robo_Reindex."Details"
        ON DELETE CASCADE
);

COMMENT ON TABLE Robo_Reindex."Errors" IS 'Родитель - Reindex."Details"';

ALTER TABLE Robo_Reindex."Errors"
    OWNER TO Gtimofeyev;

