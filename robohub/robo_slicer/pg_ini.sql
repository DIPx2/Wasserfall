CREATE TABLE Robo_Slicer.Pg_Ini
(
    Pk_Id_Pgini       SERIAL,
    Fk_Pk_Id_Conn     INTEGER NOT NULL,
    Path_Pgbg         TEXT    NOT NULL,
    Path_Out_Dir      TEXT    NOT NULL,
    Path_Out_Log_File TEXT    NOT NULL,
    PRIMARY KEY (Pk_Id_Pgini),
    FOREIGN KEY (Fk_Pk_Id_Conn) REFERENCES Robo_Reference."Servers"
        ON DELETE CASCADE
);

COMMENT ON TABLE Robo_Slicer.Pg_Ini IS 'Родитель - Reference."Servers"';

ALTER TABLE Robo_Slicer.Pg_Ini
    OWNER TO Gtimofeyev;

