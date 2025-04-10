CREATE TABLE Robo_Slicer.Pg_Barn
(
    Pk_Id_Barn        SERIAL,
    Fk_Pk_Id_Conn     INTEGER                                             NOT NULL,
    Ins_Date          BIGINT  DEFAULT (EXTRACT(EPOCH FROM NOW()))::BIGINT NOT NULL,
    Log_Mod           BIGINT  DEFAULT 0                                   NOT NULL,
    Log_Timer_Slicing BIGINT  DEFAULT 0                                   NOT NULL,
    Log_Size          BIGINT  DEFAULT 0                                   NOT NULL,
    Log_Slice_Size    INTEGER DEFAULT 0                                   NOT NULL,
    Log_Name          TEXT    DEFAULT ';'::TEXT                           NOT NULL,
    Log_Slice_Name    TEXT    DEFAULT 'nihil'::TEXT                       NOT NULL,
    PRIMARY KEY (Pk_Id_Barn),
    FOREIGN KEY (Fk_Pk_Id_Conn) REFERENCES Robo_Reference."Servers"
        ON DELETE CASCADE
);

ALTER TABLE Robo_Slicer.Pg_Barn
    OWNER TO Gtimofeyev;

