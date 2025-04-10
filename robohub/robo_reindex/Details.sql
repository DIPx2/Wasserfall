CREATE TABLE Robo_Reindex."Details"
(
    Pk_Id_Det            INTEGER GENERATED ALWAYS AS IDENTITY,
    Fk_Pk_Id_Db_J        INTEGER                                                                                   NOT NULL,
    Det_Date             BIGINT       DEFAULT EXTRACT(EPOCH FROM
                                                      ((NOW() AT TIME ZONE 'UTC'::TEXT) AT TIME ZONE '+03'::TEXT)) NOT NULL,
    Det_Clocking         BIGINT       DEFAULT 11232000                                                             NOT NULL,
    Det_Perc_Bloat       REAL         DEFAULT 0                                                                    NOT NULL,
    Det_Perc_Bloat_After REAL         DEFAULT '-999'::REAL                                                         NOT NULL,
    Det_Table_Name       VARCHAR(350) DEFAULT 'NIHIL'::CHARACTER VARYING                                           NOT NULL,
    Det_Index_Name       VARCHAR(350) DEFAULT 'NIHIL'::CHARACTER VARYING                                           NOT NULL,
    PRIMARY KEY (Pk_Id_Det),
    FOREIGN KEY (Fk_Pk_Id_Db_J) REFERENCES Robo_Reference."DataBases"
        ON DELETE CASCADE
);

COMMENT ON TABLE Robo_Reindex."Details" IS 'Родитель - Reindex."DataBases"';

COMMENT ON COLUMN Robo_Reindex."Details".Det_Date IS 'Unix-формат даты, временная метка операции.';

COMMENT ON COLUMN Robo_Reindex."Details".Det_Clocking IS 'Unix-формат времени, количество времени на обработку одного индекса';

COMMENT ON COLUMN Robo_Reindex."Details".Det_Index_Name IS 'NIHIL - у базы данных отсутствуют раздутые индексы';

ALTER TABLE Robo_Reindex."Details"
    OWNER TO Gtimofeyev;

