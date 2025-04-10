CREATE TABLE Robo_Reference."DataBases"
(
    Pk_Id_Db      SERIAL,
    Fk_Pk_Id_Conn INTEGER                                          NOT NULL,
    Db_Scheme     VARCHAR(100) DEFAULT 'Public'::CHARACTER VARYING NOT NULL,
    Db_Name       VARCHAR(100)                                     NOT NULL,
    Switch_Db     BIT(8)       DEFAULT '00000000'::"bit",
    PRIMARY KEY (Pk_Id_Db),
    FOREIGN KEY (Fk_Pk_Id_Conn) REFERENCES Robo_Reference."Servers"
        ON DELETE CASCADE
);

ALTER TABLE Robo_Reference."DataBases"
    OWNER TO Gtimofeyev;

