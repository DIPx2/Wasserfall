CREATE TABLE Robo_Reference."Servers"
(
    Pk_Id_Conn  SERIAL,
    Conn_Port   INTEGER      NOT NULL,
    Conn_Host   VARCHAR(100) NOT NULL,
    Switch_Serv BIT(8) DEFAULT '00000000'::"bit",
    PRIMARY KEY (Pk_Id_Conn)
);

COMMENT ON TABLE Robo_Reference."Servers" IS 'Родительская таблица списков подключений - сервера';

ALTER TABLE Robo_Reference."Servers"
    OWNER TO Gtimofeyev;

