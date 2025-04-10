CREATE TABLE Robo_Events.Event_Log
(
    Id          SERIAL,
    Event_Date  TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    User_Id     TEXT      DEFAULT CURRENT_USER      NOT NULL,
    Description TEXT                                NOT NULL,
    PRIMARY KEY (Id)
);

ALTER TABLE Robo_Events.Event_Log
    OWNER TO Gtimofeyev;

