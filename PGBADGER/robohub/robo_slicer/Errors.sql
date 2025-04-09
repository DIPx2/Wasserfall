CREATE TABLE Robo_Slicer."Errors"
(
    Pk_Id_Slice_Err  INTEGER GENERATED ALWAYS AS IDENTITY,
    Fk_Pk_Id_Conn    INTEGER                                    NOT NULL,
    Slice_Now_Ins    TIMESTAMP WITH TIME ZONE DEFAULT NOW()     NOT NULL,
    Slice_Err_Mess   TEXT                     DEFAULT ';'::TEXT NOT NULL,
    Slice_Err_Detail TEXT                     DEFAULT ';'::TEXT NOT NULL,
    Slice_Err_Code   TEXT                     DEFAULT ';'::TEXT NOT NULL,
    Work_Scheme      TEXT                     DEFAULT ';'::TEXT NOT NULL,
    PRIMARY KEY (Pk_Id_Slice_Err),
    FOREIGN KEY (Fk_Pk_Id_Conn) REFERENCES Robo_Reference."Servers"
        ON DELETE CASCADE
);

ALTER TABLE Robo_Slicer."Errors"
    OWNER TO Gtimofeyev;

