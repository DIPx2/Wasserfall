--ALTER TABLE Robohub.reference."DataBases" SET SCHEMA reference;
--ALTER TABLE robohub.reference."DataBases" ADD COLUMN switch BIT(8) DEFAULT B'00000000';
--ALTER TABLE robohub.reference."DataBases" DROP COLUMN toggle_switch;


-- Общий список
SELECT *
FROM (
    SELECT
        Conn_Port,
        Conn_Host,
        Db_Name,
        Db_Scheme,
        Det_Date,
        TO_TIMESTAMP(Det_Date)::TEXT AS Dtt,
        Det_Clocking,
        Det_Perc_Bloat,
        Det_Perc_Bloat_After,
        Det_Table_Name,
        Det_Index_Name,
        Pk_Id_Conn
    FROM Robohub.reference."Servers"
        LEFT JOIN robohub.reference."DataBases" Db
            ON "Servers".Pk_Id_Conn = Db.Fk_Pk_Id_Conn
        LEFT JOIN Reindex."Details" D
            ON Db.Pk_Id_Db = D.Fk_Pk_Id_Db_J
        LEFT JOIN Reindex."Errors" E
            ON D.Pk_Id_Det = E.Fk_Pk_Id_Db_K
) AS SubQuery
WHERE Det_Perc_Bloat_After != -999
  AND Det_Perc_Bloat != 25
  AND Det_Perc_Bloat_After != -333
  AND Pk_Id_Conn = 3
  AND Dtt LIKE '2025-04-01%'
ORDER BY Dtt DESC;


-- Какой за исследуемый период процент уменьшения раздутости индексов, всего
SELECT ((SUM(Det_Perc_Bloat) - SUM(Det_Perc_Bloat_After)) / SUM(Det_Perc_Bloat)) * 100
FROM Robohub.reference."Servers"
         LEFT JOIN robohub.reference."DataBases" Db ON "Servers".Pk_Id_Conn = Db.Fk_Pk_Id_Conn
         LEFT JOIN Reindex."Details" D ON Db.Pk_Id_Db = D.Fk_Pk_Id_Db_J
         LEFT JOIN Reindex."Errors" E ON D.Pk_Id_Det = E.Fk_Pk_Id_Db_K
WHERE Det_Perc_Bloat_After != -999
  AND Det_Perc_Bloat != 25
  AND Det_Perc_Bloat_After != -333
  AND Pk_Id_Conn = 3
  AND TO_TIMESTAMP(Det_Date)::TEXT LIKE '2025-04-01%';

-- Какие ошибки
SELECT TO_TIMESTAMP(Det_Date) AS Dtt,
       Conn_Host,
       Conn_Port,
       Db_Name,
       Db_Scheme,
       Err_Code,
       Err_Detail,
       Err_Label,
       Err_Message
FROM Robohub.reference."Servers"
         LEFT JOIN robohub.reference."DataBases" Db ON Robohub.reference."Servers".Pk_Id_Conn = Db.Fk_Pk_Id_Conn
         LEFT JOIN Reindex."Details" D ON Db.Pk_Id_Db = D.Fk_Pk_Id_Db_J
         LEFT JOIN Reindex."Errors" E ON D.Pk_Id_Det = E.Fk_Pk_Id_Db_K
WHERE Err_Detail NOTNULL
   OR Err_Label NOTNULL
   OR Err_Message NOTNULL
ORDER BY Dtt DESC;