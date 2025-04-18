-- Сколько после реиндексации осталось % от прежнего "раздутия" (какова эффективность)?
WITH indexed_details AS (
    SELECT
        serv.Conn_Host,
        serv.Conn_Port,
        db.Db_Name,
        det.Det_Clocking,
        det.Det_Date,
        det.Det_Index_Name,
        det.Det_Perc_Bloat,
        det.Det_Perc_Bloat_After,
        det.Det_Table_Name
        --COUNT(*) OVER (PARTITION BY det.Det_Index_Name) AS index_name_count,
        --AVG(Det_Perc_Bloat) OVER (PARTITION BY Det_Perc_Bloat) AS avg_det_perc_bloat,
        --AVG(Det_Perc_Bloat_After) OVER (PARTITION BY Det_Perc_Bloat_After) AS avg_det_perc_after
    FROM Robohub.Robo_Reference."Servers" AS serv
        LEFT JOIN Robohub.Robo_Reference."DataBases" AS db ON serv.Pk_Id_Conn = db.Fk_Pk_Id_Conn
        LEFT JOIN Robohub.Robo_Reindex."Details" AS det ON db.Pk_Id_Db = det.Fk_Pk_Id_Db_J
    WHERE
        Conn_Host = 'prd-chat-pg-02.maxbit.private'
        AND TO_TIMESTAMP(det.Det_Date) BETWEEN TIMESTAMP '2025-04-01 00:00:00'
                                           AND TIMESTAMP '2025-04-11 23:59:59'
)
SELECT
    ROUND( (100.0 * AVG(Det_Perc_Bloat_After) FILTER (WHERE Det_Perc_Bloat_After NOT IN (-999, -333)) / NULLIF( AVG(Det_Perc_Bloat) FILTER (WHERE Det_Perc_Bloat IS NOT NULL), 0 ) )::numeric, 2) AS perc_after_from_before
FROM indexed_details
WHERE Det_Perc_Bloat IS NOT NULL
  AND Det_Perc_Bloat_After IS NOT NULL
  AND Det_Perc_Bloat_After NOT IN (-999, -333);


-- Как меняется раздутость с течением времени?
WITH indexed_details AS (
    SELECT
        serv.Conn_Host,
        serv.Conn_Port,
        db.Db_Name,
        det.Det_Clocking,
        det.Det_Date,
        det.Det_Index_Name,
        det.Det_Perc_Bloat,
        det.Det_Perc_Bloat_After,
        det.Det_Table_Name
    FROM Robohub.Robo_Reference."Servers" AS serv
        LEFT JOIN Robohub.Robo_Reference."DataBases" AS db ON serv.Pk_Id_Conn = db.Fk_Pk_Id_Conn
        LEFT JOIN Robohub.Robo_Reindex."Details" AS det ON db.Pk_Id_Db = det.Fk_Pk_Id_Db_J
    WHERE
        Conn_Host = 'prd-chat-pg-02.maxbit.private'
        AND TO_TIMESTAMP(det.Det_Date) BETWEEN TIMESTAMP '2025-04-01 00:00:00'
                                           AND TIMESTAMP '2025-04-11 23:59:59'
)
SELECT
    det.Det_Index_Name,
    TO_TIMESTAMP(det.Det_Date) AS det_date,
    det.Det_Perc_Bloat,
    det.Det_Perc_Bloat_After
FROM indexed_details AS det
WHERE det.Det_Perc_Bloat IS NOT NULL
  AND det.Det_Perc_Bloat_After IS NOT NULL
  AND det.Det_Perc_Bloat_After NOT IN (-999, -333)
ORDER BY det.Det_Index_Name, det_date;


-- Какие самые частые (раздуваемые) индексы на каких базах?
WITH indexed_details AS (
    SELECT
        serv.Conn_Host,
        serv.Conn_Port,
        db.Db_Name,
        det.Det_Clocking,
        det.Det_Date,
        det.Det_Index_Name,
        det.Det_Perc_Bloat,
        det.Det_Perc_Bloat_After,
        det.Det_Table_Name
    FROM Robohub.Robo_Reference."Servers" AS serv
        LEFT JOIN Robohub.Robo_Reference."DataBases" AS db ON serv.Pk_Id_Conn = db.Fk_Pk_Id_Conn
        LEFT JOIN Robohub.Robo_Reindex."Details" AS det ON db.Pk_Id_Db = det.Fk_Pk_Id_Db_J
    WHERE
        Conn_Host = 'prd-chat-pg-02.maxbit.private'
        AND TO_TIMESTAMP(det.Det_Date) BETWEEN TIMESTAMP '2025-04-01 00:00:00'
                                           AND TIMESTAMP '2025-04-11 23:59:59'
)
SELECT
    det.Det_Index_Name,
    db.Db_Name,
    COUNT(*) AS index_count,
    AVG(det.Det_Perc_Bloat) AS avg_bloat_before,
    AVG(det.Det_Perc_Bloat_After) AS avg_bloat_after
FROM Robohub.Robo_Reindex."Details" AS det
JOIN Robohub.Robo_Reference."DataBases" AS db ON det.Fk_Pk_Id_Db_J = db.Pk_Id_Db
WHERE det.Det_Perc_Bloat IS NOT NULL
  AND det.Det_Perc_Bloat_After IS NOT NULL
  AND det.Det_Index_Name != 'NIHIL'
GROUP BY db.Db_Name, det.Det_Index_Name
HAVING COUNT(*) > 5
   AND AVG(det.Det_Perc_Bloat_After) != -999
ORDER BY db_name, det_index_name, index_count DESC, avg_bloat_after DESC;

