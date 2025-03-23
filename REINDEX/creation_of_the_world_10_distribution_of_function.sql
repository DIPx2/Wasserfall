
-- To use the the database robohub and the scheme reindex

DO
$BODY$
    DECLARE
        conn_name        TEXT DEFAULT 'x_connect';
        operation_number INTEGER;
    BEGIN
        <<FLOW>>
            DECLARE

        x_user     TEXT DEFAULT 'Wszczęsimierz_Szczęśnowszczyk';
        x_password TEXT DEFAULT 'qwerty';

            server     JSON;
            database   JSON;

            fn TEXT = $fn$
                CREATE OR REPLACE FUNCTION public.get_bloated_indexes()
                    RETURNS TABLE(
                        is_na_ text,
                        index_name_ text,
                        schema_name_ text,
                        table_name_ text,
                        index_table_name_ text,
                        real_size_bytes_ numeric,
                        size_ text,
                        extra_ratio_percent_ double precision,
                        extra_size_bytes_ numeric,
                        bloat_size_bytes_ double precision,
                        bloat_ratio_percent_ double precision,
                        bloat_ratio_actor_ numeric,
                        live_data_size_bytes_ numeric,
                        fillfactor_ integer,
                        overrided_settings_ boolean,
                        table_size_bytes_ bigint
                    )
                    LANGUAGE 'plpgsql'
                    COST 100
                    VOLATILE PARALLEL UNSAFE
                    ROWS 1000
                AS $fn_body$
                BEGIN
                RETURN QUERY
                WITH data AS (
                    -- Your existing CTE logic here
                    WITH overrided_tables AS (
                        SELECT
                            pc.oid AS table_id,
                            pn.nspname AS scheme_name,
                            pc.relname AS table_name,
                            pc.reloptions AS options
                        FROM pg_class pc
                        JOIN pg_namespace pn ON pn.oid = pc.relnamespace
                        WHERE reloptions::text ~ 'autovacuum'
                    ),
                    step0 AS (
                        SELECT
                            tbl.oid AS tblid,
                            nspname,
                            tbl.relname AS tblname,
                            idx.relname AS idxname,
                            idx.reltuples,
                            idx.relpages,
                            idx.relam,
                            indrelid,
                            indexrelid,
                            regexp_split_to_table(indkey::text, ' ')::smallint AS attnum,
                            COALESCE(SUBSTRING(array_to_string(idx.reloptions, ' ') FROM 'fillfactor=([0-9]+)')::smallint, 90) AS fillfactor,
                            pg_total_relation_size(tbl.oid) - pg_indexes_size(tbl.oid) - COALESCE(pg_total_relation_size(tbl.reltoastrelid), 0) AS table_size_bytes
                        FROM pg_index
                        JOIN pg_class idx ON idx.oid = pg_index.indexrelid
                        JOIN pg_class tbl ON tbl.oid = pg_index.indrelid
                        JOIN pg_namespace ON pg_namespace.oid = idx.relnamespace
                        JOIN pg_am a ON idx.relam = a.oid
                        WHERE a.amname = 'btree'
                            AND pg_index.indisvalid
                            AND tbl.relkind = 'r'
                            AND pg_namespace.nspname <> 'information_schema' AND pg_namespace.nspname <> 'pg_catalog'
                            AND indisprimary = FALSE
                    ),
                    step1 AS (
                        SELECT
                            i.tblid,
                            i.nspname AS schema_name,
                            i.tblname AS table_name,
                            i.idxname AS index_name,
                            i.reltuples,
                            i.relpages,
                            i.relam,
                            a.attrelid AS table_oid,
                            current_setting('block_size')::numeric AS bs,
                            fillfactor,
                            CASE WHEN version() ~ 'mingw32|64-bit|x86_64|ppc64|ia64|amd64' THEN 8 ELSE 4 END AS maxalign,
                            24 AS pagehdr,
                            16 AS pageopqdata,
                            CASE WHEN MAX(COALESCE(s.null_frac, 0)) = 0 THEN 2 ELSE 2 + ((32 + 8 - 1) / 8) END AS index_tuple_hdr_bm,
                            SUM((1 - COALESCE(s.null_frac, 0)) * COALESCE(s.avg_width, 1024)) AS nulldatawidth,
                            MAX(CASE WHEN a.atttypid = 'pg_catalog.name'::regtype THEN 1 ELSE 0 END) > 0 AS is_na,
                            i.table_size_bytes
                        FROM pg_attribute AS a
                        JOIN step0 AS i ON a.attrelid = i.indexrelid
                        JOIN pg_stats AS s ON
                            s.schemaname = i.nspname
                            AND ((s.tablename = i.tblname AND s.attname = pg_catalog.pg_get_indexdef(a.attrelid, a.attnum, TRUE))
                            OR (s.tablename = i.idxname AND s.attname = a.attname))
                        JOIN pg_type AS t ON a.atttypid = t.oid
                        WHERE a.attnum > 0
                        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 17
                    ),
                    step2 AS (
                        SELECT
                            *,
                            (
                                index_tuple_hdr_bm + maxalign -
                                CASE WHEN index_tuple_hdr_bm % maxalign = 0 THEN maxalign ELSE index_tuple_hdr_bm % maxalign END +
                                nulldatawidth + maxalign -
                                CASE WHEN nulldatawidth = 0 THEN 0 WHEN nulldatawidth::integer % maxalign = 0 THEN maxalign ELSE nulldatawidth::integer % maxalign END
                            )::numeric AS nulldatahdrwidth
                        FROM step1
                    ),
                    step3 AS (
                        SELECT
                            *,
                            COALESCE(1 + CEIL(reltuples / FLOOR((bs - pageopqdata - pagehdr) / (4 + nulldatahdrwidth)::float)), 0) AS est_pages,
                            COALESCE(1 + CEIL(reltuples / FLOOR((bs - pageopqdata - pagehdr) * fillfactor / (100 * (4 + nulldatahdrwidth)::float))), 0) AS est_pages_ff
                        FROM step2
                        JOIN pg_am am ON step2.relam = am.oid
                        WHERE am.amname = 'btree'
                    ),
                    step4 AS (
                        SELECT
                            *,
                            bs * (relpages)::bigint AS real_size,
                            bs * (relpages - est_pages)::bigint AS extra_size,
                            CASE WHEN relpages = 0 THEN NULL ELSE 100 * (relpages - est_pages)::float / relpages END AS extra_ratio,
                            CASE
                                WHEN relpages > est_pages_ff THEN bs * (relpages - est_pages_ff)
                                ELSE 0
                            END AS bloat_size,
                            CASE WHEN relpages = 0 THEN NULL ELSE 100 * (relpages - est_pages_ff)::float / relpages END AS bloat_ratio
                        FROM step3
                    )
                    SELECT
                        CASE is_na WHEN TRUE THEN 'TRUE' ELSE '' END AS is_na,
                        index_name,
                        COALESCE(step4.schema_name, 'public') AS schema_name,
                        COALESCE(NULLIF(step4.schema_name, 'public') || '.', '') || step4.table_name AS table_name,
                        LEFT(index_name, 50) || CASE
                            WHEN LENGTH(index_name) > 50 THEN '…'
                            ELSE ''
                        END || '(' || COALESCE(NULLIF(step4.schema_name, 'public') || '.', '') || step4.table_name || ')' AS index_table_name,
                        real_size AS real_size_bytes,
                        pg_size_pretty(real_size::numeric) AS size,
                        extra_ratio AS extra_ratio_percent,
                        CASE WHEN extra_size::numeric >= 0 THEN extra_size ELSE NULL END AS extra_size_bytes,
                        CASE WHEN bloat_size::numeric >= 0 THEN bloat_size ELSE NULL END AS bloat_size_bytes,
                        CASE WHEN bloat_ratio::numeric >= 0 THEN bloat_ratio ELSE NULL END AS bloat_ratio_percent,
                        CASE WHEN bloat_size::numeric >= 0 AND (real_size - bloat_size)::numeric >= 0 THEN real_size::numeric / NULLIF((real_size - bloat_size)::numeric, 0) ELSE NULL END AS bloat_ratio_factor,
                        CASE WHEN (real_size - bloat_size)::numeric >= 0 THEN (real_size - bloat_size)::numeric ELSE NULL END AS live_data_size_bytes,
                        fillfactor,
                        CASE WHEN ot.table_id IS NOT NULL THEN TRUE ELSE FALSE END AS overrided_settings,
                        table_size_bytes
                    FROM step4
                    LEFT JOIN overrided_tables ot ON ot.table_id = step4.tblid
                    ORDER BY bloat_size DESC NULLS LAST
                )
                SELECT
                    is_na::TEXT AS is_na_f,
                    index_name::TEXT AS index_name_f,
                    schema_name::TEXT AS schema_name_f,
                    table_name::TEXT AS table_name_f,
                    index_table_name::TEXT AS index_table_name_f,
                    real_size_bytes AS real_size_bytes_f,
                    size AS size_f,
                    extra_ratio_percent AS extra_ratio_percent_f,
                    extra_size_bytes AS extra_size_bytes_f,
                    bloat_size_bytes AS bloat_size_bytes_f,
                    bloat_ratio_percent AS bloat_ratio_percent_f,
                    bloat_ratio_factor AS bloat_ratio_factor_f,
                    live_data_size_bytes AS live_data_size_bytes_f,
                    fillfactor AS fillfactor_f,
                    overrided_settings AS overrided_settings_f,
                    table_size_bytes AS table_size_bytes_f
                FROM data;
                END;
                $fn_body$;
            $fn$;

        BEGIN
            FOR server IN SELECT JSON_BUILD_OBJECT('Id_Conn', Pk_Id_Conn, 'port', Conn_Port, 'host', Conn_Host) AS server FROM "Servers" WHERE "Servers".Toggle_Switch IS TRUE
                LOOP
                    FOR database IN SELECT JSON_BUILD_OBJECT('Id_Db', Pk_Id_Db, 'Id_Conn', Fk_Pk_Id_Conn, 'Scheme', Db_Scheme, 'Name', Db_Name) AS database FROM "DataBases" WHERE Fk_Pk_Id_Conn = (server ->> 'Id_Conn')::INTEGER AND "DataBases".Toggle_Switch IS TRUE
                        LOOP
                            IF conn_name IN (SELECT UNNEST(DBLINK_GET_CONNECTIONS())) THEN
                                PERFORM DBLINK_DISCONNECT(conn_name);
                            END IF;
                                PERFORM DBLINK_CONNECT(conn_name, FORMAT('dbname=%s user=%s password=%s host=%s port=%s', database ->> 'Name', x_user, x_password, server ->> 'host', server ->> 'port') );
                                PERFORM DBLINK_EXEC(conn_name, 'DROP FUNCTION IF EXISTS get_bloated_indexes()');
                                PERFORM DBLINK_EXEC(conn_name, fn);
                        END LOOP;
                    PERFORM DBLINK_DISCONNECT(conn_name);
                END LOOP;
        END FLOW;
    END;

$BODY$;