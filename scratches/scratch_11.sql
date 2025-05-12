SELECT '''' || datname || '''' || ',' FROM postgres.pg_catalog.pg_database WHERE "oid" NOT IN (1,4,5) ORDER BY 1 DESC ;

