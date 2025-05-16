DO $$ 
DECLARE 
    schema_name TEXT;
BEGIN 
    FOR schema_name IN 
        SELECT schemata.schema_name FROM information_schema.schemata AS schemata 
        WHERE schemata.schema_name LIKE 'pg_temp_%' OR schemata.schema_name LIKE 'pg_toast_temp_%'
    LOOP 
        EXECUTE format('DROP SCHEMA IF EXISTS %I CASCADE', schema_name);
    END LOOP;
END $$;