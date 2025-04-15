DO
$$
    DECLARE
        target_grantee   TEXT = 'php_notif'; -- Кому (grantee)
        expected_grantor TEXT = 'gtimofeyev'; -- От кого (grantor)
        acl              TEXT;
        parts            TEXT[];
        grantee          TEXT;
        privilege        TEXT;
        privilege_list   TEXT[];
        obj_type         TEXT;
        obj_name         TEXT;
        grant_sql        TEXT;
        rec              RECORD;

    BEGIN
        DROP TABLE IF EXISTS Temp_Grants;
        CREATE TEMP TABLE temp_grants ( grant_sql TEXT );

        -- Таблицы, Представления
        FOR rec IN
            SELECT c.Relkind,
                   n.Nspname,
                   c.Relname,
                   c.Relacl
            FROM Pg_Class c
                     JOIN Pg_Namespace n ON n.Oid = c.Relnamespace
            WHERE c.Relacl IS NOT NULL
              AND n.Nspname NOT LIKE 'pg_%'
              AND n.Nspname != 'information_schema'
            LOOP
                FOREACH acl IN ARRAY rec.Relacl
                    LOOP
                        parts = STRING_TO_ARRAY(acl::TEXT, '=');
                        grantee = parts[1];
                        IF grantee = '' THEN grantee = 'PUBLIC'; END IF;

                        parts = STRING_TO_ARRAY(parts[2], '/');
                        IF ARRAY_LENGTH(parts, 1) < 2 THEN CONTINUE; END IF;
                        IF parts[2]::REGROLE::TEXT != expected_grantor THEN CONTINUE; END IF;

                        privilege_list = ARRAY []::TEXT[];
                        IF POSITION('a' IN parts[1]) > 0 THEN
                            privilege_list = ARRAY_APPEND(privilege_list, 'INSERT');
                        END IF;
                        IF POSITION('r' IN parts[1]) > 0 THEN
                            privilege_list = ARRAY_APPEND(privilege_list, 'SELECT');
                        END IF;
                        IF POSITION('w' IN parts[1]) > 0 THEN
                            privilege_list = ARRAY_APPEND(privilege_list, 'UPDATE');
                        END IF;
                        IF POSITION('d' IN parts[1]) > 0 THEN
                            privilege_list = ARRAY_APPEND(privilege_list, 'DELETE');
                        END IF;
                        IF POSITION('D' IN parts[1]) > 0 THEN
                            privilege_list = ARRAY_APPEND(privilege_list, 'TRUNCATE');
                        END IF;
                        IF POSITION('x' IN parts[1]) > 0 THEN
                            privilege_list = ARRAY_APPEND(privilege_list, 'REFERENCES');
                        END IF;
                        IF POSITION('t' IN parts[1]) > 0 THEN
                            privilege_list = ARRAY_APPEND(privilege_list, 'TRIGGER');
                        END IF;

                        privilege = ARRAY_TO_STRING(privilege_list, ', ');

                        IF rec.Relkind = 'r' THEN
                            obj_type = 'TABLE';
                        ELSIF rec.Relkind = 'S' THEN
                            obj_type = 'SEQUENCE';
                        ELSIF rec.Relkind = 'v' THEN
                            obj_type = 'VIEW';
                        ELSE
                            CONTINUE;
                        END IF;

                        obj_name = FORMAT('%I.%I', rec.Nspname, rec.Relname);

                        grant_sql = FORMAT('GRANT %s ON %s %s TO %I;', privilege, obj_type, obj_name, target_grantee);
                        INSERT INTO temp_grants VALUES (grant_sql);
                    END LOOP;
            END LOOP;

        -- Функции
        FOR rec IN
            SELECT p.Proname, n.Nspname, p.Oid, p.Proacl
            FROM Pg_Proc p
                     JOIN Pg_Namespace n ON n.Oid = p.Pronamespace
            WHERE p.Proacl IS NOT NULL
              AND n.Nspname NOT LIKE 'pg_%'
              AND n.Nspname != 'information_schema'
            LOOP
                FOREACH acl IN ARRAY rec.Proacl
                    LOOP
                        parts = STRING_TO_ARRAY(acl::TEXT, '=');
                        grantee = parts[1];
                        IF grantee = '' THEN grantee = 'PUBLIC'; END IF;

                        parts = STRING_TO_ARRAY(parts[2], '/');
                        IF ARRAY_LENGTH(parts, 1) < 2 THEN CONTINUE; END IF;
                        IF parts[2]::REGROLE::TEXT != expected_grantor THEN CONTINUE; END IF;

                        IF POSITION('X' IN parts[1]) > 0 THEN
                            obj_type = 'FUNCTION';
                            obj_name = FORMAT('%I.%I(%s)', rec.Nspname, rec.Proname,
                                              PG_GET_FUNCTION_IDENTITY_ARGUMENTS(rec.Oid));
                            grant_sql = FORMAT('GRANT EXECUTE ON %s %s TO %I;', obj_type, obj_name, target_grantee);
                            INSERT INTO temp_grants VALUES (grant_sql);
                        END IF;
                    END LOOP;
            END LOOP;

        -- Схемы
        FOR rec IN
            SELECT n.Nspname, n.Nspacl
            FROM Pg_Namespace n
            WHERE n.Nspacl IS NOT NULL
              AND n.Nspname NOT LIKE 'pg_%'
              AND n.Nspname != 'information_schema'
            LOOP
                FOREACH acl IN ARRAY rec.Nspacl
                    LOOP
                        parts = STRING_TO_ARRAY(acl::TEXT, '=');
                        grantee = parts[1];
                        IF grantee = '' THEN grantee = 'PUBLIC'; END IF;

                        parts = STRING_TO_ARRAY(parts[2], '/');
                        IF ARRAY_LENGTH(parts, 1) < 2 THEN CONTINUE; END IF;
                        IF parts[2]::REGROLE::TEXT != expected_grantor THEN CONTINUE; END IF;

                        privilege_list = ARRAY []::TEXT[];
                        IF POSITION('U' IN parts[1]) > 0 THEN
                            privilege_list = ARRAY_APPEND(privilege_list, 'USAGE');
                        END IF;
                        IF POSITION('C' IN parts[1]) > 0 THEN
                            privilege_list = ARRAY_APPEND(privilege_list, 'CREATE');
                        END IF;

                        privilege = ARRAY_TO_STRING(privilege_list, ', ');
                        obj_name = FORMAT('%I', rec.Nspname);
                        grant_sql = FORMAT('GRANT %s ON SCHEMA %s TO %I;', privilege, obj_name, target_grantee);
                        INSERT INTO temp_grants VALUES (grant_sql);
                    END LOOP;
            END LOOP;

        -- Default privileges
        FOR rec IN
            EXECUTE FORMAT(
                    'SELECT defaclnamespace::regnamespace::text AS nspname,
                            defaclobjtype,
                            defaclacl
                     FROM pg_default_acl
                     WHERE defaclacl IS NOT NULL
                       AND defaclrole::regrole::text = %L',
                    expected_grantor
                    )
            LOOP
                FOREACH acl IN ARRAY rec.Defaclacl
                    LOOP
                        parts = STRING_TO_ARRAY(acl::TEXT, '=');
                        grantee = parts[1];
                        IF grantee = '' THEN grantee = 'PUBLIC'; END IF;

                        parts = STRING_TO_ARRAY(parts[2], '/');
                        IF ARRAY_LENGTH(parts, 1) < 2 THEN CONTINUE; END IF;

                        privilege_list = ARRAY []::TEXT[];
                        IF POSITION('a' IN parts[1]) > 0 THEN
                            privilege_list = ARRAY_APPEND(privilege_list, 'INSERT');
                        END IF;
                        IF POSITION('r' IN parts[1]) > 0 THEN
                            privilege_list = ARRAY_APPEND(privilege_list, 'SELECT');
                        END IF;
                        IF POSITION('w' IN parts[1]) > 0 THEN
                            privilege_list = ARRAY_APPEND(privilege_list, 'UPDATE');
                        END IF;
                        IF POSITION('d' IN parts[1]) > 0 THEN
                            privilege_list = ARRAY_APPEND(privilege_list, 'DELETE');
                        END IF;
                        IF POSITION('D' IN parts[1]) > 0 THEN
                            privilege_list = ARRAY_APPEND(privilege_list, 'TRUNCATE');
                        END IF;
                        IF POSITION('x' IN parts[1]) > 0 THEN
                            privilege_list = ARRAY_APPEND(privilege_list, 'REFERENCES');
                        END IF;
                        IF POSITION('t' IN parts[1]) > 0 THEN
                            privilege_list = ARRAY_APPEND(privilege_list, 'TRIGGER');
                        END IF;
                        IF POSITION('X' IN parts[1]) > 0 THEN
                            privilege_list = ARRAY_APPEND(privilege_list, 'EXECUTE');
                        END IF;
                        IF POSITION('U' IN parts[1]) > 0 THEN
                            privilege_list = ARRAY_APPEND(privilege_list, 'USAGE');
                        END IF;
                        IF POSITION('C' IN parts[1]) > 0 THEN
                            privilege_list = ARRAY_APPEND(privilege_list, 'CREATE');
                        END IF;

                        privilege = ARRAY_TO_STRING(privilege_list, ', ');

                        grant_sql = FORMAT('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA %I GRANT %s ON %sS TO %I;',
                                           expected_grantor, rec.Nspname, privilege, rec.Defaclobjtype, target_grantee);
                        INSERT INTO temp_grants VALUES (grant_sql);
                    END LOOP;
            END LOOP;

        -- Вывод всех GRANT-ов
        RAISE NOTICE '===== Сформированные GRANT-ы от % к % =====', expected_grantor, target_grantee;
        FOR rec IN SELECT * FROM temp_grants
            LOOP
                RAISE INFO '%', rec.grant_sql;
            END LOOP;
    END
$$;

DO $$
BEGIN
    EXECUTE 'CREATE EXTENSION IF NOT EXISTS postgres_fdw';

    EXECUTE '
        CREATE FOREIGN DATA WRAPPER postgres_fdw
        VALIDATOR public.postgres_fdw_validator
        HANDLER public.postgres_fdw_handler
    ';

    EXECUTE '
        CREATE SERVER "dev-msg-pg-01.maxbit.private"
        FOREIGN DATA WRAPPER postgres_fdw
        OPTIONS (host ''dev-msg-pg-01.maxbit.private'', dbname ''имя_удаленной_базы'', port ''5432'')
    ';

    EXECUTE '
        CREATE USER MAPPING FOR robo_sudo
        SERVER "dev-msg-pg-01.maxbit.private"
        OPTIONS (user ''remote_user'', password ''remote_password'')
    ';
END;
$$;

