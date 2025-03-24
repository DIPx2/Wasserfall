DO
$$
BEGIN
    PERFORM DBLINK_CONNECT('xyz_connect',
                           FORMAT('dbname=%s user=%s password=%s host=%s port=%s',
                                  'postgres', 'robo_sudo', '%dFgH8!zX4&kLmT2',
                                  'prd-chat-pg-02.maxbit.private', 5434));

    PERFORM DBLINK_EXEC(
               'xyz_connect',
               E'COPY (SELECT \'Запуск pgBadger...\') TO PROGRAM \'/usr/bin/pgbadger --begin "2025-03-24 07:45:00" --end "2025-03-24 08:30:00" --outdir /home/reports/ --outfile report-with_timer-Mon.html /var/lib/postgresql/16/mbss/log/postgresql-Mon.log 2>&1\''
       );

    PERFORM DBLINK_DISCONNECT('xyz_connect');
END;
$$
LANGUAGE plpgsql;


DO $$

BEGIN
    PERFORM DBLINK_CONNECT( 'xyz_connect', FORMAT('dbname=%s user=%s password=%s host=%s port=%s', 'postgres', 'robo_sudo', '%dFgH8!zX4&kLmT2', 'prd-chat-pg-02.maxbit.private', 5434 ));
    PERFORM DBLINK_EXEC( 'xyz_connect', E'COPY (SELECT \'Запуск pgBadger...\') TO PROGRAM \'/usr/bin/pgbadger --begin "2025-03-24 07:45:00" --end "2025-03-24 08:30:00" --outdir /home/reports/ --outfile /tmp/report-with_timer-Mon.html /var/lib/postgresql/16/mbss/log/postgresql-Mon.log 2>&1\'' );
    PERFORM DBLINK_EXEC( 'xyz_connect', E'INSERT INTO pgbadger_reports (report_name, report_content) VALUES (\'pgBadger Report\', pg_catalog.pg_read_file(\'/tmp/report-with_timer-Mon.html\'))' );
    PERFORM DBLINK_DISCONNECT('xyz_connect');
END;

$$ LANGUAGE plpgsql;


DO $$
DECLARE
    pgbadger_output TEXT;
BEGIN
    PERFORM DBLINK_CONNECT(
        'xyz_connect',
        FORMAT('dbname=%s user=%s password=%s host=%s port=%s',
            'postgres', 'robo_sudo', '%dFgH8!zX4&kLmT2',
            'prd-chat-pg-02.maxbit.private', 5434)
    );

    EXECUTE 'COPY (SELECT pg_catalog.pg_read_file(''/var/lib/postgresql/16/mbss/log/postgresql-Mon.log'', 0, 1000000)) TO STDOUT' INTO pgbadger_output;
    INSERT INTO pgbadger_reports (report_name, report_content) VALUES ('pgBadger Report', pgbadger_output);
    PERFORM DBLINK_DISCONNECT('xyz_connect');

END;
$$ LANGUAGE plpgsql;
