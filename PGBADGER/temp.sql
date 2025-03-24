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
