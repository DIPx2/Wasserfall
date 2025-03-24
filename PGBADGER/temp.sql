SELECT dblink_exec(
    'dbname=postgres',
    E'COPY (SELECT \'Запуск pgBadger...\') TO PROGRAM \'/usr/bin/pgbadger --begin "2025-03-20 07:45:00" --end "2025-03-20 08:30:00" --outdir /home/reports/ --outfile report-with_timer-Thu.html /var/lib/postgresql/16/mbss/log/postgresql-Thu.log 2>&1\''
);