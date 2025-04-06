/*

drop EXTENSION dblink CASCADE

--USE https://github.com/zubkov-andrei/pg_profile

--------------------------------infrastructure creation--------------------------------
--01-----
https://github.com/zubkov-andrei/pg_profile/releases ==>> pg_profile--4.8.tar.gz ==>> unpack
--02-----
prd-chat-pg-01.maxbit.private: cp /home/temp_dir/pg_profile* /usr/share/postgresql/17/extension/
--03-----Создать инфрастуктуру сборщика статистики
robohub: CREATE EXTENSION pg_stat_statements;
robohub: CREATE SCHEMA profile;

robohub: CREATE EXTENSION pg_profile SCHEMA profile CASCADE; ---SCHEMA profile---В каскаде будет запущено создание расширения dblink
Каскад - убрать, использовать расширение dblink со схемы public

--04-----Обеспечить локальную работу
prd-chat-pg-01: /etc/postgresql/17/main/pg_hba.conf SET host all robo_sudo 10.94.0.220/32 scram-sha-256 --If needed for an automatically created server
prd-chat-pg-01: /usr/lib/postgresql/17/bin/pg_ctl reload -D /etc/postgresql/17/main/ --If needed for an automatically created server
prd-chat-pg-01: /etc/postgresql/17/main/postgresql.conf SET shared_preload_libraries = 'pg_stat_statements' --If needed for an automatically created server. Clarification is required
prd-chat-pg-01: /usr/lib/postgresql/17/bin/pg_ctl restart -D /etc/postgresql/17/main/ ---RESTART---
--05-----Подготовить удалённый сервер для забора проб
prd-chat-pg-03: /var/lib/pgsql/16/mbss_stage/postgresql.conf SET shared_preload_libraries = pg_stat_statements | track_activities = on | track_counts = on | track_io_timing = on | track_wal_io_timing = on | track_functions = all
prd-chat-pg-03: /var/lib/pgsql/16/mbss_stage/pg_hba.conf SET host all robo_sudo 10.94.0.220/32 scram-sha-256 --robo_sudo as 'superuser' был ранее создан для задачи реиндексации
prd-chat-pg-03: /usr/pgsql-16/bin/pg_ctl restart -D /var/lib/pgsql/16/mbss_stage/ ---RESTART---
*/

/*
Вручную создана запись №2 в таблице robohub.profile.servers, т.к. команда
robohub: SELECT profile.set_server_connstr('ChatSupportDev','dbname=postgres port=15434 host=prd-chat-pg-03.maxbit.private user=robo_sudo password=%dFgH8!zX4&kLmT2');
запись не добавляла

SELECT * from robohub.profile.show_servers();
SELECT * FROM robohub.profile.take_sample('ChatSupportDevFresh');
SELECT * FROM robohub.profile.show_samples('ChatSupportDevFresh');
SELECT robohub.profile.get_report('ChatSupportDevFresh',8,11);

select * from v_sample_settings;
*/

