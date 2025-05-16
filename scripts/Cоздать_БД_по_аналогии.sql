# Сохранение pre-data (структура, последовательности)
/usr/pgsql-16/bin/pg_dump -p 15434 -U postgres -d legzo_mbss_stage --schema-only --section=pre-data --file=/home/reports/legzo_mbss_stage_predata.sql

# Сохранение структуры таблиц (включая TOAST)
/usr/pgsql-16/bin/pg_dump -p 15434 -U postgres -d legzo_mbss_stage --schema-only --file=/home/reports/legzo_mbss_stage_structure.sql

# Сохранение нужных данных из указанных таблиц
/usr/pgsql-16/bin/pg_dump -p 15434 -U postgres -d legzo_mbss_stage --data-only --table=user_online --table=user_groups --table=users --table=projects --table=migrations --file=/home/reports/legzo_mbss_stage_data_subset.sql

# Сохранение post-data (индексы, ограничения, права, GRANT, грантер/грантор)
/usr/pgsql-16/bin/pg_dump -p 15434 -U postgres -d legzo_mbss_stage --schema-only --section=post-data --file=/home/reports/legzo_mbss_stage_postdata.sql

# Создание новой базы
/usr/pgsql-16/bin/createdb -p 15434 martin_mbss_stage

# Восстановление основной структуры, включая TOAST
/usr/pgsql-16/bin/psql -p 15434 -U postgres -d martin_mbss_stage -f /home/reports/legzo_mbss_stage_structure.sql

# Восстановление pre-data (типы, последовательности, базовая структура)
/usr/pgsql-16/bin/psql -p 15434 -U postgres -d martin_mbss_stage -f /home/reports/legzo_mbss_stage_predata.sql

# Восстановление данных из выбранных таблиц
/usr/pgsql-16/bin/psql -p 15434 -U postgres -d martin_mbss_stage -f /home/reports/legzo_mbss_stage_data_subset.sql

# Восстановление post-data (индексы, ограничения, права, GRANT, грантер/грантор)
/usr/pgsql-16/bin/psql -p 15434 -U postgres -d martin_mbss_stage -f /home/reports/legzo_mbss_stage_postdata.sql



-------------------------------------------------------------------------------------------------------------------
/usr/lib/postgresql/16/bin/pg_dump -p 5434 -U postgres -d legzo_mbss_master --schema-only --section=pre-data --file=/home/reports/legzo_mbss_master_predata.sql
/usr/lib/postgresql/16/bin/pg_dump -p 5434 -U postgres -d legzo_mbss_master --schema-only --file=/home/reports/legzo_mbss_master_structure.sql
/usr/lib/postgresql/16/bin/pg_dump -p 5434 -U postgres -d legzo_mbss_master --data-only --table=user_online --table=user_groups --table=users --table=projects --table=migrations --file=/home/reports/legzo_mbss_master_data_subset.sql
/usr/lib/postgresql/16/bin/pg_dump -p 5434 -U postgres -d legzo_mbss_master --schema-only --section=post-data --file=/home/reports/legzo_mbss_master_postdata.sql
/usr/lib/postgresql/16/bin/createdb -p 15434 -p 5434 -U postgres martin_mbss_master
/usr/lib/postgresql/16/bin/psql -p 5434 -U postgres -d martin_mbss_master -f /home/reports/legzo_mbss_master_structure.sql
/usr/lib/postgresql/16/bin/psql -p 5434 -U postgres -d martin_mbss_master -f /home/reports/legzo_mbss_master_predata.sql
/usr/lib/postgresql/16/bin/psql -p 5434 -U postgres -d martin_mbss_master -f /home/reports/legzo_mbss_master_data_subset.sql
/usr/lib/postgresql/16/bin/psql -p 5434 -U postgres -d martin_mbss_master -f /home/reports/legzo_mbss_master_postdata.sql