Версия 0.00.001

При выполнении reindexing_stream():
    [23502] ERROR: null value in column "det_perc_bloat_after" of relation "Log_Bloat_Index_Details" violates not-null constraint
    Подробности: Failing row contains (162, 577, 0, 40, null, rooms_group_id_index).
    Где: SQL statement "UPDATE Public."Log_Bloat_Index_Details"
    SET Det_Perc_Bloat_After = updated_bloat_ratio
    WHERE Pk_Id_Det = Log_Str_Ident"
Добавлена проверка:
    IF updated_bloat_ratio IS NULL THEN
        updated_bloat_ratio = -999;
    END IF;

При выполнении задания /home/RegulatoryTasks/run_reindex.sh:
    NOTICE:  table "bloats_tmp" does not exist, skipping
Добавлена проверка:
    IF EXISTS (SELECT 1
               FROM Information_Schema.Tables
               WHERE Table_Schema = 'public'
                 AND Table_Name = 'bloats_tmp') THEN
        EXECUTE 'DROP TABLE bloats_tmp';
    END IF;

Изменено расположение кавычек "доллар".

--------------------------------------------------------------
Версия 0.00.000

Каталог REINDEX содержит набор PL/pgSQL-скриптов для развёртывания регламента реиндексации в комплексном обслуживания баз данных с учетом автоматизации задач; автоматизация выполняется с учетом наличия сервера автоматизации.
Скрипты предназначены для работы с минимизацией влияния на производительность рабочих систем благодаря использованию CONCURRENTLY, без остановки системы.

Работает с b-tree-индексами.

Написаны для PostgreSQL 17.4
Алгоритм поиска раздутых индексов взят с... (дай бог здоровья тому кто его написал), из всех найденных и проаналированных является самы точным: "reindex_get_bloated_indexes.sql"; произведена корректировка скрипта в операциях деления для устранения иногда возникающей ошибки деления на 0.

Типичный сценарий развертывания*:

* по умолчанию схема "public";

1) На обслуживаемых базах данных: 
	1.1) создать пользователя с необходимыми правами;
	1.2) создать функцию "get_bloated_indexes", файл "reindex_get_bloated_indexes.sql" от имени пользователя п.1.1;
2) На экземплярах сервера PostgreSQL с обслуживаемыми базами данных произвести корректровку pg_hba.conf с учетом п.1.1 и адреса сервера автоматизации;
3) Внести информацию о пользователе в переменные "x_user" и "x_password" в файле "reindex_reindexing_stream.sql";
4) На сервере автоматизации, на схеме, создать таблицы инфраструктуры: "reindex_create_tables.sql". Назначения таблиц и отдельных полей - в комментариях к таблицам;
5) Произвести инициализацию табличной инфраструткуры внесением не менее одной записи в таблицы "Bloat_Index_Db_Connections" и "Bloat_Index_Db_Connections_Details";
6) На сервере автоматизации, на схеме, выполнить скрипт "reindex_reindexing_stream.sql" для создания хранимой процедуры "reindexing stream";
	6.1) При необходимости, изменить входной параметр - пороговое значение для поиска "раздутых" индексов, - DEFAULT 25%;
7) Произвести настройку "крона".

Вручную анализировать журнальную информацию.

Планы на следующие версии:
1) С учетом того, что базовая версия предназначена кроме основной задачи в том числе и для сбора статистической ифнормации, собираемой в таблицы "Log_Bloat_Index_Operations", "Log_Bloat_Index_Details" и "Log_Bloat_Index_Errors", не производится очистка журналов:
	1.1) дополнить алгоритм: удалять устаревшие записи;
2) Использовать статистическую информацию для:
	2.1) нахождения индексов, не устранившие раздутость для их последующего анализа причин;
		2.1.1) На основании анализа, произвести модернизацию алгоритма для полного устранения раздутости;
	2.2) оптимизации графика процесса реиндексации;
3) Произвести интеграцию с Zabbix.

Алгоритм поиска раздутых индексов ("reindex_get_bloated_indexes.sql") использует следующую математику:

%разбухания = 100 * ( ( КоличествоСтраниц - РасчетноеКоличествоСтраниц ) / КоличествоСтраниц )
где
РасчетноеКоличествоСтраниц = ( 4 + РазмерОднойЗаписи * КоличествоЗаписей) / 8152
где
8152 = РазмерСтраницы (8192) - ЗагловокСтраницы (24) - СпецПространствоИндекса (16)
4 = РазмерУказателяЗаписи
РазмерОднойЗаписи = ЧётноеВыравнивание + РазмерДанныхСтраницы
где
ЧётноеВыравнивание = ( 8 - ( РазмерДанных mod 8 ) )
где
8 = ВыравнивающееСмещениеX64битныхСистем

--------------------------------------------------------------