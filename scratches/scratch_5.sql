/*
-- Список всех табличных пространств и их расположение
-- Если location пустое, значит, табличное пространство находится в каталоге данных PostgreSQL (PGDATA)
-- Пользовательские табличные пространства (например, fastspace) показывают явный путь
SELECT spcname AS name, pg_catalog.pg_tablespace_location(oid) AS location FROM pg_catalog.pg_tablespace;

-- Где физически находятся pg_default и pg_global?
-- Оба пространства хранятся внутри основного каталога данных PostgreSQL (PGDATA). Путь к PGDATA можно узнать так:
SHOW data_directory;

-- Физическая структура внутри PGDATA
-- pg_default — объекты (таблицы, индексы), созданные без указания табличного пространства, попадают сюда
-- Располагаются в подкаталогах:
-- PGDATA/base/        # Основные данные БД
-- PGDATA/pg_tblspc/   # Симлинки для пользовательских tablespaces
-- pg_global — содержит системные каталоги, общие для всего кластера PostgreSQL.
-- Находится в:
-- PGDATA/global/

-- Как найти OID базы данных для pg_default?
-- Чтобы точно определить путь к объектам в pg_default, нужно знать OID базы:
SELECT oid, datname FROM pg_database;

-- Как проверить, какие объекты в каких tablespaces?
-- Запрос для просмотра распределения таблиц по табличным пространствам:
-- (где 0 означает pg_default)
SELECT relname, reltablespace
FROM pg_class
WHERE relkind = 'r' AND reltablespace != 0;

-- Вывод:
-- pg_default → данные хранятся в PGDATA/base/.
-- pg_global → данные хранятся в PGDATA/global/.
-- Пользовательские табличные пространства → путь указывается при создании (через LOCATION), а симлинки размещаются в PGDATA/pg_tblspc/

-- USE messenger_irwin.public
-- Если нужно найти конкретный файл таблицы, можно использовать pg_relation_filepath():
--Пример вывода: base/16384/12345 (где 16384 — OID БД, 12345 — OID таблицы).
SELECT pg_relation_filepath('messages');


-- Итак: SHOW data_directory || / || base/16709033/23835271 ->> /var/lib/pgsql/14/messenger/base/16709033


-- Получение списка всех объектов БД
SELECT
    c.oid::text AS file_name,  -- Имя файла на диске (без пути)
    n.nspname AS schema_name,  -- Схема объекта
    c.relname AS object_name,  -- Имя объекта
    CASE c.relkind
        WHEN 'r' THEN 'table'
        WHEN 'i' THEN 'index'
        WHEN 'S' THEN 'sequence'
        WHEN 't' THEN 'TOAST table'
        WHEN 'v' THEN 'view'
        WHEN 'm' THEN 'materialized view'
        WHEN 'c' THEN 'composite type'
        WHEN 'f' THEN 'foreign table'
        ELSE c.relkind::text
    END AS object_type,
    pg_size_pretty(pg_total_relation_size(c.oid)) AS size
FROM
    pg_class c
LEFT JOIN
    pg_namespace n ON c.relnamespace = n.oid
WHERE
    c.relfilenode != 0  -- Исключает служебные объекты
    --AND n.nspname NOT LIKE 'pg_%'  -- Исключает системные схемы (опционально)
ORDER BY 5 DESC ;

-- Как проверить, есть ли TOAST-таблицы в БД?
SELECT
    relname,
    reltoastrelid::regclass AS toast_table,
    pg_size_pretty(pg_total_relation_size(reltoastrelid)) AS toast_size
FROM
    pg_class
WHERE
    relkind = 'r'  -- Обычные таблицы
    AND reltoastrelid != 0;  -- Имеют TOAST-таблицу


SELECT pg_column_size(attributes), pg_total_relation_size('messages') FROM messages;

SELECT relname
FROM pg_class
WHERE relname LIKE 'pg_toast%' AND relnamespace = (
    SELECT relnamespace FROM pg_class WHERE relname = 'messages'
);

SELECT column_name, data_type, udt_name
FROM information_schema.columns
WHERE table_name = 'messages' AND column_name = 'attributes';

SELECT reltoastrelid
FROM pg_class
WHERE relname = 'messages';

SELECT relname
FROM pg_class
WHERE oid = (SELECT reltoastrelid FROM pg_class WHERE relname = 'messages');

SELECT attname, attstorage
FROM pg_attribute
WHERE attrelid = 'messages'::regclass AND attname = 'attributes';


SELECT relname, relfilenode
FROM pg_class
WHERE relname = 'pg_toast_17017342';


SELECT c.oid AS class_oid, c.relname, c.relkind, c.relfilenode, n.nspname
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
ORDER BY c.relfilenode NULLS LAST;

SELECT relname, relkind, reltoastrelid, relnamespace::regnamespace, reltablespace
FROM pg_class
WHERE relfilenode = 23835271;

SELECT relname, relfilenode
FROM pg_class
WHERE oid = 17017354;

SELECT
  c.oid AS table_oid,
  c.relname AS table_name,
  c.reltoastrelid AS toast_oid,
  t.relname AS toast_table_name
FROM pg_class c
LEFT JOIN pg_class t ON t.oid = c.reltoastrelid
WHERE c.reltoastrelid != 0
ORDER BY 2;


SELECT count(*) AS chunks, pg_size_pretty(SUM(length(chunk_data))) AS size
FROM pg_toast.pg_toast_17017342;

SELECT count(*) AS inline_count
FROM messages
WHERE pg_column_size(attributes) > 5;

SELECT
  width_bucket(pg_column_size(attributes), ARRAY[5, 256, 512, 1024, 2048, 4096, 8192, 16384]) AS size_bucket,
  count(*)
FROM messages
GROUP BY 1
ORDER BY 1;

-- Шаг 1: Найти TOAST-таблицу
SELECT c.relname AS main_table,
       t.relname AS toast_table
FROM pg_class c
JOIN pg_class t ON c.reltoastrelid = t.oid
WHERE c.relname = 'messages';

-- Шаг 2: Подставить имя TOAST-таблицы
SELECT COUNT(*) FROM pg_toast.pg_toast_17017342; -- замените на имя из шага 1




Postgresql: Могут ли TOAST-данные встроены в основной файл?

ChatGPT сказал:
В PostgreSQL TOAST (The Oversized-Attribute Storage Technique) используется для хранения больших значений таких типов, как TEXT, BYTEA, JSONB, и др., которые не помещаются в обычную строку таблицы.

Короткий ответ:
Да, TOAST-данные могут быть встроены в основной файл таблицы, если они достаточно малы. Это называется in-line storage.

Подробнее:
PostgreSQL использует следующий порядок при хранении больших атрибутов:

In-line (обычное хранение):
Если значение достаточно маленькое (до ~2KB), оно хранится прямо в основной строке таблицы, в основном файле (heap).

Compressed in-line:
Если значение чуть больше, PostgreSQL может попытаться сжать его и оставить в строке (всё ещё in-line, но в сжатом виде).

Out-of-line (TOAST):
Если значение всё равно слишком большое, оно помещается во внешнюю TOAST-таблицу, и в основной строке остаётся только ссылка.

Управление поведением:
Параметр хранения можно задать при создании таблицы или столбца:

sql
Копировать
Редактировать
CREATE TABLE example (
    data TEXT STORAGE PLAIN   -- принудительно in-line, без TOAST
);
Типы STORAGE:

PLAIN – всегда in-line, без сжатия.

EXTENDED – сжатие и TOAST (по умолчанию).

EXTERNAL – без сжатия, но можно вынести в TOAST.

MAIN – попытаться оставить in-line, но можно сжать.

Проверка, как хранятся данные:
sql
Копировать
Редактировать
SELECT pg_column_size(data), pg_total_relation_size('example')
FROM example;
Для анализа TOAST можно использовать pg_toast.* или функции вроде pg_size_pretty.

Follow GPT creator profile on LinkedIn https://linkedin.com/in/khanukov/






Вы сказали:
SELECT pg_column_size(attributes), pg_total_relation_size('messages')
FROM messages;
показал, что pg_column_size;pg_total_relation_size
5;35644260352
5;35644260352
5;35644260352
5;35644260352
5;35644260352
5;35644260352


ChatGPT сказал:
Результаты pg_column_size(attributes) = 5 говорят о следующем:

Значение в столбце attributes занимает всего 5 байт в каждой строке.

Это означает, что фактические данные не хранятся в самой строке, а скорее всего находятся во внешней TOAST-таблице, а в строке — только указатель (TOAST reference).

Значение pg_column_size включает:

1 байт — тег типа (если variable-width тип),

4 байта — указатель на TOAST (если используется TOAST).

Подтверждение:
Если хотите проверить, используется ли TOAST-таблица, выполните:

sql
Копировать
Редактировать
SELECT relname
FROM pg_class
WHERE relname LIKE 'pg_toast%' AND relnamespace = (
    SELECT relnamespace FROM pg_class WHERE relname = 'messages'
);
Затем посмотрите объём TOAST-таблицы:

sql
Копировать
Редактировать
SELECT pg_size_pretty(pg_total_relation_size('pg_toast_<oid>'));
Замените pg_toast_<oid> на имя, полученное выше.

Если вы хотите вернуть данные обратно in-line:
Убедитесь, что размер значений не превышает лимиты (примерно 2KB).

Измените STORAGE:

sql
Копировать
Редактировать
ALTER TABLE messages ALTER COLUMN attributes SET STORAGE MAIN;
Принудите PostgreSQL пересохранить строки (например, UPDATE с самим собой):

sql
Копировать
Редактировать
UPDATE messages SET attributes = attributes;
Это может вернуть данные обратно в основную таблицу — если они достаточно малы.

Follow GPT creator profile on LinkedIn https://linkedin.com/in/khanukov/






Вы сказали:
SELECT relname
FROM pg_class
WHERE relname LIKE 'pg_toast%' AND relnamespace = (
    SELECT relnamespace FROM pg_class WHERE relname = 'messages'
);

пусто


ChatGPT сказал:
Если запрос не вернул строк, это означает следующее:

Возможные причины:
TOAST-таблица действительно не используется:

Значения в attributes могут быть короткими (или пустыми), и TOAST не был задействован.

pg_column_size = 5 может указывать на NULL или короткие значения типа '' (пустая строка), которые PostgreSQL оптимизирует очень сильно.

TOAST-таблица может существовать, но в другом namespace:

Ваш подзапрос ищет TOAST-таблицу в том же namespace, что и messages. Это корректно, но если messages — представление (view) или foreign table, это может не сработать.

attributes — тип, не использующий TOAST (например, INT или BOOLEAN)

TOAST работает только для типов с переменной длиной (TEXT, BYTEA, JSONB, и т.п.). Если attributes — это, например, INTEGER[], PostgreSQL может использовать другие механизмы хранения.

Что можно сделать дальше:
1. Узнать точный тип столбца:
sql
Копировать
Редактировать
SELECT column_name, data_type, udt_name
FROM information_schema.columns
WHERE table_name = 'messages' AND column_name = 'attributes';
2. Проверить, есть ли TOAST-таблица напрямую через pg_class и pg_toast_relid:
sql
Копировать
Редактировать
SELECT reltoastrelid
FROM pg_class
WHERE relname = 'messages';
Если reltoastrelid = 0, значит TOAST-таблица отсутствует.

Если не 0, получить её имя:

sql
Копировать
Редактировать
SELECT relname
FROM pg_class
WHERE oid = (SELECT reltoastrelid FROM pg_class WHERE relname = 'messages');
Если attributes — это TEXT, BYTEA, JSONB и значения реально большие, но TOAST не создаётся — возможно, вы используете STORAGE PLAIN:

sql
Копировать
Редактировать
SELECT attname, attstorage
FROM pg_attribute
WHERE attrelid = 'messages'::regclass AND attname = 'attributes';
Значения attstorage:

p = PLAIN

e = EXTENDED (по умолчанию для TEXT)

m = MAIN

x = EXTERNAL

Если стоит p, TOAST будет отключён.
*/

SELECT 'column' AS object_type, column_name AS object_name, data_type AS object_details
FROM information_schema.columns
WHERE table_name = 'messages'
UNION ALL
SELECT 'constraint' AS object_type, constraint_name AS object_name, constraint_type AS object_details
FROM information_schema.table_constraints
WHERE table_name = 'messages'
UNION ALL
SELECT 'trigger' AS object_type, trigger_name AS object_name, action_statement AS object_details
FROM information_schema.triggers
WHERE event_object_table = 'messages'
UNION ALL
SELECT 'index' AS object_type, indexname AS object_name, indexdef AS object_details
FROM pg_indexes
WHERE tablename = 'messages'
ORDER BY 3;

SELECT pg_column_size(row(true));

SELECT pg_column_size(row(click_text)) FROM messages LIMIT 1;


SELECT pg_column_size(row(id, profile_id, template_id, send_status, delivery_status, close_status, click_status, read_status, remove_status, transaction_time, click_text, click_button2, click_button1, click_button3, attributes, time_to_send, created_at, trigger_uuid, try_integration_send, repeat))
FROM messages LIMIT 1;
SELECT pg_relation_size('messages')::numeric / pg_table_size('messages')::numeric AS avg_tuple_size;
SELECT pg_relation_size('messages') AS table_size,
       pg_total_relation_size('messages') AS total_size;

SELECT relname, pg_relation_size(oid) AS size
FROM pg_class
WHERE relname LIKE 'pg_toast_%';

SELECT relname, reltoastrelid
FROM pg_class
WHERE reltoastrelid IN (2619, 2618, 17017333, 17017454);

SELECT c.relname AS table_name, t.relname AS toast_table_name
FROM pg_class c
LEFT JOIN pg_class t ON c.reltoastrelid = t.oid
WHERE t.relname IS NOT NULL;

SELECT c.relname AS table_name, t.relname AS toast_table_name
FROM pg_class c
LEFT JOIN pg_class t ON c.reltoastrelid = t.oid
WHERE t.relname IS NOT NULL;

SELECT attrelid::regclass AS table_name, attname AS column_name
FROM pg_attribute
WHERE attstorage IN ('x', 'e');

SELECT attname AS column_name
FROM pg_attribute
WHERE attrelid = 'messages'::regclass
AND attstorage IN ('x', 'e');

SELECT COUNT(*) FROM messages WHERE LENGTH(attributes::text) > 2000;

SELECT id, LENGTH(attributes::text) AS attr_size
FROM messages
ORDER BY attr_size DESC
LIMIT 10;

SELECT LENGTH(attributes::text) AS attr_size, COUNT(*)
FROM messages
GROUP BY attr_size
ORDER BY attr_size DESC
LIMIT 10;




