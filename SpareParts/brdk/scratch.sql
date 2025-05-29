EXPLAIN ANALYSE
SELECT COUNT(*) AS aggregate
FROM (SELECT r.*
      FROM "rooms" AS "r"
               LEFT JOIN (SELECT "room_id", "project_id", "user_id", "action", "created_at" FROM "action_logs" WHERE ("user_type" IN ('1', '3') AND "action" IN ('18', '19'))) AS "al" ON "r"."id" = "al"."room_id" AND "r"."project_id" = "al"."project_id"
               LEFT JOIN "room_managers" AS "rmmng" ON "rmmng"."room_id" = "r"."id" AND "rmmng"."project_id" = "r"."project_id" AND "rmmng"."is_primary" = 't'
               LEFT JOIN (SELECT "usr"."id" FROM "users" AS "usr" WHERE "usr"."name"::text ILIKE '%4\_175794%' OR "usr"."email"::text ILIKE '%4\_175794%') AS "usr" ON "rmmng"."manager_id" = "usr"."id" OR "al"."user_id" = "usr"."id"
               LEFT JOIN (SELECT "cst"."id", "cst"."project_id" FROM "customers" AS "cst" WHERE "cst"."name"::text ILIKE '%4\_175794%' OR "cst"."email"::text ILIKE '%4\_175794%') AS "cst" ON "cst"."id" = "r"."customer_id" AND "cst"."project_id" = "r"."project_id"
               LEFT JOIN (SELECT "msg"."id", "msg"."project_id", "msg"."room_id" FROM "messages" AS "msg" WHERE "msg"."msg"::text ILIKE '%4\_175794%') AS "msg" ON "msg"."room_id" = "r"."id" AND "msg"."project_id" = "r"."project_id"
      WHERE ((r.id, r.project_id) IN ((175794, 4)) OR COALESCE(usr.id, cst.id, msg.id) IS NOT NULL)
      GROUP BY "r"."id", "r"."project_id") AS "aggregate_table";

EXPLAIN ANALYSE
WITH aggregate_table AS (SELECT DISTINCT ON (rooms.id)
                               rooms.id AS room_id,
                               rooms.project_id AS room_project_id,
                               al.user_id AS action_user_id,
                               rmmng.manager_id AS primary_manager_id,
                               usr.id AS user_id,
                               cst.id AS customer_id,
                               cst.project_id AS customer_project_id,
                               msg.id AS message_id,
                               msg.project_id AS message_project_id
                        FROM rooms
                                  LEFT JOIN (SELECT room_id, project_id, user_id FROM action_logs WHERE user_type IN ('1', '3') AND action IN ('18', '19')) AS al ON rooms.id = al.room_id AND rooms.project_id = al.project_id
                                  LEFT JOIN room_managers AS rmmng ON rmmng.room_id = rooms.id AND rmmng.project_id = rooms.project_id AND rmmng.is_primary = 't'
                                  LEFT JOIN (SELECT usr.id FROM users AS usr WHERE usr.name::text ILIKE '%4\_175794%' OR usr.email::text ILIKE '%4\_175794%') AS usr ON rmmng.manager_id = usr.id OR al.user_id = usr.id
                                  LEFT JOIN (SELECT cst.id, cst.project_id FROM customers AS cst WHERE cst.name::text ILIKE '%4\_175794%' OR cst.email::text ILIKE '%4\_175794%') AS cst ON cst.id = rooms.customer_id AND cst.project_id = rooms.project_id
                                  LEFT JOIN (SELECT msg.id, msg.project_id, msg.room_id FROM messages AS msg WHERE msg.msg::text ILIKE '%4\_175794%') AS msg ON msg.room_id = rooms.id AND msg.project_id = rooms.project_id
                         WHERE ((rooms.id, rooms.project_id) IN ((175794, 4)) OR COALESCE(usr.id, cst.id, msg.id) IS NOT NULL))
SELECT COUNT(aggregate_table.room_id) AS aggregate
FROM aggregate_table;



SELECT DISTINCT ON (rooms.id)
                               rooms.id AS room_id,
                               rooms.project_id AS room_project_id,
                               al.user_id AS action_user_id,
                               rmmng.manager_id AS primary_manager_id,
                               usr.id AS user_id,
                               cst.id AS customer_id,
                               cst.project_id AS customer_project_id,
                               msg.id AS message_id,
                               msg.project_id AS message_project_id
                        FROM rooms



SELECT * FROM pg_stat_user_indexes WHERE indexrelname in (
'action_logs_action_idx',
'action_logs_created_at_idx',
'action_logs_ri_pi_idx',
'action_logs_room_id_index',
'action_logs_user_id_idx',
'action_logs_user_type_action_idx'
)

Кардинальность индекса - это количество уникальных комбинаций значений в индексированных столбцах (distinct).
Селективность - это отношение количества уникальных значений к общему количеству записей.