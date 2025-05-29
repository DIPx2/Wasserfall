SELECT * FROM "groups"  ORDER BY 1 DESC ;

EXPLAIN SELECT ;

EXPLAIN update "messages" set "remove_status" = 't' where ("profile_id" = '1193793') and ("template_id" = '1318') and remove_status is not true;