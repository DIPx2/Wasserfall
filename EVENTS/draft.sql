-- SCHEMA: events

-- DROP SCHEMA IF EXISTS events ;

CREATE SCHEMA IF NOT EXISTS events AUTHORIZATION gtimofeyev;
COMMENT ON SCHEMA events IS 'Фиксация различных событий, имеющих значение для работы баз данных';

INSERT INTO events.event_log( id, event_date, user_id, description) VALUES (DEFAULT, DEFAULT,DEFAULT, 'Начало жизни');

SELECT id, event_date, user_id, description FROM events.event_log;