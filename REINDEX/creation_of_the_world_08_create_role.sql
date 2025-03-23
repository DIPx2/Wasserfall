
-- Use server that needs to be connected to reindexing management

CREATE ROLE robo_sudo WITH
  LOGIN
  SUPERUSER
  INHERIT
  CREATEDB
  CREATEROLE
  NOREPLICATION
  BYPASSRLS
  ENCRYPTED PASSWORD 'SCRAM-SHA-256$4096:5Dr8ZtJJh7baC4Ca+V404Q==$s9DHV9azRp/B7sWFMQpWzmeOzOTWyFhXzQwraxnkTr0=:KP2jbP3uQR8oaEO4uGFQeT5d8wQ8er79HL0ev1xGEV8=';

COMMENT ON ROLE robo_sudo IS 'Не персонифицированная сущность с правами superuser для обслуживания баз данных';