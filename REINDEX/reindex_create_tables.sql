create table if not exists public."Bloat_Index_Db_Connections"
(
    pk_id_conn serial primary key,
    conn_port  integer not null,
    conn_host  text    not null
);
comment on table public."Bloat_Index_Db_Connections" is 'Родительская таблица списков подключений - сервера';

create table if not exists public."Bloat_Index_Db_Connections_Details"
(
    pk_id_db serial primary key,
    fk_pk_id_conn integer not null references public."Bloat_Index_Db_Connections" on delete cascade,
    db_scheme     char(100) default 'Public'::bpchar not null,
    db_name       text                               not null
);
comment on table public."Bloat_Index_Db_Connections_Details" is 'Дочерняя таблица списков подключений - базы данных';

create table if not exists public."Log_Bloat_Index_Operations"
(
    pk_id_op integer generated always as identity primary key,
    fk_pk_id_db integer not null references public."Bloat_Index_Db_Connections_Details" on delete cascade,
    op_date     bigint default EXTRACT(epoch FROM now()),
    op_server   char(100) not null,
    op_scheme   char(100) not null,
    op_base     char(100) not null
);
comment on table public."Log_Bloat_Index_Operations" is 'Родительская таблица - журнал операций';
comment on column public."Log_Bloat_Index_Operations".op_date is 'Unix-формат даты, временная метка операции.';

create table if not exists public."Log_Bloat_Index_Errors"
(
    pk_id_err integer generated always as identity constraint "Log_Bloat_Index_Error_pkey" primary key,
    fk_pk_id_op integer not null constraint "Log_Bloat_Index_Error_fk_pk_id_op_fkey" references public."Log_Bloat_Index_Operations" on delete cascade,
    err_label   char(100) default '*'::bpchar,
    err_message text      default '*'::text,
    err_detail  text      default '*'::text,
    err_code    text      default '*'::text
);
comment on table public."Log_Bloat_Index_Errors" is 'Дочерняя таблица - учет ошибок';

create table if not exists public."Log_Bloat_Index_Details"
(
    pk_id_det integer generated always as identity primary key,
    fk_pk_id_op integer not null references public."Log_Bloat_Index_Operations" on delete cascade,
    det_clocking         bigint default 2505600       not null,
    det_perc_bloat       real   default 0             not null,
    det_perc_bloat_after real   default '-1'::real    not null,
    det_index            text   default 'NIHIL'::text not null
);
comment on table public."Log_Bloat_Index_Details" is 'Дочерняя таблица - детализация операций';
comment on column public."Log_Bloat_Index_Details".det_clocking is 'Unix-формат времени, количество времени на обработку одного индекса';
comment on column public."Log_Bloat_Index_Details".det_index is 'NIHIL - у базы данных отсутствуют раздутые индексы';
