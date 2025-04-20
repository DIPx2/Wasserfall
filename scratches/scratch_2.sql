
-------- ТЕНЕВАЯ ТАБЛИЦА

CREATE TABLE public.messages_temp_shadow (
    id                    uuid         not null,
    msg                   text         not null
);

INSERT INTO public.messages_temp_shadow (
    id,
    msg
)
SELECT
    id,
    msg
FROM fdw_mbss.messages;