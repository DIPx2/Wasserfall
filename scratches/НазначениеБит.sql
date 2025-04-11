0   1   2   3   4   5   6   7 -- MSB 0 (Most Significant Bit) First – нумерация слева направо (от старшего к младшему) В PostgreSQL битовые строки (BIT(n)) индексируются слева направо (MSB First).
0   0   0   0   0   0   0   0
|   |   |   |   |   |   |   |
|   |   |   |   |   |   |   +------
|   |   |   |   |   |   +------
|   |   |   |   |   +------
|   |   |   |   +------
|   |   |   +------
|   |   +------ На этом сервере собирать отчеты pgbadger'ом?
|   +------ Этот сервер для обслуживающих операций?
|   +------ Эта база данных для обслуживающих операций?
+------ Этот сервер участвует в реиндексации?
+------ Эта база данных участвует в реиндексации?

UPDATE robohub.reference."Servers" SET switch_serv = switch_serv | B'00100000'
WHERE pk_id_conn = 3;

SELECT * from  robohub.reference."Servers" WHERE (switch_serv & B'00100000') = B'00100000';

C:\Users\g.timofeyev\AppData\Roaming\JetBrains\DataGrip2024.3\scratches

/*
1. Установить 8-й бит (самый левый) в 1
Используйте побитовую операцию | (OR) с B'10000000':
UPDATE your_table
SET robohub.reference."Servers".switch = robohub.reference."Servers".switch | B'10000000'
WHERE condition;
✅ Это установит самый левый бит (8-й бит) в 1, не изменяя остальные.

2. Проверить, установлен ли 8-й бит в 1
Используйте побитовое AND с B'10000000' и проверьте, не равно ли оно 0:
SELECT switch, (switch & B'10000000') = B'10000000' AS is_set FROM robohub.reference."Servers";
✅ is_set = true, если 8-й бит установлен в 1, иначе false.

Дополнительно:
Если нужно сбросить 8-й бит в 0, используйте & с B'01111111':
UPDATE your_table
SET robohub.reference."Servers".switch = robohub.reference."Servers".switch & B'01111111'
WHERE condition;
✅ Это оставит все биты неизменными, кроме 8-го, который станет 0.

Если нужно переключить (инвертировать) 8-й бит, используйте # (XOR):
UPDATE your_table
SET robohub.reference."Servers".switch = robohub.reference."Servers".switch # B'10000000'
WHERE condition;
✅ 1 → 0, 0 → 1 (переключение состояния).

 */