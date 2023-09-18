-- https://github.com/apache/incubator-doris/issues/4926
drop database if exists issue_4926

create database issue_4926
use issue_4926
WITH a AS ( SELECT 2 AS id, '1234' AS user_id UNION ALL SELECT 1 AS id, NULL AS user_id ),  b AS ( SELECT 4 AS id, '543' AS user_id UNION ALL SELECT 3 AS id, NULL AS user_id ) SELECT id, user_id FROM a UNION ALL SELECT id, user_id FROM b

drop database issue_4926
