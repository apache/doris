set sql_dialect='presto';
set enable_fallback_to_original_planner=false;
set debug_skip_fold_constant=false;
-- SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567891Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.1234567891Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678912Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.12345678912Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789123Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.123456789123Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56+04:44' = TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.1+04:44' = TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.12+04:44' = TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.123+04:44' = TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.1234+04:44' = TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.12345+04:44' = TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.123456+04:44' = TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.1234567+04:44' = TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.12345678+04:44' = TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.123456789+04:44' = TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.1234567891+04:44' = TIMESTAMP '2020-05-01 12:34:56.1234567891Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.12345678912+04:44' = TIMESTAMP '2020-05-01 12:34:56.12345678912Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.123456789123+04:44' = TIMESTAMP '2020-05-01 12:34:56.123456789123Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56+04:44' <> TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.1+04:44' <> TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.12+04:44' <> TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.123+04:44' <> TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.1234+04:44' <> TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.12345+04:44' <> TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.123456+04:44' <> TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.1234567+04:44' <> TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.12345678+04:44' <> TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.123456789+04:44' <> TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.1234567891+04:44' <> TIMESTAMP '2020-05-01 12:34:56.1234567891Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.12345678912+04:44' <> TIMESTAMP '2020-05-01 12:34:56.12345678912Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.123456789123+04:44' <> TIMESTAMP '2020-05-01 12:34:56.123456789123Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.1Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.12Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.123Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.1234Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.12345Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.123456Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.1234567Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.12345678Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.123456789Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.1234567890Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.12345678901Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.123456789012Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:57Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.2Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.13Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.124Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.1235Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.12346Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.123457Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.1234568Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.12345679Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.123456781Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.1234567891Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.12345678902Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.123456789013Asia/Kathmandu'; # differ: doris : None, presto : True
SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 11:33:56+04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 11:33:56.1+04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 11:33:56.12+04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 11:33:56.123+04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 11:33:56.1234+04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 11:33:56.12345+04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 11:33:56.123456+04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 11:33:56.1234567+04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 11:33:56.12345678+04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 11:33:56.123456789+04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 11:33:56.1234567891+04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234567891Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 11:33:56.12345678912+04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345678912Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 11:33:56.123456789123+04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456789123Asia/Kathmandu';
-- SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:55Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.0Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.11Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.122Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1233Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12344Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123455Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234566Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345677Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456788Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234567899Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345678900Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456789011Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:57Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.2Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.13Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.124Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.1235Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.12346Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.123457Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.1234568Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.12345679Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.123456790Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.1234567891Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.12345678902Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.123456789013Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:55Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:55.9Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.11Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.122Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.1233Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.12344Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.123455Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.1234566Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.12345677Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.123456788Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.1234567889Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.12345678899Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.123456789011Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:57Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.2Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.13Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.124Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1235Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12346Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123457Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1234568Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12345679Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123456790Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1234567891Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12345678902Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123456789013Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:55Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.0Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.11Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.122Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1233Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12344Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123454Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1234566Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12345677Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123456788Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1234567889Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12345678900Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123456789011Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:55Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:55.9Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.11Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.122Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1233Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12344Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123455Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1234566Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12345677Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123456788Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1234567889Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12345678899Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123456789011Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:57Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.2Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.13Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.124Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1235Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12346Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123457Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1234568Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12345679Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1234567891Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1234567891Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12345678902Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123456789013Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:55Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:57Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.0Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.2Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.11Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.13Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.122Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.124Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.1233Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.1235Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.12344Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.12346Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.123455Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.123457Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.1234566Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.1234568Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.12345677Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.12345679Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.123456788Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.123456790Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.1234567889Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.1234567891Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.12345678902Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.123456789011Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.123456789013Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.223Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.243Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.246Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.246Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.246Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.246Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.246Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.246Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.246Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.246Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.246Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.246Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.123Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.223Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.243Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.246Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.246Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.246Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.246Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.246Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.246Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.246Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.246Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.246Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.246Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : None, presto : 2020-05-02 12:34:56.000Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : None, presto : 2020-05-02 12:34:56.100Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : None, presto : 2020-05-02 12:34:56.120Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : None, presto : 2020-05-02 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : None, presto : 2020-05-02 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : None, presto : 2020-05-02 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : None, presto : 2020-05-02 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : None, presto : 2020-05-02 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : None, presto : 2020-05-02 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : None, presto : 2020-05-02 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : None, presto : 2020-05-02 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : None, presto : 2020-05-02 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : None, presto : 2020-05-02 12:34:56.123Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-02 12:34:56.000Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-02 12:34:56.100Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-02 12:34:56.120Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-02 12:34:56.123Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-02 12:34:56.123Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-02 12:34:56.123Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-02 12:34:56.123Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-02 12:34:56.123Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-02 12:34:56.123Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-02 12:34:56.123Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-02 12:34:56.123Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-02 12:34:56.123Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-02 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:54.877Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:54.977Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:54.997Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:55.000Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:55.000Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:55.000Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:55.000Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:55.000Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:55.000Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:55.000Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:55.000Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:55.000Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:55.000Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : None, presto : 2020-04-30 12:34:56.000Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : None, presto : 2020-04-30 12:34:56.100Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : None, presto : 2020-04-30 12:34:56.120Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : None, presto : 2020-04-30 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : None, presto : 2020-04-30 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : None, presto : 2020-04-30 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : None, presto : 2020-04-30 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : None, presto : 2020-04-30 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : None, presto : 2020-04-30 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : None, presto : 2020-04-30 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : None, presto : 2020-04-30 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : None, presto : 2020-04-30 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : None, presto : 2020-04-30 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-06-01 12:34:56.000Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-06-01 12:34:56.100Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-06-01 12:34:56.120Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-06-01 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-06-01 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-06-01 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-06-01 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-06-01 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-06-01 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-06-01 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-06-01 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-06-01 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-06-01 12:34:56.123Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu'; # differ: doris : None, presto : 2020-06-01 12:34:56.000Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu'; # differ: doris : None, presto : 2020-06-01 12:34:56.100Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu'; # differ: doris : None, presto : 2020-06-01 12:34:56.120Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu'; # differ: doris : None, presto : 2020-06-01 12:34:56.123Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu'; # differ: doris : None, presto : 2020-06-01 12:34:56.123Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu'; # differ: doris : None, presto : 2020-06-01 12:34:56.123Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu'; # differ: doris : None, presto : 2020-06-01 12:34:56.123Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu'; # differ: doris : None, presto : 2020-06-01 12:34:56.123Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu'; # differ: doris : None, presto : 2020-06-01 12:34:56.123Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu'; # differ: doris : None, presto : 2020-06-01 12:34:56.123Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu'; # differ: doris : None, presto : 2020-06-01 12:34:56.123Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu'; # differ: doris : None, presto : 2020-06-01 12:34:56.123Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu'; # differ: doris : None, presto : 2020-06-01 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-10-31 01:02:03.000Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.1Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-10-31 01:02:03.100Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.12Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-10-31 01:02:03.120Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.123Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-10-31 01:02:03.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.1234Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-10-31 01:02:03.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.12345Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-10-31 01:02:03.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.123456Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-10-31 01:02:03.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.1234567Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-10-31 01:02:03.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.12345678Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-10-31 01:02:03.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.123456789Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-10-31 01:02:03.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.1234567890Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-10-31 01:02:03.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.12345678901Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-10-31 01:02:03.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.123456789012Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-10-31 01:02:03.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-04-01 12:34:56.000Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-04-01 12:34:56.100Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-04-01 12:34:56.120Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-04-01 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-04-01 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-04-01 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-04-01 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-04-01 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-04-01 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-04-01 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-04-01 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-04-01 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-04-01 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-08-31 01:02:03.000Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.1Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-08-31 01:02:03.100Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.12Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-08-31 01:02:03.120Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.123Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-08-31 01:02:03.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.1234Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-08-31 01:02:03.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.12345Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-08-31 01:02:03.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.123456Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-08-31 01:02:03.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.1234567Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-08-31 01:02:03.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.12345678Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-08-31 01:02:03.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.123456789Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-08-31 01:02:03.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.1234567890Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-08-31 01:02:03.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.12345678901Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-08-31 01:02:03.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.123456789012Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-08-31 01:02:03.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.000
-- SELECT TIMESTAMP '2020-05-01 12:34:56.2Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.1Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.100
-- SELECT TIMESTAMP '2020-05-01 12:34:56.22Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.11Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.110
-- SELECT TIMESTAMP '2020-05-01 12:34:56.222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.111Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.111
-- SELECT TIMESTAMP '2020-05-01 12:34:56.2222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.1111Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.111
-- SELECT TIMESTAMP '2020-05-01 12:34:56.22222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.11111Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.111
-- SELECT TIMESTAMP '2020-05-01 12:34:56.222222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.111111Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.111
-- SELECT TIMESTAMP '2020-05-01 12:34:56.2222222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.1111111Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.111
-- SELECT TIMESTAMP '2020-05-01 12:34:56.22222222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.11111111Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.111
-- SELECT TIMESTAMP '2020-05-01 12:34:56.222222222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.111111111Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.111
-- SELECT TIMESTAMP '2020-05-01 12:34:56.2222222222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.1111111111Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.111
-- SELECT TIMESTAMP '2020-05-01 12:34:56.22222222222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.11111111111Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.111
-- SELECT TIMESTAMP '2020-05-01 12:34:56.222222222222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.111111111111Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.111
-- SELECT TIMESTAMP '2020-05-01 12:34:56.9Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.1Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.800
-- SELECT TIMESTAMP '2020-05-01 12:34:56.99Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.11Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.880
-- SELECT TIMESTAMP '2020-05-01 12:34:56.999Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.111Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.888
-- SELECT TIMESTAMP '2020-05-01 12:34:56.9999Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.1111Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.889
-- SELECT TIMESTAMP '2020-05-01 12:34:56.99999Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.11111Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.889
-- SELECT TIMESTAMP '2020-05-01 12:34:56.999999Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.111111Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.889
-- SELECT TIMESTAMP '2020-05-01 12:34:56.9999999Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.1111111Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.889
-- SELECT TIMESTAMP '2020-05-01 12:34:56.99999999Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.11111111Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.889
-- SELECT TIMESTAMP '2020-05-01 12:34:56.999999999Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.111111111Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.889
-- SELECT TIMESTAMP '2020-05-01 12:34:56.9999999999Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.1111111111Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.889
-- SELECT TIMESTAMP '2020-05-01 12:34:56.99999999999Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.11111111111Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.889
-- SELECT TIMESTAMP '2020-05-01 12:34:56.999999999999Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.111111111111Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.889
-- SELECT TIMESTAMP '2020-05-01 12:34:56.0002Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.0005Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.000
-- SELECT TIMESTAMP '2020-05-01 12:34:56.00022Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.00055Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.000
-- SELECT TIMESTAMP '2020-05-01 12:34:56.000222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.000555Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.000
-- SELECT TIMESTAMP '2020-05-01 12:34:56.0002222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.0005555Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.000
-- SELECT TIMESTAMP '2020-05-01 12:34:56.00022222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.00055555Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.000
-- SELECT TIMESTAMP '2020-05-01 12:34:56.000222222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.000555555Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.000
-- SELECT TIMESTAMP '2020-05-01 12:34:56.0002222222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.0005555555Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.000
-- SELECT TIMESTAMP '2020-05-01 12:34:56.00022222222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.00055555555Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.000
-- SELECT TIMESTAMP '2020-05-01 12:34:56.000222222222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.000555555555Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.000
-- SELECT TIMESTAMP '2020-05-01 12:34:56.0002Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.0009Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:00.999
-- SELECT TIMESTAMP '2020-05-01 12:34:56.00022Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.00099Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:00.999
-- SELECT TIMESTAMP '2020-05-01 12:34:56.000222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.000999Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:00.999
-- SELECT TIMESTAMP '2020-05-01 12:34:56.0002222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.0009999Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:00.999
-- SELECT TIMESTAMP '2020-05-01 12:34:56.00022222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.00099999Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:00.999
-- SELECT TIMESTAMP '2020-05-01 12:34:56.000222222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.000999999Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:00.999
-- SELECT TIMESTAMP '2020-05-01 12:34:56.0002222222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.0009999999Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:00.999
-- SELECT TIMESTAMP '2020-05-01 12:34:56.00022222222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.00099999999Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:00.999
-- SELECT TIMESTAMP '2020-05-01 12:34:56.000222222222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.000999999999Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:00.999
set debug_skip_fold_constant=true;
SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567891Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.1234567891Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678912Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.12345678912Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789123Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.123456789123Asia/Kathmandu';
-- SELECT TIMESTAMP '2020-05-01 11:33:56+04:44' = TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.1+04:44' = TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.12+04:44' = TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.123+04:44' = TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.1234+04:44' = TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.12345+04:44' = TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.123456+04:44' = TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.1234567+04:44' = TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.12345678+04:44' = TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.123456789+04:44' = TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.1234567891+04:44' = TIMESTAMP '2020-05-01 12:34:56.1234567891Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.12345678912+04:44' = TIMESTAMP '2020-05-01 12:34:56.12345678912Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.123456789123+04:44' = TIMESTAMP '2020-05-01 12:34:56.123456789123Asia/Kathmandu'; # differ: doris : 0, presto : True
SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu';
-- SELECT TIMESTAMP '2020-05-01 11:33:56+04:44' <> TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.1+04:44' <> TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.12+04:44' <> TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.123+04:44' <> TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.1234+04:44' <> TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.12345+04:44' <> TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.123456+04:44' <> TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.1234567+04:44' <> TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.12345678+04:44' <> TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.123456789+04:44' <> TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.1234567891+04:44' <> TIMESTAMP '2020-05-01 12:34:56.1234567891Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.12345678912+04:44' <> TIMESTAMP '2020-05-01 12:34:56.12345678912Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.123456789123+04:44' <> TIMESTAMP '2020-05-01 12:34:56.123456789123Asia/Kathmandu'; # differ: doris : 1, presto : False
SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.1Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.12Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.123Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.1234Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.12345Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.123456Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.1234567Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.12345678Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.123456789Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.1234567890Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.12345678901Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.123456789012Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:57Asia/Kathmandu';
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.2Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.13Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.124Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.1235Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.12346Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.123457Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.1234568Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.12345679Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.123456781Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.1234567891Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.12345678902Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.123456789013Asia/Kathmandu'; # differ: doris : 0, presto : True
SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu';
-- SELECT TIMESTAMP '2020-05-01 11:33:56+04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.1+04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.12+04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.123+04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.1234+04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.12345+04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.123456+04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.1234567+04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.12345678+04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.123456789+04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.1234567891+04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234567891Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.12345678912+04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345678912Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.123456789123+04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456789123Asia/Kathmandu'; # differ: doris : 1, presto : False
SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:55Asia/Kathmandu';
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.0Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.11Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.122Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1233Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12344Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123455Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234566Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345677Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456788Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234567899Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345678900Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456789011Asia/Kathmandu'; # differ: doris : 0, presto : True
SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:57Asia/Kathmandu';
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.2Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.13Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.124Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.1235Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.12346Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.123457Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.1234568Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.12345679Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.123456790Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.1234567891Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.12345678902Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.123456789013Asia/Kathmandu'; # differ: doris : 0, presto : True
SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:55Asia/Kathmandu';
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:55.9Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.11Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.122Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.1233Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.12344Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.123455Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.1234566Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.12345677Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.123456788Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.1234567889Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.12345678899Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.123456789011Asia/Kathmandu'; # differ: doris : 0, presto : True
SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:57Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.2Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.13Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.124Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1235Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12346Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123457Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1234568Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12345679Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123456790Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1234567891Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12345678902Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123456789013Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:55Asia/Kathmandu';
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.0Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.11Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.122Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1233Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12344Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123454Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1234566Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12345677Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123456788Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1234567889Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12345678900Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123456789011Asia/Kathmandu'; # differ: doris : 1, presto : False
SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:55Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:55.9Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.11Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.122Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1233Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12344Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123455Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1234566Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12345677Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123456788Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1234567889Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12345678899Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123456789011Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:57Asia/Kathmandu';
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.2Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.13Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.124Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1235Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12346Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123457Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1234568Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12345679Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1234567891Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1234567891Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12345678902Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123456789013Asia/Kathmandu'; # differ: doris : 1, presto : False
SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:55Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:57Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.0Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.2Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.11Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.13Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.122Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.124Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.1233Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.1235Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.12344Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.12346Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.123455Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.123457Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.1234566Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.1234568Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.12345677Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.12345679Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.123456788Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.123456790Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.1234567889Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.1234567891Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.12345678902Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.123456789011Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.123456789013Asia/Kathmandu';
-- SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.223Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.243Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.246Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.246Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.246Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.246Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.246Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.246Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.246Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.246Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.246Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.246Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.123Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.223Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.243Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.246Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.246Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.246Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.246Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.246Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.246Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.246Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.246Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.246Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.246Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.000Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.100Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.120Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.123Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu'; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.000Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu'; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.100Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu'; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.120Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu'; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.123Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu'; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.123Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu'; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.123Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu'; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.123Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu'; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.123Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu'; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.123Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu'; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.123Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu'; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.123Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu'; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.123Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu'; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:54.877Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:54.977Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:54.997Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:55.000Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:55.000Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:55.000Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:55.000Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:55.000Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:55.000Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:55.000Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:55.000Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:55.000Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:55.000Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : 2020-04-30 12:34:56, presto : 2020-04-30 12:34:56.000Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : 2020-04-30 12:34:56, presto : 2020-04-30 12:34:56.100Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : 2020-04-30 12:34:56, presto : 2020-04-30 12:34:56.120Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : 2020-04-30 12:34:56, presto : 2020-04-30 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : 2020-04-30 12:34:56, presto : 2020-04-30 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : 2020-04-30 12:34:56, presto : 2020-04-30 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : 2020-04-30 12:34:56, presto : 2020-04-30 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : 2020-04-30 12:34:56, presto : 2020-04-30 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : 2020-04-30 12:34:56, presto : 2020-04-30 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : 2020-04-30 12:34:56, presto : 2020-04-30 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : 2020-04-30 12:34:56, presto : 2020-04-30 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : 2020-04-30 12:34:56, presto : 2020-04-30 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : 2020-04-30 12:34:56, presto : 2020-04-30 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.000Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.100Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.120Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.123Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu'; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.000Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu'; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.100Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu'; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.120Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu'; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.123Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu'; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.123Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu'; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.123Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu'; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.123Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu'; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.123Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu'; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.123Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu'; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.123Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu'; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.123Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu'; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.123Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu'; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-11-01 01:02:03, presto : 2020-10-31 01:02:03.000Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.1Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-11-01 01:02:03, presto : 2020-10-31 01:02:03.100Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.12Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-11-01 01:02:03, presto : 2020-10-31 01:02:03.120Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.123Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-11-01 01:02:03, presto : 2020-10-31 01:02:03.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.1234Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-11-01 01:02:03, presto : 2020-10-31 01:02:03.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.12345Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-11-01 01:02:03, presto : 2020-10-31 01:02:03.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.123456Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-11-01 01:02:03, presto : 2020-10-31 01:02:03.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.1234567Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-11-01 01:02:03, presto : 2020-10-31 01:02:03.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.12345678Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-11-01 01:02:03, presto : 2020-10-31 01:02:03.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.123456789Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-11-01 01:02:03, presto : 2020-10-31 01:02:03.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.1234567890Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-11-01 01:02:03, presto : 2020-10-31 01:02:03.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.12345678901Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-11-01 01:02:03, presto : 2020-10-31 01:02:03.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.123456789012Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-11-01 01:02:03, presto : 2020-10-31 01:02:03.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-04-01 12:34:56, presto : 2020-04-01 12:34:56.000Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-04-01 12:34:56, presto : 2020-04-01 12:34:56.100Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-04-01 12:34:56, presto : 2020-04-01 12:34:56.120Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-04-01 12:34:56, presto : 2020-04-01 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-04-01 12:34:56, presto : 2020-04-01 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-04-01 12:34:56, presto : 2020-04-01 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-04-01 12:34:56, presto : 2020-04-01 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-04-01 12:34:56, presto : 2020-04-01 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-04-01 12:34:56, presto : 2020-04-01 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-04-01 12:34:56, presto : 2020-04-01 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-04-01 12:34:56, presto : 2020-04-01 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-04-01 12:34:56, presto : 2020-04-01 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-04-01 12:34:56, presto : 2020-04-01 12:34:56.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-09-01 01:02:03, presto : 2020-08-31 01:02:03.000Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.1Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-09-01 01:02:03, presto : 2020-08-31 01:02:03.100Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.12Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-09-01 01:02:03, presto : 2020-08-31 01:02:03.120Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.123Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-09-01 01:02:03, presto : 2020-08-31 01:02:03.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.1234Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-09-01 01:02:03, presto : 2020-08-31 01:02:03.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.12345Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-09-01 01:02:03, presto : 2020-08-31 01:02:03.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.123456Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-09-01 01:02:03, presto : 2020-08-31 01:02:03.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.1234567Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-09-01 01:02:03, presto : 2020-08-31 01:02:03.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.12345678Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-09-01 01:02:03, presto : 2020-08-31 01:02:03.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.123456789Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-09-01 01:02:03, presto : 2020-08-31 01:02:03.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.1234567890Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-09-01 01:02:03, presto : 2020-08-31 01:02:03.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.12345678901Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-09-01 01:02:03, presto : 2020-08-31 01:02:03.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.123456789012Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-09-01 01:02:03, presto : 2020-08-31 01:02:03.123Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:01.000
-- SELECT TIMESTAMP '2020-05-01 12:34:56.2Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.1Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:01.100
-- SELECT TIMESTAMP '2020-05-01 12:34:56.22Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.11Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:01.110
-- SELECT TIMESTAMP '2020-05-01 12:34:56.222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.111Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:01.111
-- SELECT TIMESTAMP '2020-05-01 12:34:56.2222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.1111Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:01.111
-- SELECT TIMESTAMP '2020-05-01 12:34:56.22222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.11111Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:01.111
-- SELECT TIMESTAMP '2020-05-01 12:34:56.222222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.111111Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:01.111
-- SELECT TIMESTAMP '2020-05-01 12:34:56.2222222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.1111111Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:01.111
-- SELECT TIMESTAMP '2020-05-01 12:34:56.22222222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.11111111Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:01.111
-- SELECT TIMESTAMP '2020-05-01 12:34:56.222222222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.111111111Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:01.111
-- SELECT TIMESTAMP '2020-05-01 12:34:56.2222222222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.1111111111Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:01.111
-- SELECT TIMESTAMP '2020-05-01 12:34:56.22222222222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.11111111111Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:01.111
-- SELECT TIMESTAMP '2020-05-01 12:34:56.222222222222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.111111111111Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:01.111
-- SELECT TIMESTAMP '2020-05-01 12:34:56.9Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.1Asia/Kathmandu'; # differ: doris : 2, presto : 0 00:00:01.800
-- SELECT TIMESTAMP '2020-05-01 12:34:56.99Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.11Asia/Kathmandu'; # differ: doris : 2, presto : 0 00:00:01.880
-- SELECT TIMESTAMP '2020-05-01 12:34:56.999Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.111Asia/Kathmandu'; # differ: doris : 2, presto : 0 00:00:01.888
-- SELECT TIMESTAMP '2020-05-01 12:34:56.9999Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.1111Asia/Kathmandu'; # differ: doris : 2, presto : 0 00:00:01.889
-- SELECT TIMESTAMP '2020-05-01 12:34:56.99999Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.11111Asia/Kathmandu'; # differ: doris : 2, presto : 0 00:00:01.889
-- SELECT TIMESTAMP '2020-05-01 12:34:56.999999Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.111111Asia/Kathmandu'; # differ: doris : 2, presto : 0 00:00:01.889
-- SELECT TIMESTAMP '2020-05-01 12:34:56.9999999Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.1111111Asia/Kathmandu'; # differ: doris : 2, presto : 0 00:00:01.889
-- SELECT TIMESTAMP '2020-05-01 12:34:56.99999999Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.11111111Asia/Kathmandu'; # differ: doris : 2, presto : 0 00:00:01.889
-- SELECT TIMESTAMP '2020-05-01 12:34:56.999999999Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.111111111Asia/Kathmandu'; # differ: doris : 2, presto : 0 00:00:01.889
-- SELECT TIMESTAMP '2020-05-01 12:34:56.9999999999Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.1111111111Asia/Kathmandu'; # differ: doris : 2, presto : 0 00:00:01.889
-- SELECT TIMESTAMP '2020-05-01 12:34:56.99999999999Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.11111111111Asia/Kathmandu'; # differ: doris : 2, presto : 0 00:00:01.889
-- SELECT TIMESTAMP '2020-05-01 12:34:56.999999999999Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.111111111111Asia/Kathmandu'; # differ: doris : 2, presto : 0 00:00:01.889
-- SELECT TIMESTAMP '2020-05-01 12:34:56.0002Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.0005Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:01.000
-- SELECT TIMESTAMP '2020-05-01 12:34:56.00022Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.00055Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:01.000
-- SELECT TIMESTAMP '2020-05-01 12:34:56.000222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.000555Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:01.000
-- SELECT TIMESTAMP '2020-05-01 12:34:56.0002222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.0005555Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:01.000
-- SELECT TIMESTAMP '2020-05-01 12:34:56.00022222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.00055555Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:01.000
-- SELECT TIMESTAMP '2020-05-01 12:34:56.000222222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.000555555Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:01.000
-- SELECT TIMESTAMP '2020-05-01 12:34:56.0002222222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.0005555555Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:01.000
-- SELECT TIMESTAMP '2020-05-01 12:34:56.00022222222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.00055555555Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:01.000
-- SELECT TIMESTAMP '2020-05-01 12:34:56.000222222222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.000555555555Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:01.000
-- SELECT TIMESTAMP '2020-05-01 12:34:56.0002Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.0009Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:00.999
-- SELECT TIMESTAMP '2020-05-01 12:34:56.00022Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.00099Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:00.999
-- SELECT TIMESTAMP '2020-05-01 12:34:56.000222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.000999Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:00.999
-- SELECT TIMESTAMP '2020-05-01 12:34:56.0002222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.0009999Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:00.999
-- SELECT TIMESTAMP '2020-05-01 12:34:56.00022222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.00099999Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:00.999
-- SELECT TIMESTAMP '2020-05-01 12:34:56.000222222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.000999999Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:00.999
-- SELECT TIMESTAMP '2020-05-01 12:34:56.0002222222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.0009999999Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:00.999
-- SELECT TIMESTAMP '2020-05-01 12:34:56.00022222222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.00099999999Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:00.999
-- SELECT TIMESTAMP '2020-05-01 12:34:56.000222222222Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.000999999999Asia/Kathmandu' # differ: doris : 1, presto : 0 00:00:00.999
