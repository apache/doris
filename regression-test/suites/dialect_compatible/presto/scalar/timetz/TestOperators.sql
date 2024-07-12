set sql_dialect='presto';
set enable_fallback_to_original_planner=false;
set debug_skip_fold_constant=false;
-- SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678912 Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.12345678912 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789123 Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.123456789123 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56 +04:44' = TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.1 +04:44' = TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.12 +04:44' = TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.123 +04:44' = TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.1234 +04:44' = TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.12345 +04:44' = TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.123456 +04:44' = TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.1234567 +04:44' = TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.12345678 +04:44' = TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.123456789 +04:44' = TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.1234567891 +04:44' = TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.12345678912 +04:44' = TIMESTAMP '2020-05-01 12:34:56.12345678912 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.123456789123 +04:44' = TIMESTAMP '2020-05-01 12:34:56.123456789123 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56 +04:44' <> TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.1 +04:44' <> TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.12 +04:44' <> TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.123 +04:44' <> TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.1234 +04:44' <> TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.12345 +04:44' <> TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.123456 +04:44' <> TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.1234567 +04:44' <> TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.12345678 +04:44' <> TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.123456789 +04:44' <> TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.1234567891 +04:44' <> TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.12345678912 +04:44' <> TIMESTAMP '2020-05-01 12:34:56.12345678912 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.123456789123 +04:44' <> TIMESTAMP '2020-05-01 12:34:56.123456789123 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.1 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.12 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.123 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.1234 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.12345 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.123456 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.1234567 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.12345678 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.123456789 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.1234567890 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.12345678901 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.123456789012 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:57 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.2 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.13 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.124 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.1235 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.12346 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.123457 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.1234568 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.12345679 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.123456781 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.12345678902 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.123456789013 Asia/Kathmandu'; # differ: doris : None, presto : True
SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 11:33:56 +04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 11:33:56.1 +04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 11:33:56.12 +04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 11:33:56.123 +04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 11:33:56.1234 +04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 11:33:56.12345 +04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 11:33:56.123456 +04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 11:33:56.1234567 +04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 11:33:56.12345678 +04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 11:33:56.123456789 +04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 11:33:56.1234567891 +04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 11:33:56.12345678912 +04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345678912 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 11:33:56.123456789123 +04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456789123 Asia/Kathmandu';
-- SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:55 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.0 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.11 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.122 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1233 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12344 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123455 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234566 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345677 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456788 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234567899 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345678900 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456789011 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:57 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.2 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.13 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.124 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.1235 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.12346 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.123457 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.1234568 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.12345679 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.123456790 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.12345678902 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.123456789013 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:55 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:55.9 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.11 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.122 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.1233 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.12344 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.123455 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.1234566 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.12345677 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.123456788 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.1234567889 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.12345678899 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.123456789011 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:57 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.2 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.13 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.124 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1235 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12346 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123457 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1234568 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12345679 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123456790 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12345678902 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123456789013 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:55 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.0 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.11 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.122 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1233 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12344 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123454 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1234566 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12345677 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123456788 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1234567889 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12345678900 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123456789011 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:55 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:55.9 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.11 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.122 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1233 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12344 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123455 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1234566 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12345677 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123456788 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1234567889 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12345678899 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123456789011 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:57 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.2 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.13 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.124 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1235 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12346 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123457 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1234568 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12345679 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12345678902 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123456789013 Asia/Kathmandu'; # differ: doris : None, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:55 Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:57 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.0 Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.2 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.11 Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.13 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.122 Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.124 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.1233 Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.1235 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.12344 Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.12346 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.123455 Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.123457 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.1234566 Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.1234568 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.12345677 Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.12345679 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.123456788 Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.123456790 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.1234567889 Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.12345678902 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.123456789011 Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.123456789013 Asia/Kathmandu'; # differ: doris : None, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.223 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.243 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.246 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.246 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.246 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.246 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.246 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.246 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.246 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.246 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.246 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.246 Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.123 Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.223 Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.243 Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.246 Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.246 Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.246 Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.246 Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.246 Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.246 Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.246 Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.246 Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.246 Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.246 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : None, presto : 2020-05-02 12:34:56.000 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : None, presto : 2020-05-02 12:34:56.100 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : None, presto : 2020-05-02 12:34:56.120 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : None, presto : 2020-05-02 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : None, presto : 2020-05-02 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : None, presto : 2020-05-02 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : None, presto : 2020-05-02 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : None, presto : 2020-05-02 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : None, presto : 2020-05-02 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : None, presto : 2020-05-02 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : None, presto : 2020-05-02 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : None, presto : 2020-05-02 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : None, presto : 2020-05-02 12:34:56.123 Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-02 12:34:56.000 Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-02 12:34:56.100 Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-02 12:34:56.120 Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-02 12:34:56.123 Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-02 12:34:56.123 Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-02 12:34:56.123 Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-02 12:34:56.123 Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-02 12:34:56.123 Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-02 12:34:56.123 Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-02 12:34:56.123 Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-02 12:34:56.123 Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-02 12:34:56.123 Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-02 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:54.877 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:54.977 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:54.997 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:55.000 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:55.000 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:55.000 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:55.000 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:55.000 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:55.000 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:55.000 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:55.000 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:55.000 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:55.000 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : None, presto : 2020-04-30 12:34:56.000 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : None, presto : 2020-04-30 12:34:56.100 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : None, presto : 2020-04-30 12:34:56.120 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : None, presto : 2020-04-30 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : None, presto : 2020-04-30 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : None, presto : 2020-04-30 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : None, presto : 2020-04-30 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : None, presto : 2020-04-30 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : None, presto : 2020-04-30 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : None, presto : 2020-04-30 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : None, presto : 2020-04-30 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : None, presto : 2020-04-30 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : None, presto : 2020-04-30 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-06-01 12:34:56.000 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-06-01 12:34:56.100 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-06-01 12:34:56.120 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-06-01 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-06-01 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-06-01 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-06-01 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-06-01 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-06-01 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-06-01 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-06-01 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-06-01 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-06-01 12:34:56.123 Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu'; # differ: doris : None, presto : 2020-06-01 12:34:56.000 Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu'; # differ: doris : None, presto : 2020-06-01 12:34:56.100 Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu'; # differ: doris : None, presto : 2020-06-01 12:34:56.120 Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu'; # differ: doris : None, presto : 2020-06-01 12:34:56.123 Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu'; # differ: doris : None, presto : 2020-06-01 12:34:56.123 Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu'; # differ: doris : None, presto : 2020-06-01 12:34:56.123 Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu'; # differ: doris : None, presto : 2020-06-01 12:34:56.123 Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu'; # differ: doris : None, presto : 2020-06-01 12:34:56.123 Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu'; # differ: doris : None, presto : 2020-06-01 12:34:56.123 Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu'; # differ: doris : None, presto : 2020-06-01 12:34:56.123 Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu'; # differ: doris : None, presto : 2020-06-01 12:34:56.123 Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu'; # differ: doris : None, presto : 2020-06-01 12:34:56.123 Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu'; # differ: doris : None, presto : 2020-06-01 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-10-31 01:02:03.000 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.1 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-10-31 01:02:03.100 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.12 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-10-31 01:02:03.120 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.123 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-10-31 01:02:03.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.1234 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-10-31 01:02:03.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.12345 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-10-31 01:02:03.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.123456 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-10-31 01:02:03.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.1234567 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-10-31 01:02:03.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.12345678 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-10-31 01:02:03.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.123456789 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-10-31 01:02:03.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.1234567890 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-10-31 01:02:03.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.12345678901 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-10-31 01:02:03.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.123456789012 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-10-31 01:02:03.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-04-01 12:34:56.000 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-04-01 12:34:56.100 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-04-01 12:34:56.120 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-04-01 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-04-01 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-04-01 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-04-01 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-04-01 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-04-01 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-04-01 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-04-01 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-04-01 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-04-01 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-08-31 01:02:03.000 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.1 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-08-31 01:02:03.100 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.12 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-08-31 01:02:03.120 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.123 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-08-31 01:02:03.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.1234 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-08-31 01:02:03.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.12345 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-08-31 01:02:03.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.123456 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-08-31 01:02:03.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.1234567 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-08-31 01:02:03.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.12345678 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-08-31 01:02:03.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.123456789 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-08-31 01:02:03.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.1234567890 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-08-31 01:02:03.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.12345678901 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-08-31 01:02:03.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.123456789012 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : None, presto : 2020-08-31 01:02:03.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55 Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.000
-- SELECT TIMESTAMP '2020-05-01 12:34:56.2 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.1 Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.100
-- SELECT TIMESTAMP '2020-05-01 12:34:56.22 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.11 Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.110
-- SELECT TIMESTAMP '2020-05-01 12:34:56.222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.111 Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.111
-- SELECT TIMESTAMP '2020-05-01 12:34:56.2222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.1111 Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.111
-- SELECT TIMESTAMP '2020-05-01 12:34:56.22222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.11111 Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.111
-- SELECT TIMESTAMP '2020-05-01 12:34:56.222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.111111 Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.111
-- SELECT TIMESTAMP '2020-05-01 12:34:56.2222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.1111111 Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.111
-- SELECT TIMESTAMP '2020-05-01 12:34:56.22222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.11111111 Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.111
-- SELECT TIMESTAMP '2020-05-01 12:34:56.222222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.111111111 Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.111
-- SELECT TIMESTAMP '2020-05-01 12:34:56.2222222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.1111111111 Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.111
-- SELECT TIMESTAMP '2020-05-01 12:34:56.22222222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.11111111111 Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.111
-- SELECT TIMESTAMP '2020-05-01 12:34:56.222222222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.111111111111 Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.111
-- SELECT TIMESTAMP '2020-05-01 12:34:56.9 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.1 Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.800
-- SELECT TIMESTAMP '2020-05-01 12:34:56.99 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.11 Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.880
-- SELECT TIMESTAMP '2020-05-01 12:34:56.999 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.111 Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.888
-- SELECT TIMESTAMP '2020-05-01 12:34:56.9999 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.1111 Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.889
-- SELECT TIMESTAMP '2020-05-01 12:34:56.99999 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.11111 Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.889
-- SELECT TIMESTAMP '2020-05-01 12:34:56.999999 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.111111 Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.889
-- SELECT TIMESTAMP '2020-05-01 12:34:56.9999999 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.1111111 Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.889
-- SELECT TIMESTAMP '2020-05-01 12:34:56.99999999 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.11111111 Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.889
-- SELECT TIMESTAMP '2020-05-01 12:34:56.999999999 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.111111111 Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.889
-- SELECT TIMESTAMP '2020-05-01 12:34:56.9999999999 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.1111111111 Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.889
-- SELECT TIMESTAMP '2020-05-01 12:34:56.99999999999 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.11111111111 Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.889
-- SELECT TIMESTAMP '2020-05-01 12:34:56.999999999999 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.111111111111 Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.889
-- SELECT TIMESTAMP '2020-05-01 12:34:56.0002 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.0005 Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.000
-- SELECT TIMESTAMP '2020-05-01 12:34:56.00022 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.00055 Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.000
-- SELECT TIMESTAMP '2020-05-01 12:34:56.000222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.000555 Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.000
-- SELECT TIMESTAMP '2020-05-01 12:34:56.0002222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.0005555 Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.000
-- SELECT TIMESTAMP '2020-05-01 12:34:56.00022222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.00055555 Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.000
-- SELECT TIMESTAMP '2020-05-01 12:34:56.000222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.000555555 Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.000
-- SELECT TIMESTAMP '2020-05-01 12:34:56.0002222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.0005555555 Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.000
-- SELECT TIMESTAMP '2020-05-01 12:34:56.00022222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.00055555555 Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.000
-- SELECT TIMESTAMP '2020-05-01 12:34:56.000222222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.000555555555 Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:01.000
-- SELECT TIMESTAMP '2020-05-01 12:34:56.0002 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.0009 Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:00.999
-- SELECT TIMESTAMP '2020-05-01 12:34:56.00022 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.00099 Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:00.999
-- SELECT TIMESTAMP '2020-05-01 12:34:56.000222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.000999 Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:00.999
-- SELECT TIMESTAMP '2020-05-01 12:34:56.0002222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.0009999 Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:00.999
-- SELECT TIMESTAMP '2020-05-01 12:34:56.00022222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.00099999 Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:00.999
-- SELECT TIMESTAMP '2020-05-01 12:34:56.000222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.000999999 Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:00.999
-- SELECT TIMESTAMP '2020-05-01 12:34:56.0002222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.0009999999 Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:00.999
-- SELECT TIMESTAMP '2020-05-01 12:34:56.00022222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.00099999999 Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:00.999
-- SELECT TIMESTAMP '2020-05-01 12:34:56.000222222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.000999999999 Asia/Kathmandu'; # differ: doris : None, presto : 0 00:00:00.999
set debug_skip_fold_constant=true;
SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678912 Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.12345678912 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789123 Asia/Kathmandu' = TIMESTAMP '2020-05-01 12:34:56.123456789123 Asia/Kathmandu';
-- SELECT TIMESTAMP '2020-05-01 11:33:56 +04:44' = TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.1 +04:44' = TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.12 +04:44' = TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.123 +04:44' = TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.1234 +04:44' = TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.12345 +04:44' = TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.123456 +04:44' = TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.1234567 +04:44' = TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.12345678 +04:44' = TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.123456789 +04:44' = TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.1234567891 +04:44' = TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.12345678912 +04:44' = TIMESTAMP '2020-05-01 12:34:56.12345678912 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 11:33:56.123456789123 +04:44' = TIMESTAMP '2020-05-01 12:34:56.123456789123 Asia/Kathmandu'; # differ: doris : 0, presto : True
SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu';
-- SELECT TIMESTAMP '2020-05-01 11:33:56 +04:44' <> TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.1 +04:44' <> TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.12 +04:44' <> TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.123 +04:44' <> TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.1234 +04:44' <> TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.12345 +04:44' <> TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.123456 +04:44' <> TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.1234567 +04:44' <> TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.12345678 +04:44' <> TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.123456789 +04:44' <> TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.1234567891 +04:44' <> TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.12345678912 +04:44' <> TIMESTAMP '2020-05-01 12:34:56.12345678912 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.123456789123 +04:44' <> TIMESTAMP '2020-05-01 12:34:56.123456789123 Asia/Kathmandu'; # differ: doris : 1, presto : False
SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.1 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.12 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.123 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.1234 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.12345 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.123456 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.1234567 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.12345678 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.123456789 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.1234567890 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.12345678901 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' <> TIMESTAMP '2021-05-01 12:34:56.123456789012 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:57 Asia/Kathmandu';
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.2 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.13 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.124 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.1235 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.12346 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.123457 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.1234568 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.12345679 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.123456781 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.12345678902 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' <> TIMESTAMP '2020-05-01 12:34:56.123456789013 Asia/Kathmandu'; # differ: doris : 0, presto : True
SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu';
-- SELECT TIMESTAMP '2020-05-01 11:33:56 +04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.1 +04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.12 +04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.123 +04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.1234 +04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.12345 +04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.123456 +04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.1234567 +04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.12345678 +04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.123456789 +04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.1234567891 +04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.12345678912 +04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345678912 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 11:33:56.123456789123 +04:44' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456789123 Asia/Kathmandu'; # differ: doris : 1, presto : False
SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:55 Asia/Kathmandu';
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.0 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.11 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.122 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1233 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12344 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123455 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234566 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345677 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456788 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.1234567899 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.12345678900 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' IS DISTINCT FROM TIMESTAMP '2020-05-01 12:34:56.123456789011 Asia/Kathmandu'; # differ: doris : 0, presto : True
SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:57 Asia/Kathmandu';
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.2 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.13 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.124 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.1235 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.12346 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.123457 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.1234568 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.12345679 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.123456790 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.12345678902 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.123456789013 Asia/Kathmandu'; # differ: doris : 0, presto : True
SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' < TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:55 Asia/Kathmandu';
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:55.9 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.11 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.122 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.1233 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.12344 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.123455 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.1234566 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.12345677 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.123456788 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.1234567889 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.12345678899 Asia/Kathmandu'; # differ: doris : 0, presto : True
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.123456789011 Asia/Kathmandu'; # differ: doris : 0, presto : True
SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' > TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:57 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.2 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.13 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.124 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1235 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12346 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123457 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1234568 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12345679 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123456790 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12345678902 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123456789013 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:55 Asia/Kathmandu';
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.0 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.11 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.122 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1233 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12344 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123454 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1234566 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12345677 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123456788 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.1234567889 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.12345678900 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' <= TIMESTAMP '2020-05-01 12:34:56.123456789011 Asia/Kathmandu'; # differ: doris : 1, presto : False
SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:55 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:55.9 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.11 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.122 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1233 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12344 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123455 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1234566 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12345677 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123456788 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1234567889 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12345678899 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123456789011 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:57 Asia/Kathmandu';
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.2 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.13 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.124 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1235 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12346 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123457 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1234568 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12345679 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.12345678902 Asia/Kathmandu'; # differ: doris : 1, presto : False
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' >= TIMESTAMP '2020-05-01 12:34:56.123456789013 Asia/Kathmandu'; # differ: doris : 1, presto : False
SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:55 Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:57 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.0 Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.2 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.11 Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.13 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.122 Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.124 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.1233 Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.1235 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.12344 Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.12346 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.123455 Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.123457 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.1234566 Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.1234568 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.12345677 Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.12345679 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.123456788 Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.123456790 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.1234567889 Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.1234567891 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.12345678902 Asia/Kathmandu';
SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' BETWEEN TIMESTAMP '2020-05-01 12:34:56.123456789011 Asia/Kathmandu' and TIMESTAMP '2020-05-01 12:34:56.123456789013 Asia/Kathmandu';
-- SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.223 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.243 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.246 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.246 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.246 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.246 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.246 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.246 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.246 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.246 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.246 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' + INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:57.246 Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.123 Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.223 Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.243 Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.246 Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.246 Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.246 Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.246 Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.246 Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.246 Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.246 Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.246 Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.246 Asia/Kathmandu
-- SELECT INTERVAL '1.123' SECOND + TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu'; # differ: doris : None, presto : 2020-05-01 12:34:57.246 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.000 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.100 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.120 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' + INTERVAL '1' DAY; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.123 Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu'; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.000 Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu'; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.100 Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu'; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.120 Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu'; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.123 Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu'; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.123 Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu'; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.123 Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu'; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.123 Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu'; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.123 Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu'; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.123 Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu'; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.123 Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu'; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.123 Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu'; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.123 Asia/Kathmandu
-- SELECT INTERVAL '1' DAY + TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu'; # differ: doris : 2020-05-02 12:34:56, presto : 2020-05-02 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:54.877 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:54.977 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:54.997 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:55.000 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:55.000 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:55.000 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:55.000 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:55.000 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:55.000 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:55.000 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:55.000 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:55.000 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' - INTERVAL '1.123' SECOND; # differ: doris : None, presto : 2020-05-01 12:34:55.000 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : 2020-04-30 12:34:56, presto : 2020-04-30 12:34:56.000 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : 2020-04-30 12:34:56, presto : 2020-04-30 12:34:56.100 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : 2020-04-30 12:34:56, presto : 2020-04-30 12:34:56.120 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : 2020-04-30 12:34:56, presto : 2020-04-30 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : 2020-04-30 12:34:56, presto : 2020-04-30 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : 2020-04-30 12:34:56, presto : 2020-04-30 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : 2020-04-30 12:34:56, presto : 2020-04-30 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : 2020-04-30 12:34:56, presto : 2020-04-30 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : 2020-04-30 12:34:56, presto : 2020-04-30 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : 2020-04-30 12:34:56, presto : 2020-04-30 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : 2020-04-30 12:34:56, presto : 2020-04-30 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : 2020-04-30 12:34:56, presto : 2020-04-30 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' - INTERVAL '1' DAY; # differ: doris : 2020-04-30 12:34:56, presto : 2020-04-30 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.000 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.100 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.120 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.123 Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu'; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.000 Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu'; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.100 Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu'; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.120 Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu'; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.123 Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu'; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.123 Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu'; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.123 Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu'; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.123 Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu'; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.123 Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu'; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.123 Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu'; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.123 Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu'; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.123 Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu'; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.123 Asia/Kathmandu
-- SELECT INTERVAL '1' MONTH + TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu'; # differ: doris : 2020-06-01 12:34:56, presto : 2020-06-01 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-11-01 01:02:03, presto : 2020-10-31 01:02:03.000 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.1 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-11-01 01:02:03, presto : 2020-10-31 01:02:03.100 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.12 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-11-01 01:02:03, presto : 2020-10-31 01:02:03.120 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.123 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-11-01 01:02:03, presto : 2020-10-31 01:02:03.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.1234 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-11-01 01:02:03, presto : 2020-10-31 01:02:03.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.12345 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-11-01 01:02:03, presto : 2020-10-31 01:02:03.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.123456 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-11-01 01:02:03, presto : 2020-10-31 01:02:03.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.1234567 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-11-01 01:02:03, presto : 2020-10-31 01:02:03.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.12345678 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-11-01 01:02:03, presto : 2020-10-31 01:02:03.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.123456789 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-11-01 01:02:03, presto : 2020-10-31 01:02:03.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.1234567890 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-11-01 01:02:03, presto : 2020-10-31 01:02:03.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.12345678901 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-11-01 01:02:03, presto : 2020-10-31 01:02:03.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.123456789012 Asia/Kathmandu' + INTERVAL '1' MONTH; # differ: doris : 2020-11-01 01:02:03, presto : 2020-10-31 01:02:03.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-04-01 12:34:56, presto : 2020-04-01 12:34:56.000 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-04-01 12:34:56, presto : 2020-04-01 12:34:56.100 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-04-01 12:34:56, presto : 2020-04-01 12:34:56.120 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-04-01 12:34:56, presto : 2020-04-01 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-04-01 12:34:56, presto : 2020-04-01 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-04-01 12:34:56, presto : 2020-04-01 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-04-01 12:34:56, presto : 2020-04-01 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-04-01 12:34:56, presto : 2020-04-01 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-04-01 12:34:56, presto : 2020-04-01 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-04-01 12:34:56, presto : 2020-04-01 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.1234567890 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-04-01 12:34:56, presto : 2020-04-01 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.12345678901 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-04-01 12:34:56, presto : 2020-04-01 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56.123456789012 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-04-01 12:34:56, presto : 2020-04-01 12:34:56.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-09-01 01:02:03, presto : 2020-08-31 01:02:03.000 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.1 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-09-01 01:02:03, presto : 2020-08-31 01:02:03.100 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.12 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-09-01 01:02:03, presto : 2020-08-31 01:02:03.120 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.123 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-09-01 01:02:03, presto : 2020-08-31 01:02:03.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.1234 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-09-01 01:02:03, presto : 2020-08-31 01:02:03.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.12345 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-09-01 01:02:03, presto : 2020-08-31 01:02:03.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.123456 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-09-01 01:02:03, presto : 2020-08-31 01:02:03.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.1234567 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-09-01 01:02:03, presto : 2020-08-31 01:02:03.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.12345678 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-09-01 01:02:03, presto : 2020-08-31 01:02:03.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.123456789 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-09-01 01:02:03, presto : 2020-08-31 01:02:03.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.1234567890 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-09-01 01:02:03, presto : 2020-08-31 01:02:03.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.12345678901 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-09-01 01:02:03, presto : 2020-08-31 01:02:03.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-10-01 01:02:03.123456789012 Asia/Kathmandu' - INTERVAL '1' MONTH; # differ: doris : 2020-09-01 01:02:03, presto : 2020-08-31 01:02:03.123 Asia/Kathmandu
-- SELECT TIMESTAMP '2020-05-01 12:34:56 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55 Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:01.000
-- SELECT TIMESTAMP '2020-05-01 12:34:56.2 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.1 Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:01.100
-- SELECT TIMESTAMP '2020-05-01 12:34:56.22 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.11 Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:01.110
-- SELECT TIMESTAMP '2020-05-01 12:34:56.222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.111 Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:01.111
-- SELECT TIMESTAMP '2020-05-01 12:34:56.2222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.1111 Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:01.111
-- SELECT TIMESTAMP '2020-05-01 12:34:56.22222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.11111 Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:01.111
-- SELECT TIMESTAMP '2020-05-01 12:34:56.222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.111111 Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:01.111
-- SELECT TIMESTAMP '2020-05-01 12:34:56.2222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.1111111 Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:01.111
-- SELECT TIMESTAMP '2020-05-01 12:34:56.22222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.11111111 Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:01.111
-- SELECT TIMESTAMP '2020-05-01 12:34:56.222222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.111111111 Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:01.111
-- SELECT TIMESTAMP '2020-05-01 12:34:56.2222222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.1111111111 Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:01.111
-- SELECT TIMESTAMP '2020-05-01 12:34:56.22222222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.11111111111 Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:01.111
-- SELECT TIMESTAMP '2020-05-01 12:34:56.222222222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.111111111111 Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:01.111
-- SELECT TIMESTAMP '2020-05-01 12:34:56.9 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.1 Asia/Kathmandu'; # differ: doris : 2, presto : 0 00:00:01.800
-- SELECT TIMESTAMP '2020-05-01 12:34:56.99 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.11 Asia/Kathmandu'; # differ: doris : 2, presto : 0 00:00:01.880
-- SELECT TIMESTAMP '2020-05-01 12:34:56.999 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.111 Asia/Kathmandu'; # differ: doris : 2, presto : 0 00:00:01.888
-- SELECT TIMESTAMP '2020-05-01 12:34:56.9999 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.1111 Asia/Kathmandu'; # differ: doris : 2, presto : 0 00:00:01.889
-- SELECT TIMESTAMP '2020-05-01 12:34:56.99999 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.11111 Asia/Kathmandu'; # differ: doris : 2, presto : 0 00:00:01.889
-- SELECT TIMESTAMP '2020-05-01 12:34:56.999999 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.111111 Asia/Kathmandu'; # differ: doris : 2, presto : 0 00:00:01.889
-- SELECT TIMESTAMP '2020-05-01 12:34:56.9999999 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.1111111 Asia/Kathmandu'; # differ: doris : 2, presto : 0 00:00:01.889
-- SELECT TIMESTAMP '2020-05-01 12:34:56.99999999 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.11111111 Asia/Kathmandu'; # differ: doris : 2, presto : 0 00:00:01.889
-- SELECT TIMESTAMP '2020-05-01 12:34:56.999999999 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.111111111 Asia/Kathmandu'; # differ: doris : 2, presto : 0 00:00:01.889
-- SELECT TIMESTAMP '2020-05-01 12:34:56.9999999999 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.1111111111 Asia/Kathmandu'; # differ: doris : 2, presto : 0 00:00:01.889
-- SELECT TIMESTAMP '2020-05-01 12:34:56.99999999999 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.11111111111 Asia/Kathmandu'; # differ: doris : 2, presto : 0 00:00:01.889
-- SELECT TIMESTAMP '2020-05-01 12:34:56.999999999999 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.111111111111 Asia/Kathmandu'; # differ: doris : 2, presto : 0 00:00:01.889
-- SELECT TIMESTAMP '2020-05-01 12:34:56.0002 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.0005 Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:01.000
-- SELECT TIMESTAMP '2020-05-01 12:34:56.00022 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.00055 Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:01.000
-- SELECT TIMESTAMP '2020-05-01 12:34:56.000222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.000555 Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:01.000
-- SELECT TIMESTAMP '2020-05-01 12:34:56.0002222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.0005555 Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:01.000
-- SELECT TIMESTAMP '2020-05-01 12:34:56.00022222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.00055555 Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:01.000
-- SELECT TIMESTAMP '2020-05-01 12:34:56.000222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.000555555 Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:01.000
-- SELECT TIMESTAMP '2020-05-01 12:34:56.0002222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.0005555555 Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:01.000
-- SELECT TIMESTAMP '2020-05-01 12:34:56.00022222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.00055555555 Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:01.000
-- SELECT TIMESTAMP '2020-05-01 12:34:56.000222222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.000555555555 Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:01.000
-- SELECT TIMESTAMP '2020-05-01 12:34:56.0002 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.0009 Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:00.999
-- SELECT TIMESTAMP '2020-05-01 12:34:56.00022 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.00099 Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:00.999
-- SELECT TIMESTAMP '2020-05-01 12:34:56.000222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.000999 Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:00.999
-- SELECT TIMESTAMP '2020-05-01 12:34:56.0002222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.0009999 Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:00.999
-- SELECT TIMESTAMP '2020-05-01 12:34:56.00022222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.00099999 Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:00.999
-- SELECT TIMESTAMP '2020-05-01 12:34:56.000222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.000999999 Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:00.999
-- SELECT TIMESTAMP '2020-05-01 12:34:56.0002222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.0009999999 Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:00.999
-- SELECT TIMESTAMP '2020-05-01 12:34:56.00022222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.00099999999 Asia/Kathmandu'; # differ: doris : 1, presto : 0 00:00:00.999
-- SELECT TIMESTAMP '2020-05-01 12:34:56.000222222222 Asia/Kathmandu' - TIMESTAMP '2020-05-01 12:34:55.000999999999 Asia/Kathmandu' # differ: doris : 1, presto : 0 00:00:00.999
