set sql_dialect='presto';
set enable_fallback_to_original_planner=false;
set debug_skip_fold_constant=false;
-- SELECT EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu'); # differ: doris : None, presto : 2020
-- SELECT EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu'); # differ: doris : None, presto : 2020
-- SELECT EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu'); # differ: doris : None, presto : 2020
-- SELECT EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu'); # differ: doris : None, presto : 2020
-- SELECT EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu'); # differ: doris : None, presto : 2020
-- SELECT EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu'); # differ: doris : None, presto : 2020
-- SELECT EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu'); # differ: doris : None, presto : 2020
-- SELECT EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu'); # differ: doris : None, presto : 2020
-- SELECT EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu'); # differ: doris : None, presto : 2020
-- SELECT EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu'); # differ: doris : None, presto : 2020
-- SELECT EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu'); # differ: doris : None, presto : 2020
-- SELECT EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu'); # differ: doris : None, presto : 2020
-- SELECT EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu'); # differ: doris : None, presto : 2020
-- SELECT year(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu'); # differ: doris : None, presto : 2020
-- SELECT year(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu'); # differ: doris : None, presto : 2020
-- SELECT year(TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu'); # differ: doris : None, presto : 2020
-- SELECT year(TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu'); # differ: doris : None, presto : 2020
-- SELECT year(TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu'); # differ: doris : None, presto : 2020
-- SELECT year(TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu'); # differ: doris : None, presto : 2020
-- SELECT year(TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu'); # differ: doris : None, presto : 2020
-- SELECT year(TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu'); # differ: doris : None, presto : 2020
-- SELECT year(TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu'); # differ: doris : None, presto : 2020
-- SELECT year(TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu'); # differ: doris : None, presto : 2020
-- SELECT year(TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu'); # differ: doris : None, presto : 2020
-- SELECT year(TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu'); # differ: doris : None, presto : 2020
-- SELECT year(TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu'); # differ: doris : None, presto : 2020
-- SELECT EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu'); # differ: doris : None, presto : 5
-- SELECT EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu'); # differ: doris : None, presto : 5
-- SELECT EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu'); # differ: doris : None, presto : 5
-- SELECT EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu'); # differ: doris : None, presto : 5
-- SELECT EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu'); # differ: doris : None, presto : 5
-- SELECT EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu'); # differ: doris : None, presto : 5
-- SELECT EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu'); # differ: doris : None, presto : 5
-- SELECT EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu'); # differ: doris : None, presto : 5
-- SELECT EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu'); # differ: doris : None, presto : 5
-- SELECT EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu'); # differ: doris : None, presto : 5
-- SELECT EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu'); # differ: doris : None, presto : 5
-- SELECT EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu'); # differ: doris : None, presto : 5
-- SELECT EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu'); # differ: doris : None, presto : 5
-- SELECT month(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu'); # differ: doris : None, presto : 5
-- SELECT month(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu'); # differ: doris : None, presto : 5
-- SELECT month(TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu'); # differ: doris : None, presto : 5
-- SELECT month(TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu'); # differ: doris : None, presto : 5
-- SELECT month(TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu'); # differ: doris : None, presto : 5
-- SELECT month(TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu'); # differ: doris : None, presto : 5
-- SELECT month(TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu'); # differ: doris : None, presto : 5
-- SELECT month(TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu'); # differ: doris : None, presto : 5
-- SELECT month(TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu'); # differ: doris : None, presto : 5
-- SELECT month(TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu'); # differ: doris : None, presto : 5
-- SELECT month(TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu'); # differ: doris : None, presto : 5
-- SELECT month(TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu'); # differ: doris : None, presto : 5
-- SELECT month(TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu'); # differ: doris : None, presto : 5
-- SELECT EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu'); # differ: doris : None, presto : 19
-- SELECT EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu'); # differ: doris : None, presto : 19
-- SELECT EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu'); # differ: doris : None, presto : 19
-- SELECT EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu'); # differ: doris : None, presto : 19
-- SELECT EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu'); # differ: doris : None, presto : 19
-- SELECT EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu'); # differ: doris : None, presto : 19
-- SELECT EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu'); # differ: doris : None, presto : 19
-- SELECT EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu'); # differ: doris : None, presto : 19
-- SELECT EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu'); # differ: doris : None, presto : 19
-- SELECT EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu'); # differ: doris : None, presto : 19
-- SELECT EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu'); # differ: doris : None, presto : 19
-- SELECT EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu'); # differ: doris : None, presto : 19
-- SELECT EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu'); # differ: doris : None, presto : 19
-- SELECT week(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu'); # differ: doris : None, presto : 19
-- SELECT week(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu'); # differ: doris : None, presto : 19
-- SELECT week(TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu'); # differ: doris : None, presto : 19
-- SELECT week(TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu'); # differ: doris : None, presto : 19
-- SELECT week(TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu'); # differ: doris : None, presto : 19
-- SELECT week(TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu'); # differ: doris : None, presto : 19
-- SELECT week(TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu'); # differ: doris : None, presto : 19
-- SELECT week(TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu'); # differ: doris : None, presto : 19
-- SELECT week(TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu'); # differ: doris : None, presto : 19
-- SELECT week(TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu'); # differ: doris : None, presto : 19
-- SELECT week(TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu'); # differ: doris : None, presto : 19
-- SELECT week(TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu'); # differ: doris : None, presto : 19
-- SELECT week(TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu'); # differ: doris : None, presto : 19
-- SELECT EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu'); # differ: doris : None, presto : 10
-- SELECT EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu'); # differ: doris : None, presto : 10
-- SELECT EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu'); # differ: doris : None, presto : 10
-- SELECT EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu'); # differ: doris : None, presto : 10
-- SELECT EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu'); # differ: doris : None, presto : 10
-- SELECT EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu'); # differ: doris : None, presto : 10
-- SELECT EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu'); # differ: doris : None, presto : 10
-- SELECT EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu'); # differ: doris : None, presto : 10
-- SELECT EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu'); # differ: doris : None, presto : 10
-- SELECT EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu'); # differ: doris : None, presto : 10
-- SELECT EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu'); # differ: doris : None, presto : 10
-- SELECT EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu'); # differ: doris : None, presto : 10
-- SELECT EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu'); # differ: doris : None, presto : 10
-- SELECT day(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu'); # differ: doris : None, presto : 10
-- SELECT day(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu'); # differ: doris : None, presto : 10
-- SELECT day(TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu'); # differ: doris : None, presto : 10
-- SELECT day(TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu'); # differ: doris : None, presto : 10
-- SELECT day(TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu'); # differ: doris : None, presto : 10
-- SELECT day(TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu'); # differ: doris : None, presto : 10
-- SELECT day(TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu'); # differ: doris : None, presto : 10
-- SELECT day(TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu'); # differ: doris : None, presto : 10
-- SELECT day(TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu'); # differ: doris : None, presto : 10
-- SELECT day(TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu'); # differ: doris : None, presto : 10
-- SELECT day(TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu'); # differ: doris : None, presto : 10
-- SELECT day(TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu'); # differ: doris : None, presto : 10
-- SELECT day(TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu'); # differ: doris : None, presto : 10
-- SELECT EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_MONTH'
-- SELECT EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_MONTH'
-- SELECT EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_MONTH'
-- SELECT EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_MONTH'
-- SELECT EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_MONTH'
-- SELECT EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_MONTH'
-- SELECT EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_MONTH'
-- SELECT EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_MONTH'
-- SELECT EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_MONTH'
-- SELECT EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_MONTH'
-- SELECT EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_MONTH'
-- SELECT EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_MONTH'
-- SELECT EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_MONTH'
-- SELECT day_of_month(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu'); # differ: doris : None, presto : 10
-- SELECT day_of_month(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu'); # differ: doris : None, presto : 10
-- SELECT day_of_month(TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu'); # differ: doris : None, presto : 10
-- SELECT day_of_month(TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu'); # differ: doris : None, presto : 10
-- SELECT day_of_month(TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu'); # differ: doris : None, presto : 10
-- SELECT day_of_month(TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu'); # differ: doris : None, presto : 10
-- SELECT day_of_month(TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu'); # differ: doris : None, presto : 10
-- SELECT day_of_month(TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu'); # differ: doris : None, presto : 10
-- SELECT day_of_month(TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu'); # differ: doris : None, presto : 10
-- SELECT day_of_month(TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu'); # differ: doris : None, presto : 10
-- SELECT day_of_month(TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu'); # differ: doris : None, presto : 10
-- SELECT day_of_month(TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu'); # differ: doris : None, presto : 10
-- SELECT day_of_month(TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu'); # differ: doris : None, presto : 10
-- SELECT EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu'); # differ: doris : None, presto : 12
-- SELECT EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu'); # differ: doris : None, presto : 12
-- SELECT EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu'); # differ: doris : None, presto : 12
-- SELECT EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu'); # differ: doris : None, presto : 12
-- SELECT EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu'); # differ: doris : None, presto : 12
-- SELECT EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu'); # differ: doris : None, presto : 12
-- SELECT EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu'); # differ: doris : None, presto : 12
-- SELECT EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu'); # differ: doris : None, presto : 12
-- SELECT EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu'); # differ: doris : None, presto : 12
-- SELECT EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu'); # differ: doris : None, presto : 12
-- SELECT EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu'); # differ: doris : None, presto : 12
-- SELECT EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu'); # differ: doris : None, presto : 12
-- SELECT EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu'); # differ: doris : None, presto : 12
-- SELECT hour(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu'); # differ: doris : None, presto : 12
-- SELECT hour(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu'); # differ: doris : None, presto : 12
-- SELECT hour(TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu'); # differ: doris : None, presto : 12
-- SELECT hour(TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu'); # differ: doris : None, presto : 12
-- SELECT hour(TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu'); # differ: doris : None, presto : 12
-- SELECT hour(TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu'); # differ: doris : None, presto : 12
-- SELECT hour(TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu'); # differ: doris : None, presto : 12
-- SELECT hour(TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu'); # differ: doris : None, presto : 12
-- SELECT hour(TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu'); # differ: doris : None, presto : 12
-- SELECT hour(TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu'); # differ: doris : None, presto : 12
-- SELECT hour(TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu'); # differ: doris : None, presto : 12
-- SELECT hour(TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu'); # differ: doris : None, presto : 12
-- SELECT hour(TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu'); # differ: doris : None, presto : 12
-- SELECT EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu'); # differ: doris : None, presto : 34
-- SELECT EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu'); # differ: doris : None, presto : 34
-- SELECT EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu'); # differ: doris : None, presto : 34
-- SELECT EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu'); # differ: doris : None, presto : 34
-- SELECT EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu'); # differ: doris : None, presto : 34
-- SELECT EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu'); # differ: doris : None, presto : 34
-- SELECT EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu'); # differ: doris : None, presto : 34
-- SELECT EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu'); # differ: doris : None, presto : 34
-- SELECT EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu'); # differ: doris : None, presto : 34
-- SELECT EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu'); # differ: doris : None, presto : 34
-- SELECT EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu'); # differ: doris : None, presto : 34
-- SELECT EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu'); # differ: doris : None, presto : 34
-- SELECT EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu'); # differ: doris : None, presto : 34
-- SELECT minute(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu'); # differ: doris : None, presto : 34
-- SELECT minute(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu'); # differ: doris : None, presto : 34
-- SELECT minute(TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu'); # differ: doris : None, presto : 34
-- SELECT minute(TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu'); # differ: doris : None, presto : 34
-- SELECT minute(TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu'); # differ: doris : None, presto : 34
-- SELECT minute(TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu'); # differ: doris : None, presto : 34
-- SELECT minute(TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu'); # differ: doris : None, presto : 34
-- SELECT minute(TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu'); # differ: doris : None, presto : 34
-- SELECT minute(TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu'); # differ: doris : None, presto : 34
-- SELECT minute(TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu'); # differ: doris : None, presto : 34
-- SELECT minute(TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu'); # differ: doris : None, presto : 34
-- SELECT minute(TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu'); # differ: doris : None, presto : 34
-- SELECT minute(TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu'); # differ: doris : None, presto : 34
-- SELECT EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu'); # differ: doris : None, presto : 56
-- SELECT EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu'); # differ: doris : None, presto : 56
-- SELECT EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu'); # differ: doris : None, presto : 56
-- SELECT EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu'); # differ: doris : None, presto : 56
-- SELECT EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu'); # differ: doris : None, presto : 56
-- SELECT EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu'); # differ: doris : None, presto : 56
-- SELECT EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu'); # differ: doris : None, presto : 56
-- SELECT EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu'); # differ: doris : None, presto : 56
-- SELECT EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu'); # differ: doris : None, presto : 56
-- SELECT EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu'); # differ: doris : None, presto : 56
-- SELECT EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu'); # differ: doris : None, presto : 56
-- SELECT EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu'); # differ: doris : None, presto : 56
-- SELECT EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu'); # differ: doris : None, presto : 56
-- SELECT second(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu'); # differ: doris : None, presto : 56
-- SELECT second(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu'); # differ: doris : None, presto : 56
-- SELECT second(TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu'); # differ: doris : None, presto : 56
-- SELECT second(TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu'); # differ: doris : None, presto : 56
-- SELECT second(TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu'); # differ: doris : None, presto : 56
-- SELECT second(TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu'); # differ: doris : None, presto : 56
-- SELECT second(TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu'); # differ: doris : None, presto : 56
-- SELECT second(TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu'); # differ: doris : None, presto : 56
-- SELECT second(TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu'); # differ: doris : None, presto : 56
-- SELECT second(TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu'); # differ: doris : None, presto : 56
-- SELECT second(TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu'); # differ: doris : None, presto : 56
-- SELECT second(TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu'); # differ: doris : None, presto : 56
-- SELECT second(TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu'); # differ: doris : None, presto : 56
-- SELECT millisecond(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56 Asia/Kathmandu] is invalid
-- SELECT millisecond(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1 Asia/Kathmandu] is invalid
-- SELECT millisecond(TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12 Asia/Kathmandu] is invalid
-- SELECT millisecond(TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123 Asia/Kathmandu] is invalid
-- SELECT millisecond(TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1234 Asia/Kathmandu] is invalid
-- SELECT millisecond(TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12345 Asia/Kathmandu] is invalid
-- SELECT millisecond(TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123456 Asia/Kathmandu] is invalid
-- SELECT millisecond(TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1234567 Asia/Kathmandu] is invalid
-- SELECT millisecond(TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12345678 Asia/Kathmandu] is invalid
-- SELECT millisecond(TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123456789 Asia/Kathmandu] is invalid
-- SELECT millisecond(TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1234567890 Asia/Kathmandu] is invalid
-- SELECT millisecond(TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12345678901 Asia/Kathmandu] is invalid
-- SELECT millisecond(TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123456789012 Asia/Kathmandu] is invalid
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.1 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.12 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.123 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.1234 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.12345 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.123456 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.1234567 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.12345678 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.123456789 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.1234567891 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.12345678912 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.123456789123 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56 +08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.1 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1 +08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.12 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12 +08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.123 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123 +08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.1234 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1234 +08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.12345 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12345 +08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.123456 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123456 +08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.1234567 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1234567 +08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.12345678 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12345678 +08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.123456789 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123456789 +08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.1234567891 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1234567891 +08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.12345678912 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12345678912 +08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.123456789123 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123456789123 +08:35] is invalid
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.1 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.12 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.123 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.1234 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.12345 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.123456 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.1234567 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.12345678 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.123456789 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.1234567891 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.12345678912 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.123456789123 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56 -08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.1 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1 -08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.12 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12 -08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.123 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123 -08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.1234 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1234 -08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.12345 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12345 -08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.123456 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123456 -08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.1234567 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1234567 -08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.12345678 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12345678 -08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.123456789 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123456789 -08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.1234567891 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1234567891 -08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.12345678912 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12345678912 -08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.123456789123 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123456789123 -08:35] is invalid
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1234 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12345 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123456 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1234567 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12345678 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123456789 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1234567891 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12345678912 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123456789123 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56 +08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.1 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1 +08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.12 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12 +08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.123 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123 +08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.1234 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1234 +08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.12345 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12345 +08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.123456 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123456 +08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.1234567 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1234567 +08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.12345678 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12345678 +08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.123456789 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123456789 +08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.1234567891 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1234567891 +08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.12345678912 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12345678912 +08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.123456789123 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123456789123 +08:35] is invalid
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1234 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12345 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123456 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1234567 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12345678 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123456789 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1234567891 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12345678912 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123456789123 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56 -08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.1 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1 -08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.12 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12 -08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.123 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123 -08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.1234 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1234 -08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.12345 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12345 -08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.123456 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123456 -08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.1234567 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1234567 -08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.12345678 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12345678 -08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.123456789 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123456789 -08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.1234567891 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1234567891 -08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.12345678912 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12345678912 -08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.123456789123 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123456789123 -08:35] is invalid
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56 -00:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1 -00:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12 -00:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123 -00:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1234 -00:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12345 -00:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123456 -00:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1234567 -00:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12345678 -00:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123456789 -00:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1234567891 -00:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12345678912 -00:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123456789123 -00:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56 -00:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56 -00:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.1 -00:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1 -00:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.12 -00:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12 -00:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.123 -00:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123 -00:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.1234 -00:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1234 -00:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.12345 -00:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12345 -00:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.123456 -00:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123456 -00:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.1234567 -00:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1234567 -00:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.12345678 -00:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12345678 -00:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.123456789 -00:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123456789 -00:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.1234567891 -00:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1234567891 -00:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.12345678912 -00:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12345678912 -00:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.123456789123 -00:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123456789123 -00:35] is invalid
-- SELECT EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_WEEK'
-- SELECT EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_WEEK'
-- SELECT EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_WEEK'
-- SELECT EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_WEEK'
-- SELECT EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_WEEK'
-- SELECT EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_WEEK'
-- SELECT EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_WEEK'
-- SELECT EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_WEEK'
-- SELECT EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_WEEK'
-- SELECT EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_WEEK'
-- SELECT EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_WEEK'
-- SELECT EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_WEEK'
-- SELECT EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_WEEK'
-- SELECT day_of_week(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu'); # differ: doris : None, presto : 7
-- SELECT day_of_week(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu'); # differ: doris : None, presto : 7
-- SELECT day_of_week(TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu'); # differ: doris : None, presto : 7
-- SELECT day_of_week(TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu'); # differ: doris : None, presto : 7
-- SELECT day_of_week(TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu'); # differ: doris : None, presto : 7
-- SELECT day_of_week(TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu'); # differ: doris : None, presto : 7
-- SELECT day_of_week(TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu'); # differ: doris : None, presto : 7
-- SELECT day_of_week(TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu'); # differ: doris : None, presto : 7
-- SELECT day_of_week(TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu'); # differ: doris : None, presto : 7
-- SELECT day_of_week(TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu'); # differ: doris : None, presto : 7
-- SELECT day_of_week(TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu'); # differ: doris : None, presto : 7
-- SELECT day_of_week(TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu'); # differ: doris : None, presto : 7
-- SELECT day_of_week(TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu'); # differ: doris : None, presto : 7
-- SELECT EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOW'
-- SELECT EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOW'
-- SELECT EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOW'
-- SELECT EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOW'
-- SELECT EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOW'
-- SELECT EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOW'
-- SELECT EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOW'
-- SELECT EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOW'
-- SELECT EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOW'
-- SELECT EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOW'
-- SELECT EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOW'
-- SELECT EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOW'
-- SELECT EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOW'
-- SELECT EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_YEAR'
-- SELECT EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_YEAR'
-- SELECT EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_YEAR'
-- SELECT EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_YEAR'
-- SELECT EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_YEAR'
-- SELECT EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_YEAR'
-- SELECT EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_YEAR'
-- SELECT EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_YEAR'
-- SELECT EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_YEAR'
-- SELECT EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_YEAR'
-- SELECT EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_YEAR'
-- SELECT EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_YEAR'
-- SELECT EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_YEAR'
-- SELECT day_of_year(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu'); # differ: doris : None, presto : 131
-- SELECT day_of_year(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu'); # differ: doris : None, presto : 131
-- SELECT day_of_year(TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu'); # differ: doris : None, presto : 131
-- SELECT day_of_year(TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu'); # differ: doris : None, presto : 131
-- SELECT day_of_year(TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu'); # differ: doris : None, presto : 131
-- SELECT day_of_year(TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu'); # differ: doris : None, presto : 131
-- SELECT day_of_year(TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu'); # differ: doris : None, presto : 131
-- SELECT day_of_year(TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu'); # differ: doris : None, presto : 131
-- SELECT day_of_year(TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu'); # differ: doris : None, presto : 131
-- SELECT day_of_year(TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu'); # differ: doris : None, presto : 131
-- SELECT day_of_year(TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu'); # differ: doris : None, presto : 131
-- SELECT day_of_year(TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu'); # differ: doris : None, presto : 131
-- SELECT day_of_year(TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu'); # differ: doris : None, presto : 131
-- SELECT EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOY'
-- SELECT EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOY'
-- SELECT EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOY'
-- SELECT EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOY'
-- SELECT EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOY'
-- SELECT EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOY'
-- SELECT EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOY'
-- SELECT EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOY'
-- SELECT EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOY'
-- SELECT EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOY'
-- SELECT EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOY'
-- SELECT EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOY'
-- SELECT EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOY'
-- SELECT EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu'); # differ: doris : None, presto : 2
-- SELECT EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu'); # differ: doris : None, presto : 2
-- SELECT EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu'); # differ: doris : None, presto : 2
-- SELECT EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu'); # differ: doris : None, presto : 2
-- SELECT EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu'); # differ: doris : None, presto : 2
-- SELECT EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu'); # differ: doris : None, presto : 2
-- SELECT EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu'); # differ: doris : None, presto : 2
-- SELECT EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu'); # differ: doris : None, presto : 2
-- SELECT EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu'); # differ: doris : None, presto : 2
-- SELECT EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu'); # differ: doris : None, presto : 2
-- SELECT EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu'); # differ: doris : None, presto : 2
-- SELECT EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu'); # differ: doris : None, presto : 2
-- SELECT EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu'); # differ: doris : None, presto : 2
-- SELECT quarter(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu'); # differ: doris : None, presto : 2
-- SELECT quarter(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu'); # differ: doris : None, presto : 2
-- SELECT quarter(TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu'); # differ: doris : None, presto : 2
-- SELECT quarter(TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu'); # differ: doris : None, presto : 2
-- SELECT quarter(TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu'); # differ: doris : None, presto : 2
-- SELECT quarter(TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu'); # differ: doris : None, presto : 2
-- SELECT quarter(TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu'); # differ: doris : None, presto : 2
-- SELECT quarter(TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu'); # differ: doris : None, presto : 2
-- SELECT quarter(TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu'); # differ: doris : None, presto : 2
-- SELECT quarter(TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu'); # differ: doris : None, presto : 2
-- SELECT quarter(TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu'); # differ: doris : None, presto : 2
-- SELECT quarter(TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu'); # differ: doris : None, presto : 2
-- SELECT quarter(TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu'); # differ: doris : None, presto : 2
-- SELECT week_of_year(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu'); # differ: doris : None, presto : 19
-- SELECT week_of_year(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu'); # differ: doris : None, presto : 19
-- SELECT week_of_year(TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu'); # differ: doris : None, presto : 19
-- SELECT week_of_year(TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu'); # differ: doris : None, presto : 19
-- SELECT week_of_year(TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu'); # differ: doris : None, presto : 19
-- SELECT week_of_year(TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu'); # differ: doris : None, presto : 19
-- SELECT week_of_year(TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu'); # differ: doris : None, presto : 19
-- SELECT week_of_year(TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu'); # differ: doris : None, presto : 19
-- SELECT week_of_year(TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu'); # differ: doris : None, presto : 19
-- SELECT week_of_year(TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu'); # differ: doris : None, presto : 19
-- SELECT week_of_year(TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu'); # differ: doris : None, presto : 19
-- SELECT week_of_year(TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu'); # differ: doris : None, presto : 19
-- SELECT week_of_year(TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu'); # differ: doris : None, presto : 19
-- SELECT EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YEAR_OF_WEEK'
-- SELECT EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YEAR_OF_WEEK'
-- SELECT EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YEAR_OF_WEEK'
-- SELECT EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YEAR_OF_WEEK'
-- SELECT EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YEAR_OF_WEEK'
-- SELECT EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YEAR_OF_WEEK'
-- SELECT EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YEAR_OF_WEEK'
-- SELECT EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YEAR_OF_WEEK'
-- SELECT EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YEAR_OF_WEEK'
-- SELECT EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YEAR_OF_WEEK'
-- SELECT EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YEAR_OF_WEEK'
-- SELECT EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YEAR_OF_WEEK'
-- SELECT EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YEAR_OF_WEEK'
-- SELECT year_of_week(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56 Asia/Kathmandu] is invalid
-- SELECT year_of_week(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1 Asia/Kathmandu] is invalid
-- SELECT year_of_week(TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12 Asia/Kathmandu] is invalid
-- SELECT year_of_week(TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123 Asia/Kathmandu] is invalid
-- SELECT year_of_week(TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1234 Asia/Kathmandu] is invalid
-- SELECT year_of_week(TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12345 Asia/Kathmandu] is invalid
-- SELECT year_of_week(TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123456 Asia/Kathmandu] is invalid
-- SELECT year_of_week(TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1234567 Asia/Kathmandu] is invalid
-- SELECT year_of_week(TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12345678 Asia/Kathmandu] is invalid
-- SELECT year_of_week(TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123456789 Asia/Kathmandu] is invalid
-- SELECT year_of_week(TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1234567890 Asia/Kathmandu] is invalid
-- SELECT year_of_week(TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12345678901 Asia/Kathmandu] is invalid
-- SELECT year_of_week(TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123456789012 Asia/Kathmandu] is invalid
-- SELECT EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YOW'
-- SELECT EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YOW'
-- SELECT EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YOW'
-- SELECT EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YOW'
-- SELECT EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YOW'
-- SELECT EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YOW'
-- SELECT EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YOW'
-- SELECT EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YOW'
-- SELECT EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YOW'
-- SELECT EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YOW'
-- SELECT EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YOW'
-- SELECT EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YOW'
-- SELECT EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YOW'
set debug_skip_fold_constant=true;
SELECT EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu');
SELECT EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu');
SELECT EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu');
SELECT EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu');
SELECT EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu');
SELECT EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu');
SELECT EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu');
SELECT EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu');
SELECT EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu');
SELECT EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu');
SELECT EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu');
SELECT EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu');
SELECT EXTRACT(YEAR FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu');
SELECT year(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu');
SELECT year(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu');
SELECT year(TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu');
SELECT year(TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu');
SELECT year(TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu');
SELECT year(TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu');
SELECT year(TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu');
SELECT year(TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu');
SELECT year(TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu');
SELECT year(TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu');
SELECT year(TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu');
SELECT year(TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu');
SELECT year(TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu');
SELECT EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu');
SELECT EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu');
SELECT EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu');
SELECT EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu');
SELECT EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu');
SELECT EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu');
SELECT EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu');
SELECT EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu');
SELECT EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu');
SELECT EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu');
SELECT EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu');
SELECT EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu');
SELECT EXTRACT(MONTH FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu');
SELECT month(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu');
SELECT month(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu');
SELECT month(TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu');
SELECT month(TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu');
SELECT month(TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu');
SELECT month(TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu');
SELECT month(TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu');
SELECT month(TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu');
SELECT month(TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu');
SELECT month(TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu');
SELECT month(TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu');
SELECT month(TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu');
SELECT month(TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu');
SELECT EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu');
SELECT EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu');
SELECT EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu');
SELECT EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu');
SELECT EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu');
SELECT EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu');
SELECT EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu');
SELECT EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu');
SELECT EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu');
SELECT EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu');
SELECT EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu');
SELECT EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu');
SELECT EXTRACT(WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu');
SELECT week(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu');
SELECT week(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu');
SELECT week(TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu');
SELECT week(TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu');
SELECT week(TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu');
SELECT week(TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu');
SELECT week(TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu');
SELECT week(TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu');
SELECT week(TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu');
SELECT week(TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu');
SELECT week(TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu');
SELECT week(TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu');
SELECT week(TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu');
SELECT EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu');
SELECT EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu');
SELECT EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu');
SELECT EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu');
SELECT EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu');
SELECT EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu');
SELECT EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu');
SELECT EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu');
SELECT EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu');
SELECT EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu');
SELECT EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu');
SELECT EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu');
SELECT EXTRACT(DAY FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu');
SELECT day(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu');
SELECT day(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu');
SELECT day(TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu');
SELECT day(TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu');
SELECT day(TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu');
SELECT day(TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu');
SELECT day(TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu');
SELECT day(TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu');
SELECT day(TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu');
SELECT day(TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu');
SELECT day(TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu');
SELECT day(TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu');
SELECT day(TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu');
-- SELECT EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_MONTH'
-- SELECT EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_MONTH'
-- SELECT EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_MONTH'
-- SELECT EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_MONTH'
-- SELECT EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_MONTH'
-- SELECT EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_MONTH'
-- SELECT EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_MONTH'
-- SELECT EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_MONTH'
-- SELECT EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_MONTH'
-- SELECT EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_MONTH'
-- SELECT EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_MONTH'
-- SELECT EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_MONTH'
-- SELECT EXTRACT(DAY_OF_MONTH FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_MONTH'
SELECT day_of_month(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu');
SELECT day_of_month(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu');
SELECT day_of_month(TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu');
SELECT day_of_month(TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu');
SELECT day_of_month(TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu');
SELECT day_of_month(TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu');
SELECT day_of_month(TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu');
SELECT day_of_month(TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu');
SELECT day_of_month(TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu');
SELECT day_of_month(TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu');
SELECT day_of_month(TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu');
SELECT day_of_month(TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu');
SELECT day_of_month(TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu');
SELECT EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu');
SELECT EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu');
SELECT EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu');
SELECT EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu');
SELECT EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu');
SELECT EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu');
SELECT EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu');
SELECT EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu');
SELECT EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu');
SELECT EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu');
SELECT EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu');
SELECT EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu');
SELECT EXTRACT(HOUR FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu');
SELECT hour(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu');
SELECT hour(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu');
SELECT hour(TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu');
SELECT hour(TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu');
SELECT hour(TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu');
SELECT hour(TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu');
SELECT hour(TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu');
SELECT hour(TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu');
SELECT hour(TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu');
SELECT hour(TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu');
SELECT hour(TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu');
SELECT hour(TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu');
SELECT hour(TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu');
SELECT EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu');
SELECT EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu');
SELECT EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu');
SELECT EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu');
SELECT EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu');
SELECT EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu');
SELECT EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu');
SELECT EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu');
SELECT EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu');
SELECT EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu');
SELECT EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu');
SELECT EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu');
SELECT EXTRACT(MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu');
SELECT minute(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu');
SELECT minute(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu');
SELECT minute(TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu');
SELECT minute(TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu');
SELECT minute(TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu');
SELECT minute(TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu');
SELECT minute(TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu');
SELECT minute(TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu');
SELECT minute(TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu');
SELECT minute(TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu');
SELECT minute(TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu');
SELECT minute(TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu');
SELECT minute(TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu');
SELECT EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu');
SELECT EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu');
SELECT EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu');
SELECT EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu');
SELECT EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu');
SELECT EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu');
SELECT EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu');
SELECT EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu');
SELECT EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu');
SELECT EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu');
SELECT EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu');
SELECT EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu');
SELECT EXTRACT(SECOND FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu');
SELECT second(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu');
SELECT second(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu');
SELECT second(TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu');
SELECT second(TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu');
SELECT second(TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu');
SELECT second(TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu');
SELECT second(TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu');
SELECT second(TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu');
SELECT second(TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu');
SELECT second(TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu');
SELECT second(TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu');
SELECT second(TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu');
SELECT second(TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu');
-- SELECT millisecond(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56 Asia/Kathmandu] is invalid
-- SELECT millisecond(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1 Asia/Kathmandu] is invalid
-- SELECT millisecond(TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12 Asia/Kathmandu] is invalid
-- SELECT millisecond(TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123 Asia/Kathmandu] is invalid
-- SELECT millisecond(TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1234 Asia/Kathmandu] is invalid
-- SELECT millisecond(TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12345 Asia/Kathmandu] is invalid
-- SELECT millisecond(TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123456 Asia/Kathmandu] is invalid
-- SELECT millisecond(TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1234567 Asia/Kathmandu] is invalid
-- SELECT millisecond(TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12345678 Asia/Kathmandu] is invalid
-- SELECT millisecond(TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123456789 Asia/Kathmandu] is invalid
-- SELECT millisecond(TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1234567890 Asia/Kathmandu] is invalid
-- SELECT millisecond(TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12345678901 Asia/Kathmandu] is invalid
-- SELECT millisecond(TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123456789012 Asia/Kathmandu] is invalid
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.1 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.12 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.123 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.1234 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.12345 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.123456 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.1234567 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.12345678 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.123456789 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.1234567891 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.12345678912 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.123456789123 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56 +08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.1 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1 +08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.12 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12 +08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.123 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123 +08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.1234 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1234 +08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.12345 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12345 +08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.123456 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123456 +08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.1234567 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1234567 +08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.12345678 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12345678 +08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.123456789 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123456789 +08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.1234567891 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1234567891 +08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.12345678912 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12345678912 +08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.123456789123 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123456789123 +08:35] is invalid
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.1 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.12 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.123 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.1234 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.12345 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.123456 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.1234567 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.12345678 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.123456789 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.1234567891 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.12345678912 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-05-10 12:34:56.123456789123 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_HOUR'
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56 -08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.1 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1 -08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.12 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12 -08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.123 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123 -08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.1234 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1234 -08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.12345 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12345 -08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.123456 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123456 -08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.1234567 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1234567 -08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.12345678 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12345678 -08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.123456789 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123456789 -08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.1234567891 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1234567891 -08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.12345678912 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12345678912 -08:35] is invalid
-- SELECT timezone_hour(TIMESTAMP '2020-05-10 12:34:56.123456789123 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123456789123 -08:35] is invalid
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1234 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12345 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123456 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1234567 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12345678 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123456789 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1234567891 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12345678912 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123456789123 +08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56 +08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.1 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1 +08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.12 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12 +08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.123 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123 +08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.1234 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1234 +08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.12345 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12345 +08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.123456 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123456 +08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.1234567 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1234567 +08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.12345678 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12345678 +08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.123456789 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123456789 +08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.1234567891 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1234567891 +08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.12345678912 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12345678912 +08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.123456789123 +08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123456789123 +08:35] is invalid
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1234 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12345 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123456 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1234567 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12345678 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123456789 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1234567891 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12345678912 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123456789123 -08:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56 -08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.1 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1 -08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.12 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12 -08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.123 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123 -08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.1234 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1234 -08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.12345 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12345 -08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.123456 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123456 -08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.1234567 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1234567 -08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.12345678 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12345678 -08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.123456789 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123456789 -08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.1234567891 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1234567891 -08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.12345678912 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12345678912 -08:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.123456789123 -08:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123456789123 -08:35] is invalid
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56 -00:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1 -00:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12 -00:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123 -00:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1234 -00:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12345 -00:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123456 -00:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1234567 -00:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12345678 -00:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123456789 -00:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.1234567891 -00:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.12345678912 -00:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-05-10 12:34:56.123456789123 -00:35'); # error: errCode = 2, detailMessage = Can not found function 'TIMEZONE_MINUTE'
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56 -00:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56 -00:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.1 -00:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1 -00:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.12 -00:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12 -00:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.123 -00:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123 -00:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.1234 -00:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1234 -00:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.12345 -00:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12345 -00:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.123456 -00:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123456 -00:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.1234567 -00:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1234567 -00:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.12345678 -00:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12345678 -00:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.123456789 -00:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123456789 -00:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.1234567891 -00:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1234567891 -00:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.12345678912 -00:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12345678912 -00:35] is invalid
-- SELECT timezone_minute(TIMESTAMP '2020-05-10 12:34:56.123456789123 -00:35'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123456789123 -00:35] is invalid
-- SELECT EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_WEEK'
-- SELECT EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_WEEK'
-- SELECT EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_WEEK'
-- SELECT EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_WEEK'
-- SELECT EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_WEEK'
-- SELECT EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_WEEK'
-- SELECT EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_WEEK'
-- SELECT EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_WEEK'
-- SELECT EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_WEEK'
-- SELECT EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_WEEK'
-- SELECT EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_WEEK'
-- SELECT EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_WEEK'
-- SELECT EXTRACT(DAY_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_WEEK'
SELECT day_of_week(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu');
SELECT day_of_week(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu');
SELECT day_of_week(TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu');
SELECT day_of_week(TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu');
SELECT day_of_week(TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu');
SELECT day_of_week(TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu');
SELECT day_of_week(TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu');
SELECT day_of_week(TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu');
SELECT day_of_week(TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu');
SELECT day_of_week(TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu');
SELECT day_of_week(TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu');
SELECT day_of_week(TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu');
SELECT day_of_week(TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu');
-- SELECT EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOW'
-- SELECT EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOW'
-- SELECT EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOW'
-- SELECT EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOW'
-- SELECT EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOW'
-- SELECT EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOW'
-- SELECT EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOW'
-- SELECT EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOW'
-- SELECT EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOW'
-- SELECT EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOW'
-- SELECT EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOW'
-- SELECT EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOW'
-- SELECT EXTRACT(DOW FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOW'
-- SELECT EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_YEAR'
-- SELECT EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_YEAR'
-- SELECT EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_YEAR'
-- SELECT EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_YEAR'
-- SELECT EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_YEAR'
-- SELECT EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_YEAR'
-- SELECT EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_YEAR'
-- SELECT EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_YEAR'
-- SELECT EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_YEAR'
-- SELECT EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_YEAR'
-- SELECT EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_YEAR'
-- SELECT EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_YEAR'
-- SELECT EXTRACT(DAY_OF_YEAR FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_YEAR'
SELECT day_of_year(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu');
SELECT day_of_year(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu');
SELECT day_of_year(TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu');
SELECT day_of_year(TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu');
SELECT day_of_year(TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu');
SELECT day_of_year(TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu');
SELECT day_of_year(TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu');
SELECT day_of_year(TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu');
SELECT day_of_year(TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu');
SELECT day_of_year(TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu');
SELECT day_of_year(TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu');
SELECT day_of_year(TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu');
SELECT day_of_year(TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu');
-- SELECT EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOY'
-- SELECT EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOY'
-- SELECT EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOY'
-- SELECT EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOY'
-- SELECT EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOY'
-- SELECT EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOY'
-- SELECT EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOY'
-- SELECT EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOY'
-- SELECT EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOY'
-- SELECT EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOY'
-- SELECT EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOY'
-- SELECT EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOY'
-- SELECT EXTRACT(DOY FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'DOY'
SELECT EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu');
SELECT EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu');
SELECT EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu');
SELECT EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu');
SELECT EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu');
SELECT EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu');
SELECT EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu');
SELECT EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu');
SELECT EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu');
SELECT EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu');
SELECT EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu');
SELECT EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu');
SELECT EXTRACT(QUARTER FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu');
SELECT quarter(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu');
SELECT quarter(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu');
SELECT quarter(TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu');
SELECT quarter(TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu');
SELECT quarter(TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu');
SELECT quarter(TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu');
SELECT quarter(TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu');
SELECT quarter(TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu');
SELECT quarter(TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu');
SELECT quarter(TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu');
SELECT quarter(TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu');
SELECT quarter(TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu');
SELECT quarter(TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu');
SELECT week_of_year(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu');
SELECT week_of_year(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu');
SELECT week_of_year(TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu');
SELECT week_of_year(TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu');
SELECT week_of_year(TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu');
SELECT week_of_year(TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu');
SELECT week_of_year(TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu');
SELECT week_of_year(TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu');
SELECT week_of_year(TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu');
SELECT week_of_year(TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu');
SELECT week_of_year(TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu');
SELECT week_of_year(TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu');
SELECT week_of_year(TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu');
-- SELECT EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YEAR_OF_WEEK'
-- SELECT EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YEAR_OF_WEEK'
-- SELECT EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YEAR_OF_WEEK'
-- SELECT EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YEAR_OF_WEEK'
-- SELECT EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YEAR_OF_WEEK'
-- SELECT EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YEAR_OF_WEEK'
-- SELECT EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YEAR_OF_WEEK'
-- SELECT EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YEAR_OF_WEEK'
-- SELECT EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YEAR_OF_WEEK'
-- SELECT EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YEAR_OF_WEEK'
-- SELECT EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YEAR_OF_WEEK'
-- SELECT EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YEAR_OF_WEEK'
-- SELECT EXTRACT(YEAR_OF_WEEK FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YEAR_OF_WEEK'
-- SELECT year_of_week(TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56 Asia/Kathmandu] is invalid
-- SELECT year_of_week(TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1 Asia/Kathmandu] is invalid
-- SELECT year_of_week(TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12 Asia/Kathmandu] is invalid
-- SELECT year_of_week(TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123 Asia/Kathmandu] is invalid
-- SELECT year_of_week(TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1234 Asia/Kathmandu] is invalid
-- SELECT year_of_week(TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12345 Asia/Kathmandu] is invalid
-- SELECT year_of_week(TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123456 Asia/Kathmandu] is invalid
-- SELECT year_of_week(TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1234567 Asia/Kathmandu] is invalid
-- SELECT year_of_week(TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12345678 Asia/Kathmandu] is invalid
-- SELECT year_of_week(TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123456789 Asia/Kathmandu] is invalid
-- SELECT year_of_week(TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.1234567890 Asia/Kathmandu] is invalid
-- SELECT year_of_week(TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.12345678901 Asia/Kathmandu] is invalid
-- SELECT year_of_week(TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu'); # error: errCode = 2, detailMessage = date/datetime literal [2020-05-10 12:34:56.123456789012 Asia/Kathmandu] is invalid
-- SELECT EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YOW'
-- SELECT EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.1 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YOW'
-- SELECT EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.12 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YOW'
-- SELECT EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.123 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YOW'
-- SELECT EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.1234 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YOW'
-- SELECT EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.12345 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YOW'
-- SELECT EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.123456 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YOW'
-- SELECT EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.1234567 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YOW'
-- SELECT EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.12345678 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YOW'
-- SELECT EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.123456789 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YOW'
-- SELECT EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.1234567890 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YOW'
-- SELECT EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.12345678901 Asia/Kathmandu'); # error: errCode = 2, detailMessage = Can not found function 'YOW'
-- SELECT EXTRACT(YOW FROM TIMESTAMP '2020-05-10 12:34:56.123456789012 Asia/Kathmandu') # error: errCode = 2, detailMessage = Can not found function 'YOW'
