set sql_dialect='presto';
set enable_fallback_to_original_planner=false;
set debug_skip_fold_constant=false;
SELECT EXTRACT(YEAR FROM DATE '2020-05-10');
SELECT EXTRACT(YEAR FROM DATE '1960-05-10');
SELECT year(DATE '2020-05-10');
SELECT year(DATE '1960-05-10');
SELECT EXTRACT(MONTH FROM DATE '2020-05-10');
SELECT EXTRACT(MONTH FROM DATE '1960-05-10');
SELECT month(DATE '2020-05-10');
SELECT month(DATE '1960-05-10');
SELECT EXTRACT(WEEK FROM DATE '2020-05-10');
SELECT EXTRACT(WEEK FROM DATE '1960-05-10');
SELECT week(DATE '2020-05-10');
SELECT week(DATE '1960-05-10');
SELECT EXTRACT(DAY FROM DATE '2020-05-10');
SELECT EXTRACT(DAY FROM DATE '1960-05-10');
SELECT day(DATE '2020-05-10');
SELECT day(DATE '1960-05-10');
-- SELECT EXTRACT(DAY_OF_MONTH FROM DATE '2020-05-10'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_MONTH'
-- SELECT EXTRACT(DAY_OF_MONTH FROM DATE '1960-05-10'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_MONTH'
SELECT day_of_month(DATE '2020-05-10');
SELECT day_of_month(DATE '1960-05-10');
-- SELECT EXTRACT(DAY_OF_WEEK FROM DATE '2020-05-10'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_WEEK'
-- SELECT EXTRACT(DAY_OF_WEEK FROM DATE '1960-05-10'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_WEEK'
SELECT day_of_week(DATE '2020-05-10');
SELECT day_of_week(DATE '1960-05-10');
-- SELECT EXTRACT(DOW FROM DATE '2020-05-10'); # error: errCode = 2, detailMessage = Can not found function 'DOW'
-- SELECT EXTRACT(DOW FROM DATE '1960-05-10'); # error: errCode = 2, detailMessage = Can not found function 'DOW'
SELECT dow(DATE '2020-05-10');
SELECT dow(DATE '1960-05-10');
-- SELECT EXTRACT(DAY_OF_YEAR FROM DATE '2020-05-10'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_YEAR'
-- SELECT EXTRACT(DAY_OF_YEAR FROM DATE '1960-05-10'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_YEAR'
SELECT day_of_year(DATE '2020-05-10');
SELECT day_of_year(DATE '1960-05-10');
-- SELECT EXTRACT(DOY FROM DATE '2020-05-10'); # error: errCode = 2, detailMessage = Can not found function 'DOY'
-- SELECT EXTRACT(DOY FROM DATE '1960-05-10'); # error: errCode = 2, detailMessage = Can not found function 'DOY'
SELECT doy(DATE '2020-05-10');
SELECT doy(DATE '1960-05-10');
SELECT EXTRACT(QUARTER FROM DATE '2020-05-10');
SELECT EXTRACT(QUARTER FROM DATE '1960-05-10');
SELECT quarter(DATE '2020-05-10');
SELECT quarter(DATE '1960-05-10');
-- SELECT EXTRACT(YEAR_OF_WEEK FROM DATE '2020-05-10'); # error: errCode = 2, detailMessage = Can not found function 'YEAR_OF_WEEK'
-- SELECT EXTRACT(YEAR_OF_WEEK FROM DATE '1960-05-10'); # error: errCode = 2, detailMessage = Can not found function 'YEAR_OF_WEEK'
-- SELECT year_of_week(DATE '2020-05-10'); # error: errCode = 2, detailMessage = Can not found function 'year_of_week'
-- SELECT year_of_week(DATE '1960-05-10'); # error: errCode = 2, detailMessage = Can not found function 'year_of_week'
-- SELECT EXTRACT(YOW FROM DATE '2020-05-10'); # error: errCode = 2, detailMessage = Can not found function 'YOW'
-- SELECT EXTRACT(YOW FROM DATE '1960-05-10'); # error: errCode = 2, detailMessage = Can not found function 'YOW'
-- SELECT yow(DATE '2020-05-10'); # error: errCode = 2, detailMessage = Can not found function 'yow'
-- SELECT yow(DATE '1960-05-10'); # error: errCode = 2, detailMessage = Can not found function 'yow'
set debug_skip_fold_constant=true;
SELECT EXTRACT(YEAR FROM DATE '2020-05-10');
SELECT EXTRACT(YEAR FROM DATE '1960-05-10');
SELECT year(DATE '2020-05-10');
SELECT year(DATE '1960-05-10');
SELECT EXTRACT(MONTH FROM DATE '2020-05-10');
SELECT EXTRACT(MONTH FROM DATE '1960-05-10');
SELECT month(DATE '2020-05-10');
SELECT month(DATE '1960-05-10');
SELECT EXTRACT(WEEK FROM DATE '2020-05-10');
SELECT EXTRACT(WEEK FROM DATE '1960-05-10');
SELECT week(DATE '2020-05-10');
SELECT week(DATE '1960-05-10');
SELECT EXTRACT(DAY FROM DATE '2020-05-10');
SELECT EXTRACT(DAY FROM DATE '1960-05-10');
SELECT day(DATE '2020-05-10');
SELECT day(DATE '1960-05-10');
-- SELECT EXTRACT(DAY_OF_MONTH FROM DATE '2020-05-10'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_MONTH'
-- SELECT EXTRACT(DAY_OF_MONTH FROM DATE '1960-05-10'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_MONTH'
SELECT day_of_month(DATE '2020-05-10');
SELECT day_of_month(DATE '1960-05-10');
-- SELECT EXTRACT(DAY_OF_WEEK FROM DATE '2020-05-10'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_WEEK'
-- SELECT EXTRACT(DAY_OF_WEEK FROM DATE '1960-05-10'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_WEEK'
SELECT day_of_week(DATE '2020-05-10');
SELECT day_of_week(DATE '1960-05-10');
-- SELECT EXTRACT(DOW FROM DATE '2020-05-10'); # error: errCode = 2, detailMessage = Can not found function 'DOW'
-- SELECT EXTRACT(DOW FROM DATE '1960-05-10'); # error: errCode = 2, detailMessage = Can not found function 'DOW'
SELECT dow(DATE '2020-05-10');
SELECT dow(DATE '1960-05-10');
-- SELECT EXTRACT(DAY_OF_YEAR FROM DATE '2020-05-10'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_YEAR'
-- SELECT EXTRACT(DAY_OF_YEAR FROM DATE '1960-05-10'); # error: errCode = 2, detailMessage = Can not found function 'DAY_OF_YEAR'
SELECT day_of_year(DATE '2020-05-10');
SELECT day_of_year(DATE '1960-05-10');
-- SELECT EXTRACT(DOY FROM DATE '2020-05-10'); # error: errCode = 2, detailMessage = Can not found function 'DOY'
-- SELECT EXTRACT(DOY FROM DATE '1960-05-10'); # error: errCode = 2, detailMessage = Can not found function 'DOY'
SELECT doy(DATE '2020-05-10');
SELECT doy(DATE '1960-05-10');
SELECT EXTRACT(QUARTER FROM DATE '2020-05-10');
SELECT EXTRACT(QUARTER FROM DATE '1960-05-10');
SELECT quarter(DATE '2020-05-10');
SELECT quarter(DATE '1960-05-10');
-- SELECT EXTRACT(YEAR_OF_WEEK FROM DATE '2020-05-10'); # error: errCode = 2, detailMessage = Can not found function 'YEAR_OF_WEEK'
-- SELECT EXTRACT(YEAR_OF_WEEK FROM DATE '1960-05-10'); # error: errCode = 2, detailMessage = Can not found function 'YEAR_OF_WEEK'
-- SELECT year_of_week(DATE '2020-05-10'); # error: errCode = 2, detailMessage = Can not found function 'year_of_week'
-- SELECT year_of_week(DATE '1960-05-10'); # error: errCode = 2, detailMessage = Can not found function 'year_of_week'
-- SELECT EXTRACT(YOW FROM DATE '2020-05-10'); # error: errCode = 2, detailMessage = Can not found function 'YOW'
-- SELECT EXTRACT(YOW FROM DATE '1960-05-10'); # error: errCode = 2, detailMessage = Can not found function 'YOW'
-- SELECT yow(DATE '2020-05-10'); # error: errCode = 2, detailMessage = Can not found function 'yow'
-- SELECT yow(DATE '1960-05-10') # error: errCode = 2, detailMessage = Can not found function 'yow'
