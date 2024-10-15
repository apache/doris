--dialect option
set sql_dialect='clickhouse';
--math operator
select plus(1, 2),
       minus(1, 2),
       multiply(2, 5),
       divide(4, 2),
       intDiv(5,2),
       intDivOrZero(2,0),
       intDivOrZero(2,1),
       modulo(3, 2),
       moduloOrZero(1, 0),
       moduloOrZero(7, 3),
       negate(-1);

---Type Conversion
SELECT toInt64(64), toInt32(32), toInt16('16'), toInt8(8.8);
SELECT toInt64OrZero('123123'), toInt8OrZero('123qwe123');
SELECT toInt64OrNull('123123'), toInt8OrNull('123qwe123');
SELECT toInt64OrDefault('123123', cast('-1' as Int64)), toInt8OrDefault('123qwe123', cast('-1' as Int8));
select toFloat32('123.123'),toFloat64('12.3123');

select toFloat32OrZero('123.1d23'),toFloat64OrZero('12.31d23');

select toFloat32OrNull('123.1d23'),toFloat64OrNull('12.31d23');

select toDate('2022-10-10 10:10:10'),toDate('2022-10-10 10:10:10', 'Asia/Shanghai');

SELECT toDateOrZero('2022-12-30'), toDateOrZero('');

SELECT toDateOrNull('2022-12-30'), toDateOrNull('');

SELECT toDateTime('2022-12-30 13:44:17');

SELECT toDateTimeOrZero('2022-12-30 13:44:17'), toDateTimeOrZero('');

SELECT toDateTimeOrNull('2022-12-30 13:44:17'), toDateTimeOrNull('');

SELECT toDecimal32OrNull(toString(-1.111), 5) AS val;

SELECT toDecimal32OrZero(toString(-1.111), 5) AS val;

select toDate('2022-12-30 13:44:17'),t from (select '2019-10-30' as t);

WITH
    toDate('2019-01-01') AS date,
    INTERVAL 1 WEEK AS interval_week,
    toIntervalWeek(1) AS interval_to_week
SELECT
    date + interval_week,
    date + interval_to_week;


SELECT parseDateTime('2021-01-04+23:00:00', '%Y-%m-%d+%H:%i:%s'),parseDateTimeOrNull('2021-01-04+23:00:00', '%Y-%m-%d+%H:%i:%s');

SELECT length('doris'),OCTET_LENGTH('doris');

SELECT range(5), range(1, 5), range(1, 5, 2);

SELECT arrayConcat([1, 2], [3, 4], [5, 6]) AS res, arrayElement(res,1);

--doris:true,clickhouse:1
SELECT has([1, 2, NULL], NULL);

SELECT indexOf([1, 3, NULL, NULL], NULL);

select arrayCount(x -> x, [0, 1, 2, 3]);

SELECT countEqual([1, 2, NULL, NULL], NULL);

SELECT arrayEnumerate([0, 1, 2, 3]),arrayEnumerateUniq([0, 1, 2, 3,3]);

SELECT arrayPopBack([1, 2, 3]) AS res;

SELECT arrayPopFront([1, 2, 3]) AS res;

SELECT arrayPushBack(['a'], 'b') AS res;

SELECT arrayPushFront(['b'], 'a') AS res;

SELECT arraySlice([1, 2, NULL, 4, 5], 2, 3) AS res;

SELECT arraySort(['hello', 'world', '!']);

SELECT arrayReverseSort([1, 3, 3, 0]),arrayReverseSort(['hello', 'world', '!']);

--SELECT arrayShuffle([1, 2, 3, 4]);

SELECT arrayUniq([1, 2, 2, 3]) AS unique_count;

SELECT arrayDifference([1, 2, 3, 4]);

SELECT arrayDistinct([1, 2, 2, 3, 1]);

SELECT
    arrayIntersect([1, 2], [1, 3], [2, 3]) AS no_intersect,
    arrayIntersect([1, 2], [1, 3], [1, 4]) AS intersect;

SELECT arrayReverse([1, 2, 3]),reverse([1, 2, 3]);

SELECT arrayCompact([1, 1, 2, 3, 3, 3]);

SELECT arrayMap(x -> (x + 2), [1, 2, 3]) as res;

SELECT arrayFilter(x -> x LIKE '%World%', ['Hello', 'abc World']) AS res;

SELECT arrayFirst(x->x>1,[1,2,3]);

SELECT arrayLast(x->x>1,[1,2,3]);

SELECT arrayFirstIndex(x->x>1,[1,2,3]);

SELECT arrayLastIndex(x->x>1,[1,2,3]);

-- lambda expr not support
-- SELECT arrayMin([1, 2, 4]) AS res1, arrayMin(x -> (-x), [1, 2, 4]) AS res2,arrayMax([1, 2, 4]) AS res3,arrayMax(x -> (-x), [1, 2, 4]) AS res4;
SELECT arrayMin([1, 2, 4]) AS res1,arrayMax([1, 2, 4]) AS res3;

SELECT arrayCumSum([1, 1, 1, 1]) AS res;

SELECT arrayProduct([1,2,3,4,5,6]) as res;

select bitAnd(3,5) ans1,bitOr(3,5) ans2,bitXor(3,5) ans3,bitNot(-1) ans4;

SELECT 101 AS a, bitShiftRight(a, 2) AS a_shifted;

SELECT bitCount(333);

SELECT bitmapToArray(bitmapSubsetInRange(bitmapBuild([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,100,200,500]), toInt32(30), toInt32(200))) AS res;

SELECT bitmapToArray(bitmapSubsetLimit(bitmapBuild([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,100,200,500]), toInt32(30), toInt32(200))) AS res;

SELECT bitmapToArray(bitmapSubsetLimit(bitmapBuild([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,100,200,500]), toInt32(30), toInt32(200))) AS res;

SELECT bitmapContains(bitmapBuild([1,5,7,9]), toInt32(9)) AS res;

SELECT bitmapContains(bitmapBuild([1,5,7,9]), toInt32(9)) AS res;

SELECT bitmapHasAll(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;

SELECT bitmapToArray(bitmapAnd(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res;

SELECT bitmapToArray(bitmapOr(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res;

SELECT bitmapToArray(bitmapXor(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res;

SELECT bitmapToArray(bitmapAndnot(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res;

SELECT bitmapCardinality(bitmapBuild([1, 2, 3, 4, 5])) AS res;

SELECT bitmapMin(bitmapBuild([1, 2, 3, 4, 5])) AS res;

SELECT bitmapMax(bitmapBuild([1, 2, 3, 4, 5])) AS res;

SELECT bitmapAndCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;

SELECT bitmapOrCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;

SELECT bitmapXorCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;

SELECT bitmapAndnotCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;

WITH
    1 as left1,
    2 as right1
SELECT
    left1,
    right1,
    multiIf(left1 < right1, 'left is smaller', left1 > right1, 'left is greater', left1 = right1, 'Both equal', 'Null value') AS result;

SELECT toYear(toDateTime('2023-04-21 10:20:30'));

SELECT toQuarter(toDateTime('2023-04-21 10:20:30'));

SELECT toMonth(toDateTime('2023-04-21 10:20:30'));

SELECT toDayOfYear(toDateTime('2023-04-21 10:20:30'));

SELECT toDayOfMonth(toDateTime('2023-04-21 10:20:30'));

SELECT toHour(toDateTime('2023-04-21 10:20:30'));

SELECT toMinute(toDateTime('2023-04-21 10:20:30'));

SELECT toSecond(toDateTime('2023-04-21 10:20:30'));

SELECT
    '2017-11-05 08:07:47' AS dt_str,
    toUnixTimestamp(dt_str) AS from_str;

SELECT toStartOfDay(toDateTime('2023-04-21 10:20:30'));

SELECT
    toStartOfHour(toDateTime('2023-04-21 10:20:30')),
    toStartOfHour(toDateTime64('2023-04-21', 6));

SELECT
    toStartOfMinute(toDateTime('2023-04-21 10:20:30')),
    toStartOfMinute(toDateTime64('2023-04-21 10:20:30.5300', 8));

--doris:2020-01-01 10:20:31 clickhouse:2020-01-01 10:20:30
WITH toDateTime64('2020-01-01 10:20:30.999', 3) AS dt64
SELECT toStartOfSecond(dt64);

SELECT toDate('2016-12-27') AS date, toWeek(date) AS week0, toWeek(date,1) AS week1, toWeek(date,7) AS week7;

SELECT date_add(YEAR, 3, toDate('2018-01-01')),dateAdd(YEAR, 3, toDate('2018-01-01')),DATE_ADD(YEAR, 3, toDate('2018-01-01'));

SELECT date_sub(YEAR, 3, toDate('2018-01-01')),dateSub(YEAR, 3, toDate('2018-01-01'));

SELECT date_sub(toDate('2018-01-01'), INTERVAL 3 YEAR),dateSub(toDate('2018-01-01'), INTERVAL 3 YEAR);

select timestamp_add(toDate('2018-01-01'), INTERVAL 3 MONTH),timeStampAdd(toDate('2018-01-01'), INTERVAL 3 MONTH);

select timestamp_sub(MONTH, 5, toDateTime('2018-12-18 01:02:03')),timeStampSub(MONTH, 5, toDateTime('2018-12-18 01:02:03'));

SELECT addDate(toDate('2018-01-01'), INTERVAL 3 YEAR);

SELECT subDate(toDate('2018-01-01'), INTERVAL 3 YEAR);

SELECT
    toYYYYMM(toDateTime('2024-05-13 12:07:33'), 'US/Eastern');

SELECT toYYYYMMDD(toDateTime('2024-05-13 12:07:33'), 'US/Eastern');

SELECT toYYYYMMDDhhmmss(toDateTime('2024-05-13 12:07:33'), 'US/Eastern');

WITH
    toDate('2024-01-01') AS `date`,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    addYears(date, 1) AS add_years_with_date,
    addYears(date_time, 1) AS add_years_with_date_time,
    addYears(date_time_string, 1) AS add_years_with_date_time_string;

WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    addQuarters(date, 1) AS add_quarters_with_date,
    addQuarters(date_time, 1) AS add_quarters_with_date_time,
    addQuarters(date_time_string, 1) AS add_quarters_with_date_time_string;

WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    addMonths(date, 6) AS add_months_with_date,
    addMonths(date_time, 6) AS add_months_with_date_time,
    addMonths(date_time_string, 6) AS add_months_with_date_time_string;

WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    addWeeks(date, 5) AS add_weeks_with_date,
    addWeeks(date_time, 5) AS add_weeks_with_date_time,
    addWeeks(date_time_string, 5) AS add_weeks_with_date_time_string;

WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    addDays(date, 5) AS add_days_with_date,
    addDays(date_time, 5) AS add_days_with_date_time,
    addDays(date_time_string, 5) AS add_days_with_date_time_string;

WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    addHours(date, 12) AS add_hours_with_date,
    addHours(date_time, 12) AS add_hours_with_date_time,
    addHours(date_time_string, 12) AS add_hours_with_date_time_string;

WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    addMinutes(date, 20) AS add_minutes_with_date,
    addMinutes(date_time, 20) AS add_minutes_with_date_time,
    addMinutes(date_time_string, 20) AS add_minutes_with_date_time_string;

WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    addSeconds(date, 30) AS add_seconds_with_date,
    addSeconds(date_time, 30) AS add_seconds_with_date_time,
    addSeconds(date_time_string, 30) AS add_seconds_with_date_time_string;

WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    subtractYears(date, 1) AS subtract_years_with_date,
    subtractYears(date_time, 1) AS subtract_years_with_date_time,
    subtractYears(date_time_string, 1) AS subtract_years_with_date_time_string;

WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    subtractQuarters(date, 1) AS subtract_quarters_with_date,
    subtractQuarters(date_time, 1) AS subtract_quarters_with_date_time,
    subtractQuarters(date_time_string, 1) AS subtract_quarters_with_date_time_string;

WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    subtractMonths(date, 1) AS subtract_months_with_date,
    subtractMonths(date_time, 1) AS subtract_months_with_date_time,
    subtractMonths(date_time_string, 1) AS subtract_months_with_date_time_string;

WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    subtractWeeks(date, 1) AS subtract_weeks_with_date,
    subtractWeeks(date_time, 1) AS subtract_weeks_with_date_time,
    subtractWeeks(date_time_string, 1) AS subtract_weeks_with_date_time_string;

WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    subtractDays(date, 31) AS subtract_days_with_date,
    subtractDays(date_time, 31) AS subtract_days_with_date_time,
    subtractDays(date_time_string, 31) AS subtract_days_with_date_time_string;

WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    subtractHours(date, 12) AS subtract_hours_with_date,
    subtractHours(date_time, 12) AS subtract_hours_with_date_time,
    subtractHours(date_time_string, 12) AS subtract_hours_with_date_time_string;

WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    subtractMinutes(date, 30) AS subtract_minutes_with_date,
    subtractMinutes(date_time, 30) AS subtract_minutes_with_date_time,
    subtractMinutes(date_time_string, 30) AS subtract_minutes_with_date_time_string;

WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    subtractSeconds(date, 60) AS subtract_seconds_with_date,
    subtractSeconds(date_time, 60) AS subtract_seconds_with_date_time,
    subtractSeconds(date_time_string, 60) AS subtract_seconds_with_date_time_string;

SELECT formatDateTime(toDate('2010-01-04'), '%Y');

SELECT SHA1('abc'),SHA224('abc'),SHA256('abc');

select IPv4NumToString(3232235521),INET_NTOA(3232235521);

select IPv4StringToNum('192.168.0.1'),INET_ATON('192.168.0.1');

with '0.0.0.0' as str
select str, IPv4StringToNumOrNull(str);

SELECT arrayExists(x -> x=1,[1,2,3]);

SELECT arrayFirst(x -> x>1,[1,2,3]);

SELECT arrayLast(x -> x>1,[1,2,3]);

SELECT arrayFirstIndex(x -> x>0,[1,2,3]);

SELECT arrayLastIndex(x -> x>1,[1,2,3]);

SELECT arraySum([2, 3]) AS res;

SELECT arrayAvg([1, 2, 4]) AS res;

SELECT arrayCumSum([1, 1, 1, 1]) AS res;

SELECT arrayProduct([1,2,3,4,5,6]) as res;

SELECT bitAnd(1,1);

SELECT bitOr(1,1);

SELECT bitXor(1,1);

SELECT bitNot(0);

SELECT bitShiftLeft(1, 2);

SELECT bitShiftRight(8, 2);

SELECT bitCount(333);

SELECT bitmapBuild([1, 2, 3, 4, 5]) AS res;

SELECT bitmapToArray(bitmapBuild([1, 2, 3, 4, 5])) AS res;

SELECT bitmapToArray(bitmapSubsetInRange(bitmapBuild([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,100,200,500]), toInt32(30), toInt32(200))) AS res;

SELECT bitmapToArray(bitmapSubsetLimit(bitmapBuild([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,100,200,500]), toInt32(30), toInt32(200))) AS res;

SELECT bitmapToArray(subBitmap(bitmapBuild([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,100,200,500]), toInt32(10), toInt32(10))) AS res;


SELECT bitmapContains(bitmapBuild([1,5,7,9]), toInt32(9)) AS res;

SELECT bitmapHasAny(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;

SELECT bitmapHasAll(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;

SELECT bitmapCardinality(bitmapBuild([1, 2, 3, 4, 5])) AS res;

SELECT bitmapMin(bitmapBuild([1, 2, 3, 4, 5])) AS res;

SELECT bitmapMin(bitmapBuild([1, 2, 3, 4, 5])) AS res;

SELECT bitmapToArray(bitmapAnd(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res;

SELECT bitmapToArray(bitmapOr(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res;

SELECT bitmapToArray(bitmapXor(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res;

SELECT bitmapToArray(bitmapAndnot(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]))) AS res;

SELECT bitmapAndCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;

SELECT bitmapOrCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;

SELECT bitmapXorCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;

SELECT bitmapAndnotCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])) AS res;

SELECT if(1, plus(2, 2), plus(2, 6));

SELECT multiIf(1 < 2, 'left is smaller', 1 > 2, 'left is greater', 1 = 2, 'Both equal', 'Null value') AS result;

SELECT 1<2;

SELECT NULL < 1, 2 < NULL, NULL < NULL, NULL = NULL;

SELECT greatest(1, 2, toInt8(3), 3.) result;

SELECT least(1, 2, toInt8(3), 3.) result;

