set sql_dialect='presto';
set enable_fallback_to_original_planner=false;
set debug_skip_fold_constant=false;
-- SELECT to_iso8601(TIMESTAMP '2001-08-22 03:04:05.321'); # error: errCode = 2, detailMessage = Can not found function 'to_iso8601'
-- SELECT from_unixtime(980172245); # differ: doris : 2001-01-22 22:04:05, presto : 2001-01-22 14:04:05.000
-- SELECT from_unixtime(980172245.888); # differ: doris : 2001-01-22 22:04:05, presto : 2001-01-22 14:04:05.888
-- SELECT from_unixtime(123456789123456789); # differ: doris : None, presto : +294247-01-10 04:00:54.775
-- SELECT from_unixtime_nanos(1234567890123456789); # error: errCode = 2, detailMessage = Can not found function 'from_unixtime_nanos'
-- SELECT from_unixtime_nanos(999999999); # error: errCode = 2, detailMessage = Can not found function 'from_unixtime_nanos'
-- SELECT from_unixtime_nanos(-1234567890123456789); # error: errCode = 2, detailMessage = Can not found function 'from_unixtime_nanos'
-- SELECT from_unixtime_nanos(-999999999); # error: errCode = 2, detailMessage = Can not found function 'from_unixtime_nanos'
-- SELECT from_unixtime_nanos(DECIMAL '1234'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT from_unixtime_nanos(DECIMAL '1234');	                                   ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT from_unixtime_nanos(DECIMAL '1234.0'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT from_unixtime_nanos(DECIMAL '1234.0');	                                   ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT from_unixtime_nanos(DECIMAL '1234.499'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT from_unixtime_nanos(DECIMAL '1234.499');	                                   ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT from_unixtime_nanos(DECIMAL '1234.500'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT from_unixtime_nanos(DECIMAL '1234.500');	                                   ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT from_unixtime_nanos(DECIMAL '-1234'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT from_unixtime_nanos(DECIMAL '-1234');	                                   ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT from_unixtime_nanos(DECIMAL '-1234.0'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT from_unixtime_nanos(DECIMAL '-1234.0');	                                   ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT from_unixtime_nanos(DECIMAL '-1234.499'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT from_unixtime_nanos(DECIMAL '-1234.499');	                                   ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT from_unixtime_nanos(DECIMAL '-1234.500'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT from_unixtime_nanos(DECIMAL '-1234.500');	                                   ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT from_unixtime_nanos(DECIMAL '12345678900123456789'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT from_unixtime_nanos(DECIMAL '12345678900123456789');	                                   ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT from_unixtime_nanos(DECIMAL '12345678900123456789.000000'); # error: errCode = 2, detailMessage = Syntax error in line 1:	...om_unixtime_nanos(DECIMAL '12345678900123456789.000000');	                             ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT from_unixtime_nanos(DECIMAL '12345678900123456789.499'); # error: errCode = 2, detailMessage = Syntax error in line 1:	...om_unixtime_nanos(DECIMAL '12345678900123456789.499');	                             ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT from_unixtime_nanos(DECIMAL '12345678900123456789.500'); # error: errCode = 2, detailMessage = Syntax error in line 1:	...om_unixtime_nanos(DECIMAL '12345678900123456789.500');	                             ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT from_unixtime_nanos(DECIMAL '-12345678900123456789'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT from_unixtime_nanos(DECIMAL '-12345678900123456789');	                                   ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT from_unixtime_nanos(DECIMAL '-12345678900123456789.000000'); # error: errCode = 2, detailMessage = Syntax error in line 1:	...om_unixtime_nanos(DECIMAL '-12345678900123456789.00000...	                             ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT from_unixtime_nanos(DECIMAL '-12345678900123456789.499'); # error: errCode = 2, detailMessage = Syntax error in line 1:	...om_unixtime_nanos(DECIMAL '-12345678900123456789.499');	                             ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT from_unixtime_nanos(DECIMAL '-12345678900123456789.500'); # error: errCode = 2, detailMessage = Syntax error in line 1:	...om_unixtime_nanos(DECIMAL '-12345678900123456789.500');	                             ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT from_unixtime(980172245, 1, 10); # error: errCode = 2, detailMessage = Can not found function 'FROM_UNIXTIME' which has 3 arity. Candidate functions are: [FROM_UNIXTIMEorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@1541cdc5, FROM_UNIXTIMEorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@24ddb1e8]
-- SELECT from_unixtime(7200, 'Asia/Shanghai'); # differ: doris : Asia/Shanghai, presto : 1970-01-01 10:00:00.000 Asia/Shanghai
-- SELECT from_unixtime(7200, 'Asia/Tokyo'); # differ: doris : Asia/Tokyo, presto : 1970-01-01 11:00:00.000 Asia/Tokyo
-- SELECT from_unixtime(7200, 'Europe/Kiev'); # differ: doris : Europe/Kiev, presto : 1970-01-01 05:00:00.000 Europe/Kiev
-- SELECT from_unixtime(7200, 'America/New_York'); # differ: doris : America/New_York, presto : 1969-12-31 21:00:00.000 America/New_York
-- SELECT from_unixtime(7200, 'America/Chicago'); # differ: doris : America/Chicago, presto : 1969-12-31 20:00:00.000 America/Chicago
-- SELECT from_unixtime(7200, 'America/Los_Angeles'); # differ: doris : America/Los_Angeles, presto : 1969-12-31 18:00:00.000 America/Los_Angeles
-- SELECT date('2001-08-22'); # differ: doris : 2001-08-22, presto : 2001-08-22
-- SELECT date(TIMESTAMP '2001-08-22 03:04:05.321 +07:09'); # differ: doris : None, presto : 2001-08-22
-- SELECT date(TIMESTAMP '2001-08-22 03:04:05.321'); # differ: doris : 2001-08-22, presto : 2001-08-22
-- SELECT from_iso8601_timestamp('2001-08-22T03:04:05.321-11:00'); # error: errCode = 2, detailMessage = Can not found function 'from_iso8601_timestamp'
-- SELECT from_iso8601_timestamp('2001-08-22T03:04:05.321+07:09'); # error: errCode = 2, detailMessage = Can not found function 'from_iso8601_timestamp'
-- SELECT from_iso8601_date('2001-08-22'); # error: errCode = 2, detailMessage = Can not found function 'from_iso8601_date'
-- SELECT from_iso8601_timestamp_nanos('2001-08-22T12:34:56.123456789Z'); # error: errCode = 2, detailMessage = Can not found function 'from_iso8601_timestamp_nanos'
-- SELECT from_iso8601_timestamp_nanos('2001-08-22T07:34:56.123456789-05:00'); # error: errCode = 2, detailMessage = Can not found function 'from_iso8601_timestamp_nanos'
-- SELECT from_iso8601_timestamp_nanos('2001-08-22T13:34:56.123456789+01:00'); # error: errCode = 2, detailMessage = Can not found function 'from_iso8601_timestamp_nanos'
-- SELECT from_iso8601_timestamp_nanos('2001-08-22T12:34:56.123Z'); # error: errCode = 2, detailMessage = Can not found function 'from_iso8601_timestamp_nanos'
-- SELECT to_iso8601(DATE '2001-08-22'); # error: errCode = 2, detailMessage = Can not found function 'to_iso8601'
-- SELECT current_timezone(); # error: errCode = 2, detailMessage = Can not found function 'current_timezone'
-- SELECT millisecond(INTERVAL '90061.234' SECOND); # error: errCode = 2, detailMessage = Can not found function 'millisecond'
-- SELECT second(INTERVAL '90061.234' SECOND); # error: errCode = 2, detailMessage = Invalid call to sql on unbound object
-- SELECT minute(INTERVAL '90061.234' SECOND); # error: errCode = 2, detailMessage = Invalid call to sql on unbound object
-- SELECT hour(INTERVAL '90061.234' SECOND); # error: errCode = 2, detailMessage = Invalid call to sql on unbound object
-- SELECT year_of_week(DATE '2001-08-22'); # error: errCode = 2, detailMessage = Can not found function 'year_of_week'
-- SELECT yow(DATE '2001-08-22'); # error: errCode = 2, detailMessage = Can not found function 'yow'
-- SELECT year_of_week(DATE '2005-01-02'); # error: errCode = 2, detailMessage = Can not found function 'year_of_week'
-- SELECT year_of_week(DATE '2008-12-28'); # error: errCode = 2, detailMessage = Can not found function 'year_of_week'
-- SELECT year_of_week(DATE '2008-12-29'); # error: errCode = 2, detailMessage = Can not found function 'year_of_week'
-- SELECT year_of_week(DATE '2009-12-31'); # error: errCode = 2, detailMessage = Can not found function 'year_of_week'
-- SELECT year_of_week(DATE '2010-01-03'); # error: errCode = 2, detailMessage = Can not found function 'year_of_week'
-- SELECT last_day_of_month(DATE '2001-08-22'); # differ: doris : 2001-08-31, presto : 2001-08-31
-- SELECT last_day_of_month(DATE '2019-08-01'); # differ: doris : 2019-08-31, presto : 2019-08-31
-- SELECT last_day_of_month(DATE '2019-08-31'); # differ: doris : 2019-08-31, presto : 2019-08-31
-- SELECT last_day_of_month(TIMESTAMP '2019-08-01 00:00:00.000'); # differ: doris : 2019-08-31, presto : 2019-08-31
-- SELECT last_day_of_month(TIMESTAMP '2019-08-01 17:00:00.000'); # differ: doris : 2019-08-31, presto : 2019-08-31
-- SELECT last_day_of_month(TIMESTAMP '2019-08-01 23:59:59.999'); # differ: doris : 2019-08-31, presto : 2019-08-31
-- SELECT last_day_of_month(TIMESTAMP '2019-08-31 23:59:59.999'); # differ: doris : 2019-09-30, presto : 2019-08-31
-- SELECT last_day_of_month(TIMESTAMP '2001-08-22 03:04:05.321 +07:09'); # differ: doris : None, presto : 2001-08-31
-- SELECT date_trunc('day', DATE'2001-08-22'); # differ: doris : 2001-08-22, presto : 2001-08-22
-- SELECT date_trunc('week', DATE'2001-08-22'); # differ: doris : 2001-08-20, presto : 2001-08-20
-- SELECT date_trunc('month', DATE'2001-08-22'); # differ: doris : 2001-08-01, presto : 2001-08-01
-- SELECT date_trunc('quarter', DATE'2001-08-22'); # differ: doris : 2001-07-01, presto : 2001-07-01
-- SELECT date_trunc('year', DATE'2001-08-22'); # differ: doris : 2001-01-01, presto : 2001-01-01
-- SELECT date_add('day', 0, DATE '2001-08-22'); # differ: doris : 2001-08-22, presto : 2001-08-22
-- SELECT date_add('day', 3, DATE '2001-08-22'); # differ: doris : 2001-08-25, presto : 2001-08-25
-- SELECT date_add('week', 3, DATE '2001-08-22'); # differ: doris : 2001-09-12, presto : 2001-09-12
-- SELECT date_add('month', 3, DATE '2001-08-22'); # differ: doris : 2001-11-22, presto : 2001-11-22
-- SELECT date_add('quarter', 3, DATE '2001-08-22'); # error: errCode = 2, detailMessage = 	no viable alternative at input 'DATE_ADD(CAST('2001-08-22' AS DATE), INTERVAL 3 QUARTER'(line 1, pos 55)	
-- SELECT date_add('year', 3, DATE '2001-08-22'); # differ: doris : 2004-08-22, presto : 2004-08-22
SELECT date_diff('day', DATE '1960-05-03', DATE '2001-08-22');
-- SELECT date_diff('week', DATE '1960-05-03', DATE '2001-08-22'); # differ: doris : 15086, presto : 2155
SELECT date_diff('month', DATE '1960-05-03', DATE '2001-08-22');
-- SELECT date_diff('quarter', DATE '1960-05-03', DATE '2001-08-22'); # differ: doris : 15086, presto : 165
SELECT date_diff('year', DATE '1960-05-03', DATE '2001-08-22');
-- SELECT parse_datetime('2020-08-18 03:04:05.678', 'yyyy-MM-dd HH:mm:ss.SSS'); # differ: doris : None, presto : 2020-08-18 03:04:05.678 UTC
-- SELECT parse_datetime('1960/01/22 03:04', 'yyyy/MM/dd HH:mm'); # differ: doris : 1960-01-22 03:04:00, presto : 1960-01-22 03:04:00.000 UTC
-- SELECT parse_datetime('1960/01/22 03:04 Asia/Oral', 'yyyy/MM/dd HH:mm ZZZZZ'); # differ: doris : None, presto : 1960-01-22 03:04:00.000 Asia/Oral
-- SELECT parse_datetime('1960/01/22 03:04 +0500', 'yyyy/MM/dd HH:mm Z'); # differ: doris : None, presto : 1960-01-22 03:04:00.000 +05:00
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%a'); # differ: doris : Tuesday, presto : Tue
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%b');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%c');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%d');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%e');
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%f'); # differ: doris : 000000, presto : 321000
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%H');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%h');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%I');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%i');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%j');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%k');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%l');
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%M'); # differ: doris : 04, presto : January
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%m');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%p');
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%r'); # differ: doris : 010405 PM, presto : 01:04:05 PM
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%S');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%s');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%T');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%v');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%W');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%Y');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%y');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%%');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', 'foo');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%g');
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%4'); # differ: doris : , presto : 4
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%x %v');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%Y年%m月%d日');
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%a'); # differ: doris : None, presto : Tue
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%b'); # differ: doris : None, presto : Jan
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%c'); # differ: doris : None, presto : 1
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%d'); # differ: doris : None, presto : 09
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%e'); # differ: doris : None, presto : 9
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%f'); # differ: doris : None, presto : 321000
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%H'); # differ: doris : None, presto : 13
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%h'); # differ: doris : None, presto : 01
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%I'); # differ: doris : None, presto : 01
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%i'); # differ: doris : None, presto : 04
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%j'); # differ: doris : None, presto : 009
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%k'); # differ: doris : None, presto : 13
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%l'); # differ: doris : None, presto : 1
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%M'); # differ: doris : None, presto : January
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%m'); # differ: doris : None, presto : 01
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%p'); # differ: doris : None, presto : PM
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%r'); # differ: doris : None, presto : 01:04:05 PM
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%S'); # differ: doris : None, presto : 05
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%s'); # differ: doris : None, presto : 05
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%T'); # differ: doris : None, presto : 13:04:05
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%v'); # differ: doris : None, presto : 02
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%W'); # differ: doris : None, presto : Tuesday
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%Y'); # differ: doris : None, presto : 2001
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%y'); # differ: doris : None, presto : 01
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%%'); # differ: doris : None, presto : %
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', 'foo'); # differ: doris : None, presto : foo
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%g'); # differ: doris : None, presto : g
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%4'); # differ: doris : None, presto : 4
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%x %v'); # differ: doris : None, presto : 2001 02
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%Y年%m月%d日'); # differ: doris : None, presto : 2001年01月09日
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.32', '%f'); # differ: doris : 000000, presto : 320000
SELECT date_format(TIMESTAMP '2001-01-09 00:04:05.32', '%k');
-- SELECT date_parse('2013', '%Y'); # differ: doris : 2013-01-01, presto : 2013-01-01 00:00:00.000
-- SELECT date_parse('2013-05', '%Y-%m'); # differ: doris : 2013-05-01, presto : 2013-05-01 00:00:00.000
-- SELECT date_parse('2013-05-17', '%Y-%m-%d'); # differ: doris : 2013-05-17, presto : 2013-05-17 00:00:00.000
-- SELECT date_parse('2013-05-17 12:35:10', '%Y-%m-%d %h:%i:%s'); # differ: doris : 2013-05-17 00:35:10, presto : 2013-05-17 00:35:10.000
-- SELECT date_parse('2013-05-17 12:35:10 PM', '%Y-%m-%d %h:%i:%s %p'); # differ: doris : 2013-05-17 12:35:10, presto : 2013-05-17 12:35:10.000
-- SELECT date_parse('2013-05-17 12:35:10 AM', '%Y-%m-%d %h:%i:%s %p'); # differ: doris : 2013-05-17 00:35:10, presto : 2013-05-17 00:35:10.000
-- SELECT date_parse('2013-05-17 00:35:10', '%Y-%m-%d %H:%i:%s'); # differ: doris : 2013-05-17 00:35:10, presto : 2013-05-17 00:35:10.000
-- SELECT date_parse('2013-05-17 23:35:10', '%Y-%m-%d %H:%i:%s'); # differ: doris : 2013-05-17 23:35:10, presto : 2013-05-17 23:35:10.000
-- SELECT date_parse('abc 2013-05-17 fff 23:35:10 xyz', 'abc %Y-%m-%d fff %H:%i:%s xyz'); # differ: doris : 2013-05-17 23:35:10, presto : 2013-05-17 23:35:10.000
-- SELECT date_parse('2013 14', '%Y %y'); # differ: doris : 2014-01-01, presto : 2014-01-01 00:00:00.000
-- SELECT date_parse('1998 53', '%x %v'); # differ: doris : None, presto : 1998-12-28 00:00:00.000
-- SELECT date_parse('1.1', '%s.%f'); # differ: doris : None, presto : 1970-01-01 00:00:01.100
-- SELECT date_parse('1.01', '%s.%f'); # differ: doris : None, presto : 1970-01-01 00:00:01.010
-- SELECT date_parse('1.2006', '%s.%f'); # differ: doris : None, presto : 1970-01-01 00:00:01.200
-- SELECT date_parse('59.123456789', '%s.%f'); # differ: doris : None, presto : 1970-01-01 00:00:59.123
-- SELECT date_parse('0', '%k'); # differ: doris : None, presto : 1970-01-01 00:00:00.000
-- SELECT date_parse('28-JAN-16 11.45.46.421000 PM', '%d-%b-%y %l.%i.%s.%f %p'); # differ: doris : 2016-01-28 23:45:46.421000, presto : 2016-01-28 23:45:46.421
-- SELECT date_parse('11-DEC-70 11.12.13.456000 AM', '%d-%b-%y %l.%i.%s.%f %p'); # differ: doris : 1970-12-11 11:12:13.456000, presto : 1970-12-11 11:12:13.456
-- SELECT date_parse('31-MAY-69 04.59.59.999000 AM', '%d-%b-%y %l.%i.%s.%f %p'); # differ: doris : 2069-05-31 04:59:59.999000, presto : 2069-05-31 04:59:59.999
-- SELECT parse_duration('1234 ns'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234 us'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234 ms'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234 s'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234 m'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234 h'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234 d'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234.567 ns'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234.567 ms'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234.567 s'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234.567 m'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234.567 h'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234.567 d'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234ns'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234us'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234ms'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234s'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234m'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234h'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234d'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234.567ns'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234.567ms'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234.567s'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234.567m'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234.567h'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234.567d'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT to_milliseconds(parse_duration('1ns')); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT to_milliseconds(parse_duration('1ms')); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT to_milliseconds(parse_duration('1s')); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT to_milliseconds(parse_duration('1h')); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT to_milliseconds(parse_duration('1d')); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT with_timezone(TIMESTAMP '2001-08-22 03:04:05.321', 'UTC'); # error: errCode = 2, detailMessage = Can not found function 'with_timezone'
-- SELECT with_timezone(TIMESTAMP '2001-08-22 03:04:05.321', '+13'); # error: errCode = 2, detailMessage = Can not found function 'with_timezone'
-- SELECT with_timezone(TIMESTAMP '2001-08-22 03:04:05.321', '-14'); # error: errCode = 2, detailMessage = Can not found function 'with_timezone'
-- SELECT with_timezone(TIMESTAMP '2001-08-22 03:04:05.321', '+00:45'); # error: errCode = 2, detailMessage = Can not found function 'with_timezone'
-- SELECT with_timezone(TIMESTAMP '2001-08-22 03:04:05.321', 'Asia/Shanghai'); # error: errCode = 2, detailMessage = Can not found function 'with_timezone'
-- SELECT with_timezone(TIMESTAMP '2001-08-22 03:04:05.321', 'America/New_York'); # error: errCode = 2, detailMessage = Can not found function 'with_timezone'
-- SELECT with_timezone(TIMESTAMP '2001-06-01 03:04:05.321', 'America/Los_Angeles'); # error: errCode = 2, detailMessage = Can not found function 'with_timezone'
-- SELECT with_timezone(TIMESTAMP '2001-12-01 03:04:05.321', 'America/Los_Angeles'); # error: errCode = 2, detailMessage = Can not found function 'with_timezone'
set debug_skip_fold_constant=true;
-- SELECT to_iso8601(TIMESTAMP '2001-08-22 03:04:05.321'); # error: errCode = 2, detailMessage = Can not found function 'to_iso8601'
-- SELECT from_unixtime(980172245); # differ: doris : 2001-01-22 22:04:05, presto : 2001-01-22 14:04:05.000
-- SELECT from_unixtime(980172245.888); # differ: doris : 2001-01-22 22:04:05, presto : 2001-01-22 14:04:05.888
-- SELECT from_unixtime(123456789123456789); # differ: doris : None, presto : +294247-01-10 04:00:54.775
-- SELECT from_unixtime_nanos(1234567890123456789); # error: errCode = 2, detailMessage = Can not found function 'from_unixtime_nanos'
-- SELECT from_unixtime_nanos(999999999); # error: errCode = 2, detailMessage = Can not found function 'from_unixtime_nanos'
-- SELECT from_unixtime_nanos(-1234567890123456789); # error: errCode = 2, detailMessage = Can not found function 'from_unixtime_nanos'
-- SELECT from_unixtime_nanos(-999999999); # error: errCode = 2, detailMessage = Can not found function 'from_unixtime_nanos'
-- SELECT from_unixtime_nanos(DECIMAL '1234'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT from_unixtime_nanos(DECIMAL '1234');	                                   ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT from_unixtime_nanos(DECIMAL '1234.0'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT from_unixtime_nanos(DECIMAL '1234.0');	                                   ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT from_unixtime_nanos(DECIMAL '1234.499'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT from_unixtime_nanos(DECIMAL '1234.499');	                                   ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT from_unixtime_nanos(DECIMAL '1234.500'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT from_unixtime_nanos(DECIMAL '1234.500');	                                   ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT from_unixtime_nanos(DECIMAL '-1234'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT from_unixtime_nanos(DECIMAL '-1234');	                                   ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT from_unixtime_nanos(DECIMAL '-1234.0'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT from_unixtime_nanos(DECIMAL '-1234.0');	                                   ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT from_unixtime_nanos(DECIMAL '-1234.499'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT from_unixtime_nanos(DECIMAL '-1234.499');	                                   ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT from_unixtime_nanos(DECIMAL '-1234.500'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT from_unixtime_nanos(DECIMAL '-1234.500');	                                   ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT from_unixtime_nanos(DECIMAL '12345678900123456789'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT from_unixtime_nanos(DECIMAL '12345678900123456789');	                                   ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT from_unixtime_nanos(DECIMAL '12345678900123456789.000000'); # error: errCode = 2, detailMessage = Syntax error in line 1:	...om_unixtime_nanos(DECIMAL '12345678900123456789.000000');	                             ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT from_unixtime_nanos(DECIMAL '12345678900123456789.499'); # error: errCode = 2, detailMessage = Syntax error in line 1:	...om_unixtime_nanos(DECIMAL '12345678900123456789.499');	                             ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT from_unixtime_nanos(DECIMAL '12345678900123456789.500'); # error: errCode = 2, detailMessage = Syntax error in line 1:	...om_unixtime_nanos(DECIMAL '12345678900123456789.500');	                             ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT from_unixtime_nanos(DECIMAL '-12345678900123456789'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT from_unixtime_nanos(DECIMAL '-12345678900123456789');	                                   ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT from_unixtime_nanos(DECIMAL '-12345678900123456789.000000'); # error: errCode = 2, detailMessage = Syntax error in line 1:	...om_unixtime_nanos(DECIMAL '-12345678900123456789.00000...	                             ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT from_unixtime_nanos(DECIMAL '-12345678900123456789.499'); # error: errCode = 2, detailMessage = Syntax error in line 1:	...om_unixtime_nanos(DECIMAL '-12345678900123456789.499');	                             ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT from_unixtime_nanos(DECIMAL '-12345678900123456789.500'); # error: errCode = 2, detailMessage = Syntax error in line 1:	...om_unixtime_nanos(DECIMAL '-12345678900123456789.500');	                             ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT from_unixtime(980172245, 1, 10); # error: errCode = 2, detailMessage = Can not found function 'FROM_UNIXTIME' which has 3 arity. Candidate functions are: [FROM_UNIXTIMEorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@1541cdc5, FROM_UNIXTIMEorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@24ddb1e8]
-- SELECT from_unixtime(7200, 'Asia/Shanghai'); # differ: doris : Asia/Shanghai, presto : 1970-01-01 10:00:00.000 Asia/Shanghai
-- SELECT from_unixtime(7200, 'Asia/Tokyo'); # differ: doris : Asia/Tokyo, presto : 1970-01-01 11:00:00.000 Asia/Tokyo
-- SELECT from_unixtime(7200, 'Europe/Kiev'); # differ: doris : Europe/Kiev, presto : 1970-01-01 05:00:00.000 Europe/Kiev
-- SELECT from_unixtime(7200, 'America/New_York'); # differ: doris : America/New_York, presto : 1969-12-31 21:00:00.000 America/New_York
-- SELECT from_unixtime(7200, 'America/Chicago'); # differ: doris : America/Chicago, presto : 1969-12-31 20:00:00.000 America/Chicago
-- SELECT from_unixtime(7200, 'America/Los_Angeles'); # differ: doris : America/Los_Angeles, presto : 1969-12-31 18:00:00.000 America/Los_Angeles
-- SELECT date('2001-08-22'); # differ: doris : 2001-08-22, presto : 2001-08-22
-- SELECT date(TIMESTAMP '2001-08-22 03:04:05.321 +07:09'); # differ: doris : 2001-08-22, presto : 2001-08-22
-- SELECT date(TIMESTAMP '2001-08-22 03:04:05.321'); # differ: doris : 2001-08-22, presto : 2001-08-22
-- SELECT from_iso8601_timestamp('2001-08-22T03:04:05.321-11:00'); # error: errCode = 2, detailMessage = Can not found function 'from_iso8601_timestamp'
-- SELECT from_iso8601_timestamp('2001-08-22T03:04:05.321+07:09'); # error: errCode = 2, detailMessage = Can not found function 'from_iso8601_timestamp'
-- SELECT from_iso8601_date('2001-08-22'); # error: errCode = 2, detailMessage = Can not found function 'from_iso8601_date'
-- SELECT from_iso8601_timestamp_nanos('2001-08-22T12:34:56.123456789Z'); # error: errCode = 2, detailMessage = Can not found function 'from_iso8601_timestamp_nanos'
-- SELECT from_iso8601_timestamp_nanos('2001-08-22T07:34:56.123456789-05:00'); # error: errCode = 2, detailMessage = Can not found function 'from_iso8601_timestamp_nanos'
-- SELECT from_iso8601_timestamp_nanos('2001-08-22T13:34:56.123456789+01:00'); # error: errCode = 2, detailMessage = Can not found function 'from_iso8601_timestamp_nanos'
-- SELECT from_iso8601_timestamp_nanos('2001-08-22T12:34:56.123Z'); # error: errCode = 2, detailMessage = Can not found function 'from_iso8601_timestamp_nanos'
-- SELECT to_iso8601(DATE '2001-08-22'); # error: errCode = 2, detailMessage = Can not found function 'to_iso8601'
-- SELECT current_timezone(); # error: errCode = 2, detailMessage = Can not found function 'current_timezone'
-- SELECT millisecond(INTERVAL '90061.234' SECOND); # error: errCode = 2, detailMessage = Can not found function 'millisecond'
-- SELECT second(INTERVAL '90061.234' SECOND); # error: errCode = 2, detailMessage = Invalid call to sql on unbound object
-- SELECT minute(INTERVAL '90061.234' SECOND); # error: errCode = 2, detailMessage = Invalid call to sql on unbound object
-- SELECT hour(INTERVAL '90061.234' SECOND); # error: errCode = 2, detailMessage = Invalid call to sql on unbound object
-- SELECT year_of_week(DATE '2001-08-22'); # error: errCode = 2, detailMessage = Can not found function 'year_of_week'
-- SELECT yow(DATE '2001-08-22'); # error: errCode = 2, detailMessage = Can not found function 'yow'
-- SELECT year_of_week(DATE '2005-01-02'); # error: errCode = 2, detailMessage = Can not found function 'year_of_week'
-- SELECT year_of_week(DATE '2008-12-28'); # error: errCode = 2, detailMessage = Can not found function 'year_of_week'
-- SELECT year_of_week(DATE '2008-12-29'); # error: errCode = 2, detailMessage = Can not found function 'year_of_week'
-- SELECT year_of_week(DATE '2009-12-31'); # error: errCode = 2, detailMessage = Can not found function 'year_of_week'
-- SELECT year_of_week(DATE '2010-01-03'); # error: errCode = 2, detailMessage = Can not found function 'year_of_week'
-- SELECT last_day_of_month(DATE '2001-08-22'); # differ: doris : 2001-08-31, presto : 2001-08-31
-- SELECT last_day_of_month(DATE '2019-08-01'); # differ: doris : 2019-08-31, presto : 2019-08-31
-- SELECT last_day_of_month(DATE '2019-08-31'); # differ: doris : 2019-08-31, presto : 2019-08-31
-- SELECT last_day_of_month(TIMESTAMP '2019-08-01 00:00:00.000'); # differ: doris : 2019-08-31, presto : 2019-08-31
-- SELECT last_day_of_month(TIMESTAMP '2019-08-01 17:00:00.000'); # differ: doris : 2019-08-31, presto : 2019-08-31
-- SELECT last_day_of_month(TIMESTAMP '2019-08-01 23:59:59.999'); # differ: doris : 2019-08-31, presto : 2019-08-31
-- SELECT last_day_of_month(TIMESTAMP '2019-08-31 23:59:59.999'); # differ: doris : 2019-09-30, presto : 2019-08-31
-- SELECT last_day_of_month(TIMESTAMP '2001-08-22 03:04:05.321 +07:09'); # differ: doris : 2001-08-31, presto : 2001-08-31
-- SELECT date_trunc('day', DATE'2001-08-22'); # differ: doris : 2001-08-22, presto : 2001-08-22
-- SELECT date_trunc('week', DATE'2001-08-22'); # differ: doris : 2001-08-20, presto : 2001-08-20
-- SELECT date_trunc('month', DATE'2001-08-22'); # differ: doris : 2001-08-01, presto : 2001-08-01
-- SELECT date_trunc('quarter', DATE'2001-08-22'); # differ: doris : 2001-07-01, presto : 2001-07-01
-- SELECT date_trunc('year', DATE'2001-08-22'); # differ: doris : 2001-01-01, presto : 2001-01-01
-- SELECT date_add('day', 0, DATE '2001-08-22'); # differ: doris : 2001-08-22, presto : 2001-08-22
-- SELECT date_add('day', 3, DATE '2001-08-22'); # differ: doris : 2001-08-25, presto : 2001-08-25
-- SELECT date_add('week', 3, DATE '2001-08-22'); # differ: doris : 2001-09-12, presto : 2001-09-12
-- SELECT date_add('month', 3, DATE '2001-08-22'); # differ: doris : 2001-11-22, presto : 2001-11-22
-- SELECT date_add('quarter', 3, DATE '2001-08-22'); # error: errCode = 2, detailMessage = 	no viable alternative at input 'DATE_ADD(CAST('2001-08-22' AS DATE), INTERVAL 3 QUARTER'(line 1, pos 55)	
-- SELECT date_add('year', 3, DATE '2001-08-22'); # differ: doris : 2004-08-22, presto : 2004-08-22
SELECT date_diff('day', DATE '1960-05-03', DATE '2001-08-22');
-- SELECT date_diff('week', DATE '1960-05-03', DATE '2001-08-22'); # differ: doris : 15086, presto : 2155
SELECT date_diff('month', DATE '1960-05-03', DATE '2001-08-22');
-- SELECT date_diff('quarter', DATE '1960-05-03', DATE '2001-08-22'); # differ: doris : 15086, presto : 165
SELECT date_diff('year', DATE '1960-05-03', DATE '2001-08-22');
-- SELECT parse_datetime('2020-08-18 03:04:05.678', 'yyyy-MM-dd HH:mm:ss.SSS'); # differ: doris : None, presto : 2020-08-18 03:04:05.678 UTC
-- SELECT parse_datetime('1960/01/22 03:04', 'yyyy/MM/dd HH:mm'); # differ: doris : 1960-01-22 03:04:00, presto : 1960-01-22 03:04:00.000 UTC
-- SELECT parse_datetime('1960/01/22 03:04 Asia/Oral', 'yyyy/MM/dd HH:mm ZZZZZ'); # differ: doris : None, presto : 1960-01-22 03:04:00.000 Asia/Oral
-- SELECT parse_datetime('1960/01/22 03:04 +0500', 'yyyy/MM/dd HH:mm Z'); # differ: doris : None, presto : 1960-01-22 03:04:00.000 +05:00
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%a'); # differ: doris : Tuesday, presto : Tue
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%b');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%c');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%d');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%e');
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%f'); # differ: doris : 000000, presto : 321000
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%H');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%h');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%I');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%i');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%j');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%k');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%l');
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%M'); # differ: doris : 04, presto : January
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%m');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%p');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%r');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%S');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%s');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%T');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%v');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%W');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%Y');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%y');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%%');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', 'foo');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%g');
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%4'); # differ: doris : %, presto : 4
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%x %v');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321', '%Y年%m月%d日');
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%a'); # differ: doris : Tuesday, presto : Tue
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%b');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%c');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%d');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%e');
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%f'); # differ: doris : 000000, presto : 321000
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%H');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%h');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%I');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%i');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%j');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%k');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%l');
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%M'); # differ: doris : 04, presto : January
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%m');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%p');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%r');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%S');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%s');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%T');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%v');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%W');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%Y');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%y');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%%');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', 'foo');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%g');
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%4'); # differ: doris : %, presto : 4
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%x %v');
SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.321 +07:09', '%Y年%m月%d日');
-- SELECT date_format(TIMESTAMP '2001-01-09 13:04:05.32', '%f'); # differ: doris : 000000, presto : 320000
SELECT date_format(TIMESTAMP '2001-01-09 00:04:05.32', '%k');
-- SELECT date_parse('2013', '%Y'); # differ: doris : 2013-01-01, presto : 2013-01-01 00:00:00.000
-- SELECT date_parse('2013-05', '%Y-%m'); # differ: doris : 2013-05-01, presto : 2013-05-01 00:00:00.000
-- SELECT date_parse('2013-05-17', '%Y-%m-%d'); # differ: doris : 2013-05-17, presto : 2013-05-17 00:00:00.000
-- SELECT date_parse('2013-05-17 12:35:10', '%Y-%m-%d %h:%i:%s'); # differ: doris : 2013-05-17 00:35:10, presto : 2013-05-17 00:35:10.000
-- SELECT date_parse('2013-05-17 12:35:10 PM', '%Y-%m-%d %h:%i:%s %p'); # differ: doris : 2013-05-17 12:35:10, presto : 2013-05-17 12:35:10.000
-- SELECT date_parse('2013-05-17 12:35:10 AM', '%Y-%m-%d %h:%i:%s %p'); # differ: doris : 2013-05-17 00:35:10, presto : 2013-05-17 00:35:10.000
-- SELECT date_parse('2013-05-17 00:35:10', '%Y-%m-%d %H:%i:%s'); # differ: doris : 2013-05-17 00:35:10, presto : 2013-05-17 00:35:10.000
-- SELECT date_parse('2013-05-17 23:35:10', '%Y-%m-%d %H:%i:%s'); # differ: doris : 2013-05-17 23:35:10, presto : 2013-05-17 23:35:10.000
-- SELECT date_parse('abc 2013-05-17 fff 23:35:10 xyz', 'abc %Y-%m-%d fff %H:%i:%s xyz'); # differ: doris : 2013-05-17 23:35:10, presto : 2013-05-17 23:35:10.000
-- SELECT date_parse('2013 14', '%Y %y'); # differ: doris : 2014-01-01, presto : 2014-01-01 00:00:00.000
-- SELECT date_parse('1998 53', '%x %v'); # differ: doris : None, presto : 1998-12-28 00:00:00.000
-- SELECT date_parse('1.1', '%s.%f'); # differ: doris : None, presto : 1970-01-01 00:00:01.100
-- SELECT date_parse('1.01', '%s.%f'); # differ: doris : None, presto : 1970-01-01 00:00:01.010
-- SELECT date_parse('1.2006', '%s.%f'); # differ: doris : None, presto : 1970-01-01 00:00:01.200
-- SELECT date_parse('59.123456789', '%s.%f'); # differ: doris : None, presto : 1970-01-01 00:00:59.123
-- SELECT date_parse('0', '%k'); # differ: doris : None, presto : 1970-01-01 00:00:00.000
-- SELECT date_parse('28-JAN-16 11.45.46.421000 PM', '%d-%b-%y %l.%i.%s.%f %p'); # differ: doris : 2016-01-28 23:45:46.421000, presto : 2016-01-28 23:45:46.421
-- SELECT date_parse('11-DEC-70 11.12.13.456000 AM', '%d-%b-%y %l.%i.%s.%f %p'); # differ: doris : 1970-12-11 11:12:13.456000, presto : 1970-12-11 11:12:13.456
-- SELECT date_parse('31-MAY-69 04.59.59.999000 AM', '%d-%b-%y %l.%i.%s.%f %p'); # differ: doris : 2069-05-31 04:59:59.999000, presto : 2069-05-31 04:59:59.999
-- SELECT parse_duration('1234 ns'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234 us'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234 ms'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234 s'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234 m'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234 h'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234 d'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234.567 ns'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234.567 ms'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234.567 s'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234.567 m'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234.567 h'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234.567 d'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234ns'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234us'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234ms'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234s'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234m'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234h'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234d'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234.567ns'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234.567ms'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234.567s'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234.567m'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234.567h'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT parse_duration('1234.567d'); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT to_milliseconds(parse_duration('1ns')); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT to_milliseconds(parse_duration('1ms')); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT to_milliseconds(parse_duration('1s')); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT to_milliseconds(parse_duration('1h')); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT to_milliseconds(parse_duration('1d')); # error: errCode = 2, detailMessage = Can not found function 'parse_duration'
-- SELECT with_timezone(TIMESTAMP '2001-08-22 03:04:05.321', 'UTC'); # error: errCode = 2, detailMessage = Can not found function 'with_timezone'
-- SELECT with_timezone(TIMESTAMP '2001-08-22 03:04:05.321', '+13'); # error: errCode = 2, detailMessage = Can not found function 'with_timezone'
-- SELECT with_timezone(TIMESTAMP '2001-08-22 03:04:05.321', '-14'); # error: errCode = 2, detailMessage = Can not found function 'with_timezone'
-- SELECT with_timezone(TIMESTAMP '2001-08-22 03:04:05.321', '+00:45'); # error: errCode = 2, detailMessage = Can not found function 'with_timezone'
-- SELECT with_timezone(TIMESTAMP '2001-08-22 03:04:05.321', 'Asia/Shanghai'); # error: errCode = 2, detailMessage = Can not found function 'with_timezone'
-- SELECT with_timezone(TIMESTAMP '2001-08-22 03:04:05.321', 'America/New_York'); # error: errCode = 2, detailMessage = Can not found function 'with_timezone'
-- SELECT with_timezone(TIMESTAMP '2001-06-01 03:04:05.321', 'America/Los_Angeles'); # error: errCode = 2, detailMessage = Can not found function 'with_timezone'
-- SELECT with_timezone(TIMESTAMP '2001-12-01 03:04:05.321', 'America/Los_Angeles'); # error: errCode = 2, detailMessage = Can not found function 'with_timezone'
