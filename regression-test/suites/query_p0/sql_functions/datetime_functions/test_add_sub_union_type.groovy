// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite('test_add_sub_union_type') {
	sql """ set time_zone = '+08:00'; """

	sql """drop table if exists test_add_sub_union_type"""

	sql """
		create table test_add_sub_union_type (
			dt datetimev2(6),
			iv_year_month string,
			iv_day_hour string,
			iv_day_minute string,
			iv_day_second string,
			iv_day_microsecond string,
			iv_hour_minute string,
			iv_hour_second string,
			iv_hour_microsecond string,
			iv_minute_second string,
			iv_minute_microsecond string,
			iv_second_microsecond string
		) duplicate key(dt)
		distributed by hash(dt) buckets 1
		properties('replication_num'='1');
	"""

	sql """
		insert into test_add_sub_union_type values
		('2023-01-02 03:04:05.123456', '1-2', '1 2', '1 02:03', '1 02:03:04',
		 '1 02:03:04.123456', '2:30', '2:30:40', '2:30:40.123456', '45:50',
		 '45:50.123456', '59.654321'),
		('2024-02-29 10:00:00.000000', '-0-2', '-1 0', '-1 00:01', '-1 00:00:02',
		 '-1 00:00:03.000001', '-0:45', '-0:45:10', '-0:45:10.000010', '-10:20',
		 '-10:20.000020', '-5.000005');
	"""

	// YEAR_MONTH add/sub
	qt_year_month_add_1 """select date_add('2023-01-02', interval '1-2' year_month) AS year_month"""
	qt_year_month_add_2 """select date_add('2023-01-02 03:04:05.123456', interval '-0-3' year_month) AS year_month"""
	qt_year_month_add_3 """select date_add(dt, interval iv_year_month year_month) AS year_month from test_add_sub_union_type where dt='2023-01-02 03:04:05.123456'"""
	qt_year_month_add_4 """select date_add(dt, interval iv_year_month year_month) AS year_month from test_add_sub_union_type where dt='2024-02-29 10:00:00.000000'"""
	qt_year_month_add_5 """select date_add('2023-01-31', interval '1-01' year_month) AS year_month"""
	qt_year_month_add_6 """select date_add('2023-02-28 12:00:00.000000+08:00', interval '1-01' year_month) AS year_month"""
	qt_year_month_add_7 """select date_add('2023-03-31 12:00:00.000000+00:00', interval '1-01' year_month) AS year_month"""
	qt_year_month_add_8 """select date_add('2023-04-30 12:00:00.000000-05:30', interval '1-01' year_month) AS year_month"""
	qt_year_month_add_9 """select date_add('2023-01-02', interval '3&4' year_month) AS year_month"""
	qt_year_month_add_10 """select date_add('2023-01-02', interval '1--2' year_month) AS year_month"""
	qt_year_month_add_11 """select date_add('2023-01-02', interval '5' year_month) AS year_month"""
	qt_year_month_add_12 """select date_add('2023-01-02', interval '1234-11' year_month) AS year_month"""
	qt_year_month_add_13 """select date_add('2021-1-31', interval '1-1' year_month) AS year_month"""
	qt_year_month_add_14 """select date_add('2023-1-31', interval '1-1' year_month) AS year_month"""
	test {
		sql """select date_add('2023-01-02', interval '1-2-3' year_month) AS year_month"""
		exception "Operation year_month_add of 1-2-3 is invalid"
	}
	testFoldConst("select date_add('2023-01-02', interval '3&4' year_month) AS year_month")
	testFoldConst("select date_add('2023-01-02', interval '1--2' year_month) AS year_month")
	testFoldConst("select date_add('2023-01-02', interval '5' year_month) AS year_month")
	testFoldConst("select date_add('2023-01-02', interval '1234-11' year_month) AS year_month")
	testFoldConst("select date_add('2023-05-31', interval '1-01' year_month) AS year_month")
	testFoldConst("select date_add('2023-06-30 12:00:00.000000+08:00', interval '1-01' year_month) AS year_month")
	testFoldConst("select date_add('2023-07-31 12:00:00.000000+00:00', interval '1-01' year_month) AS year_month")
	testFoldConst("select date_add('2023-08-31 12:00:00.000000-05:30', interval '1-01' year_month) AS year_month")
	testFoldConst("select date_add('2023-01-02 03:04:05.123456', interval '2-11' year_month) AS year_month")
	testFoldConst("select date_add('2024-02-29 23:59:59', interval '-0-0' year_month) AS year_month")
	testFoldConst("select date_add('2021-1-31', interval '1-1' year_month) AS year_month")
	testFoldConst("select date_add('2023-1-31', interval '1-1' year_month) AS year_month")

	qt_year_month_sub_1 """select date_sub('2023-01-02', interval '1-2' year_month) AS year_month"""
	qt_year_month_sub_2 """select date_sub(dt, interval iv_year_month year_month) AS year_month from test_add_sub_union_type where dt='2023-01-02 03:04:05.123456'"""
	qt_year_month_sub_3 """select date_sub('2023-03-31', interval '0-11' year_month) AS year_month"""
	qt_year_month_sub_4 """select date_sub('2023-04-30 12:00:00.000000+08:00', interval '0-11' year_month) AS year_month"""
	qt_year_month_sub_5 """select date_sub('2023-05-31 12:00:00.000000+00:00', interval '0-11' year_month) AS year_month"""
	qt_year_month_sub_6 """select date_sub('2023-06-30 12:00:00.000000-05:30', interval '0-11' year_month) AS year_month"""
	qt_year_month_sub_7 """select date_sub('2023-01-02', interval '5@6' year_month) AS year_month"""
	testFoldConst("select date_sub('2023-07-31 23:59:59', interval '0-6' year_month) AS year_month")
	testFoldConst("select date_sub('2023-08-31 12:00:00.000000+08:00', interval '0-11' year_month) AS year_month")
	testFoldConst("select date_sub('2023-09-30 12:00:00.000000+00:00', interval '0-11' year_month) AS year_month")
	testFoldConst("select date_sub('2023-10-31 12:00:00.000000-05:30', interval '0-11' year_month) AS year_month")
	testFoldConst("select date_sub('0001-01-01 00:00:00', interval '-10-00' year_month) AS year_month")

	// YEAR_MONTH extract
	qt_year_month_extract_1 """select extract(year_month from dt) from test_add_sub_union_type order by dt"""
	qt_year_month_extract_2 """select extract(year_month from '2023-01-02')"""
	qt_year_month_extract_3 """select extract(year_month from '2023-01-02 03:04:05.123456')"""
	qt_year_month_extract_4 """select extract(year_month from '2023-01-01 01:04:05.123456+12:00')"""
	qt_year_month_extract_5 """select extract(year_month from '2023-01-02 03:04:05.123456-05:30')"""
	testFoldConst("select extract(year_month from '2023-01-02')")
	testFoldConst("select extract(year_month from '2023-01-02 03:04:05.123456')")
	testFoldConst("select extract(year_month from '2023-01-01 01:04:05.123456+12:00')")
	testFoldConst("select extract(year_month from '2023-01-02 03:04:05.123456-05:30')")

	// DAY_HOUR add/sub
	qt_day_hour_add_1 """select date_add('2023-01-02 03:04:05', interval '1 2' day_hour) AS day_hour"""
	qt_day_hour_add_2 """select date_add('2023-01-02 03:04:05.123456', interval '-2 03' day_hour) AS day_hour"""
	qt_day_hour_add_3 """select date_add(dt, interval iv_day_hour day_hour) AS day_hour from test_add_sub_union_type where dt='2023-01-02 03:04:05.123456'"""
	qt_day_hour_add_4 """select date_add(dt, interval iv_day_hour day_hour) AS day_hour from test_add_sub_union_type where dt='2024-02-29 10:00:00.000000'"""
	qt_day_hour_add_5 """select date_add('2023-01-02 03:04:05', interval '7&8' day_hour) AS day_hour"""
	qt_day_hour_add_6 """select date_add('2023-01-02 03:04:05.123456', interval '1@@2' day_hour) AS day_hour"""
	qt_day_hour_add_7 """select date_add('2023-01-02 03:04:05.123456', interval '12' day_hour) AS day_hour"""
	qt_day_hour_add_8 """select date_add('2023-01-02 03:04:05.123456', interval '999 23' day_hour) AS day_hour"""
	qt_day_hour_add_9 """select date_add('2023-02-28', interval '1 10' day_hour) AS day_hour"""
	qt_day_hour_add_10 """select date_add('2023-03-31 01:00:00+08:00', interval '1 10' day_hour) AS day_hour"""
	qt_day_hour_add_11 """select date_add('2023-04-30 01:00:00+00:00', interval '1 10' day_hour) AS day_hour"""
	qt_day_hour_add_12 """select date_add('2023-05-31 01:00:00-05:30', interval '1 10' day_hour) AS day_hour"""
	testFoldConst("select date_add('2023-01-02 03:04:05', interval '7&8' day_hour) AS day_hour")
	testFoldConst("select date_add('2023-01-02 03:04:05.123456', interval '1@@2' day_hour) AS day_hour")
	testFoldConst("select date_add('2023-01-02 03:04:05.123456', interval '12' day_hour) AS day_hour")
	testFoldConst("select date_add('2023-01-02 03:04:05.123456', interval '999 23' day_hour) AS day_hour")
	testFoldConst("select date_add('2023-06-30 23:00:00', interval '2 10' day_hour) AS day_hour")
	testFoldConst("select date_add('2023-07-31 23:00:00', interval '-0 00' day_hour) AS day_hour")
	testFoldConst("select date_add('2023-08-31', interval '1 10' day_hour) AS day_hour")
	testFoldConst("select date_add('2023-09-30 01:00:00+08:00', interval '1 10' day_hour) AS day_hour")
	testFoldConst("select date_add('2023-10-31 01:00:00+00:00', interval '1 10' day_hour) AS day_hour")
	testFoldConst("select date_add('2023-11-30 01:00:00-05:30', interval '1 10' day_hour) AS day_hour")
	test {
		sql """select date_add('2023-01-02 03:04:05', interval '1 2 3 4' day_hour) AS day_hour"""
		exception "Operation day_hour_add of"
	}

	qt_day_hour_sub_1 """select date_sub('2023-01-02 03:04:05', interval '1 2' day_hour) AS day_hour"""
	qt_day_hour_sub_2 """select date_sub(dt, interval iv_day_hour day_hour) AS day_hour from test_add_sub_union_type where dt='2023-01-02 03:04:05.123456'"""
	qt_day_hour_sub_3 """select date_sub('2023-01-02 03:04:05', interval '9@10' day_hour) AS day_hour"""
	qt_day_hour_sub_4 """select date_sub('2023-11-30', interval '1 10' day_hour) AS day_hour"""
	qt_day_hour_sub_5 """select date_sub('2023-10-31 01:00:00+08:00', interval '1 10' day_hour) AS day_hour"""
	qt_day_hour_sub_6 """select date_sub('2023-09-30 01:00:00+00:00', interval '1 10' day_hour) AS day_hour"""
	qt_day_hour_sub_7 """select date_sub('2023-08-31 01:00:00-05:30', interval '1 10' day_hour) AS day_hour"""
	testFoldConst("select date_sub('2023-01-02 03:04:05', interval '9@10' day_hour) AS day_hour")
	testFoldConst("select date_sub('2023-06-30 23:00:00', interval '2 10' day_hour) AS day_hour")
	testFoldConst("select date_sub('2023-07-31 23:00:00', interval '000 000' day_hour) AS day_hour")
	testFoldConst("select date_sub('2023-08-31', interval '1 10' day_hour) AS day_hour")
		testFoldConst("select date_sub('2022-12-31 01:00:00+08:00', interval '1 10' day_hour) AS day_hour")
		testFoldConst("select date_sub('2022-12-31 01:00:00+00:00', interval '1 10' day_hour) AS day_hour")
testFoldConst("select date_sub('2022-12-31 01:00:00-05:30', interval '1 10' day_hour) AS day_hour")

	// DAY_HOUR extract
	qt_day_hour_extract_1 """select extract(day_hour from dt) from test_add_sub_union_type order by dt"""
	qt_day_hour_extract_2 """select extract(day_hour from '2023-01-02')"""
	qt_day_hour_extract_3 """select extract(day_hour from '2023-01-02 03:04:05.123456')"""
	qt_day_hour_extract_4 """select extract(day_hour from '2023-01-02 03:04:05.123456+08:00')"""
	qt_day_hour_extract_5 """select extract(day_hour from '2023-01-02 03:04:05.123456-05:30')"""
	testFoldConst("select extract(day_hour from '2023-01-02')")
	testFoldConst("select extract(day_hour from '2023-01-02 03:04:05.123456')")
	testFoldConst("select extract(day_hour from '2023-01-02 03:04:05.123456+08:00')")
	testFoldConst("select extract(day_hour from '2023-01-02 03:04:05.123456-05:30')")

	// DAY_MINUTE add/sub
	qt_day_minute_add_1 """select date_add('2023-01-02 03:04:05', interval '1 02:03' day_minute) AS day_minute"""
	qt_day_minute_add_2 """select date_add('2023-01-02 03:04:05.123456', interval '-2 03:04' day_minute) AS day_minute"""
	qt_day_minute_add_3 """select date_add(dt, interval iv_day_minute day_minute) AS day_minute from test_add_sub_union_type where dt='2023-01-02 03:04:05.123456'"""
	qt_day_minute_add_4 """select date_add(dt, interval iv_day_minute day_minute) AS day_minute from test_add_sub_union_type where dt='2024-02-29 10:00:00.000000'"""
	qt_day_minute_add_5 """select date_add('2023-01-02 03:04:05', interval '3&04:05' day_minute) AS day_minute"""
	qt_day_minute_add_6 """select date_add('2023-01-02 03:04:05.123456', interval '1::02**03' day_minute) AS day_minute"""
	qt_day_minute_add_7 """select date_add('2023-01-02 03:04:05.123456', interval '30' day_minute) AS day_minute"""
	qt_day_minute_add_8 """select date_add('2023-01-02 03:04:05.123456', interval '999 23:59' day_minute) AS day_minute"""
	qt_day_minute_add_9 """select date_add('2023-02-28', interval '1 10:59' day_minute) AS day_minute"""
	qt_day_minute_add_10 """select date_add('2023-03-31 01:00:00+08:00', interval '1 10:59' day_minute) AS day_minute"""
	qt_day_minute_add_11 """select date_add('2023-04-30 01:00:00+00:00', interval '1 10:59' day_minute) AS day_minute"""
	qt_day_minute_add_12 """select date_add('2023-05-31 01:00:00-05:30', interval '1 10:59' day_minute) AS day_minute"""
	testFoldConst("select date_add('2023-01-02 03:04:05', interval '3&04:05' day_minute) AS day_minute")
	testFoldConst("select date_add('2023-01-02 03:04:05.123456', interval '1::02**03' day_minute) AS day_minute")
	testFoldConst("select date_add('2023-01-02 03:04:05.123456', interval '30' day_minute) AS day_minute")
	testFoldConst("select date_add('2023-01-02 03:04:05.123456', interval '999 23:59' day_minute) AS day_minute")
	testFoldConst("select date_add('2023-06-30 23:59:59', interval '0 10:59' day_minute) AS day_minute")
	testFoldConst("select date_add('2023-07-31 23:59:59', interval '-1 00:00' day_minute) AS day_minute")
	testFoldConst("select date_add('2023-08-31', interval '1 10:59' day_minute) AS day_minute")
	testFoldConst("select date_add('2023-09-30 01:00:00+08:00', interval '1 10:59' day_minute) AS day_minute")
	testFoldConst("select date_add('2023-10-31 01:00:00+00:00', interval '1 10:59' day_minute) AS day_minute")
	testFoldConst("select date_add('2023-11-30 01:00:00-05:30', interval '1 10:59' day_minute) AS day_minute")
	test {
		sql """select date_add('2023-01-02 03:04:05', interval '1 2 3 4 5' day_minute) AS day_minute"""
		exception "Operation day_minute_add of"
	}

	qt_day_minute_sub_1 """select date_sub('2023-01-02 03:04:05', interval '1 02:03' day_minute) AS day_minute"""
	qt_day_minute_sub_2 """select date_sub(dt, interval iv_day_minute day_minute) AS day_minute from test_add_sub_union_type where dt='2023-01-02 03:04:05.123456'"""
	qt_day_minute_sub_3 """select date_sub('2023-01-02 03:04:05', interval '5@06:07' day_minute) AS day_minute"""
	qt_day_minute_sub_4 """select date_sub('2023-11-30', interval '1 10:59' day_minute) AS day_minute"""
	qt_day_minute_sub_5 """select date_sub('2023-10-31 01:00:00+08:00', interval '1 10:59' day_minute) AS day_minute"""
	qt_day_minute_sub_6 """select date_sub('2023-09-30 01:00:00+00:00', interval '1 10:59' day_minute) AS day_minute"""
	qt_day_minute_sub_7 """select date_sub('2023-08-31 01:00:00-05:30', interval '1 10:59' day_minute) AS day_minute"""
	testFoldConst("select date_sub('2023-01-02 03:04:05', interval '5@06:07' day_minute) AS day_minute")
	testFoldConst("select date_sub('2023-06-30 23:59:59', interval '0 10:59' day_minute) AS day_minute")
	testFoldConst("select date_sub('2023-07-31 23:59:59', interval '000 00:00' day_minute) AS day_minute")
	testFoldConst("select date_sub('2023-08-31', interval '1 10:59' day_minute) AS day_minute")
	testFoldConst("select date_sub('2025-04-30 01:00:00+08:00', interval '1 10:59' day_minute) AS day_minute")
	testFoldConst("select date_sub('2025-05-31 01:00:00+00:00', interval '1 10:59' day_minute) AS day_minute")
testFoldConst("select date_sub('2025-06-30 01:00:00-05:30', interval '1 10:59' day_minute) AS day_minute")

	// DAY_MINUTE extract
	qt_day_minute_extract_1 """select extract(day_minute from dt) from test_add_sub_union_type order by dt"""
	qt_day_minute_extract_2 """select extract(day_minute from '2023-01-02')"""
	qt_day_minute_extract_3 """select extract(day_minute from '2023-01-02 03:04:05.123456')"""
	qt_day_minute_extract_4 """select extract(day_minute from '2023-01-02 03:04:05.123456+08:00')"""
	qt_day_minute_extract_5 """select extract(day_minute from '2023-01-02 03:04:05.123456-05:30')"""
	testFoldConst("select extract(day_minute from '2023-01-02')")
	testFoldConst("select extract(day_minute from '2023-01-02 03:04:05.123456')")
	testFoldConst("select extract(day_minute from '2023-01-02 03:04:05.123456+08:00')")
	testFoldConst("select extract(day_minute from '2023-01-02 03:04:05.123456-05:30')")

	// DAY_SECOND add/sub
	qt_day_second_add_1 """select date_add('2023-01-02 03:04:05', interval '1 02:03:04' day_second) AS day_second"""
	qt_day_second_add_2 """select date_add('2023-01-02 03:04:05.123456', interval '-2 03:04:05' day_second) AS day_second"""
	qt_day_second_add_3 """select date_add(dt, interval iv_day_second day_second) AS day_second from test_add_sub_union_type where dt='2023-01-02 03:04:05.123456'"""
	qt_day_second_add_4 """select date_add(dt, interval iv_day_second day_second) AS day_second from test_add_sub_union_type where dt='2024-02-29 10:00:00.000000'"""
	qt_day_second_add_5 """select date_add(dt, interval c day_second) AS day_second from (select dt, '0 0:0:7' c from test_add_sub_union_type limit 1) t"""
	qt_day_second_add_6 """select date_add('2023-01-02 03:04:05', interval '7&8:9' day_second) AS day_second"""
	qt_day_second_add_7 """select date_add('2023-01-02 03:04:05.123456', interval '1::02**03::04' day_second) AS day_second"""
	qt_day_second_add_8 """select date_add('2023-01-02 03:04:05.123456', interval '2:3:4' day_second) AS day_second"""
	qt_day_second_add_9 """select date_add('2023-01-02 03:04:05.123456', interval '999 23:59:59' day_second) AS day_second"""
	qt_day_second_add_10 """select date_add('2021-12-31', interval '1 10:20:30' day_second) AS day_second"""
	qt_day_second_add_11 """select date_add('2021-11-30 01:00:00+08:00', interval '1 10:20:30' day_second) AS day_second"""
	qt_day_second_add_12 """select date_add('2021-10-31 01:00:00+00:00', interval '1 10:20:30' day_second) AS day_second"""
	qt_day_second_add_13 """select date_add('2021-09-30 01:00:00-05:30', interval '1 10:20:30' day_second) AS day_second"""
	testFoldConst("select date_add('2023-01-02 03:04:05', interval '7&8:9' day_second) AS day_second")
	testFoldConst("select date_add('2023-01-02 03:04:05.123456', interval '1::02**03::04' day_second) AS day_second")
	testFoldConst("select date_add('2023-01-02 03:04:05.123456', interval '2:3:4' day_second) AS day_second")
	testFoldConst("select date_add('2023-01-02 03:04:05.123456', interval '999 23:59:59' day_second) AS day_second")
	testFoldConst("select date_add('2021-11-30 23:59:50', interval '0 00:00:15' day_second) AS day_second")
	testFoldConst("select date_add('2021-10-31 23:59:50', interval '-0 00:00:00' day_second) AS day_second")
	testFoldConst("select date_add('2020-02-29', interval '1 10:20:30' day_second) AS day_second")
	testFoldConst("select date_add('2016-02-29 01:00:00+08:00', interval '1 10:20:30' day_second) AS day_second")
	testFoldConst("select date_add('2024-01-02 01:00:00+00:00', interval '1 10:20:30' day_second) AS day_second")
	testFoldConst("select date_add('2024-03-31 01:00:00-05:30', interval '1 10:20:30' day_second) AS day_second")
	test {
		sql """select date_add('2023-01-02 03:04:05', interval '1 2 3 4 5 6' day_second) AS day_second"""
		exception "Operation day_second_add of"
	}

	qt_day_second_sub_1 """select date_sub('2023-01-02 03:04:05', interval '1 02:03:04' day_second) AS day_second"""
	qt_day_second_sub_2 """select date_sub(dt, interval iv_day_second day_second) AS day_second from test_add_sub_union_type where dt='2023-01-02 03:04:05.123456'"""
	qt_day_second_sub_3 """select date_sub('2023-01-02 03:04:05', interval '5@06:07' day_second) AS day_second"""
	qt_day_second_sub_4 """select date_sub('2019-02-28', interval '1 10:20:30' day_second) AS day_second"""
	qt_day_second_sub_5 """select date_sub('2019-01-31 01:00:00+08:00', interval '1 10:20:30' day_second) AS day_second"""
	qt_day_second_sub_6 """select date_sub('2019-03-31 01:00:00+00:00', interval '1 10:20:30' day_second) AS day_second"""
	qt_day_second_sub_7 """select date_sub('2019-04-30 01:00:00-05:30', interval '1 10:20:30' day_second) AS day_second"""
	testFoldConst("select date_sub('2023-01-02 03:04:05', interval '5@06:07' day_second) AS day_second")
	testFoldConst("select date_sub('2018-12-31 23:59:50', interval '0 00:00:15' day_second) AS day_second")
	testFoldConst("select date_sub('2018-11-30 23:59:50', interval '10 10:10:10' day_second) AS day_second")
	testFoldConst("select date_sub('2022-02-28', interval '1 10:20:30' day_second) AS day_second")
	testFoldConst("select date_sub('2022-03-31 01:00:00+08:00', interval '1 10:20:30' day_second) AS day_second")
	testFoldConst("select date_sub('2022-04-30 01:00:00+00:00', interval '1 10:20:30' day_second) AS day_second")
	testFoldConst("select date_sub('2022-05-31 01:00:00-05:30', interval '1 10:20:30' day_second) AS day_second")

	// DAY_SECOND extract
	qt_day_second_extract_1 """select extract(day_second from dt) from test_add_sub_union_type order by dt"""
	qt_day_second_extract_2 """select extract(day_second from '2023-01-02')"""
	qt_day_second_extract_3 """select extract(day_second from '2023-01-02 03:04:05.123456')"""
	qt_day_second_extract_4 """select extract(day_second from '2023-01-02 03:04:05.123456+08:00')"""
	qt_day_second_extract_5 """select extract(day_second from '2023-01-02 03:04:05.123456-05:30')"""
	testFoldConst("select extract(day_second from '2023-01-02')")
	testFoldConst("select extract(day_second from '2023-01-02 03:04:05.123456')")
	testFoldConst("select extract(day_second from '2023-01-02 03:04:05.123456+08:00')")
	testFoldConst("select extract(day_second from '2023-01-02 03:04:05.123456-05:30')")

	// DAY_MICROSECOND add/sub
	qt_day_microsecond_add_1 """select date_add('2023-01-02 03:04:05.123456', interval '1 02:03:04.567890' day_microsecond) AS day_microsecond"""
	qt_day_microsecond_add_2 """select date_add('2023-01-02 03:04:05.123456', interval '-0 00:00:00.000001' day_microsecond) AS day_microsecond"""
	qt_day_microsecond_add_3 """select date_add(dt, interval iv_day_microsecond day_microsecond) AS day_microsecond from test_add_sub_union_type where dt='2023-01-02 03:04:05.123456'"""
	qt_day_microsecond_add_4 """select date_add(dt, interval iv_day_microsecond day_microsecond) AS day_microsecond from test_add_sub_union_type where dt='2024-02-29 10:00:00.000000'"""
	qt_day_microsecond_add_5 """select date_add('2023-01-02 03:04:05.123456', interval '7&8:9!?6.123' day_microsecond) AS day_microsecond"""
	qt_day_microsecond_add_6 """select date_add('2023-01-02 03:04:05.123456', interval '1!2@3#4_5^' day_microsecond) AS day_microsecond"""
	qt_day_microsecond_add_7 """select date_add('2023-01-02 03:04:05.123456', interval '1::02**03::04..5' day_microsecond) AS day_microsecond"""
	qt_day_microsecond_add_8 """select date_add('2023-01-02 03:04:05.123456', interval '1 2:3:4' day_microsecond) AS day_microsecond"""
	qt_day_microsecond_add_9 """select date_add('2023-01-02 03:04:05.123456', interval '999 23:59:59.999999' day_microsecond) AS day_microsecond"""
	qt_day_microsecond_add_10 """select date_add('2024-07-31', interval '0 10:20:30.123456' day_microsecond) AS day_microsecond"""
	qt_day_microsecond_add_11 """select date_add('2024-08-31 01:00:00+08:00', interval '0 10:20:30.123456' day_microsecond) AS day_microsecond"""
	qt_day_microsecond_add_12 """select date_add('2024-09-30 01:00:00+00:00', interval '0 10:20:30.123456' day_microsecond) AS day_microsecond"""
	qt_day_microsecond_add_13 """select date_add('2024-10-31 01:00:00-05:30', interval '0 10:20:30.123456' day_microsecond) AS day_microsecond"""
	testFoldConst("select date_add('2023-01-02 03:04:05.123456', interval '7&8:9!?6.123' day_microsecond) AS day_microsecond")
	testFoldConst("select date_add('2023-01-02 03:04:05.123456', interval '1!2@3#4_5^' day_microsecond) AS day_microsecond")
	testFoldConst("select date_add('2023-01-02 03:04:05.123456', interval '1::02**03::04..5' day_microsecond) AS day_microsecond")
	testFoldConst("select date_add('2023-01-02 03:04:05.123456', interval '1 2:3:4' day_microsecond) AS day_microsecond")
	testFoldConst("select date_add('2023-01-02 03:04:05.123456', interval '999 23:59:59.999999' day_microsecond) AS day_microsecond")
	testFoldConst("select date_add('2000-02-29 23:59:59.999999', interval '0 00:00:00.000005' day_microsecond) AS day_microsecond")
	testFoldConst("select date_add('2004-02-29 23:59:59.123456', interval '-0 00:00:00.000010' day_microsecond) AS day_microsecond")
	testFoldConst("select date_add('2025-12-31', interval '0 10:20:30.123456' day_microsecond) AS day_microsecond")
	testFoldConst("select date_add('2025-11-30 01:00:00+08:00', interval '0 10:20:30.123456' day_microsecond) AS day_microsecond")
	testFoldConst("select date_add('2026-01-01 01:00:00+00:00', interval '0 10:20:30.123456' day_microsecond) AS day_microsecond")
	testFoldConst("select date_add('2015-06-30 01:00:00-05:30', interval '0 10:20:30.123456' day_microsecond) AS day_microsecond")
	test {
		sql """select date_add('2023-01-02 03:04:05.123456', interval '1 2 3 4 5 6 7' day_microsecond) AS day_microsecond"""
		exception "Operation day_microsecond_add of"
	}

	qt_day_microsecond_sub_1 """select date_sub('2023-01-02 03:04:05.123456', interval '1 02:03:04.567890' day_microsecond) AS day_microsecond"""
	qt_day_microsecond_sub_2 """select date_sub(dt, interval iv_day_microsecond day_microsecond) AS day_microsecond from test_add_sub_union_type where dt='2023-01-02 03:04:05.123456'"""
	qt_day_microsecond_sub_3 """select date_sub('2023-01-02 03:04:05.123456', interval '6!5@4#3_2^!' day_microsecond) AS day_microsecond"""
	qt_day_microsecond_sub_4 """select date_sub('2017-02-28', interval '0 10:20:30.123456' day_microsecond) AS day_microsecond"""
	qt_day_microsecond_sub_5 """select date_sub('2017-03-31 01:00:00+08:00', interval '0 10:20:30.123456' day_microsecond) AS day_microsecond"""
	qt_day_microsecond_sub_6 """select date_sub('2017-04-30 01:00:00+00:00', interval '0 10:20:30.123456' day_microsecond) AS day_microsecond"""
	qt_day_microsecond_sub_7 """select date_sub('2017-05-31 01:00:00-05:30', interval '0 10:20:30.123456' day_microsecond) AS day_microsecond"""
	testFoldConst("select date_sub('2023-01-02 03:04:05.123456', interval '6!5@4#3_2^!' day_microsecond) AS day_microsecond")
	testFoldConst("select date_sub('2008-02-29 23:59:59.999999', interval '0 00:00:00.000005' day_microsecond) AS day_microsecond")
	testFoldConst("select date_sub('0001-01-01 00:00:00.000001', interval '0 00:00:00.000001' day_microsecond) AS day_microsecond")
	testFoldConst("select date_sub('2016-03-31', interval '0 10:20:30.123456' day_microsecond) AS day_microsecond")
	testFoldConst("select date_sub('2016-04-30 01:00:00+08:00', interval '0 10:20:30.123456' day_microsecond) AS day_microsecond")
	testFoldConst("select date_sub('2016-05-31 01:00:00+00:00', interval '0 10:20:30.123456' day_microsecond) AS day_microsecond")
	testFoldConst("select date_sub('2016-06-30 01:00:00-05:30', interval '0 10:20:30.123456' day_microsecond) AS day_microsecond")

	// DAY_MICROSECOND extract
	qt_day_microsecond_extract_1 """select extract(day_microsecond from dt) from test_add_sub_union_type order by dt"""
	qt_day_microsecond_extract_2 """select extract(day_microsecond from '2023-01-02')"""
	qt_day_microsecond_extract_3 """select extract(day_microsecond from '2023-01-02 03:04:05.123456')"""
	qt_day_microsecond_extract_4 """select extract(day_microsecond from '2023-01-02 03:04:05.123456+08:00')"""
	qt_day_microsecond_extract_5 """select extract(day_microsecond from '2023-01-02 03:04:05.123456-05:30')"""
	testFoldConst("select extract(day_microsecond from '2023-01-02')")
	testFoldConst("select extract(day_microsecond from '2023-01-02 03:04:05.123456')")
	testFoldConst("select extract(day_microsecond from '2023-01-02 03:04:05.123456+08:00')")
	testFoldConst("select extract(day_microsecond from '2023-01-02 03:04:05.123456-05:30')")

	// HOUR_MINUTE add/sub
	qt_hour_minute_add_1 """select date_add('2023-01-02 03:04:05', interval '2:30' hour_minute) AS hour_minute"""
	qt_hour_minute_add_2 """select date_add('2023-01-02 03:04:05.123456', interval '-0:45' hour_minute) AS hour_minute"""
	qt_hour_minute_add_3 """select date_add(dt, interval iv_hour_minute hour_minute) AS hour_minute from test_add_sub_union_type where dt='2023-01-02 03:04:05.123456'"""
	qt_hour_minute_add_4 """select date_add(dt, interval iv_hour_minute hour_minute) AS hour_minute from test_add_sub_union_type where dt='2024-02-29 10:00:00.000000'"""
	qt_hour_minute_add_5 """select date_add('2023-01-02 03:04:05', interval '3&45' hour_minute) AS hour_minute"""
	qt_hour_minute_add_6 """select date_add('2023-01-02 03:04:05.123456', interval '1::2' hour_minute) AS hour_minute"""
	qt_hour_minute_add_7 """select date_add('2023-01-02 03:04:05.123456', interval '5' hour_minute) AS hour_minute"""
	qt_hour_minute_add_8 """select date_add('2023-01-02 03:04:05.123456', interval '1000:59' hour_minute) AS hour_minute"""
	qt_hour_minute_add_9 """select date_add('2018-01-31', interval '10:59' hour_minute) AS hour_minute"""
	qt_hour_minute_add_10 """select date_add('2018-02-28 01:00:00+08:00', interval '10:59' hour_minute) AS hour_minute"""
	qt_hour_minute_add_11 """select date_add('2018-03-31 01:00:00+00:00', interval '10:59' hour_minute) AS hour_minute"""
	qt_hour_minute_add_12 """select date_add('2018-04-30 01:00:00-05:30', interval '10:59' hour_minute) AS hour_minute"""
	testFoldConst("select date_add('2023-01-02 03:04:05', interval '3&45' hour_minute) AS hour_minute")
	testFoldConst("select date_add('2023-01-02 03:04:05.123456', interval '1::2' hour_minute) AS hour_minute")
	testFoldConst("select date_add('2023-01-02 03:04:05.123456', interval '5' hour_minute) AS hour_minute")
	testFoldConst("select date_add('2023-01-02 03:04:05.123456', interval '1000:59' hour_minute) AS hour_minute")
	testFoldConst("select date_add('2019-05-31 23:59:59', interval '10:59' hour_minute) AS hour_minute")
	testFoldConst("select date_add('2019-06-30 23:59:59', interval '-00:00' hour_minute) AS hour_minute")
	testFoldConst("select date_add('2019-07-31', interval '10:59' hour_minute) AS hour_minute")
	testFoldConst("select date_add('2019-12-01 01:00:00+08:00', interval '10:59' hour_minute) AS hour_minute")
	testFoldConst("select date_add('2020-02-29 01:00:00+00:00', interval '10:59' hour_minute) AS hour_minute")
	testFoldConst("select date_add('2020-03-31 01:00:00-05:30', interval '10:59' hour_minute) AS hour_minute")
	test {
		sql """select date_add('2023-01-02 03:04:05', interval '1:2:3' hour_minute) AS hour_minute"""
		exception "Operation hour_minute_add of"
	}

	qt_hour_minute_sub_1 """select date_sub('2023-01-02 03:04:05', interval '2:30' hour_minute) AS hour_minute"""
	qt_hour_minute_sub_2 """select date_sub(dt, interval iv_hour_minute hour_minute) AS hour_minute from test_add_sub_union_type where dt='2023-01-02 03:04:05.123456'"""
	qt_hour_minute_sub_3 """select date_sub('2023-01-02 03:04:05', interval '6@07' hour_minute) AS hour_minute"""
	qt_hour_minute_sub_4 """select date_sub('2018-08-31', interval '10:59' hour_minute) AS hour_minute"""
	qt_hour_minute_sub_5 """select date_sub('2018-09-30 01:00:00+08:00', interval '10:59' hour_minute) AS hour_minute"""
	qt_hour_minute_sub_6 """select date_sub('2018-10-31 01:00:00+00:00', interval '10:59' hour_minute) AS hour_minute"""
	qt_hour_minute_sub_7 """select date_sub('2018-11-30 01:00:00-05:30', interval '10:59' hour_minute) AS hour_minute"""
	testFoldConst("select date_sub('2023-01-02 03:04:05', interval '6@07' hour_minute) AS hour_minute")
	testFoldConst("select date_sub('2019-08-31 23:59:59', interval '10:59' hour_minute) AS hour_minute")
	testFoldConst("select date_sub('2019-09-30 23:59:59', interval '99:59' hour_minute) AS hour_minute")
	testFoldConst("select date_sub('2019-10-31', interval '10:59' hour_minute) AS hour_minute")
	testFoldConst("select date_sub('2019-11-30 01:00:00+08:00', interval '10:59' hour_minute) AS hour_minute")
	testFoldConst("select date_sub('2019-12-31 01:00:00+00:00', interval '10:59' hour_minute) AS hour_minute")
	testFoldConst("select date_sub('2020-01-31 01:00:00-05:30', interval '10:59' hour_minute) AS hour_minute")

	// HOUR_MINUTE extract
	qt_hour_minute_extract_1 """select extract(hour_minute from dt) from test_add_sub_union_type order by dt"""
	qt_hour_minute_extract_2 """select extract(hour_minute from '2023-01-02')"""
	qt_hour_minute_extract_3 """select extract(hour_minute from '2023-01-02 03:04:05.123456')"""
	qt_hour_minute_extract_4 """select extract(hour_minute from '2023-01-02 03:04:05.123456+08:00')"""
	qt_hour_minute_extract_5 """select extract(hour_minute from '2023-01-02 03:04:05.123456-05:30')"""
	testFoldConst("select extract(hour_minute from '2023-01-02')")
	testFoldConst("select extract(hour_minute from '2023-01-02 03:04:05.123456')")
	testFoldConst("select extract(hour_minute from '2023-01-02 03:04:05.123456+08:00')")
	testFoldConst("select extract(hour_minute from '2023-01-02 03:04:05.123456-05:30')")

	// HOUR_SECOND add/sub
	qt_hour_second_add_1 """select date_add('2023-01-02 03:04:05', interval '2:30:40' hour_second) AS hour_second"""
	qt_hour_second_add_2 """select date_add('2023-01-02 03:04:05.123456', interval '-1:00:05' hour_second) AS hour_second"""
	qt_hour_second_add_3 """select date_add(dt, interval iv_hour_second hour_second) AS hour_second from test_add_sub_union_type where dt='2023-01-02 03:04:05.123456'"""
	qt_hour_second_add_4 """select date_add(dt, interval iv_hour_second hour_second) AS hour_second from test_add_sub_union_type where dt='2024-02-29 10:00:00.000000'"""
	qt_hour_second_add_5 """select date_add('2023-01-02 03:04:05', interval '7&8:9' hour_second) AS hour_second"""
	qt_hour_second_add_6 """select date_add('2023-01-02 03:04:05.123456', interval '1::2**3' hour_second) AS hour_second"""
	qt_hour_second_add_7 """select date_add('2023-01-02 03:04:05.123456', interval '40' hour_second) AS hour_second"""
	qt_hour_second_add_8 """select date_add('2023-01-02 03:04:05.123456', interval '5000:59:59' hour_second) AS hour_second"""
	qt_hour_second_add_9 """select date_add('2017-06-30', interval '10:20:30' hour_second) AS hour_second"""
	qt_hour_second_add_10 """select date_add('2017-07-31 01:00:00+08:00', interval '10:20:30' hour_second) AS hour_second"""
	qt_hour_second_add_11 """select date_add('2017-08-31 01:00:00+00:00', interval '10:20:30' hour_second) AS hour_second"""
	qt_hour_second_add_12 """select date_add('2017-09-30 01:00:00-05:30', interval '10:20:30' hour_second) AS hour_second"""
	testFoldConst("select date_add('2023-01-02 03:04:05', interval '7&8:9' hour_second) AS hour_second")
	testFoldConst("select date_add('2023-01-02 03:04:05.123456', interval '1::2**3' hour_second) AS hour_second")
	testFoldConst("select date_add('2023-01-02 03:04:05.123456', interval '40' hour_second) AS hour_second")
	testFoldConst("select date_add('2023-01-02 03:04:05.123456', interval '5000:59:59' hour_second) AS hour_second")
	testFoldConst("select date_add('2016-07-31 23:59:50', interval '0:00:15' hour_second) AS hour_second")
	testFoldConst("select date_add('2016-06-30 23:59:50', interval '-00:00:00' hour_second) AS hour_second")
	testFoldConst("select date_add('2016-05-31', interval '10:20:30' hour_second) AS hour_second")
	testFoldConst("select date_add('2016-08-31 01:00:00+08:00', interval '10:20:30' hour_second) AS hour_second")
	testFoldConst("select date_add('2016-09-30 01:00:00+00:00', interval '10:20:30' hour_second) AS hour_second")
	testFoldConst("select date_add('2016-10-31 01:00:00-05:30', interval '10:20:30' hour_second) AS hour_second")
	test {
		sql """select date_add('2023-01-02 03:04:05', interval '1:2:3:4' hour_second) AS hour_second"""
		exception "Operation hour_second_add of"
	}

	qt_hour_second_sub_1 """select date_sub('2023-01-02 03:04:05', interval '2:30:40' hour_second) AS hour_second"""
	qt_hour_second_sub_2 """select date_sub(dt, interval iv_hour_second hour_second) AS hour_second from test_add_sub_union_type where dt='2023-01-02 03:04:05.123456'"""
	qt_hour_second_sub_3 """select date_sub('2023-01-02 03:04:05', interval '6@7:8' hour_second) AS hour_second"""
	qt_hour_second_sub_4 """select date_sub('2015-12-31', interval '10:20:30' hour_second) AS hour_second"""
	qt_hour_second_sub_5 """select date_sub('2015-11-30 01:00:00+08:00', interval '10:20:30' hour_second) AS hour_second"""
	qt_hour_second_sub_6 """select date_sub('2015-10-31 01:00:00+00:00', interval '10:20:30' hour_second) AS hour_second"""
	qt_hour_second_sub_7 """select date_sub('2015-09-30 01:00:00-05:30', interval '10:20:30' hour_second) AS hour_second"""
	testFoldConst("select date_sub('2023-01-02 03:04:05', interval '6@7:8' hour_second) AS hour_second")
	testFoldConst("select date_sub('2014-12-31 23:59:50', interval '0:00:15' hour_second) AS hour_second")
	testFoldConst("select date_sub('2014-11-30 23:59:50', interval '10*-10:10' hour_second) AS hour_second")
	testFoldConst("select date_sub('2014-10-31', interval '10:20:30' hour_second) AS hour_second")
	testFoldConst("select date_sub('2014-09-30 01:00:00+08:00', interval '10:20:30' hour_second) AS hour_second")
	testFoldConst("select date_sub('2014-08-31 01:00:00+00:00', interval '10:20:30' hour_second) AS hour_second")
	testFoldConst("select date_sub('2014-07-31 01:00:00-05:30', interval '10:20:30' hour_second) AS hour_second")

	// HOUR_SECOND extract
	qt_hour_second_extract_1 """select extract(hour_second from dt) from test_add_sub_union_type order by dt"""
	qt_hour_second_extract_2 """select extract(hour_second from '2023-01-02')"""
	qt_hour_second_extract_3 """select extract(hour_second from '2023-01-02 03:04:05.123456')"""
	qt_hour_second_extract_4 """select extract(hour_second from '2023-01-02 03:04:05.123456+08:00')"""
	qt_hour_second_extract_5 """select extract(hour_second from '2023-01-02 03:04:05.123456-05:30')"""
	testFoldConst("select extract(hour_second from '2023-01-02')")
	testFoldConst("select extract(hour_second from '2023-01-02 03:04:05.123456')")
	testFoldConst("select extract(hour_second from '2023-01-02 03:04:05.123456+08:00')")
	testFoldConst("select extract(hour_second from '2023-01-02 03:04:05.123456-05:30')")

	// HOUR_MICROSECOND add/sub
	qt_hour_microsecond_add_1 """select date_add('2023-01-02 03:04:05.123456', interval '2:30:40.123456' hour_microsecond) AS hour_microsecond"""
	qt_hour_microsecond_add_2 """select date_add('2023-01-02 03:04:05.123456', interval '-0:00:00.000001' hour_microsecond) AS hour_microsecond"""
	qt_hour_microsecond_add_3 """select date_add(dt, interval iv_hour_microsecond hour_microsecond) AS hour_microsecond from test_add_sub_union_type where dt='2023-01-02 03:04:05.123456'"""
	qt_hour_microsecond_add_4 """select date_add(dt, interval iv_hour_microsecond hour_microsecond) AS hour_microsecond from test_add_sub_union_type where dt='2024-02-29 10:00:00.000000'"""
	qt_hour_microsecond_add_5 """select date_add('2023-01-02 03:04:05.123456', interval '1!2:3.000004' hour_microsecond) AS hour_microsecond"""
	qt_hour_microsecond_add_6 """select date_add('2023-01-02 03:04:05.123456', interval '1::2**3..4' hour_microsecond) AS hour_microsecond"""
	qt_hour_microsecond_add_7 """select date_add('2023-01-02 03:04:05.123456', interval '5.6' hour_microsecond) AS hour_microsecond"""
	qt_hour_microsecond_add_8 """select date_add('2023-01-02 03:04:05.123456', interval '100000:59:59.999999' hour_microsecond) AS hour_microsecond"""
	qt_hour_microsecond_add_9 """select date_add('2013-12-31', interval '10:20:30.123456' hour_microsecond) AS hour_microsecond"""
	qt_hour_microsecond_add_10 """select date_add('2013-11-30 01:00:00+08:00', interval '10:20:30.123456' hour_microsecond) AS hour_microsecond"""
	qt_hour_microsecond_add_11 """select date_add('2013-10-31 01:00:00+00:00', interval '10:20:30.123456' hour_microsecond) AS hour_microsecond"""
	qt_hour_microsecond_add_12 """select date_add('2013-09-30 01:00:00-05:30', interval '10:20:30.123456' hour_microsecond) AS hour_microsecond"""
	testFoldConst("select date_add('2023-01-02 03:04:05.123456', interval '1!2:3.000004' hour_microsecond) AS hour_microsecond")
	testFoldConst("select date_add('2023-01-02 03:04:05.123456', interval '1::2**3..4' hour_microsecond) AS hour_microsecond")
	testFoldConst("select date_add('2023-01-02 03:04:05.123456', interval '5.6' hour_microsecond) AS hour_microsecond")
	testFoldConst("select date_add('2023-01-02 03:04:05.123456', interval '100000:59:59.999999' hour_microsecond) AS hour_microsecond")
	testFoldConst("select date_add('2008-12-31 23:59:59.999999', interval '0:00:00.000005' hour_microsecond) AS hour_microsecond")
	testFoldConst("select date_add('2009-02-28 23:59:59.123456', interval '-00:00:00.000009' hour_microsecond) AS hour_microsecond")
	testFoldConst("select date_add('2012-08-31', interval '10:20:30.123456' hour_microsecond) AS hour_microsecond")
	testFoldConst("select date_add('2012-07-31 01:00:00+08:00', interval '10:20:30.123456' hour_microsecond) AS hour_microsecond")
	testFoldConst("select date_add('2012-06-30 01:00:00+00:00', interval '10:20:30.123456' hour_microsecond) AS hour_microsecond")
	testFoldConst("select date_add('2012-05-31 01:00:00-05:30', interval '10:20:30.123456' hour_microsecond) AS hour_microsecond")
	test {
		sql """select date_add('2023-01-02 03:04:05.123456', interval '1:2:3:4.5' hour_microsecond) AS hour_microsecond"""
		exception "Operation hour_microsecond_add of"
	}

	qt_hour_microsecond_sub_1 """select date_sub('2023-01-02 03:04:05.123456', interval '2:30:40.123456' hour_microsecond) AS hour_microsecond"""
	qt_hour_microsecond_sub_2 """select date_sub(dt, interval iv_hour_microsecond hour_microsecond) AS hour_microsecond from test_add_sub_union_type where dt='2023-01-02 03:04:05.123456'"""
	qt_hour_microsecond_sub_3 """select date_sub('2023-01-02 03:04:05.123456', interval '9#8:7.000006' hour_microsecond) AS hour_microsecond"""
	qt_hour_microsecond_sub_4 """select date_sub('2011-12-31', interval '10:20:30.123456' hour_microsecond) AS hour_microsecond"""
	qt_hour_microsecond_sub_5 """select date_sub('2011-11-30 01:00:00+08:00', interval '10:20:30.123456' hour_microsecond) AS hour_microsecond"""
	qt_hour_microsecond_sub_6 """select date_sub('2011-10-31 01:00:00+00:00', interval '10:20:30.123456' hour_microsecond) AS hour_microsecond"""
	qt_hour_microsecond_sub_7 """select date_sub('2011-09-30 01:00:00-05:30', interval '10:20:30.123456' hour_microsecond) AS hour_microsecond"""
	testFoldConst("select date_sub('2023-01-02 03:04:05.123456', interval '9#8:7.000006' hour_microsecond) AS hour_microsecond")
	testFoldConst("select date_sub('2007-12-31 23:59:59.999999', interval '0:00:00.000005' hour_microsecond) AS hour_microsecond")
	testFoldConst("select date_sub('2007-11-30 23:59:59.999999', interval '99:59:59.999999' hour_microsecond) AS hour_microsecond")
	testFoldConst("select date_sub('2010-08-31', interval '10:20:30.123456' hour_microsecond) AS hour_microsecond")
	testFoldConst("select date_sub('2010-07-31 01:00:00+08:00', interval '10:20:30.123456' hour_microsecond) AS hour_microsecond")
	testFoldConst("select date_sub('2010-06-30 01:00:00+00:00', interval '10:20:30.123456' hour_microsecond) AS hour_microsecond")
	testFoldConst("select date_sub('2010-05-31 01:00:00-05:30', interval '10:20:30.123456' hour_microsecond) AS hour_microsecond")

	// HOUR_MICROSECOND extract
	qt_hour_microsecond_extract_1 """select extract(hour_microsecond from dt) from test_add_sub_union_type order by dt"""
	qt_hour_microsecond_extract_2 """select extract(hour_microsecond from '2023-01-02')"""
	qt_hour_microsecond_extract_3 """select extract(hour_microsecond from '2023-01-02 03:04:05.123456')"""
	qt_hour_microsecond_extract_4 """select extract(hour_microsecond from '2023-01-02 03:04:05.123456+08:00')"""
	qt_hour_microsecond_extract_5 """select extract(hour_microsecond from '2023-01-02 03:04:05.123456-05:30')"""
	testFoldConst("select extract(hour_microsecond from '2023-01-02')")
	testFoldConst("select extract(hour_microsecond from '2023-01-02 03:04:05.123456')")
	testFoldConst("select extract(hour_microsecond from '2023-01-02 03:04:05.123456+08:00')")
	testFoldConst("select extract(hour_microsecond from '2023-01-02 03:04:05.123456-05:30')")

	// MINUTE_SECOND add/sub
	qt_minute_second_add_1 """select date_add('2023-01-02 03:04:05', interval '45:50' minute_second) AS minute_second"""
	qt_minute_second_add_2 """select date_add('2023-01-02 03:04:05.123456', interval '-10:20' minute_second) AS minute_second"""
	qt_minute_second_add_3 """select date_add(dt, interval iv_minute_second minute_second) AS minute_second from test_add_sub_union_type where dt='2023-01-02 03:04:05.123456'"""
	qt_minute_second_add_4 """select date_add(dt, interval iv_minute_second minute_second) AS minute_second from test_add_sub_union_type where dt='2024-02-29 10:00:00.000000'"""
	qt_minute_second_add_5 """select date_add('2023-01-02 03:04:05', interval '7&8' minute_second) AS minute_second"""
	qt_minute_second_add_6 """select date_add('2023-01-02 03:04:05.123456', interval '1::2' minute_second) AS minute_second"""
	qt_minute_second_add_7 """select date_add('2023-01-02 03:04:05.123456', interval '30' minute_second) AS minute_second"""
	qt_minute_second_add_8 """select date_add('2023-01-02 03:04:05.123456', interval '500000:59' minute_second) AS minute_second"""
	qt_minute_second_add_10 """select date_add('2009-12-31', interval '10:20' minute_second) AS minute_second"""
	qt_minute_second_add_11 """select date_add('2009-11-30 01:00:00+08:00', interval '10:20' minute_second) AS minute_second"""
	qt_minute_second_add_12 """select date_add('2009-10-31 01:00:00+00:00', interval '10:20' minute_second) AS minute_second"""
	qt_minute_second_add_13 """select date_add('2009-09-30 01:00:00-05:30', interval '10:20' minute_second) AS minute_second"""
	testFoldConst("select date_add('2023-01-02 03:04:05', interval '7&8' minute_second) AS minute_second")
	testFoldConst("select date_add('2023-01-02 03:04:05.123456', interval '1::2' minute_second) AS minute_second")
	testFoldConst("select date_add('2023-01-02 03:04:05.123456', interval '30' minute_second) AS minute_second")
	testFoldConst("select date_add('2023-01-02 03:04:05.123456', interval '500000:59' minute_second) AS minute_second")
	testFoldConst("select date_add('2008-11-30 23:59:50', interval '00:15' minute_second) AS minute_second")
	testFoldConst("select date_add('2008-10-31 23:59:50', interval '-00:00' minute_second) AS minute_second")
	testFoldConst("select date_add('2008-09-30', interval '10:20' minute_second) AS minute_second")
	testFoldConst("select date_add('2008-08-31 01:00:00+08:00', interval '10:20' minute_second) AS minute_second")
	testFoldConst("select date_add('2008-07-31 01:00:00+00:00', interval '10:20' minute_second) AS minute_second")
	testFoldConst("select date_add('2008-06-30 01:00:00-05:30', interval '10:20' minute_second) AS minute_second")
	test {
		sql """select date_add('2023-01-02 03:04:05', interval '1:2:3' minute_second) AS minute_second"""
		exception "Operation minute_second_add of"
	}

	qt_minute_second_sub_1 """select date_sub('2023-01-02 03:04:05', interval '45:50' minute_second) AS minute_second"""
	qt_minute_second_sub_2 """select date_sub(dt, interval iv_minute_second minute_second) AS minute_second from test_add_sub_union_type where dt='2023-01-02 03:04:05.123456'"""
	qt_minute_second_sub_3 """select date_sub('2023-01-02 03:04:05', interval '9@10' minute_second) AS minute_second"""
	qt_minute_second_sub_4 """select date_sub('2007-12-31', interval '10:20' minute_second) AS minute_second"""
	qt_minute_second_sub_5 """select date_sub('2007-11-30 01:00:00+08:00', interval '10:20' minute_second) AS minute_second"""
	qt_minute_second_sub_6 """select date_sub('2007-10-31 01:00:00+00:00', interval '10:20' minute_second) AS minute_second"""
	qt_minute_second_sub_7 """select date_sub('2007-09-30 01:00:00-05:30', interval '10:20' minute_second) AS minute_second"""
	testFoldConst("select date_sub('2023-01-02 03:04:05', interval '9@10' minute_second) AS minute_second")
	testFoldConst("select date_sub('2006-12-31 23:59:50', interval '00:15' minute_second) AS minute_second")
	testFoldConst("select date_sub('2006-11-30 23:59:50', interval '99:59' minute_second) AS minute_second")
	testFoldConst("select date_sub('2006-08-31', interval '10:20' minute_second) AS minute_second")
	testFoldConst("select date_sub('2006-07-31 01:00:00+08:00', interval '10:20' minute_second) AS minute_second")
	testFoldConst("select date_sub('2006-06-30 01:00:00+00:00', interval '10:20' minute_second) AS minute_second")
	testFoldConst("select date_sub('2006-05-31 01:00:00-05:30', interval '10:20' minute_second) AS minute_second")

	// MINUTE_SECOND extract
	qt_minute_second_extract_1 """select extract(minute_second from dt) from test_add_sub_union_type order by dt"""
	qt_minute_second_extract_2 """select extract(minute_second from '2023-01-02')"""
	qt_minute_second_extract_3 """select extract(minute_second from '2023-01-02 03:04:05.123456')"""
	qt_minute_second_extract_4 """select extract(minute_second from '2023-01-02 03:04:05.123456+08:00')"""
	qt_minute_second_extract_5 """select extract(minute_second from '2023-01-02 03:04:05.123456-05:30')"""
	testFoldConst("select extract(minute_second from '2023-01-02')")
	testFoldConst("select extract(minute_second from '2023-01-02 03:04:05.123456')")
	testFoldConst("select extract(minute_second from '2023-01-02 03:04:05.123456+08:00')")
	testFoldConst("select extract(minute_second from '2023-01-02 03:04:05.123456-05:30')")

	// MINUTE_MICROSECOND add/sub
	qt_minute_microsecond_add_1 """select date_add('2023-01-02 03:04:05.123456', interval '45:50.123456' minute_microsecond) AS minute_microsecond"""
	qt_minute_microsecond_add_2 """select date_add('2023-01-02 03:04:05.123456', interval '-10:20.000020' minute_microsecond) AS minute_microsecond"""
	qt_minute_microsecond_add_3 """select date_add(dt, interval iv_minute_microsecond minute_microsecond) AS minute_microsecond from test_add_sub_union_type where dt='2023-01-02 03:04:05.123456'"""
	qt_minute_microsecond_add_4 """select date_add(dt, interval iv_minute_microsecond minute_microsecond) AS minute_microsecond from test_add_sub_union_type where dt='2024-02-29 10:00:00.000000'"""
	qt_minute_microsecond_add_5 """select date_add('2023-01-02 03:04:05.123456', interval '3:4,56' minute_microsecond) AS minute_microsecond"""
	qt_minute_microsecond_add_6 """select date_add('2023-01-02 03:04:05.123456', interval '7&8.009' minute_microsecond) AS minute_microsecond"""
	qt_minute_microsecond_add_7 """select date_add('2023-01-02 03:04:05.123456', interval '1::2..3' minute_microsecond) AS minute_microsecond"""
	qt_minute_microsecond_add_8 """select date_add('2023-01-02 03:04:05.123456', interval '9.8' minute_microsecond) AS minute_microsecond"""
	qt_minute_microsecond_add_9 """select date_add('2023-01-02 03:04:05.123456', interval '700000:59.999999' minute_microsecond) AS minute_microsecond"""
	qt_minute_microsecond_add_10 """select date_add('2007-12-30', interval '10:20.123456' minute_microsecond) AS minute_microsecond"""
	qt_minute_microsecond_add_11 """select date_add('2007-11-30 01:00:00+08:00', interval '10:20.123456' minute_microsecond) AS minute_microsecond"""
	qt_minute_microsecond_add_12 """select date_add('2007-10-31 01:00:00+00:00', interval '10:20.123456' minute_microsecond) AS minute_microsecond"""
	qt_minute_microsecond_add_13 """select date_add('2007-09-30 01:00:00-05:30', interval '10:20.123456' minute_microsecond) AS minute_microsecond"""
	testFoldConst("select date_add('2023-01-02 03:04:05.123456', interval '3:4,56' minute_microsecond) AS minute_microsecond")
	testFoldConst("select date_add('2023-01-02 03:04:05.123456', interval '7&8.009' minute_microsecond) AS minute_microsecond")
	testFoldConst("select date_add('2023-01-02 03:04:05.123456', interval '1::2..3' minute_microsecond) AS minute_microsecond")
	testFoldConst("select date_add('2023-01-02 03:04:05.123456', interval '9.8' minute_microsecond) AS minute_microsecond")
	testFoldConst("select date_add('2023-01-02 03:04:05.123456', interval '700000:59.999999' minute_microsecond) AS minute_microsecond")
	testFoldConst("select date_add('2006-12-31 23:59:59.999999', interval '00:00.000005' minute_microsecond) AS minute_microsecond")
	testFoldConst("select date_add('2006-11-30 23:59:59.999999', interval '-00:00.000001' minute_microsecond) AS minute_microsecond")
	testFoldConst("select date_add('2006-09-30', interval '10:20.123456' minute_microsecond) AS minute_microsecond")
	testFoldConst("select date_add('2006-04-30 01:00:00+08:00', interval '10:20.123456' minute_microsecond) AS minute_microsecond")
	testFoldConst("select date_add('2006-03-31 01:00:00+00:00', interval '10:20.123456' minute_microsecond) AS minute_microsecond")
	testFoldConst("select date_add('2006-02-28 01:00:00-05:30', interval '10:20.123456' minute_microsecond) AS minute_microsecond")
	test {
		sql """select date_add('2023-01-02 03:04:05.123456', interval '1:2:3.4' minute_microsecond) AS minute_microsecond"""
		exception "Operation minute_microsecond_add of"
	}

	qt_minute_microsecond_sub_1 """select date_sub('2023-01-02 03:04:05.123456', interval '45:50.123456' minute_microsecond) AS minute_microsecond"""
	qt_minute_microsecond_sub_2 """select date_sub(dt, interval iv_minute_microsecond minute_microsecond) AS minute_microsecond from test_add_sub_union_type where dt='2023-01-02 03:04:05.123456'"""
	qt_minute_microsecond_sub_3 """select date_sub('2023-01-02 03:04:05.123456', interval '9@10.001' minute_microsecond) AS minute_microsecond"""
	qt_minute_microsecond_sub_4 """select date_sub('2005-12-31', interval '10:20.123456' minute_microsecond) AS minute_microsecond"""
	qt_minute_microsecond_sub_5 """select date_sub('2005-11-30 01:00:00+08:00', interval '10:20.123456' minute_microsecond) AS minute_microsecond"""
	qt_minute_microsecond_sub_6 """select date_sub('2005-10-31 01:00:00+00:00', interval '10:20.123456' minute_microsecond) AS minute_microsecond"""
	qt_minute_microsecond_sub_7 """select date_sub('2005-09-30 01:00:00-05:30', interval '10:20.123456' minute_microsecond) AS minute_microsecond"""
	testFoldConst("select date_sub('2023-01-02 03:04:05.123456', interval '9@10.001' minute_microsecond) AS minute_microsecond")
	testFoldConst("select date_sub('2005-08-31 23:59:59.999999', interval '00:00.000005' minute_microsecond) AS minute_microsecond")
	testFoldConst("select date_sub('2005-07-31 23:59:59.123456', interval '00:59.999999' minute_microsecond) AS minute_microsecond")
	testFoldConst("select date_sub('2005-06-30', interval '10:20.123456' minute_microsecond) AS minute_microsecond")
	testFoldConst("select date_sub('2005-04-30 01:00:00+08:00', interval '10:20.123456' minute_microsecond) AS minute_microsecond")
	testFoldConst("select date_sub('2005-03-31 01:00:00+00:00', interval '10:20.123456' minute_microsecond) AS minute_microsecond")
	testFoldConst("select date_sub('2005-02-28 01:00:00-05:30', interval '10:20.123456' minute_microsecond) AS minute_microsecond")

	// MINUTE_MICROSECOND extract
	qt_minute_microsecond_extract_1 """select extract(minute_microsecond from dt) from test_add_sub_union_type order by dt"""
	qt_minute_microsecond_extract_2 """select extract(minute_microsecond from '2023-01-02')"""
	qt_minute_microsecond_extract_3 """select extract(minute_microsecond from '2023-01-02 03:04:05.123456')"""
	qt_minute_microsecond_extract_4 """select extract(minute_microsecond from '2023-01-02 03:04:05.123456+08:00')"""
	qt_minute_microsecond_extract_5 """select extract(minute_microsecond from '2023-01-02 03:04:05.123456-05:30')"""
	testFoldConst("select extract(minute_microsecond from '2023-01-02')")
	testFoldConst("select extract(minute_microsecond from '2023-01-02 03:04:05.123456')")
	testFoldConst("select extract(minute_microsecond from '2023-01-02 03:04:05.123456+08:00')")
	testFoldConst("select extract(minute_microsecond from '2023-01-02 03:04:05.123456-05:30')")

	// SECOND_MICROSECOND add/sub
	qt_second_microsecond_add_1 """select date_add('2023-01-02 03:04:05.123456', interval '59.654321' second_microsecond) AS second_microsecond"""
	qt_second_microsecond_add_2 """select date_add('2023-01-02 03:04:05.123456', interval '-5.000005' second_microsecond) AS second_microsecond"""
	qt_second_microsecond_add_3 """select date_add(dt, interval iv_second_microsecond second_microsecond) AS second_microsecond from test_add_sub_union_type where dt='2023-01-02 03:04:05.123456'"""
	qt_second_microsecond_add_4 """select date_add(dt, interval iv_second_microsecond second_microsecond) AS second_microsecond from test_add_sub_union_type where dt='2024-02-29 10:00:00.000000'"""
	qt_second_microsecond_add_5 """select date_add('2023-01-02 03:04:05.123456', interval '1,2' second_microsecond) AS second_microsecond"""
	qt_second_microsecond_add_6 """select date_add('2023-01-02 03:04:05.123456', interval '7!8' second_microsecond) AS second_microsecond"""
	qt_second_microsecond_add_7 """select date_add('2023-01-02 03:04:05.123456', interval '1..2' second_microsecond) AS second_microsecond"""
	qt_second_microsecond_add_8 """select date_add('2023-01-02 03:04:05.123456', interval '7' second_microsecond) AS second_microsecond"""
	qt_second_microsecond_add_9 """select date_add('2023-01-02 03:04:05.123456', interval '100000000.999999' second_microsecond) AS second_microsecond"""
	qt_second_microsecond_add_10 """select date_add('2004-12-31', interval '10.123456' second_microsecond) AS second_microsecond"""
	qt_second_microsecond_add_11 """select date_add('2004-11-30 01:00:00+08:00', interval '10.123456' second_microsecond) AS second_microsecond"""
	testFoldConst("select date_add('2023-01-02 03:04:05.123456', interval '1,2' second_microsecond) AS second_microsecond")
	testFoldConst("select date_add('2023-01-02 03:04:05.123456', interval '7!8' second_microsecond) AS second_microsecond")
	testFoldConst("select date_add('2023-01-02 03:04:05.123456', interval '1..2' second_microsecond) AS second_microsecond")
	testFoldConst("select date_add('2023-01-02 03:04:05.123456', interval '7' second_microsecond) AS second_microsecond")
	testFoldConst("select date_add('2023-01-02 03:04:05.123456', interval '100000000.999999' second_microsecond) AS second_microsecond")
	testFoldConst("select date_add('2003-12-31 23:59:59.999999', interval '0.000010' second_microsecond) AS second_microsecond")
	testFoldConst("select date_add('2003-11-30 23:59:59.999999', interval '-0.000001' second_microsecond) AS second_microsecond")
	testFoldConst("select date_add('2003-10-31 01:00:00+08:00', interval '10.123456' second_microsecond) AS second_microsecond")
	test {
		sql """select date_add('2023-01-02 03:04:05.123456', interval '1:2.3' second_microsecond) AS second_microsecond"""
		exception "Operation second_microsecond_add of"
	}

	qt_second_microsecond_sub_1 """select date_sub('2023-01-02 03:04:05.123456', interval '59.654321' second_microsecond) AS second_microsecond"""
	qt_second_microsecond_sub_2 """select date_sub(dt, interval iv_second_microsecond second_microsecond) AS second_microsecond from test_add_sub_union_type where dt='2023-01-02 03:04:05.123456'"""
	qt_second_microsecond_sub_3 """select date_sub('2023-01-02 03:04:05.123456', interval '9@8' second_microsecond) AS second_microsecond"""
	qt_second_microsecond_sub_4 """select date_sub('2002-12-31', interval '10.123456' second_microsecond) AS second_microsecond"""
	qt_second_microsecond_sub_5 """select date_sub('2002-11-30 01:00:00+08:00', interval '10.123456' second_microsecond) AS second_microsecond"""
	testFoldConst("select date_sub('2023-01-02 03:04:05.123456', interval '9@8' second_microsecond) AS second_microsecond")
	testFoldConst("select date_sub('2002-12-30 23:59:59.999999', interval '0.000010' second_microsecond) AS second_microsecond")
	testFoldConst("select date_sub('0001-01-01 00:00:00.000000', interval '0.000001' second_microsecond) AS second_microsecond")
	testFoldConst("select date_sub('2002-10-31 01:00:00+08:00', interval '10.123456' second_microsecond) AS second_microsecond")

	// SECOND_MICROSECOND extract
	qt_second_microsecond_extract_1 """select extract(second_microsecond from dt) from test_add_sub_union_type order by dt"""
	qt_second_microsecond_extract_2 """select extract(second_microsecond from '2023-01-02')"""
	qt_second_microsecond_extract_3 """select extract(second_microsecond from '2023-01-02 03:04:05.123456')"""
	qt_second_microsecond_extract_4 """select extract(second_microsecond from '2023-01-02 03:04:05.123456+08:00')"""
	qt_second_microsecond_extract_5 """select extract(second_microsecond from '2023-01-02 03:04:05.123456-05:30')"""
	testFoldConst("select extract(second_microsecond from '2023-01-02')")
	testFoldConst("select extract(second_microsecond from '2023-01-02 03:04:05.123456')")
	testFoldConst("select extract(second_microsecond from '2023-01-02 03:04:05.123456+08:00')")
	testFoldConst("select extract(second_microsecond from '2023-01-02 03:04:05.123456-05:30')")

}
