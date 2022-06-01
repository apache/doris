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

import java.text.SimpleDateFormat

suite("test_date_function", "query") {
    def tableName = "test_date_function"

    sql """ SET enable_vectorized_engine = TRUE; """

    // convert_tz
    // 转换datetime值，从 from_tz 给定时区转到 to_tz 给定时区，并返回结果值。 如果参数无效该函数返回NULL。
    qt_sql """ SELECT convert_tz('2019-08-01 13:21:03', 'Asia/Shanghai', 'America/Los_Angeles') result; """
    qt_sql """ SELECT convert_tz('2019-08-01 13:21:03', '+08:00', 'America/Los_Angeles') result; """

    qt_sql """ SELECT convert_tz('2019-08-01 13:21:03', 'Asia/Shanghai', 'Europe/London') result; """
    qt_sql """ SELECT convert_tz('2019-08-01 13:21:03', '+08:00', 'Europe/London') result; """

    qt_sql """ SELECT convert_tz('2019-08-01 13:21:03', '+08:00', 'America/London') result; """


    // curdate,current_date
    // 获取当前的日期，以DATE类型返回
    String curdate_str = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
    def curdate_result = sql """ SELECT CURDATE() """
    def curdate_date_result = sql """ SELECT CURRENT_DATE() """
    assertTrue(curdate_str == curdate_result[0][0].toString())
    assertTrue(curdate_str == curdate_date_result[0][0].toString())

    // DATETIME CURRENT_TIMESTAMP()
    //获得当前的时间，以Datetime类型返回
    def current_timestamp_result = """ SELECT current_timestamp() """
    assertTrue(current_timestamp_result[0].size() == 1)

    // TIME CURTIME()
    def curtime_result = sql """ SELECT CURTIME() """
    assertTrue(curtime_result[0].size() == 1)

    // DATE_ADD
    qt_sql """ select date_add('2010-11-30 23:59:59', INTERVAL 2 YEAR) result; """
    qt_sql """ select date_add('2010-11-30 23:59:59', INTERVAL 2 MONTH) result; """
    qt_sql """ select date_add('2010-11-30 23:59:59', INTERVAL 2 DAY) result; """
    qt_sql """ select date_add('2010-11-30 23:59:59', INTERVAL 2 HOUR) result; """
    qt_sql """ select date_add('2010-11-30 23:59:59', INTERVAL 2 MINUTE) result; """
    qt_sql """ select date_add('2010-11-30 23:59:59', INTERVAL 2 SECOND) result; """

    // DATE_FORMAT
    qt_sql """ select date_format('2009-10-04 22:23:00', '%W %M %Y') """
    qt_sql """ select date_format('2007-10-04 22:23:00', '%H:%i:%s') """
    qt_sql """ select date_format('1900-10-04 22:23:00', '%D %y %a %d %m %b %j') """
    qt_sql """ select date_format('1997-10-04 22:23:00', '%H %k %I %r %T %S %w') """
    qt_sql """ select date_format('1999-01-01 00:00:00', '%X %V') """
    qt_sql """ select date_format('2006-06-01', '%d') """
    qt_sql """ select date_format('2006-06-01', '%%%d') """
    qt_sql """ select date_format('2009-10-04 22:23:00', 'yyyy-MM-dd') """

    // DATE_SUB
    qt_sql """ select date_sub('2010-11-30 23:59:59', INTERVAL 2 YEAR) """
    qt_sql """ select date_sub('2010-11-30 23:59:59', INTERVAL 2 MONTH) """
    qt_sql """ select date_sub('2010-11-30 23:59:59', INTERVAL 2 DAY) """
    qt_sql """ select date_sub('2010-11-30 23:59:59', INTERVAL 2 HOUR) """
    qt_sql """ select date_sub('2010-11-30 23:59:59', INTERVAL 2 MINUTE) """
    qt_sql """ select date_sub('2010-11-30 23:59:59', INTERVAL 2 SECOND) """


    // DATEDIFF
    qt_sql """ select datediff(CAST('2007-12-31 23:59:59' AS DATETIME), CAST('2007-12-30' AS DATETIME)) """
    qt_sql """ select datediff(CAST('2010-11-30 23:59:59' AS DATETIME), CAST('2010-12-31' AS DATETIME)) """
    qt_sql """ select datediff('2010-10-31', '2010-10-15') """

    // DAY
    qt_sql """ select day('1987-01-31') """
    qt_sql """ select day('2004-02-29') """

    // DAYNAME
    qt_sql """ select dayname('2007-02-03 00:00:00') """

    // DAYOFMONTH
    qt_sql """ select dayofmonth('1987-01-31') """

    // DAYOFWEEK
    qt_sql """ select dayofweek('2019-06-25') """
    qt_sql """ select dayofweek(cast(20190625 as date)) """

    // DAYOFYEAR
    qt_sql """ select dayofyear('2007-02-03 10:00:00') """
    qt_sql """ select dayofyear('2007-02-03') """

    // FROM_DAYS
    // 通过距离0000-01-01日的天数计算出哪一天
    qt_sql """ select from_days(730669) """
    qt_sql """ select from_days(1) """

    // FROM_UNIXTIME
    qt_sql """ select from_unixtime(1196440219) """
    qt_sql """ select from_unixtime(1196440219, 'yyyy-MM-dd HH:mm:ss') """
    qt_sql """ select from_unixtime(1196440219, '%Y-%m-%d') """
    qt_sql """ select from_unixtime(1196440219, '%Y-%m-%d %H:%i:%s') """
    qt_sql """ select from_unixtime(253402272000, '%Y-%m-%d %H:%i:%s') """

    // HOUR
    qt_sql """ select hour('2018-12-31 23:59:59') """
    qt_sql """ select hour('2018-12-31') """

    // MAKEDATE
    qt_sql """ select makedate(2021,1), makedate(2021,100), makedate(2021,400) """

    // MINUTE
    qt_sql """ select minute('2018-12-31 23:59:59') """
    qt_sql """ select minute('2018-12-31') """

    // MONTH
    qt_sql """ select month('1987-01-01 23:59:59') """
    qt_sql """ select month('1987-01-01') """

    // MONTHNAME
    qt_sql """ select monthname('2008-02-03 00:00:00') """
    qt_sql """ select monthname('2008-02-03') """

    // NOW
    def now_result = sql """ select now() """
    assertTrue(now_result[0].size() == 1)

    // SECOND
    qt_sql """ select second('2018-12-31 23:59:59') """
    qt_sql """ select second('2018-12-31 00:00:00') """

    // STR_TO_DATE
    qt_sql """ select str_to_date('2014-12-21 12:34:56', '%Y-%m-%d %H:%i:%s') """
    qt_sql """ select str_to_date('2014-12-21 12:34%3A56', '%Y-%m-%d %H:%i%%3A%s') """
    qt_sql """ select str_to_date('200442 Monday', '%X%V %W') """
    qt_sql """ select str_to_date("2020-09-01", "%Y-%m-%d %H:%i:%s") """

    // TIME_ROUND
    qt_sql """ SELECT YEAR_FLOOR('20200202000000') """
    qt_sql """ SELECT MONTH_CEIL(CAST('2020-02-02 13:09:20' AS DATETIME), 3) """
    qt_sql """ SELECT WEEK_CEIL('2020-02-02 13:09:20', '2020-01-06') """
    qt_sql """ SELECT MONTH_CEIL(CAST('2020-02-02 13:09:20' AS DATETIME), 3, CAST('1970-01-09 00:00:00' AS DATETIME)) """

    // TIMEDIFF
    qt_sql """ SELECT TIMEDIFF(now(),utc_timestamp()) """
    qt_sql """ SELECT TIMEDIFF('2019-07-11 16:59:30','2019-07-11 16:59:21') """
    qt_sql """ SELECT TIMEDIFF('2019-01-01 00:00:00', NULL) """

    // TIMESTAMPADD
    qt_sql """ SELECT TIMESTAMPADD(YEAR,1,'2019-01-02') """
    qt_sql """ SELECT TIMESTAMPADD(MONTH,1,'2019-01-02') """
    qt_sql """ SELECT TIMESTAMPADD(WEEK,1,'2019-01-02') """
    qt_sql """ SELECT TIMESTAMPADD(DAY,1,'2019-01-02') """
    qt_sql """ SELECT TIMESTAMPADD(HOUR,1,'2019-01-02') """
    qt_sql """ SELECT TIMESTAMPADD(MINUTE,1,'2019-01-02') """
    qt_sql """ SELECT TIMESTAMPADD(SECOND,1,'2019-01-02') """

    // TIMESTAMPDIFF
    qt_sql """ SELECT TIMESTAMPDIFF(MONTH,'2003-02-01','2003-05-01') """
    qt_sql """ SELECT TIMESTAMPDIFF(YEAR,'2002-05-01','2001-01-01') """
    qt_sql """ SELECT TIMESTAMPDIFF(MINUTE,'2003-02-01','2003-05-01 12:05:55') """
    qt_sql """ SELECT TIMESTAMPDIFF(SECOND,'2003-02-01','2003-05-01') """
    qt_sql """ SELECT TIMESTAMPDIFF(HOUR,'2003-02-01','2003-05-01') """
    qt_sql """ SELECT TIMESTAMPDIFF(DAY,'2003-02-01','2003-05-01') """
    qt_sql """ SELECT TIMESTAMPDIFF(WEEK,'2003-02-01','2003-05-01') """

    // TO_DAYS
    qt_sql """ select to_days('2007-10-07') """
    qt_sql """ select to_days('2050-10-07') """

    // UNIX_TIMESTAMP
    def unin_timestamp_str = """ select unix_timestamp() """
    assertTrue(unin_timestamp_str[0].size() == 1)
    qt_sql """ select unix_timestamp('2007-11-30 10:30:19') """
    qt_sql """ select unix_timestamp('2007-11-30 10:30-19', '%Y-%m-%d %H:%i-%s') """
    qt_sql """ select unix_timestamp('2007-11-30 10:30%3A19', '%Y-%m-%d %H:%i%%3A%s') """
    qt_sql """ select unix_timestamp('1969-01-01 00:00:00') """

    // UTC_TIMESTAMP
    def utc_timestamp_str = sql """ select utc_timestamp(),utc_timestamp() + 1 """
    assertTrue(utc_timestamp_str[0].size() == 2)
    // WEEK
    qt_sql """ select week('2020-1-1') """
    qt_sql """ select week('2020-7-1',1) """

    // WEEKDAY
    //TODO 添加 WEKKDAY case

    // WEEKOFYEAR
    qt_sql """ select weekofyear('2008-02-20 00:00:00') """

    // YEAR
    qt_sql """ select year('1987-01-01') """
    qt_sql """ select year('2050-01-01') """

    // YEARWEEK
    qt_sql """ select yearweek('2021-1-1') """
    qt_sql """ select yearweek('2020-7-1') """


}
