#!/bin/env python
# -*- coding: utf-8 -*-
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
this file is to test the functin about datetime
"""
import sys
sys.path.append("../lib/")
from palo_qe_client import QueryBase

table_name = "test"
join_name = "baseall"


def setup_module():
    """
    init config
    """
    global runner, enable_vectorized_engine
    runner = QueryBase()
    ret = runner.get_sql_result('show variables like "enable_vectorized_engine"')
    if ret[0][1] == 'false':
        enable_vectorized_engine = False
    elif ret[0][1] == 'true':
        enable_vectorized_engine = True
    else:
        enable_vectorized_engine = None



def test_query_date_timestamp():
    """
    {
    "title": "test_query_datetime_function.test_query_date_timestamp",
    "describe": "test for date function UNIX_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_DATE, CURRENT_TIME, CURTIME",
    "tag": "function,p0"
    }
    """
    """
    test for date function UNIX_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_DATE, CURRENT_TIME, CURTIME
    """
    """
    line = "SELECT FROM_UNIXTIME( 1249488000, '%Y%m%d' )"
    runner.check(line)
    """
    line = "select k11, UNIX_TIMESTAMP(k11) from %s \
            order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    """
    line = "select k10, UNIX_TIMESTAMP(k10) from %s \
            order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    """
    line = "select UNIX_TIMESTAMP()"
    runner.check(line)
    line = "select CURRENT_TIMESTAMP()"
    runner.check(line)
    line = 'SELECT CURDATE()'
    runner.check(line)
    line = 'SELECT CURRENT_DATE()'
    runner.check(line)
    line = 'SELECT CURRENT_TIME()'
    runner.checkok(line)
    line = 'SELECT CURTIME()'
    runner.checkok(line)


def test_query_date_format():
    """
    {
    "title": "test_query_datetime_function.test_query_date_format",
    "describe": "test for date function date_format",
    "tag": "function,p0"
    }
    """
    """
    test for date function date_format
    """
    line = "select k11, date_format(k11, '%%D %%y %%a %%d %%m %%b %%j') from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k10, date_format(k10, '%%D %%y %%a %%d %%m %%b %%j') from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k11, date_format(k11, '%%H %%k %%I %%r %%T %%S %%w') from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k10, date_format(k10, '%%H %%k %%I %%r %%T %%S %%w') from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k11, date_format(k11, '%%W %%M %%Y') from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k10, date_format(k10, '%%W %%M %%Y') from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k11, date_format(k11, '%%H:%%i:%%s') from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k10, date_format(k10, '%%H:%%i:%%s') from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k11, date_format(k11, '%%X %%V') from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k10, date_format(k10, '%%X %%V') from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k11, date_format(k11, '%%d') from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k10, date_format(k10, '%%d') from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)


def test_query_date_str_to_date():
    """
    {
    "title": "test_query_datetime_function.test_query_date_str_to_date",
    "describe": "test for date function str_to_date",
    "tag": "function,p0"
    }
    """
    """
    test for date function str_to_date
    """
    line = "select str_to_date('2008-4-2 15:3:28','%Y-%m-%d %H:%i:%s')"
    runner.check(line)
    line = "select str_to_date('2008-08-09 08:9:30', '%Y-%m-%d %h:%i:%s')"
    runner.check(line)
    line = "select str_to_date('11/09/2011', '%m/%d/%Y')"
    runner.check(line)
    line = "select str_to_date('11.09.2011 11:09:30', '%m.%d.%Y %h:%i:%s')"
    runner.check(line)
    line = "select str_to_date('11/09/11' , '%m/%d/%y')"
    runner.check(line)
    line = "select str_to_date('11.09.2011', '%m.%d.%Y')"
    runner.check(line)
    """
    line = "select str_to_date('11:09:30', '%h:%i:%s')"
    runner.check(line)
    """
    line = "select str_to_date('2003-01-02 10:11:12', '%Y-%m-%d %H:%i:%S')"
    runner.check(line)
    line = "select str_to_date('03-01-02 8:11:2.123456', '%y-%m-%d %H:%i:%S.%#')"
    runner.check(line)
    line = "select str_to_date('0003-01-02 8:11:2.123456', '%Y-%m-%d %H:%i:%S.%#') "
    runner.check(line)
    line = "select str_to_date('03-01-02 8:11:2.123456',   '%Y-%m-%d %H:%i:%S.%#') "
    runner.check(line)
    line = "select str_to_date('2003-01-02 10:11:12 PM', '%Y-%m-%d %h:%i:%S %p') "
    runner.check(line)
    line1 = "select str_to_date('2003-01-02 01:11:12.12345AM', '%Y-%m-%d %h:%i:%S.%f%p')"
    line2 = "select str_to_date('2003-01-02 01:11:12', '%Y-%m-%d %H:%i:%S')"
    runner.check2(line1, line2)
    line1 = "select str_to_date('2003-01-02 02:11:12.12345AM', '%Y-%m-%d %h:%i:%S.%f %p')"
    line2 = "select str_to_date('2003-01-02 02:11:12', '%Y-%m-%d %H:%i:%S')"
    runner.check2(line1, line2)
    # line = "select str_to_date('2003-01-02 12:11:12.12345 am', '%Y-%m-%d %h:%i:%S.%f%p') "
    # runner.check(line)
    line = "select str_to_date('2003-01-02 11:11:12Pm', '%Y-%m-%d %h:%i:%S%p')"
    runner.check(line)
    line = "select str_to_date('10:20:10', '%H:%i:%s'), str_to_date('10:20:10', '%h:%i:%s.%f'), " \
           "str_to_date('10:20:10', '%T')"
    runner.check(line)
    line = "select str_to_date('10:20:10AM', '%h:%i:%s%p'), str_to_date('10:20:10AM', '%r')" 
    runner.check(line)
    line = "select str_to_date('15-01-2001 12:59:58', '%d-%m-%Y %H:%i:%S')" 
    runner.check(line)
    line = "select str_to_date('15 September 2001', '%d %M %Y'), str_to_date('15 SEPTEMB 2001', '%d %M %Y')" 
    runner.check(line)
    line = "select str_to_date('15 MAY 2001', '%d %b %Y'), str_to_date('15th May 2001', '%D %b %Y')"
    runner.check(line)
    line = "select str_to_date('Sunday 15 MAY 2001', '%W %d %b %Y')"
    runner.check(line)
    line = "select str_to_date('Sund 15 MAY 2001', '%W %d %b %Y')"
    runner.check(line)
    line = "select str_to_date('Tuesday 00 2002', '%W %U %Y')"
    runner.check(line)
    line = "select str_to_date('Thursday 53 1998', '%W %u %Y')"
    runner.check(line)
    line = "select str_to_date('Sunday 01 2001', '%W %v %x')"
    runner.check(line)
    line = "select str_to_date('Tuesday 52 2001', '%W %V %X')"
    runner.check(line)
    line = "select str_to_date('060 2004', '%j %Y')"
    runner.check(line)
    line = "select str_to_date('4 53 1998', '%w %u %Y')"
    runner.check(line)
    line = "select str_to_date('15-01-2001', '%d-%m-%Y %H:%i:%S')"
    runner.check(line)
    line = "select str_to_date('15-01-20', '%d-%m-%y')"
    runner.check(line)
    line = "select str_to_date('15-2001-1', '%d-%Y-%c')"
    runner.check(line)
    # 2020-00-20
    line1 = "select cast(str_to_date(substring('2020-02-09', 1, 1024), '%b %e %Y') as char)"
    line2 = "select null"
    runner.check2(line1, line2)


def test_query_date_operate():
    """
    {
    "title": "test_query_datetime_function.test_query_date_operate",
    "describe": "test for operate function of date and datetime +,-",
    "tag": "function,p0"
    }
    """
    """
    test for operate function of date and datetime +,-
    """
    line = "select k11 + INTERVAL 1 DAY - INTERVAL 1 SECOND from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k10 + INTERVAL 1 DAY - INTERVAL 1 SECOND from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k1, k10, k11 from %s order by k1, k10" % (table_name) 
    runner.check(line)
    line = "select k1, k10, k11 from %s where k10>=\"2015-04-02 00:00:00\" " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k1, k10, k11 from %s where k11>=\"2015-04-02 00:00:00\" " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE(k11) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k11 + interval 10 year, interval 10 year + k10, k11 -interval 10 year from %s " \
           "where k10 < '2016-03-21 00:00:00' and k11 < '2016-03-21 00:00:00' order by 1, 2, 3" \
           % (table_name)
    runner.check(line)
    line = "select k11 + interval 13 month, interval 13 month + k10, k11 -interval 13 month from %s " \
           "where k10 < '2016-03-21 00:00:00' and k11 < '2016-03-21 00:00:00' order by 1, 2, 3" \
           % (table_name)
    runner.check(line)
    line = "select k11 + interval 13 week, interval 13 week + k10, k11 -interval 13 week from %s " \
           "where k10 < '2016-03-21 00:00:00' and k11 < '2016-03-21 00:00:00' " \
           "order by 1, 2, 3" % (table_name)
    runner.check(line)
    line = "select k11 + interval 13 day, interval 13 day + k10, k11 -interval 13 day from %s " \
           "where k10 < '2016-03-21 00:00:00' and k11 < '2016-03-21 00:00:00' " \
           "order by 1, 2, 3" % (table_name)
    runner.check(line)
    line = "select k11 + interval 366 day, interval 365 day + k10, k11 -interval 366 day from %s " \
           "where k10 < '2016-03-21 00:00:00' and k11 < '2016-03-21 00:00:00' " \
           "order by 1, 2, 3" % (table_name)
    runner.check(line)
    line = "select k11 + interval 61 minute, interval 60 minute + k10, k11 -interval 61 minute from %s " \
           "where k10 < '2016-03-21 00:00:00' and k11 < '2016-03-21 00:00:00' " \
           "order by 1, 2, 3" % (table_name)
    runner.check(line)
    line = "select k11 + interval 60 second, interval 60 second + k10, k11 -interval 13 day from %s " \
           "where k10 < '2016-03-21 00:00:00' and k11 < '2016-03-21 00:00:00' " \
           "order by 1, 2, 3" % (table_name)
    runner.check(line)


def test_query_date_datediff():
    """
    {
    "title": "test_query_datetime_function.test_query_date_datediff",
    "describe": "test for date function datediff",
    "tag": "function,p0"
    }
    """
    """
    test for date function datediff
    """
    line = "select DATEDIFF(cast(k10 as datetime), k11) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATEDIFF(k11, cast(k10 as datetime)) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATEDIFF(k11, k10) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)


def test_query_date_extract():
    """
    {
    "title": "test_query_datetime_function.test_query_date_extract",
    "describe": "test for date function extract",
    "tag": "function,p0"
    }
    """
    """
    test for date function extract
    """
    line = "select extract(year from k10) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select extract(year from k11) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select extract(month from k10) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select extract(month from k11) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select extract(day from k10) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select extract(day from k11) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select extract(hour from k11) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select extract(minute from k11) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select extract(second from k11) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)


def test_query_date_name():
    """
    {
    "title": "test_query_datetime_function.test_query_date_name",
    "describe": "test for date function monthname, dayname, dayofmonth,dayofyear, weekofyear, dayofmonth,year, month, day, minute, hour, second",
    "tag": "function,p0"
    }
    """
    """
    test for date function monthname, dayname, dayofmonth, dayofyear, weekofyear, dayofmonth,
    year, month, day, minute, hour, second
    """
    line = "select k11, monthname(k11) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k10, monthname(k10) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k11, dayname(k11) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k10, dayname(k10) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select dayofmonth(k10), dayofmonth(k11) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select dayofyear(k10), dayofyear(k11) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select weekofyear(k10), weekofyear(k11) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select dayofmonth(k10)+dayofyear(k11) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select year(k10), year(k11) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select month(k10), month(k11) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select day(k10), day(k11) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select minute(k10), minute(k11) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select hour(k10), hour(k11) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select second(k10), second(k11) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select to_days(k10), to_days(k11) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select from_days(k2) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)


def test_query_date_add_1():
    """
    {
    "title": "test_query_datetime_function.test_query_date_add_1",
    "describe": "test for DATE_ADD",
    "tag": "function,p0"
    }
    """
    """
    test for DATE_ADD
    """
    line = "select DATE_ADD(k11, INTERVAL 10 YEAR) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_ADD(k11, INTERVAL \"10\" YEAR) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_ADD(k10, INTERVAL 10 YEAR) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_ADD(k10, INTERVAL \"10\" YEAR) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)

    line = "select DATE_ADD(k11, INTERVAL 10 MONTH) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_ADD(k11, INTERVAL \"10\" MONTH) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_ADD(k10, INTERVAL 10 MONTH) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_ADD(k10, INTERVAL \"10\" MONTH) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_ADD(k11, INTERVAL -13 MONTH) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_ADD(k11, INTERVAL \"13\" MONTH) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_ADD(k10, INTERVAL 13 MONTH) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_ADD(k10, INTERVAL \"-13\" MONTH) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_ADD(k10, INTERVAL 13 WEEK) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_ADD(k10, INTERVAL \"-13\" WEEK) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)

    line = "select DATE_ADD(k11, INTERVAL 10 DAY) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_ADD(k11, INTERVAL \"10\" DAY) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_ADD(k10, INTERVAL 10 DAY) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_ADD(k10, INTERVAL \"10\" DAY) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_ADD(k11, INTERVAL -13 DAY) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_ADD(k11, INTERVAL \"13\" DAY) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_ADD(k10, INTERVAL 13 DAY) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_ADD(k10, INTERVAL \"-13\" DAY) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)

    line = "select DATE_ADD(k11, INTERVAL 10 HOUR) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_ADD(k11, INTERVAL \"10\" HOUR) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_ADD(k10, INTERVAL 10 HOUR) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_ADD(k10, INTERVAL \"10\" HOUR) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_ADD(k11, INTERVAL -33 HOUR) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_ADD(k11, INTERVAL \"33\" HOUR) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_ADD(k10, INTERVAL 33 HOUR) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_ADD(k10, INTERVAL \"-33\" HOUR) from %s order by k1, k2, k3, k4" \
           % (table_name)
    runner.check(line)


def test_query_date_add_2():
    """
    {
    "title": "test_query_datetime_function.test_query_date_add_2",
    "describe": "test for DATE_ADD",
    "tag": "function,p0"
    }
    """
    """
    test for DATE_ADD
    """
    line = "select DATE_ADD(k11, INTERVAL 10 MINUTE) from %s order by k1, k2, k3, k4" \
           % (table_name)
    runner.check(line)
    line = "select DATE_ADD(k11, INTERVAL \"10\" MINUTE) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_ADD(k10, INTERVAL 10 MINUTE) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_ADD(k10, INTERVAL \"10\" MINUTE) from %s order by k1, k2, k3, k4" \
           % (table_name)
    runner.check(line)
    line = "select DATE_ADD(k11, INTERVAL -73 MINUTE) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_ADD(k11, INTERVAL \"73\" MINUTE) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_ADD(k10, INTERVAL 60 MINUTE) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_ADD(k10, INTERVAL \"-60\" MINUTE) from %s order by k1, k2, k3, k4" \
           % (table_name)
    runner.check(line)

    line = "select DATE_ADD(k11, INTERVAL 10 SECOND) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_ADD(k11, INTERVAL \"10\" SECOND) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_ADD(k10, INTERVAL 10 SECOND) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_ADD(k10, INTERVAL \"10\" SECOND) from %s order by k1, k2, k3, k4" \
           % (table_name)
    runner.check(line)
    line = "select DATE_ADD(k11, INTERVAL -73 SECOND) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_ADD(k11, INTERVAL \"73\" SECOND) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_ADD(k10, INTERVAL 73 SECOND) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_ADD(k10, INTERVAL \"-73\" SECOND) from %s order by k1, k2, k3, k4" \
           % (table_name)
    runner.check(line)
    line = "select ADDDATE(k10, 30) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select ADDDATE(k11, 30) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select ADDDATE(k11, interval 31 day) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select ADDDATE(k10, interval 31 day) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)


def test_query_date_sub_1():
    """
    {
    "title": "test_query_datetime_function.test_query_date_sub_1",
    "describe": "test for date_sub",
    "tag": "function,p0"
    }
    """
    """
    test for date_sub
    """
    line = "select DATE_SUB(k11, INTERVAL 10 YEAR) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_SUB(k11, INTERVAL \"10\" YEAR) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_SUB(k10, INTERVAL 10 YEAR) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_SUB(k10, INTERVAL \"10\" YEAR) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)

    line = "select DATE_SUB(k11, INTERVAL 10 MONTH) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_SUB(k11, INTERVAL \"10\" MONTH) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_SUB(k10, INTERVAL 10 MONTH) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_SUB(k10, INTERVAL \"10\" MONTH) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_SUB(k11, INTERVAL -13 MONTH) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_SUB(k11, INTERVAL \"13\" MONTH) from %s \
		    order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_SUB(k10, INTERVAL 13 MONTH) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_SUB(k10, INTERVAL \"-13\" MONTH) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_ADD(k10, INTERVAL 13 WEEK) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_ADD(k10, INTERVAL \"-13\" WEEK) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)

    line = "select DATE_SUB(k11, INTERVAL 10 DAY) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_SUB(k11, INTERVAL \"10\" DAY) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_SUB(k10, INTERVAL 10 DAY) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_SUB(k10, INTERVAL \"10\" DAY) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_SUB(k11, INTERVAL -13 DAY) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_SUB(k11, INTERVAL \"13\" DAY) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_SUB(k10, INTERVAL 13 DAY) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_SUB(k10, INTERVAL \"-13\" DAY) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)

    line = "select DATE_SUB(k11, INTERVAL 10 HOUR) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_SUB(k11, INTERVAL \"10\" HOUR) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_SUB(k10, INTERVAL 10 HOUR) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_SUB(k10, INTERVAL \"10\" HOUR) from %s order by k1, k2, k3, k4" \
           % (table_name)
    runner.check(line)
    line = "select DATE_SUB(k11, INTERVAL -33 HOUR) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_SUB(k11, INTERVAL \"33\" HOUR) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_SUB(k10, INTERVAL 33 HOUR) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_SUB(k10, INTERVAL \"-33\" HOUR) from %s order by k1, k2, k3, k4" \
           % (table_name)
    runner.check(line)

    line = "select DATE_SUB(k11, INTERVAL 10 MINUTE) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_SUB(k11, INTERVAL \"10\" MINUTE) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_SUB(k10, INTERVAL 10 MINUTE) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_SUB(k10, INTERVAL \"10\" MINUTE) from %s order by k1, k2, k3, k4" \
           % (table_name)
    runner.check(line)
    line = "select DATE_SUB(k11, INTERVAL -73 MINUTE) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)


def test_query_date_sub_2():
    """
    {
    "title": "test_query_datetime_function.test_query_date_sub_2",
    "describe": "test for date_sub",
    "tag": "function,p0"
    }
    """
    """
    test for date_sub
    """
    line = "select DATE_SUB(k11, INTERVAL \"73\" MINUTE) from %s " \
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_SUB(k10, INTERVAL 60 MINUTE) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_SUB(k10, INTERVAL \"-60\" MINUTE) from %s order by k1, k2, k3, k4"\
		    % (table_name)
    runner.check(line)

    line = "select DATE_SUB(k11, INTERVAL 10 SECOND) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_SUB(k11, INTERVAL \"10\" SECOND) from %s \
		    order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_SUB(k10, INTERVAL 10 SECOND) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_SUB(k10, INTERVAL \"10\" SECOND) from %s order by k1, k2, k3, k4" \
		    % (table_name)
    runner.check(line)
    line = "select DATE_SUB(k11, INTERVAL -73 SECOND) from %s \
		    order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_SUB(k11, INTERVAL \"73\" SECOND) from %s \
		    order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_SUB(k10, INTERVAL 73 SECOND) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select DATE_SUB(k10, INTERVAL \"-73\" SECOND) from %s order by k1, k2, k3, k4" \
		    % (table_name)
    runner.check(line)


def test_query_time_from_unixtime():
    """
    {
    "title": "test_query_datetime_function.test_query_time_from_unixtime",
    "describe": "test for from_uinxtime",
    "tag": "function,p0"
    }
    """
    maxstamp = 2147454847
    for i in (1, 10000000, 211592898, 908901337, 2147454846):
        line = "select from_unixtime(%d,'%%Y-%%m-%%d %%H:%%i:%%s')" % (i)
        runner.check(line)
        line = "select from_unixtime(%d,'%%Y-%%m-%%d')" %(i)
        runner.check(line)

    line = "select from_unixtime(0,'%Y-%m-%d %H:%i:%s')"
    runner.check(line)
    line = "select from_unixtime(%d,'%%Y-%%m-%%d %%H:%%i:%%s')" % (maxstamp)
    runner.check(line)
    line = "select from_unixtime(%d,'%%Y-%%m-%%d %%H:%%i:%%s')" % (maxstamp + 1)
    runner.check(line)
 

def test_query_time_operate():
    """
    {
    "title": "test_query_datetime_function.test_query_time_operate",
    "describe": "test date and datetime compute function",
    "tag": "function,p0"
    }
    """
    """test date and datetime compute function"""
    # adddate(), subdate
    line = 'select adddate(k10, 41), subdate(k11, 41) from baseall order by k1;'
    runner.check(line)
    # days_add, days_sub
    line1 = 'select days_add(k10, 41), days_sub(k11, 41) from baseall order by k1;'
    line2 = 'select date_add(k10, interval 41 day), date_sub(k11, interval 41 day) from baseall order by k1'
    runner.check2(line1, line2)
    # hours_add, hours_sub
    line1 = 'select hours_add(k10, 41), hours_sub(k11, 41) from baseall order by k1;'
    line2 = 'select date_add(k10, interval 41 hour), date_sub(k11, interval 41 hour) from baseall order by k1'
    runner.check2(line1, line2)
    # microseconds_add, microseconds_sub
    if not enable_vectorized_engine:
        line1 = 'select cast(microseconds_add(k10, 41) as char), cast(microseconds_sub(k11, 41) as char) ' \
                'from baseall order by k1;'
        line2 = 'select cast(date_add(k10, interval 41 microsecond) as char), ' \
                'cast(date_sub(k11, interval 41 microsecond) as char) from baseall order by k1'
        runner.check2(line1, line2)
    # months_sub, months_add
    line1 = 'select months_add(k10, 5), months_sub(k11, 5) from baseall order by k1;'
    line2 = 'select date_add(k10, interval 5 month), date_sub(k11, interval 5 month) from baseall order by k1'
    runner.check2(line1, line2)
    # minutes_add, minutes_sub
    line1 = 'select minutes_add(k10, 41), minutes_sub(k11, 41) from baseall order by k1;'
    line2 = 'select date_add(k10, interval 41 minute), date_sub(k11, interval 41 minute) from baseall order by k1'
    runner.check2(line1, line2)
    # seconds_add, seconds_sub
    line1 = 'select seconds_add(k10, 41), seconds_sub(k11, 41) from baseall order by k1;'
    line2 = 'select date_add(k10, interval 41 second), date_sub(k11, interval 41 second) from baseall order by k1'
    runner.check2(line1, line2)
    # weeks_add, weeks_sub
    line1 = 'select weeks_add(k10, 41), weeks_sub(k11, 41) from baseall order by k1;'
    line2 = 'select date_add(k10, interval 41 week), date_sub(k11, interval 41 week) from baseall order by k1'
    runner.check2(line1, line2)
    # years_add, years_sub
    line1 = 'select years_add(k10, 41), years_sub(k11, 41) from baseall order by k1;'
    line2 = 'select date_add(k10, interval 41 year), date_sub(k11, interval 41 year) from baseall order by k1'
    runner.check2(line1, line2)
    # add_months
    line1 = 'select add_months(k10, 5), add_months(k11, 3), add_months(k11, -1) from baseall order by k1;'
    line2 = 'select date_add(k10, interval 5 month), date_add(k11, interval 3 month), \
            date_sub(k11, interval 1 month) from baseall order by k1'
    runner.check2(line1, line2)


def test_query_date_dayofweek():
    """
    {
    "title": "test_query_datetime_function.test_query_date_dayofweek",
    "describe": "test for dayofweek,闰年2月判断，datetime， 里面嵌套函数，其他非法格式，过去，现在，将来",
    "tag": "function,p0,fuzz"
    }
    """
    """
    test for dayofweek
    闰年2月判断，datetime， 里面嵌套函数，其他非法格式，过去，现在，将来
    """
    # 每个类型校验
    for index in range(11):
        line = "select dayofweek(k%s),k%s from %s order by k1, k2, k3, k4" %\
             (index + 1, index + 1, join_name)
        if index in [2]:
            # int ,mysql is all null
            line = "select dayofweek(k3),k3 from %s where k3 between 100 and 1231 order by k3" % join_name
            print(line)
            res = runner.query_palo.do_sql(line)
            print(res)
            excepted = ((2, 103), (1, 1001), (1, 1001), (2, 1002), (2, 1002))
            assert excepted == res, "res %s, excepted %s" % (res, excepted)
            continue
        if index in [4]:
            # decimal not suport
            runner.checkwrong(line)
            continue
        if index in [7]:
            # double ,mysql is all null
            line = "select dayofweek(k8),k8 from %s where k8 between 100 and 1231 order by k8" % join_name
            print(line)
            res = runner.query_palo.do_sql(line)
            print(res[0])
            excepted = (1, 123.456)
            assert excepted == res[0], "res %s, excepted %s" % (res[0], excepted) 
            continue
        if index in [8]:
            # FLOAT ,palo is all null,mysql has specific data
            line = "select dayofweek(k9), k9 from %s where k9 between 100 and 1231 order by k9" % join_name
            print(line)
            res = runner.query_palo.do_sql(line)
            print(res[0])
            excepted = (None, 789.25)
            assert excepted == res[0], "res %s, excepted %s" % (res, excepted)
            continue
        runner.check(line)
    # 闰年和平年
    line = "select dayofweek('2016-02-28'), dayofweek('2016-02-29'), dayofweek('2017-02-29'),\
           dayofweek('2017-02-28')"
    runner.check(line)
    line = "select dayofweek('2019-06-25'), dayofweek(now()), dayofweek('3019-06-25')"
    runner.check(line)
    line = "select dayofweek(date_sub(k11, INTERVAL 10 YEAR)), dayofweek(date_add(k10, INTERVAL 10 month)),\
     dayofweek(date_add(k10, INTERVAL 10 day)), dayofweek(date_sub(k11, INTERVAL 24 hour)), \
     dayofweek(date_add(k11, INTERVAL 24 MINUTE)), dayofweek(date_sub(k11, INTERVAL 24 SECOND)) \
     from %s order by k1, k2, k3, k4 limit 1" % (join_name)
    runner.check(line)
    # adddate,subdate
    line1 = "select dayofweek(adddate(k10, 41)), dayofweek(subdate(k10, 41)), \
        dayofweek(hours_add(k11, 41)), dayofweek(minutes_add(k11, 41)) from %s order by k1" % join_name
    line2 = "select dayofweek(adddate(k10, 41)), dayofweek(subdate(k10, 41)), \
        dayofweek(k11 + INTERVAL 41 HOUR), dayofweek(k11 + INTERVAL 41 MINUTE) from %s order by k1" % join_name
    runner.check2(line1, line2)
    # err
    line = "select dayofweek('2019-06-25 31:00:00'), dayofweek('2019-06-25 13:70:00'), \
       dayofweek('2019-06-25 13:10:70'), dayofweek('2019-06-25 31'), dayofweek('2019-06-35')"
    runner.check(line)
    # checkwrong
    line1 = "select dayofweek('10:00:00') ,dayofweek(100+10), dayofweek(1200/10),\
             dayofweek(10 * 12 +10)"
    runner.check(line1)
    line = "select dayofweek(k1 + k2) ,dayofweek(k1 - k2), dayofweek(k1 * k2),\
           dayofweek(k1/k2) from %s where k1=2" % join_name 
    runner.check(line)
    line = "select dayofweek('1998-01-01', '1998-01-01')"
    runner.checkwrong(line)


def test_query_date_timediff():
    """
    {
    "title": "test_query_datetime_function.test_query_date_timediff",
    "describe": "test for timediff,各类型列，边界值，非法格式，函数相关操作",
    "tag": "function,p0,fuzz"
    }
    """
    """
    test for timediff
    各类型列，边界值，非法格式，函数相关操作
    """
    # 每个类型校验
    for index in range(11):
        line = "select TIMEDIFF(k%s, '2019-02-28 00:00:00') from %s order by k1, k2, k3, k4\
           limit 1" % (index + 1, table_name)
        if index in [4]:
            runner.checkwrong(line)
            continue
        # 界限
        if index in [9, 10]:
            continue
        runner.check(line)
    # 边界值
    line = "select timediff('1019-02-28 00:00:00', '2019-02-28 00:00:00'), \
        timediff('2015-01-25 00:00:00', '2015-02-28 22:50:00'), \
        timediff('2015-01-25 00:00:00', '2015-01-25 00:00:00')"
    res = runner.query_palo.do_sql(line)
    # excepted = '((datetime.timedelta(-365244, 86057), datetime.timedelta(-35, 4200), datetime.timedelta(0)),)'
    # print("sql: %s, res: %s, excepted: %s" % (line, res, excepted))
    # assert excepted == str(res), "res %s, excepted %s" % (res, excepted)
    runner.checkok(line)
    # 函数
    line = "select timediff(adddate(k11, 1), k11), timediff(subdate(k11, 1), k11), \
        timediff(hours_add(k11, 41), k11), timediff(minutes_add(k11, 41), k11), \
        timediff(seconds_add(k11, 41), k11)  from %s order by k1 limit 1" % join_name
    line2 = "select timediff(adddate(k11, 1), k11), timediff(subdate(k11, 1), k11), \
        timediff((k11 + INTERVAL 41 HOUR), k11), timediff((k11 + INTERVAL 41 MINUTE), k11), \
        timediff((k11 + INTERVAL 41 SECOND), k11)  from %s order by k1 limit 1" % join_name
    runner.check2(line, line2)

    line = "select timediff(date_add(k11, INTERVAL 10 YEAR), k11),\
     timediff(date_add(k11, INTERVAL 10 month), k11) from %s order by k1 limit 1" % join_name
    res = runner.query_palo.do_sql(line)
    # excepted = "((datetime.timedelta(3652), datetime.timedelta(306)),)"
    # print("sql: %s, res: %s, excepted: %s" % (line, res, excepted))
    # assert excepted == str(res), "res %s, excepted %s" % (str(res), excepted)
    runner.check(line)
    # 界限
    line = "select timediff(date_add(k11, INTERVAL 10 day), k11), \
      timediff(date_add(k11, INTERVAL 10 hour), k11),\
      timediff(date_add(k11, INTERVAL 10 minute), k11),\
      timediff(date_add(k11, INTERVAL 10 second), k11) from %s order by k1 limit 1" % join_name
    runner.check(line)
    line1 = "select timediff('1944-02-18', '1944-02-28')"
    line2 = "select timediff(cast('1944-02-18' as datetime), cast('1944-02-28' as datetime))"
    runner.check2(line1, line2)
    # fixed
    line = "select timediff(cast('1944-02-18 10:10:10' as datetime), \
        cast('1944-02-18' as datetime)), timediff(cast('1944-02-18 10:10:10' as datetime), \
        cast('19440218' as datetime))"
    runner.check(line)
    sql = "SELECT TIMEDIFF('2010-01-01 10:00:00','10:00:00')"
    runner.check(sql)
    # 与mysql不一致,忽略
    sql = "SELECT TIMEDIFF('11:00:00','10:00:00')"
    # runner.check(sql)
    res = runner.query_palo.do_sql(sql)
    print(sql, res[0])
    excepted = (None,)
    assert res[0] == excepted, "res %s, excepted %s" % (res, excepted)
    # 与mysql不一致,非datetime,忽略
    sql = "SELECT TIMEDIFF(cast('11:00:00'as datetime),cast('10:00:00' as datetime))"
    # runner.check(sql)
    res = runner.query_palo.do_sql(sql)
    print(sql, res[0])
    assert res[0] == (None,), "res %s, excepted %s" % (res, excepted)
    sql = "select datediff(CAST('2010-11-30 23:59:59' AS DATETIME), \
        CAST('2010-12-31' AS DATETIME));"
    runner.check(sql)
    sql= "SELECT TIMEDIFF('2010-01-01 10:00:00',cast('2010-01-01' as datetime))"
    runner.check(sql)
    # 真是不存在的天数，返回NULL
    sql = "select datediff(CAST('2010-11-31 23:59:59' AS DATETIME), \
            CAST('2010-12-31' AS DATETIME));"
    runner.check(sql)
    sql = " SELECT TIMEDIFF('2019-01-01 00:00:00', NULL), TIMEDIFF('2019-01-01 00:00:00', 'sss')"
    runner.check(sql)
    line = "select timediff(cast(k1 as datetime), cast(k2 as datetime)) from %s limit 1" % join_name
    runner.check(sql)
    line = "select timediff(CAST('1944-02-25 00:00:00' as datetime))"
    runner.checkwrong(line)


def test_query_date_utc_timestamp():
    """
    {
    "title": "test_query_datetime_function.test_query_date_utc_timestamp",
    "describe": "test for utc_timestamp, 各类型列，非法格式，函数相关操作",
    "tag": "function,p0,fuzz"
    }
    """
    """
    test for utc_timestamp
    各类型列，非法格式，函数相关操作
    """
    line = "select UTC_TIMESTAMP() as utc"
    runner.checkok(line)
    # 6 是精确位数
    line = "select UTC_TIMESTAMP(6) as utc"
    runner.checkwrong(line)
    line = "select UTC_TIMESTAMP as utc" # mysql ok.palo no
    runner.checkwrong(line)
    line = "select UTC_TIMESTAMP() + 10 as utc" 
    runner.checkok(line)

    # 每个类型校验
    for index in range(11):
        line = "select utc_timestamp() - k%s,utc_timestamp() + k%s from %s \
            order by k1 limit 1" % (index + 1, index + 1, join_name)
        # char, varchar,return null
        if index in [5,6]:
            res = runner.query_palo.do_sql(line)
            print(line, res[0])
            excepted = (None, None,)
            assert res[0] == excepted, "res %s, excepted %s" % (res[0], excepted)
            continue
        runner.checkok(line)
    # ＋－＊／
    line = "select utc_timestamp() + 5, utc_timestamp() - 5, utc_timestamp()/3, \
    utc_timestamp()*2, utc_timestamp() + utc_timestamp()"
    runner.checkok(line)
    # 函数
    line = "select timediff(utc_timestamp(),cast(utc_timestamp() + k1 as datetime))\
            from %s order by k1" % join_name
    runner.checkok(line)
    line = "select cast(utc_timestamp() +200000 as datetime), \
    cast(utc_timestamp() -20000 as datetime)"
    runner.checkok(line)
    line = "select datediff('2019-09-08', cast(utc_timestamp() +20000 as datetime))"
    runner.checkok(line)
    line = "select  utc_timestamp() - '1', utc_timestamp()"
    runner.checkok(line)


def test_query_time_convert_tz():
    """
    {
    "title": "test_query_datetime_function.test_query_time_convert_tz",
    "describe": "test for convert_tz",转化指定时间的时区
    "tag": "function,p0"
    }
    """
    line1 = "select convert_tz(k11, '+00:00', '+01:00'), convert_tz(k11, '+01:00', '+00:00'), \
            convert_tz(k11, 'GMT', 'Egypt') from baseall order by k1"
    line2 = 'select date_add(k11, interval 1 hour), date_sub(k11, interval 1 hour), \
            date_add(k11, interval 2 hour) from baseall order by k1'
    runner.check2(line1, line2)


def test_query_timestampdiff():
    """
    {
    "title": "test_query_datetime_function.test_query_timestampdiff",
    "describe": "test for timestampdiff",
    "tag": "function,p0"
    }
    """
    # day
    line = "select TIMESTAMPDIFF(DAY, k10, cast(k11 as date)), TIMESTAMPDIFF(DAY, cast(k10 as datetime), k11), \
            TIMESTAMPDIFF(DAY, k10, k11) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    # week
    line = "select TIMESTAMPDIFF(WEEK, k10, cast(k11 as date)), TIMESTAMPDIFF(WEEK, cast(k10 as datetime), k11), \
            TIMESTAMPDIFF(WEEK, k10, k11) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    # month
    line = "select TIMESTAMPDIFF(MONTH, k10, k11) from %s order by k1, k2, k3, k4" % (table_name)
    # runner.check(line)
    line = "select TIMESTAMPDIFF(MONTH, k10, k11), TIMESTAMPDIFF(MONTH, cast(k10 as datetime), k11) \
            from %s order by k1, k2, k3, k4" % (table_name)
    # runner.check(line)
    line = "select TIMESTAMPDIFF(MONTH, k10, k11), TIMESTAMPDIFF(MONTH, cast(k10 as datetime), k11) \
            from %s order by k1, k2, k3, k4" % (table_name)
    # runner.check(line)
    line = "select TIMESTAMPDIFF(MONTH, cast(k10 as datetime), cast(k11 as datetime)) \
            from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    # year
    line = "select TIMESTAMPDIFF(YEAR, k10, cast(k11 as date)), TIMESTAMPDIFF(YEAR, cast(k10 as datetime), k11), \
            TIMESTAMPDIFF(YEAR, k10, k11) from %s order by k1, k2, k3, k4" % (table_name)
    # runner.check(line)
    line = "select TIMESTAMPDIFF(YEAR, cast(k10 as datetime), cast(k11 as datetime)) \
            from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)


def test_query_time_diff():
    """
    {
    "title": "test_query_datetime_function.test_query_time_diff",
    "describe": "test for time_diff"
    "tag": "function,p0"
    }
    """
    # days_diff
    line1 = "select DAYS_DIFF(cast(k11 as date), k10), DAYS_DIFF(cast(k10 as datetime), k11), DAYS_DIFF(k10, k11)\
            from %s order by k1, k2, k3, k4" % (table_name)
    line2 = "select TIMESTAMPDIFF(DAY, k10, cast(k11 as date)), TIMESTAMPDIFF(DAY, k11, cast(k10 as datetime)), \
            TIMESTAMPDIFF(DAY, k11, k10) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check2(line1, line2)
    # hours_diff
    line1 = "select HOURS_DIFF(cast(k11 as date), k10), HOURS_DIFF(cast(k10 as datetime), k11), \
            HOURS_DIFF(k10, k11) from %s where (k10 between '1960-01-01 00:00:00' and '2020-01-01 00:00:00') \
            and (k11 between '1960-01-01 00:00:00' and '2020-01-01 00:00:00') order by k1, k2, k3, k4" % (table_name)
    line2 = "select TIMESTAMPDIFF(HOUR, k10, cast(k11 as date)), TIMESTAMPDIFF(HOUR, k11, cast(k10 as datetime)), \
            TIMESTAMPDIFF(HOUR, k11, k10) from %s where (k10 between '1960-01-01 00:00:00' and '2020-01-01 00:00:00') \
            and (k11 between '1960-01-01 00:00:00' and '2020-01-01 00:00:00') order by k1, k2, k3, k4" % (table_name)
    runner.check2(line1, line2)
    # minutes_diff
    line1 = "select MINUTES_DIFF(cast(k11 as date), k10), MINUTES_DIFF(cast(k10 as datetime), k11), \
            MINUTES_DIFF(k10, k11) from %s where (k10 between '1960-01-01 00:00:00' and '2020-01-01 00:00:00') \
            and (k11 between '1960-01-01 00:00:00' and '2020-01-01 00:00:00') order by k1, k2, k3, k4" % (table_name)
    line2 = "select TIMESTAMPDIFF(MINUTE, k10, cast(k11 as date)), TIMESTAMPDIFF(MINUTE, k11, cast(k10 as datetime)), \
            TIMESTAMPDIFF(MINUTE, k11, k10) from %s where (k10 between '1960-01-01 00:00:00' and '2020-01-01 00:00:00') \
            and (k11 between '1960-01-01 00:00:00' and '2020-01-01 00:00:00') order by k1, k2, k3, k4" % (table_name)
    runner.check2(line1, line2)
    # months_diff
    line1 = "select MONTHS_DIFF(cast(k11 as date), k10), MONTHS_DIFF(cast(k10 as datetime), k11), \
            MONTHS_DIFF(k10, k11) from %s order by k1, k2, k3, k4" % (table_name)
    line2 = "select TIMESTAMPDIFF(MONTH, k10, cast(k11 as date)), TIMESTAMPDIFF(MONTH, k11, cast(k10 as datetime)), \
             TIMESTAMPDIFF(MONTH, k11, k10) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check2_palo(line1, line2)
    # seconds_diff
    line1 = "select SECONDS_DIFF(cast(k11 as date), k10), SECONDS_DIFF(cast(k10 as datetime), k11), \
            SECONDS_DIFF(k10, k11) from %s where (k10 between '1960-01-01 00:00:00' and '2020-01-01 00:00:00') \
            and (k11 between '1960-01-01 00:00:00' and '2020-01-01 00:00:00') order by k1, k2, k3, k4" % (table_name)
    line2 = "select TIMESTAMPDIFF(SECOND, k10, cast(k11 as date)), TIMESTAMPDIFF(SECOND, k11, cast(k10 as datetime)), \
            TIMESTAMPDIFF(SECOND, k11, k10) from %s where (k10 between '1960-01-01 00:00:00' and '2020-01-01 00:00:00') \
            and (k11 between '1960-01-01 00:00:00' and '2020-01-01 00:00:00') order by k1, k2, k3, k4" % (table_name)
    runner.check2(line1, line2)
    # weeks_diff
    line1 = "select WEEKS_DIFF(cast(k11 as date), k10), WEEKS_DIFF(cast(k10 as datetime), k11), WEEKS_DIFF(k10, k11)\
            from %s order by k1, k2, k3, k4" % (table_name)
    line2 = "select TIMESTAMPDIFF(WEEK, k10, cast(k11 as date)), TIMESTAMPDIFF(WEEK, k11, cast(k10 as datetime)), \
            TIMESTAMPDIFF(WEEK, k11, k10) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check2(line1, line2)
    # years_diff
    line1 = "select YEARS_DIFF(cast(k10 as datetime), k11) from %s order by k1, k2, k3, k4" % (table_name)
    line2 = "select TIMESTAMPDIFF(YEAR, k11, cast(k10 as datetime)) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check2(line1, line2)


def test_query_timestampadd():
    """
    {
    "title": "test_query_datetime_function.test_query_timestampadd",
    "describe": "test for timestampadd",
    "tag": "function,p0"
    }
    """
    # second
    line = "select TIMESTAMPADD(SECOND, k1, k10), TIMESTAMPADD(SECOND, k2, k11), TIMESTAMPADD(SECOND, 100, k10) \
            from %s order by k1,k2,k3,k4" % (table_name)
    runner.check(line)
    # minute
    line = "select TIMESTAMPADD(MINUTE, k1, k10), TIMESTAMPADD(MINUTE, k2, k11), TIMESTAMPADD(MINUTE, 100, k10) \
            from %s order by k1,k2,k3,k4" % (table_name)
    runner.check(line)
    # hour
    line = "select TIMESTAMPADD(HOUR, k1, k10), TIMESTAMPADD(HOUR, k2, k11), TIMESTAMPADD(HOUR, 100, k10) \
            from %s order by k1,k2,k3,k4" % (table_name)
    runner.check(line)
    # day
    line = "select TIMESTAMPADD(DAY, k1, k10), TIMESTAMPADD(DAY, k2, k11), TIMESTAMPADD(DAY, 100, k10) \
            from %s order by k1,k2,k3,k4" % (table_name)
    runner.check(line)
    # week
    line = "select TIMESTAMPADD(WEEK, k1, k10), TIMESTAMPADD(WEEK, k2, k11), TIMESTAMPADD(WEEK, 100, k10) \
            from %s order by k1,k2,k3,k4" % (table_name)
    runner.check(line)
    # month
    line = "select TIMESTAMPADD(MONTH, k1, k10), TIMESTAMPADD(MONTH, k1, k11), TIMESTAMPADD(MONTH, 100, k10) \
            from %s order by k1,k2,k3,k4" % (table_name)
    runner.check(line)
    # year
    line = "select TIMESTAMPADD(YEAR, k1, k10), TIMESTAMPADD(YEAR, k2, k11), TIMESTAMPADD(YEAR, 100, k10) \
            from %s order by k1,k2,k3,k4" % (table_name)
    runner.check(line)


def test_query_to_date():
    """
    {
    "title": "test_query_datetime_function.test_query_to_date",
    "describe": "test for to_date",
    "tag": "function,p0"
    }
    """
    line1 = "select TO_DATE(k10), TO_DATE(k11),TO_DATE(TIMESTAMPADD(HOUR, k1, k10)) from %s order by k1,k2,k3,k4" \
                % (table_name)
    line2 = "select DATE_FORMAT(k10, '%Y-%m-%d'), DATE_FORMAT(k11, '%Y-%m-%d'),DATE_FORMAT(TIMESTAMPADD(HOUR, k1, k10), \
            '%Y-%m-%d') from test order by k1,k2,k3,k4"
    runner.check2(line1, line2)


def test_query_quarter():
    """
    {
    "title": "test_query_datetime_function.test_query_quarter",
    "describe": "test for quarter",返回日期所属季度
    "tag": "function,p0"
    }
    """
    line = "select QUARTER(k10), QUARTER(k11), QUARTER(TIMESTAMPADD(DAY, k1, k10)) from %s order by k1,k2,k3,k4" \
            % table_name
    runner.check(line)


def test_query_makedate():
    """
    {
    "title": "test_query_datetime_function.test_query_makedate",
    "describe": "返回指定年份指定日期,issues #5999"
    "tag": "function,p0"
    }
    """
    line = "select makedate(2021, 59), makedate(2021, 60), makedate(2021, 61), makedate(2021, 0), makedate(2021, 366)"
    runner.check(line)
    line = "select makedate(2020, 59), makedate(2020, 60), makedate(2020, 61), makedate(2020, 0), makedate(2020, 367)"
    runner.check(line)
    line = "select makedate(1900, 59), makedate(1900, 60), makedate(1900, 61)"
    runner.check(line)
    line = "select makedate(2021, -1), makedate(2021, 1.1), makedate(2000, 20000)"
    runner.check(line)


def test_query_week():
    """
    {
    "title": "test_query_datetime_function.test_query_week",
    "describe": "返回指定日期的星期数,issues #5999"
    "tag": "function,p0"
    }
    """
    line = "select week(k10), week(k11) from %s order by k1,k2,k3,k4" % table_name
    runner.check(line)
    line = "select week(k10, 0), week(k10, 1), week(k10, 2), week(k10, 3), week(k10, 4), week(k10, 5), week(k10, 6), \
            week(k10, 7) from %s order by k1,k2,k3,k4" % table_name
    runner.check(line)
    line = "select week(k11, 0), week(k11, 1), week(k11, 2), week(k11, 3), week(k11, 4), week(k11, 5), week(k11, 6), \
            week(k11, 7) from %s order by k1,k2,k3,k4" % table_name
    runner.check(line)
    line = "select week('19980202', 7), week('19980101', 2), week('19981231', 0)"
    runner.check(line)
    line = "select week(k1) from %s order by k1,k2,k3,k4" % table_name
    runner.check(line)


def test_query_yearweek():
    """
    {
    "title": "test_query_datetime_function.test_query_yearweek",
    "describe": "返回指定日期的年份和周数,issues #5999"
    "tag": "function,p0"
    }
    """
    line = "select yearweek(k10), yearweek(k11) from %s order by k1,k2,k3,k4" % table_name
    runner.check(line)
    line = "select yearweek(k10, 0), yearweek(k10, 1), yearweek(k10, 2), yearweek(k10, 3), yearweek(k10, 4), \
            yearweek(k10, 5), yearweek(k10, 6), yearweek(k10, 7) from %s order by k1,k2,k3,k4" % table_name
    runner.check(line)
    line = "select yearweek(k11, 0), yearweek(k11, 1), yearweek(k11, 2), yearweek(k11, 3), yearweek(k11, 4), \
            yearweek(k11, 5), yearweek(k11, 6), yearweek(k11, 7) from %s order by k1,k2,k3,k4" % table_name
    runner.check(line)
    line = "select yearweek('19980202', 7), yearweek('19980101', 2), yearweek('19981231', 0)"
    runner.check(line)
    line = "select yearweek(k1) from %s order by k1,k2,k3,k4" % table_name
    runner.check(line)


def test_query_time_round():
    """
    {
    "title": "test_query_datetime_function.test_query_time_round",
    "describe": ,返回指定时间的上下界
    "tag": "function,p0"
    }
    """
    # year_floor
    line1 = "select year_floor('2021-09-02'), year_floor('2021-09-02 12:34:56'), year_floor('2021-09-02','2018-02-02'), \
            year_floor('2021-09-02', 5, '2018-02-02'), year_floor(cast('2021-09-02 12:34:56' as datetime), 6)"
    line2 = "select '2021-01-01 00:00:00', '2021-01-01 00:00:00', '2021-02-02 00:00:00', '2018-02-02 00:00:00', \
            '2018-01-01 00:00:00'"
    runner.check2(line1, line2)
    # year_ceil
    line1 = "select year_ceil('2021-09-02'), year_ceil('2021-09-02 12:34:56'), year_ceil('2021-09-02','2018-02-02'), \
            year_ceil('2021-09-02', 5, '2018-02-02'), year_ceil(cast('2021-09-02 12:34:56' as datetime), 6)"
    line2 = "select '2022-01-01 00:00:00', '2022-01-01 00:00:00', '2022-02-02 00:00:00', '2023-02-02 00:00:00' ,\
            '2024-01-01 00:00:00'"
    runner.check2(line1, line2)
    # month_floor
    line1 = "select month_floor('2021-09-02'), month_floor('2021-09-02 12:34:56'), month_floor('2021-09-02', \
            '2018-02-02'), month_floor('2021-09-02', 13, '2018-02-02'), month_floor(cast('2021-09-02 12:34:56' \
            as datetime), 3)"
    line2 = "select '2021-09-01 00:00:00', '2021-09-01 00:00:00', '2021-09-02 00:00:00', '2021-05-02 00:00:00', \
            '2021-07-01 00:00:00'"
    runner.check2(line1, line2)
    # month_ceil
    line1 = "select month_ceil('2021-09-02'), month_ceil('2021-09-02 12:34:56'), month_ceil('2021-09-02', \
            '2018-02-02'), month_ceil('2021-09-02', 13, '2018-02-02'), month_ceil(cast('2021-09-02 12:34:56' \
            as datetime), 3)"
    line2 = "select '2021-10-01 00:00:00', '2021-10-01 00:00:00', '2021-09-02 00:00:00', '2022-06-02 00:00:00', \
            '2021-10-01 00:00:00'"
    runner.check2(line1, line2)
    # week_floor
    line1 = "select week_floor('2021-09-02'), week_floor('2021-09-02 12:34:56'), week_floor('2021-09-02', \
            '2018-02-02'), week_floor('2021-09-02', 53, '2018-02-02'), week_floor(cast('2021-09-02 12:34:56' \
            as datetime), 22)"
    line2 = "select '2021-08-29 00:00:00', '2021-08-29 00:00:00', '2021-08-27 00:00:00', '2021-02-19 00:00:00', \
            '2021-06-13 00:00:00'"
    runner.check2(line1, line2)
    # week_ceil
    line1 = "select week_ceil('2021-09-02'), week_ceil('2021-09-02 12:34:56'), week_ceil('2021-09-02', \
            '2018-02-02'), week_ceil('2021-09-02', 53, '2018-02-02'), week_ceil(cast('2021-09-02 12:34:56' \
            as datetime), 22)"
    line2 = "select '2021-09-05 00:00:00', '2021-09-05 00:00:00', '2021-09-03 00:00:00', '2022-02-25 00:00:00', \
            '2021-11-14 00:00:00'"
    runner.check2(line1, line2)
    # day_floor
    line1 = "select day_floor('2021-09-02'), day_floor('2021-09-02 12:34:56'), day_floor('2021-09-02', \
            '2018-02-02 12:34:56'), day_floor('2021-09-02 12:34:56', 53, '2018-02-02 21:43:05'), \
            day_floor(cast('2021-09-02 12:34:56' as datetime), 5)"
    line2 = "select '2021-09-02 00:00:00', '2021-09-02 00:00:00', '2021-09-01 12:34:56', '2021-07-28 21:43:05', \
            '2021-08-31 00:00:00'"
    runner.check2(line1, line2)
    # day_ceil
    line1 = "select day_ceil('2021-09-02'), day_ceil('2021-09-02 12:34:56'), day_ceil('2021-09-02', \
            '2018-02-02 12:34:56'), day_ceil('2021-09-02 12:34:56', 53, '2018-02-02 21:43:05'), \
            day_ceil(cast('2021-09-02 12:34:56' as datetime), 5)"
    line2 = "select '2021-09-02 00:00:00', '2021-09-03 00:00:00', '2021-09-02 12:34:56', '2021-09-19 21:43:05', \
            '2021-09-05 00:00:00'"
    runner.check2(line1, line2)
    # hour_floor
    line1 = "select hour_floor('2021-09-02'), hour_floor('2021-09-02 12:34:56'), hour_floor('2021-09-02 21:43:05', \
            '2018-02-02 12:34:56'), hour_floor('2021-09-02 12:34:56', 25, '2018-02-02 21:43:05'), \
            hour_floor(cast('2021-09-02 12:34:56' as datetime), 25)"
    line2 = "select '2021-09-02 00:00:00', '2021-09-02 12:00:00', '2021-09-02 21:34:56', '2021-09-02 04:43:05', \
            '2021-09-01 21:00:00'"
    runner.check2(line1, line2)
    # hour_ceil
    line1 = "select hour_ceil('2021-09-02'), hour_ceil('2021-09-02 12:34:56'), hour_ceil('2021-09-02 21:43:05', \
            '2018-02-02 12:34:56'), hour_ceil('2021-09-02 12:34:56', 25, '2018-02-02 21:43:05'), \
            hour_ceil(cast('2021-09-02 12:34:56' as datetime), 25)"
    line2 = "select '2021-09-02 00:00:00', '2021-09-02 13:00:00', '2021-09-02 22:34:56', '2021-09-03 05:43:05', \
            '2021-09-02 22:00:00'"
    runner.check2(line1, line2)
    # minute_floor
    line1 = "select minute_floor('2021-09-02'), minute_floor('2021-09-02 12:34:56'), minute_floor('2021-09-02 21:43:05', \
            '2018-02-02 12:34:56'), minute_floor('2021-09-02 12:34:56', 61, '2018-02-02 21:43:05'), \
            minute_floor(cast('2021-09-02 12:34:56' as datetime), 61)"
    line2 = "select '2021-09-02 00:00:00', '2021-09-02 12:34:00', '2021-09-02 21:42:56', '2021-09-02 12:11:05', \
            '2021-09-02 12:15:00'"
    runner.check2(line1, line2)
    # minute_ceil
    line1 = "select minute_ceil('2021-09-02'), minute_ceil('2021-09-02 12:34:56'), minute_ceil('2021-09-02 21:43:05', \
            '2018-02-02 12:34:56'), minute_ceil('2021-09-02 12:34:56', 61, '2018-02-02 21:43:05'), \
            minute_ceil(cast('2021-09-02 12:34:56' as datetime), 61)"
    line2 = "select '2021-09-02 00:00:00', '2021-09-02 12:35:00', '2021-09-02 21:43:56', '2021-09-02 13:12:05', \
            '2021-09-02 13:16:00'"
    runner.check2(line1, line2)
    # second_floor
    line1 = "select second_floor('2021-09-02'), second_floor('2021-09-02 12:34:56'), second_floor('2021-09-02 21:43:05', \
            '2018-02-02 12:34:56'), second_floor('2021-09-02 12:34:56', 61, '2018-02-02 21:43:05'), \
            second_floor(cast('2021-09-02 12:34:56' as datetime), 61)"
    line2 = "select '2021-09-02 00:00:00', '2021-09-02 12:34:56', '2021-09-02 21:43:05', '2021-09-02 12:34:28', \
            '2021-09-02 12:34:19'"
    runner.check2(line1, line2)
    # second_ceil
    line1 = "select second_ceil('2021-09-02'), second_ceil('2021-09-02 12:34:56'), second_ceil('2021-09-02 21:43:05', \
            '2018-02-02 12:34:56'), second_ceil('2021-09-02 12:34:56', 61, '2018-02-02 21:43:05'), \
            second_ceil(cast('2021-09-02 12:34:56' as datetime), 61)"
    line2 = "select '2021-09-02 00:00:00', '2021-09-02 12:34:56', '2021-09-02 21:43:05', '2021-09-02 12:35:29', \
            '2021-09-02 12:35:20'"
    runner.check2(line1, line2)
        

def teardown_module():
    """
    end 
    """
    print("End")
    # mysql_cursor.close()
    # mysql_con.close()


if __name__ == "__main__":
    print("test")
    setup_module()
    test_query_date_dayofweek()
    test_query_date_timediff()
    test_query_date_utc_timestamp()
