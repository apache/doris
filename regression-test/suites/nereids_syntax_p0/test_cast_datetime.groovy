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

import java.sql.Date
import java.time.LocalDateTime

suite("test_cast_datetime") {

    sql "drop table if exists casttbl"
    sql """CREATE TABLE casttbl ( 
        a int null,
        mydate date NULL,
        mydatev2 DATEV2 null,
        mydatetime datetime null,
        mydatetimev2 datetimev2 null
    ) ENGINE=OLAP
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`a`) BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "in_memory" = "false",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false"
    );
    """

    sql "insert into casttbl values (1, '2000-01-01', '2000-01-01 12:12:12', '2000-01-01', '2000-01-01 12:12:12');"

    sql "set enable_nereids_planner=true;"
    sql "set enable_fallback_to_original_planner=false"

//when BE storage support 'Date < Date', we should remove this case
//currently, if we rewrite expr to CAST(mydatetime AS DATE) < date '2019-06-01', BE returns 0 tuple. 
    qt_1 "select count(1) from casttbl where CAST(CAST(mydatetime AS DATE) AS DATETIME) < date '2019-06-01';"

    sql "insert into casttbl values(2, '', '', '', ''), (3, '2020', '2020', '2020', '2020')"
    qt_2 "select * from casttbl"
    qt_3 "select a, '' = mydate, '' = mydatev2, '' = mydatetime, '' = mydatetimev2 from casttbl"

    def wrong_date_strs = [
        "date '2020-01'",
        "datev1 '2020-01'",
        "datev2 '2020-01'",
        "timestamp '2020-01'",
        "'' > date '2019-06-01'",
        "'' > date_sub('2019-06-01', -10)",
        "'' > cast('2019-06-01 00:00:00' as datetime)",
        "date_add('', 1)",
        "date_add('2020',1)",
        "date_add('08-09',1)",
        "date_add('abcd',1)",
        "date_add('2020-15-20',1)",
        "date_add('2020-10-32',1)",
        "date_add('2021-02-29',1)",
        "date_add('99999-10-10',1)",
        "date_add('10-30',1)",
        "date_add('10-30 10:10:10',1)",
        "date_add('2020-01 00:00:00', 1)",
        "MICROSECOND('invalid_time')",
        "MICROSECOND('12.34.56.123456')",
        "MICROSECOND('12:34:56')",
        "MICROSECOND('12:34:56.1234')",
        "MICROSECOND('12345')",
        "MICROSECOND('12:34:56.1')",
        "MICROSECOND('12:34:56.01')",
        "MICROSECOND('12:34:56.abcdef')",
        "MICROSECOND('NaN')",
        "MonthName('abcd-ef-gh')",
        "DATE('2023-02-28 24:00:00')",
        "DATE('2023-02-28 23:59:60')",
        "DATE('170141183460469231731687303715884105727')",
        "DATE('１月１日')",
        "DATE('１２３４５６７８')",
        "DATE('2023-1-32')",
        "DATE('1-1-2023')",
        "DATE('January 32, 2023')",
        "DATE('February 29, 2023')",
        "DATE('April 31, 2023')",
        "DATE('02/29/2023')",
        "DATE('13/01/2023')",
        "DATEV2('20230229')",
        "DATEV2('abc')",
        "DATEV2('日本語')",
        "DATEV2('ññó')",
        "DATEV2('')",
        "DATEV2(' ')",
        "DATEV2('12:34:56')",
        "DATEV2('NaN')",
        "DATEV2('infinity')",
        "DATEV2('\$2023')",
        "DATEV2('0xFF')",
        "DATEV2('123,456')",
        "DATEV2('１２３４５')",
        "DATEV2('1.2e+3')",
        "DATEV2(' -2023 ')",
        "DATEV2('3.1415π')",
        "DATEV2('12/31/2023')",
        "DATEV2('32-01-2023')",
        "DATEV2('2023/02/29')",
        "DATEV2('1.7976931348623157E308')",
        "DATEV2('1E-30')",
        "DATEV2('true')",
        "DATEV2('false')",
        "DATEV2('NULL')",
        "DATEV2('123LATIN')",
        "DATEV2('0x2023')",
        "DATEV2('123.45test')",
        "DATEV2('2023-W50-5')",
        "DATEV2('2023-367')",
        "DATEV2('3.14.15')",
        "DATEV2('+-2023')",
        "DATEV2('１２３.４５')",
        "DATEV2('2023-02-28 25:00')",
        "DATEV2('2023-02-29T00:00:00')",
        // "DATEV2('0000-00-00 00:00:00')", depends on be conf.
        "DATEV2('January 32, 2023')",
        "DATEV2('February 29, 2023')",
        "DATEV2('April 31, 2023')",
        "DATEV2('13/01/2023')",
        "DATEV2('2023-1-32')",
        "DATEV2('1-1-2023')",
        "DATEV2('１月１日')",
        "DATEV2('１２３４５６７８')",
        "DATEV2('2023-02-28 24:00:00')",
        "DATEV2('2023-02-28 23:59:60')",
        "DATEV2('170141183460469231731687303715884105727')",
        "DATEV2('0')",
        "DATEV2('123456789012345678901234567890')",
        "DATEV2('１２３４５６７８９０')",
        "QUARTER('20230229')",
        "QUARTER('abc')",
        "QUARTER('日本語')",
        "QUARTER('ññó')",
        "QUARTER('')",
        "QUARTER(' ')",
        "QUARTER('12:34:56')",
        "QUARTER('NaN')",
        "QUARTER('infinity')",
        "QUARTER('\$2023')",
        "QUARTER('0xFF')",
        "QUARTER('123,456')",
        "QUARTER('１２３４５')",
        "QUARTER('1.2e+3')",
        "QUARTER(' -2023 ')",
        "QUARTER('3.1415π')",
        "QUARTER('12/31/2023')",
        "QUARTER('32-01-2023')",
        "QUARTER('2023/02/29')",
        "QUARTER('1.7976931348623157E308')",
        "QUARTER('1E-30')",
        "QUARTER('true')",
        "QUARTER('false')",
        "QUARTER('NULL')",
        "QUARTER('123LATIN')",
        "QUARTER('0x2023')",
        "QUARTER('123.45test')",
        "QUARTER('2023-W50-5')",
        "QUARTER('2023-367')",
        "QUARTER('3.14.15')",
        "QUARTER('+-2023')",
        "QUARTER('１２３.４５')",
        "QUARTER('2023-02-28 25:00')",
        "QUARTER('10000-01-01')",
        "QUARTER('January 32, 2023')",
        "QUARTER('February 29, 2023')",
        "QUARTER('April 31, 2023')",
        "QUARTER('13/01/2023')",
        "QUARTER('１月１日')",
        "QUARTER('170141183460469231731687303715884105727')",
        "QUARTER('0')",
        "QUARTER('123456789012345678901234567890')",
        "QUARTER('999999999999999999999999999999-99-99')",
        "QUARTER('１２３４５６７８９０')",
        "QUARTER('2023-02-28 24:00:00')",
        "QUARTER('2023-02-28 23:59:60')",
        "QUARTER('2023-1-32')",
        "QUARTER('1-1-2023')",
        "QUARTER('9999999999999999-99-99')",
        "QUARTER('123.456.789')",
        "QUARTER('+-1.57')",
        "QUARTER('0x1A')",
        "QUARTER('3π/2')",
        "QUARTER('2023-02-29T00:00:00')",
        "QUARTER('2023年七月十五日')",
        "QUARTER('１２３４５六七八')",
        "QUARTER('1/0')",
        "QUARTER('Q4-2023')",
        "QUARTER('2023-Q4')",
        "QUARTER('2023Q4')",
        "WEEK('invalid_date')",
        "WEEK('12:34:56.789')",
        "WEEK('')",
        "WEEK('2023-W30-1')",
        "WEEK('2023-06-15', WEEK('invalid_date'))",
        "YEAR('20230229')",
        "YEAR('abc')",
        "YEAR('日本語')",
        "YEAR('ññó')",
        "YEAR('')",
        "YEAR(' ')",
        "YEAR('12:34:56')",
        "YEAR('NaN')",
        "YEAR('infinity')",
        "YEAR('\$2023')",
        "YEAR('0xFF')",
        "YEAR('123,456')",
        "YEAR('１２３４５')",
        "YEAR('1.2e+3')",
        "YEAR(' -2023 ')",
        "YEAR('3.1415π')",
        "YEAR('12/31/2023')",
        "YEAR('32-01-2023')",
        "YEAR('2023/02/29')",
        "YEAR('1.7976931348623157E308')",
        "YEAR('1E-30')",
        "YEAR('true')",
        "YEAR('false')",
        "YEAR('NULL')",
        "YEAR('123LATIN')",
        "YEAR('0x2023')",
        "YEAR('123.45test')",
        "YEAR('2023-W50-5')",
        "YEAR('2023-367')",
        "YEAR('3.14.15')",
        "YEAR('+-2023')",
        "YEAR('１２３.４５')",
        // "YEAR('2023-02-28 25:00')",
        "YEAR('10000-01-01')",
        "YEAR('January 32, 2023')",
        "YEAR('February 29, 2023')",
        "YEAR('April 31, 2023')",
        "YEAR('13/01/2023')",
        "YEAR('１月１日')",
        "YEAR('170141183460469231731687303715884105727')",
        "YEAR('0')",
        "YEAR('123456789012345678901234567890')",
        "YEAR('999999999999999999999999999999-99-99')",
        "YEAR('１２３４５６７８９０')",
        // "YEAR('2023-02-28 24:00:00')",
        // "YEAR('2023-02-28 23:59:60')",
        "YEAR('2023-1-32')",
        "YEAR('1-1-2023')",
        "YEAR('9999999999999999-99-99')",
        "YEAR('123.456.789')",
        "YEAR('+-1.57')",
        "YEAR('0x1A')",
        "YEAR('3π/2')",
        "YEAR('2023-02-29T00:00:00')",
        "DATE_TRUNC('1st Jun 2007 14:15:20', 'second')",
        "DATE_TRUNC('1st Jun 2007 14:15:20', 'minute')",
        "DATE_TRUNC('1st Jun 2007 14:15:20', 'hour')",
        "DATE_TRUNC('1st Jun 2007', 'day')",
        "DATE_TRUNC('1st Jun 2007', 'week')",
        "DATE_TRUNC('1st Jun 2007', 'month')",
        "DATE_TRUNC('1st Jun 2007', 'quarter')",
        "DATE_TRUNC('1st Jun 2007', 'year')",
        "DATE_TRUNC('15th Dec 2012 20:30:40', 'second')",
        "DATE_TRUNC('15th Dec 2012 20:30:40', 'minute')",
        "DATE_TRUNC('15th Dec 2012 20:30:40', 'hour')",
        "DATE_TRUNC('15th Dec 2012', 'day')",
        "DATE_TRUNC('15th Dec 2012', 'week')",
        "DATE_TRUNC('15th Dec 2012', 'month')",
        "DATE_TRUNC('15th Dec 2012', 'quarter')",
        "DATE_TRUNC('15th Dec 2012', 'year')",
        "DATE_TRUNC('22nd Mar 2020 07:55:05', 'second')",
        "DATE_TRUNC('22nd Mar 2020 07:55:05', 'minute')",
        "DATE_TRUNC('22nd Mar 2020 07:55:05', 'hour')",
        "DATE_TRUNC('22nd Mar 2020', 'day')",
        "DATE_TRUNC('22nd Mar 2020', 'week')",
        "DATE_TRUNC('22nd Mar 2020', 'month')",
        "DATE_TRUNC('22nd Mar 2020', 'quarter')",
        "DATE_TRUNC('22nd Mar 2020', 'year')",
        "DATE_TRUNC('2021-02-29 11:25:35', 'second')",
        "DATE_TRUNC('2021-02-29 11:25:35', 'minute')",
        "DATE_TRUNC('2021-02-29 11:25:35', 'hour')",
        "DATE_TRUNC('2023/04/31 18:40:10', 'second')",
        "DATE_TRUNC('2023/04/31 18:40:10', 'minute')",
        "DATE_TRUNC('2023/04/31 18:40:10', 'hour')",
        "DATE_TRUNC('2023/04/31', 'day')",
        "DATE_TRUNC('2023/04/31', 'week')",
        "DATE_TRUNC('2023/04/31', 'month')",
        "DATE_TRUNC('2023/04/31', 'quarter')",
        "DATE_TRUNC('2023/04/31', 'year')",
        "WEEKDAY('invalid_date')",
        "WEEKDAY('12:34:56.789')",
        "WEEKDAY('')",
        "WEEKDAY('2023-W40-1')",
        "WEEKDAY('10-Oct-2023')",
        "WEEKDAY('October 10, 2023')",
        "WEEKOFYEAR('invalid_date')",
        "weekofyear('12:34:56.789')",
        "weekofyear('')",
        "weekofyear('2023-W30-1')",
        "YEARWEEK('invalid_date')",
        "yearweek('12:34:56.789')",
        "yearweek('')",
        "yearweek('2023-W30-1')",
        "YEARWEEK('2023-06-15', WEEK('invalid_date'))",
        "yearweek('2023-06-15', WEEK('invalid_date'))",
        "YEARWEEK('2023-06-15', WEEK('invalid_date'))",
        "yearweek('2023-06-15', WEEK('invalid_date'))",
        "HOUR('2024-01-01 12:00:00+')",
        "timestamp('2023-02-29 14:30:00')",
        "timestamp('1999-04-31 08:15:00')",
        "timestamp('2000-00-00 00:00:00')",
        "timestamp('1st Jun 2007 09:45:30')",
        "timestamp('三〇〇〇-一-一')",
        "timestamp('31/04/2022 16:20')",
        "TIMESTAMP('2023/04/31')",
        "TO_DATE('1st Jun 2007')",
        "TO_DATE('15th Dec 2012')",
        "TO_DATE('22nd Mar 2020')",
        "TO_DATE('2023/04/31')",
        "TO_DATE('1st Jun 2007 11:15:00')",
        "TO_DATE('15th Dec 2012 17:30:00')",
        "TO_DATE('22nd Mar 2020 05:45:00')",
        "TO_DATE('2024-04-31 10:30:45')",
        "TO_DATE('1st Jun 2007 14:20')",
        "TO_DATE('31/12/1999')",
        "TO_DATE('2025-13-01 00:00')",
        "TO_DATE('2007-Jun-1st')",
        "TO_DATE('12-31-1999 23:59')",
        "TO_DATE('30-Feb-2023')",
        "TO_DATE('2026-02-28 24:00:00')",
        "to_monday('1st Jun 2007')",
        "to_monday('15th Dec 2012')",
        "to_monday('22nd Mar 2020')",
        "to_monday('2023/04/31')",
        "to_monday('2023-2-29')",
        "to_monday('31st Apr 1999')",
        "to_monday('0th Mar 2025')",
        "to_monday('2025-04-31 08:15:00')",
        "to_monday('1st Jun 2007')",
        "LAST_DAY('1st Jun 2007')",
        "LAST_DAY('15th Dec 2012')",
        "LAST_DAY('22nd Mar 2020')",
        "LAST_DAY('2023/04/31')",
        "LAST_DAY('1st Jun 2007')",
        "LAST_DAY('07/20/1969')",
        "LAST_DAY('15-May-1999')",
        "LAST_DAY('31十二月2023')",
        "LAST_DAY('29-Feb-2023')",
        "LAST_DAY('2023/04/31')",
        "LAST_DAY('0-0-0')",
        "TO_DAYS('1st Jun 2007')",
        "TO_DAYS('15th Dec 2012')",
        "TO_DAYS('22nd Mar 2020')",
        "TO_DAYS('2023/04/31')",
        "TO_DAYS('1st Jun 2007 11:15:00')",
        "TO_DAYS('15th Dec 2012 17:30:00')",
        "TO_DAYS('22nd Mar 2020 05:45:00')",
        "TO_DAYS('1st Jun 2007')",
        "TO_DAYS('3rd Mar 1990')",
        "TO_DAYS('20230431')",
        "TO_DAYS('2007-Jun-01')",
        "TO_DAYS('2007/Jun/01')",
        "TO_DAYS('31-Apr-2023')",
        "FROM_DAYS(TO_DAYS('1st Jun 2007'))",
        "FROM_DAYS(TO_DAYS('15th Dec 2012'))",
        "FROM_DAYS(TO_DAYS('22nd Mar 2020'))",
        "FROM_DAYS(TO_DAYS('2023/04/31'))",
    ]

    def hour_strs = [
        "HOUR('2024-01-01 12:00:00')",
        "HOUR('2024-01-01 12:00:00.')",
    ]
    def invalid_hour_strs = [
        "HOUR('2024-01-01 12:00:00:')",
        "HOUR('2024-01-01 12:00:00\\\"')",
        "HOUR('2024-01-01 12:00:00\\'')",
        "HOUR('2024-01-01 12:00:00<')",
        "HOUR('2024-01-01 12:00:00>')",
        "HOUR('2024-01-01 12:00:00,')",
        "HOUR('2024-01-01 12:00:00?')",
        "HOUR('2024-01-01 12:00:00/')",
        "HOUR('2024-01-01 12:00:00!')",
        "HOUR('2024-01-01 12:00:00@')",
        "HOUR('2024-01-01 12:00:00#')",
        "HOUR('2024-01-01 12:00:00\$')",
        "HOUR('2024-01-01 12:00:00%')",
        "HOUR('2024-01-01 12:00:00^')",
        "HOUR('2024-01-01 12:00:00&')",
        "HOUR('2024-01-01 12:00:00*')",
        "HOUR('2024-01-01 12:00:00(')",
        "HOUR('2024-01-01 12:00:00)')",
        "HOUR('2024-01-01 12:00:00-')",
        "HOUR('2024-01-01 12:00:00_')",
        "HOUR('2024-01-01 12:00:00=')",
        "HOUR('2024-01-01 12:00:00[')",
        "HOUR('2024-01-01 12:00:00]')",
        "HOUR('2024-01-01 12:00:00{')",
        "HOUR('2024-01-01 12:00:00}')",
        "HOUR('2024-01-01 12:00:00|')",
        "HOUR('2024-01-01 12:00:00;')",
        "HOUR('2024-01-01 12:00:00:')",
        "HOUR('2024-01-01 12:00:00:')",
        "HOUR('2024-01-01 12:00:00\\'')",
        "HOUR('2024-01-01 12:00:00<')",
        "HOUR('2024-01-01 12:00:00>')",
        "HOUR('2024-01-01 12:00:00,')",
        "HOUR('2024-01-01 12:00:00?')",
        "HOUR('2024-01-01 12:00:00/')",
    ]

    for (def val : [true, false]) {
        sql "set debug_skip_fold_constant = ${val}"

        for (def s : wrong_date_strs) {
            test {
                sql "SELECT ${s}"
                result([[null]])
            }
        }

        for (def s : hour_strs) {
            test {
                sql "SELECT ${s}"
                result([[12]])
            }
        }
        for (def s : invalid_hour_strs) {
            test {
                sql "SELECT ${s}"
                result([[null]])
            }
        }

        test {
            sql "select cast('123.123' as date)"
            result([[null]])
        }

        /*
        test {
            sql "select DATE('2023年01月01日')"
            result([[Date.valueOf('2023-01-01')]])
        }

        test {
            sql "select DATEV2('2023年01月01日')"
            result([[Date.valueOf('2023-01-01')]])
        }

        test {
            sql "select QUARTER('2023年01月01日')"
            result([[1]])
        }

        test {
            sql "select YEAR('2023年01月01日')"
            result([[2023]])
        }
        */

        test {
            sql "select to_monday('1970-01-04')"
            result([[Date.valueOf('1970-01-01')]])
        }

        test {
            sql "select to_monday('1969-12-31')"
            result([[Date.valueOf('1969-12-29')]])
        }

        test {
            sql "select to_monday('1969-12-31 23:59:59.999999')"
            result([[Date.valueOf('1969-12-29')]])
        }

        // to_monday(1970-01-01 ~ 170-01-04) will return 1970-01-01
        for (def s : ['1970-01-01', '1970-01-01 00:00:00.000001', '1970-01-02 12:00:00', '1970-01-01', '1970-01-04 23:59:59.999999']) {
            test {
                sql "select to_monday('${s}')"
                result([[Date.valueOf('1970-01-01')]])
            }
        }

        test {
            sql "select to_monday('1970-01-05')"
            result([[Date.valueOf('1970-01-05')]])
        }

        test {
            sql "select to_monday('1970-01-05 00:00:00.000001')"
            result([[Date.valueOf('1970-01-05')]])
        }

        test {
            sql "select cast('123.123' as datetime)"
            result([[null]])
        }

        test {
            sql "select cast('123.123.123' as datetime)"
            result([[null]])
        }

        /*
        test {
            sql "SELECT DATEADD(DAY, 1, '2025年06月20日')"
            result([[LocalDateTime.parse('2025-06-21T00:00:00')]])
        }
        */

        test {
            sql "select date_add('2023-02-28 16:00:00 UTC', INTERVAL 1 DAY)"
            result([[LocalDateTime.parse('2023-03-02T00:00:00')]])
        }

        test {
            sql "select date_add('2023-02-28 16:00:00 America/New_York', INTERVAL 1 DAY)"
            result([[LocalDateTime.parse('2023-03-02T05:00:00')]])
        }

        test {
            sql "select date_add('2023-02-28 16:00:00UTC', INTERVAL 1 DAY)"
            result([[LocalDateTime.parse('2023-03-02T00:00:00')]])
        }

        test {
            sql "select date_add('2023-02-28 16:00:00America/New_York', INTERVAL 1 DAY)"
            result([[LocalDateTime.parse('2023-03-02T05:00:00')]])
        }

        /*
        test {
            sql "select date_add('2023-02-28 UTC', INTERVAL 1 DAY)"
            result([[LocalDateTime.parse('2023-03-01T00:00:00')]])
        }

        test {
            sql "select date_add('2023-02-28 America/New_York', INTERVAL 1 DAY)"
            result([[LocalDateTime.parse('2023-03-01T00:00:00')]])
        }

        test {
            sql "select date_add('2023-02-28UTC', INTERVAL 1 DAY)"
            result([[LocalDateTime.parse('2023-03-01T00:00:00')]])
        }

        test {
            sql "select date_add('2023-02-28America/New_York', INTERVAL 1 DAY)"
            result([[LocalDateTime.parse('2023-03-01T00:00:00')]])
        }

        test {
            sql "select date_add('2023-02-28 UTC', INTERVAL 1 DAY)"
            result([[LocalDateTime.parse('2023-03-01T00:00:00')]])
        }

        test {
            sql "select date_add('2023-02-28 America/New_York', INTERVAL 1 DAY)"
            result([[LocalDateTime.parse('2023-03-01T00:00:00')]])
        }

        test {
            sql "select date_add('2024-02-29 UTC', INTERVAL 6 DAY)"
            result([[LocalDateTime.parse('2024-03-06T00:00:00')]])
        }

        test {
            sql "select date_add('2020-02-29 America/New_York', INTERVAL -3 DAY)"
            result([[LocalDateTime.parse('2020-02-26T00:00:00')]])
        }
        */

        test {
            sql "select date_add('2023-03-12 01:30:00 Europe/London', INTERVAL 1 DAY)"
            result([[LocalDateTime.parse('2023-03-13T09:30')]])
        }

        test {
            sql "select date_add('2023-11-05 01:30:00 America/New_York', INTERVAL 1 DAY)"
            result([[LocalDateTime.parse('2023-11-06T13:30')]])
        }

        /*
        test {
            sql "select date '2025年1月20日'"
            result([[Date.valueOf('2025-01-20')]])
        }

        test {
            sql "select timestamp '2025年1月20日10时20分5秒'"
            result([[LocalDateTime.parse('2025-01-20T10:20:05')]])
        }
        */
    }
}
