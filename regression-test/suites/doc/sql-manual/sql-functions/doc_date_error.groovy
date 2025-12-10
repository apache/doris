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

suite("doc_date_error") {
    // CURRENT_TIMESTAMP scale out of range
    test {
        sql """select CURRENT_TIMESTAMP(-1);"""
        exception "Scale of Datetime/Time must between 0 and 6. Scale was set to: -1"
    }
    test {
        sql """select CURRENT_TIMESTAMP(7);"""
        exception "Scale of Datetime/Time must between 0 and 6. Scale was set to: 7"
    }

    // Result not in valid date range [0000,9999]
    test {
        sql """select DATE_ADD('0001-01-28', INTERVAL -2 YEAR);"""
        exception "Operation year_add of 0001-01-28, -2 out of range"
    }
    test {
        sql """select DATE_ADD('9999-01-28', INTERVAL 2 YEAR);"""
        exception "Operation year_add of 9999-01-28, 2 out of range"
    }
    test {
        sql """select DATE_ADD('2023-12-31 23:00:00', INTERVAL 2 sa);"""
        exception "mismatched input 'sa' expecting"
    }

    // date_ceil out of range
    test {
        sql """select date_ceil("9999-07-13",interval 5 year);"""
        exception "Operation year_ceil of 9999-07-13 00:00:00, 5, 0001-01-01 00:00:00 out of range"
    }

    // Invalid period (negative)
    test {
        sql """select date_ceil("2023-01-13 22:28:18",interval -5 month);"""
        exception "Operation month_ceil of 2023-01-13 22:28:18, -5, 0001-01-01 00:00:00 out of range"
    }


    // // Result string exceeds function length limit
    // test {
    //     sql """select date_format('2022-01-12',repeat('a',129));"""
    //     exception "Operation date_format of invalid or oversized format is invalid"
    // }

    // Exceeds minimum date
    test {
        sql """select date_sub('0000-01-01', INTERVAL 1 DAY);"""
        exception "Operation day_add of 0000-01-01, -1 out of range"
    }
    test {
        sql """select date_sub('9999-01-01', INTERVAL -1 YEAR);"""
        exception "Operation year_add of 9999-01-01, 1 out of range"
    }

    // date_trunc unsupported time unit
    test {
        sql """select date_trunc('2010-12-02 19:28:30', 'quar');"""
        exception "date_trunc function time unit param only support argument is year|quarter|month|week|day|hour|minute|second"
    }

    // day_ceil period is negative
    test {
        sql """select day_ceil("2023-07-13 22:28:18", -2);"""
        exception "Operation day_ceil of 2023-07-13 22:28:18, -2 out of range"
    }

    // day_ceil result exceeds max date
    test {
        sql """select day_ceil("9999-12-31", 5);"""
        exception "Operation day_ceil of 9999-12-31 00:00:00, 5 out of range"
    }

    // day_floor period is negative
    test {
        sql """select day_floor("2023-07-13 22:28:18", -2);"""
        exception "Operation day_floor of 2023-07-13 22:28:18, -2 out of range"
    }

    // from_days input is negative
    test {
        sql """select from_days(-60);"""
        exception "Operation from_days of -60 out of range"
    }

    // from_days days out of valid range
    test {
        sql """select from_days(99999999);"""
        exception "Operation from_days of 99999999 out of range"
    }

    // from_microsecond input is negative
    test {
        sql """select from_microsecond(-1);"""
        exception "Operation from_microsecond of -1 out of range"
    }

    // from_microsecond exceeds max time range
    test {
        sql """select from_microsecond(999999999999999999);"""
        exception "Operation from_microsecond of 999999999999999999 out of range"
    }

    // from_millisecond input is negative
    test {
        sql """select from_millisecond(-1);"""
        exception "Operation from_millisecond of -1 out of range"
    }

    // from_millisecond exceeds max date
    test {
        sql """select from_millisecond(999999999999999999);"""
        exception "Operation from_millisecond of 999999999999999999 out of range"
    }

    // from_second input is negative
    test {
        sql """select from_second(-1);"""
        exception "Operation from_second of -1 out of range"
    }

    // from_second exceeds max date range
    test {
        sql """select from_second(999999999999999);"""
        exception "Operation from_second of 999999999999999 out of range"
    }

    // from_unixtime exceeds max range
    test {
        sql """select from_unixtime(253402281999);"""
        exception "Operation from_unixtime_new of 253402281999, yyyy-MM-dd HH:mm:ss is invalid"
    }

    // hour_ceil result exceeds max datetime range
    test {
        sql """select hour_ceil("9999-12-31 22:28:18", 6);"""
        exception "Operation hour_ceil of 9999-12-31 22:28:18, 6 out of range"
    }

    // hour_ceil period <= 0
    test {
        sql """select hour_ceil("2023-07-13 22:28:18", 0);"""
        exception "Operation hour_ceil of 2023-07-13 22:28:18, 0 out of range"
    }

    // hour_floor period is negative
    test {
        sql """select hour_floor('2023-12-31 23:59:59', -3);"""
        exception "Operation hour_floor of 2023-12-31 23:59:59, -3 out of range"
    }

    // hours_add out of datetime range
    test {
        sql """select hours_add('9999-12-31 23:59:59', 2);"""
        exception "Operation hour_add of 9999-12-31 23:59:59, 2 out of range"
    }
    test {
        sql """select hours_add('0000-01-01',-2);"""
        exception "Operation hour_add of 0000-01-01 00:00:00, -2 out of range"
    }

    // hours_sub out of datetime range
    test {
        sql """select hours_sub('9999-12-31 12:00:00', -20);"""
        exception "Operation hour_add of 9999-12-31 12:00:00, 20 out of range"
    }
    test {
        sql """select hours_sub('0000-01-01 12:00:00', 20);"""
        exception "Operation hour_add of 0000-01-01 12:00:00, -20 out of range"
    }

    // LOCALTIME scale out of range
    test {
        sql """select LOCALTIME(-1);"""
        exception "Scale of Datetime/Time must between 0 and 6. Scale was set to: -1"
    }
    test {
        sql """select LOCALTIME(7);"""
        exception "Scale of Datetime/Time must between 0 and 6. Scale was set to: 7"
    }

    // MAKEDATE year out of range
    test {
        sql """SELECT MAKEDATE(9999, 366);"""
        exception "Operation makedate of 9999, 366 out of range"
    }

    // MICROSECONDS_ADD result out of range
    test {
        sql """SELECT MICROSECONDS_ADD('9999-12-31 23:59:59.999999', 2000000) AS after_add;"""
        exception "Operation microsecond_add of 9999-12-31 23:59:59.999999, 2000000 out of range"
    }

    // MICROSECONDS_SUB result out of range
    test {
        sql """SELECT MICROSECONDS_SUB('0000-01-01 00:00:00.000000', 1000000) AS after_sub;"""
        exception "Operation microsecond_add of 0000-01-01 00:00:00, -1000000 out of range"
    }

    // MILLISECONDS_ADD result out of range
    test {
        sql """SELECT MILLISECONDS_ADD('9999-12-31 23:59:59.999', 2000) AS after_add;"""
        exception "Operation millisecond_add of 9999-12-31 23:59:59.999000, 2000 out of range"
    }

    // MILLISECONDS_SUB result out of range
    test {
        sql """SELECT MILLISECONDS_SUB('0000-01-01', 1500);"""
        exception "Operation millisecond_add of 0000-01-01 00:00:00, -1500 out of range"
    }

    // minute_ceil result exceeds max datetime
    test {
        sql """select minute_ceil("9999-12-31 23:59:18", 6);"""
        exception "Operation minute_ceil of 9999-12-31 23:59:18, 6 out of range"
    }

    // minute_ceil period is non-positive
    test {
        sql """SELECT MINUTE_CEIL('2023-07-13 22:28:18', -5) AS result;"""
        exception "Operation minute_ceil of 2023-07-13 22:28:18, -5 out of range"
    }

    // MINUTES_ADD result out of range
    test {
        sql """SELECT MINUTES_ADD('9999-12-31 23:59:59', 2) AS result;"""
        exception "Operation minute_add of 9999-12-31 23:59:59, 2 out of range"
    }

    // MINUTES_SUB result out of range
    test {
        sql """SELECT MINUTES_SUB('0000-01-01 00:00:00', 1) AS result;"""
        exception "Operation minute_add of 0000-01-01 00:00:00, -1 out of range"
    }

    // month_ceil result exceeds max date
    test {
        sql """SELECT MONTH_CEIL('9999-12-13 22:28:18',5) AS result;"""
        exception "Operation month_ceil of 9999-12-13 22:28:18, 5 out of range"
    }

    // month_ceil period is non-positive
    test {
        sql """SELECT MONTH_CEIL('2023-07-13 22:28:18', -5) AS result;"""
        exception "Operation month_ceil of 2023-07-13 22:28:18, -5 out of range"
    }

    // minute_floor period is non-positive
    test {
        sql """SELECT MINUTE_FLOOR('2023-07-13 22:28:18', -5) AS result;"""
        exception "Operation minute_floor of 2023-07-13 22:28:18, -5 out of range"
    }

    // MONTHS_ADD result out of range
    test {
        sql """SELECT MONTHS_ADD('9999-12-31', 1) AS result;"""
        exception "Operation month_add of 9999-12-31, 1 out of range"
    }
    test {
        sql """SELECT MONTHS_ADD('0000-01-01',- 1) AS result;"""
        exception "Operation month_add of 0000-01-01, -1 out of range"
    }

    // MONTHS_SUB result out of range
    test {
        sql """SELECT MONTHS_SUB('0000-01-01', 1) AS result;"""
        exception "Operation month_add of 0000-01-01, -1 out of range"
    }
    test {
        sql """SELECT MONTHS_SUB('9999-12-31', -1) AS result;"""
        exception "Operation month_add of 9999-12-31, 1 out of range"
    }

    // NEXT_DAY invalid weekday
    test {
        sql """SELECT NEXT_DAY('2023-07-13', 'ABC') AS result;"""
        exception "Function next_day failed to parse weekday: ABC"
    }

    // NOW invalid precision
    test {
        sql """SELECT NOW(7) AS result;"""
        exception "Scale of Datetime/Time must between 0 and 6. Scale was set to: 7"
    }
    test {
        sql """select NOW(-1);"""
        exception "Scale of Datetime/Time must between 0 and 6. Scale was set to: -1"
    }

    // QUARTERS_ADD result out of range
    test {
        sql """SELECT QUARTERS_ADD('9999-10-31', 2) AS result;"""
        exception "Operation month_add of 9999-10-31, 6 out of range"
    }
    test {
        sql """SELECT QUARTERS_ADD('0000-01-01',-2) AS result;"""
        exception "month_add of 0000-01-01, -6 out of range"
    }

    // // QUARTERS_SUB result out of range
    // test {
    //     sql """SELECT QUARTERS_SUB('0000-02-30', 1) AS result;"""
    //     exception "Operation quarter_sub of 0000-04-30, 1 out of range"
    // }
    test {
        sql """SELECT QUARTERS_SUB('9999-12-31', -1) AS result;"""
        exception "Operation month_add of 9999-12-31, 3 out of range"
    }

    // SECOND_CEIL result exceeds max datetime
    test {
        sql """SELECT SECOND_CEIL('9999-12-31 23:59:59', 2) AS result;"""
        exception "Operation second_ceil of 9999-12-31 23:59:59, 2 out of range"
    }

    // SECOND_CEIL period is non-positive
    test {
        sql """SELECT SECOND_CEIL('2025-01-23 12:34:56', -3) AS result;"""
        exception "Operation second_ceil of 2025-01-23 12:34:56, -3 out of range"
    }

    // SECOND_FLOOR period is non-positive
    test {
        sql """SELECT SECOND_FLOOR('2025-01-23 12:34:56', -3) AS result;"""
        exception "Operation second_floor of 2025-01-23 12:34:56, -3 out of range"
    }

    // SECONDS_ADD result out of range
    test {
        sql """SELECT SECONDS_ADD('9999-12-31 23:59:59', 2) AS result;"""
        exception "Operation second_add of 9999-12-31 23:59:59, 2 out of range"
    }
    test {
        sql """select seconds_add('0000-01-01 00:00:30',-31);"""
        exception "Operation second_add of 0000-01-01 00:00:30, -31 out of range"
    }

    // SECONDS_SUB result out of range
    test {
        sql """SELECT SECONDS_SUB('0000-01-01 00:00:00', 1) AS result;"""
        exception "Operation second_add of 0000-01-01 00:00:00, -1 out of range"
    }

    // TIMESTAMPADD invalid unit
    test {
        sql """SELECT TIMESTAMPADD(MIN, 5, '2023-01-01') AS result;"""
        exception "Unsupported time stamp diff time unit: MIN, supported time unit: YEAR/QUARTER/MONTH/WEEK/DAY/HOUR/MINUTE/SECOND"
    }

    // TIMESTAMPADD unit not supported, out of range
    test {
        sql """SELECT TIMESTAMPADD(YEAR,10000, '2023-12-31 23:59:59') AS result;"""
        exception "Operation year_add of 2023-12-31 23:59:59, 10000 out of range"
    }

    // unix_timestamp invalid format
    test {
        sql """select unix_timestamp('2007-11-30 10:30-19', 's');"""
        exception "Operation unix_timestamp of 2007-11-30 10:30-19, s is invalid"
    }

    // WEEK_CEIL result exceeds max datetime
    test {
        sql """SELECT WEEK_CEIL('9999-12-31 22:28:18', 2) AS result;"""
        exception "Operation week_ceil of 9999-12-31 22:28:18, 2 out of range"
    }

    // WEEK_CEIL invalid period (non-positive)
    test {
        sql """SELECT WEEK_CEIL('2023-07-13', 0) AS result;"""
        exception "Operation week_ceil of 2023-07-13 00:00:00, 0 out of range"
    }

    // WEEK_FLOOR invalid period (non-positive)
    test {
        sql """SELECT WEEK_FLOOR('2023-07-13', 0) AS result;"""
        exception "Operation week_floor of 2023-07-13 00:00:00, 0 out of range"
    }

    // WEEKS_ADD result out of range
    test {
        sql """SELECT WEEKS_ADD('9999-12-31',1);"""
        exception "Operation week_add of 9999-12-31, 1 out of range"
    }
    test {
        sql """SELECT WEEKS_ADD('0000-01-01',-1);"""
        exception "Operation week_add of 0000-01-01, -1 out of range"
    }

    // WEEKS_SUB result out of range (lower bound)
    test {
        sql """SELECT WEEKS_SUB('0000-01-01', 1);"""
        exception "Operation week_add of 0000-01-01, -1 out of range"
    }

    // WEEKS_SUB result out of range (upper bound)
    test {
        sql """SELECT WEEKS_SUB('9999-12-31', -1);"""
        exception "Operation week_add of 9999-12-31, 1 out of range"
    }

    // YEAR_CEIL invalid period (non-positive)
    test {
        sql """SELECT YEAR_CEIL('2023-07-13', 0) AS result;"""
        exception "Operation year_ceil of 2023-07-13 00:00:00, 0 out of range"
    }

    // YEAR_CEIL result exceeds max datetime
    test {
        sql """SELECT YEAR_CEIL('9999-12-31 22:28:18', 5) AS result;"""
        exception "Operation year_ceil of 9999-12-31 22:28:18, 5 out of range"
    }

    // YEAR_FLOOR invalid period (non-positive)
    test {
        sql """SELECT YEAR_FLOOR('2023-07-13', 0) AS result;"""
        exception "Operation year_floor of 2023-07-13 00:00:00, 0 out of range"
    }

    // YEARS_ADD result out of range (upper bound)
    test {
        sql """SELECT YEARS_ADD('9999-12-31', 1);"""
        exception "Operation year_add of 9999-12-31, 1 out of range"
    }

    // YEARS_ADD result out of range (lower bound)
    test {
        sql """SELECT YEARS_ADD('0000-01-01', -1);"""
        exception "Operation year_add of 0000-01-01, -1 out of range"
    }

    // YEARS_SUB result out of range (upper bound)
    test {
        sql """SELECT YEARS_SUB('9999-12-31', -1);"""
        exception "Operation year_add of 9999-12-31, 1 out of range"
    }

    // YEARS_SUB result out of range (lower bound)
    test {
        sql """SELECT YEARS_SUB('0000-01-01', 1);"""
        exception "Operation year_add of 0000-01-01, -1 out of range"
    }
}


