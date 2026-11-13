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

// Issue 68723: FE folding used Java LocalDateTime, which treats year 0 as leap.
// BE is_leap requires `&& year` (be/src/util/time_lut.h), so year 0 is a common
// year. Expected values below follow that BE rule:
//   date_add('0000-02-28', 1) = 0000-03-01
//   last_day('0000-02-01')    = 0000-02-28
//   dayofyear: 0000-02-28=59, 0000-03-01=60, 0000-03-03=62, 0000-12-31=365
//   weekday:   0000-02-28=1 (Tue), 0000-03-01=2 (Wed)
//   week(date, mode) = 0 for these year-0 dates (verified on BE DateV2Value::calc_week)
// Dates after March 1 are included because FE has other year-0 special cases.
suite("test_year0_common_year_fold") {
    sql "drop table if exists test_year0_fold_datev2"
    sql "drop table if exists test_year0_fold_datetimev2"

    sql """
        create table test_year0_fold_datev2 (
            id int,
            d datev2
        ) duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_year0_fold_datev2 values
            (1, '0000-02-01'),
            (2, '0000-02-28'),
            (3, '0000-03-01'),
            (4, '0000-03-03'),
            (5, '0000-12-31')
    """

    sql """
        create table test_year0_fold_datetimev2 (
            id int,
            dt datetimev2
        ) duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_year0_fold_datetimev2 values
            (1, '0000-02-01 12:34:56'),
            (2, '0000-02-28 12:34:56'),
            (3, '0000-03-01 12:34:56'),
            (4, '0000-03-03 12:34:56'),
            (5, '0000-12-31 12:34:56')
    """

    // Direct 0000-02-29 literals are rejected by DateLiteral.checkDate (year > 0).
    // Default enable_strict_cast=false turns the same CAST into NULL; the exception
    // path needs the strict session variable. The live cluster reports ISO timestamps
    // in the message: [0000-02-29T00:00:00].
    sql "set enable_strict_cast = true"
    test {
        sql "select cast('0000-02-29' as datev2)"
        exception "date/datetime literal [0000-02-29T00:00:00] is out of range"
    }
    test {
        sql "select cast('0000-02-29 00:00:00' as datetimev2)"
        exception "datetime literal [0000-02-29T00:00:00] is out of range"
    }
    sql "set enable_strict_cast = false"

    def datev2Consts = """
        select
            date_add(cast('0000-02-28' as datev2), 1),
            last_day(cast('0000-02-01' as datev2)),
            dayofyear(cast('0000-02-28' as datev2)),
            dayofyear(cast('0000-03-01' as datev2)),
            dayofyear(cast('0000-03-03' as datev2)),
            dayofyear(cast('0000-12-31' as datev2)),
            weekday(cast('0000-02-28' as datev2)),
            weekday(cast('0000-03-01' as datev2)),
            week(cast('0000-02-28' as datev2), 0),
            week(cast('0000-02-28' as datev2), 1),
            week(cast('0000-02-28' as datev2), 2),
            week(cast('0000-02-28' as datev2), 3),
            week(cast('0000-02-28' as datev2), 4),
            week(cast('0000-02-28' as datev2), 5),
            week(cast('0000-02-28' as datev2), 6),
            week(cast('0000-02-28' as datev2), 7),
            week(cast('0000-03-01' as datev2), 0),
            week(cast('0000-03-03' as datev2), 0),
            week(cast('0000-12-31' as datev2), 0)
    """
    def datetimev2Consts = """
        select
            date_add(cast('0000-02-28 12:34:56' as datetimev2), 1),
            last_day(cast('0000-02-01 12:34:56' as datetimev2)),
            dayofyear(cast('0000-02-28 12:34:56' as datetimev2)),
            dayofyear(cast('0000-03-01 12:34:56' as datetimev2)),
            dayofyear(cast('0000-03-03 12:34:56' as datetimev2)),
            dayofyear(cast('0000-12-31 12:34:56' as datetimev2)),
            weekday(cast('0000-02-28 12:34:56' as datetimev2)),
            weekday(cast('0000-03-01 12:34:56' as datetimev2)),
            week(cast('0000-02-28 12:34:56' as datetimev2), 0),
            week(cast('0000-02-28 12:34:56' as datetimev2), 1),
            week(cast('0000-02-28 12:34:56' as datetimev2), 2),
            week(cast('0000-02-28 12:34:56' as datetimev2), 3),
            week(cast('0000-02-28 12:34:56' as datetimev2), 4),
            week(cast('0000-02-28 12:34:56' as datetimev2), 5),
            week(cast('0000-02-28 12:34:56' as datetimev2), 6),
            week(cast('0000-02-28 12:34:56' as datetimev2), 7),
            week(cast('0000-03-01 12:34:56' as datetimev2), 0),
            week(cast('0000-03-03 12:34:56' as datetimev2), 0),
            week(cast('0000-12-31 12:34:56' as datetimev2), 0)
    """

    // FE constant folding.
    sql "set enable_fold_constant_by_be = false"
    sql "set debug_skip_fold_constant = false"
    order_qt_fe_datev2 "${datev2Consts}"
    order_qt_fe_datetimev2 "${datetimev2Consts}"

    // Same constants executed on BE.
    sql "set debug_skip_fold_constant = true"
    order_qt_be_datev2 "${datev2Consts}"
    order_qt_be_datetimev2 "${datetimev2Consts}"

    // Column values are executed on BE even when FE folding is on.
    sql "set debug_skip_fold_constant = false"
    order_qt_tbl_datev2 """
        select id, date_add(d, 1), last_day(d), dayofyear(d), weekday(d),
               week(d, 0), week(d, 1), week(d, 2), week(d, 3),
               week(d, 4), week(d, 5), week(d, 6), week(d, 7)
        from test_year0_fold_datev2
        order by id
    """
    order_qt_tbl_datetimev2 """
        select id, date_add(dt, 1), last_day(dt), dayofyear(dt), weekday(dt),
               week(dt, 0), week(dt, 1), week(dt, 2), week(dt, 3),
               week(dt, 4), week(dt, 5), week(dt, 6), week(dt, 7)
        from test_year0_fold_datetimev2
        order by id
    """
}
