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

suite("test_months_between") {
    sql "drop table if exists months_between_args;"
    sql """
        create table months_between_args (
            k0 int,
            date1_not_null datev2 not null,
            date2_not_null datev2 not null,
            datetime1_not_null datetimev2 not null,
            datetime2_not_null datetimev2 not null,
            round_off_not_null boolean not null,
            date1_null datev2 null,
            date2_null datev2 null,
            datetime1_null datetimev2 null,
            datetime2_null datetimev2 null,
            round_off_null boolean null
        )
        DISTRIBUTED BY HASH(k0)
        PROPERTIES
        (
            "replication_num" = "1"
        );
    """

    order_qt_empty_nullable "select months_between(date1_null, date2_null), months_between(date1_null, date2_null, round_off_null), months_between(datetime1_null, datetime2_null, round_off_null), months_between(date1_null, datetime2_null, round_off_null), months_between(datetime1_null, date2_null, round_off_null) from months_between_args"
    order_qt_empty_not_nullable "select months_between(date1_not_null, date2_not_null), months_between(date1_not_null, date2_not_null, round_off_not_null), months_between(datetime1_not_null, datetime2_not_null, round_off_not_null), months_between(date1_not_null, datetime2_not_null, round_off_not_null), months_between(datetime1_not_null, date2_not_null, round_off_not_null) from months_between_args"
    order_qt_empty_partial_nullable "select months_between(date1_null, date2_not_null), months_between(date1_not_null, date2_null), months_between(date1_not_null, date2_not_null), months_between(datetime1_null, datetime2_not_null), months_between(datetime1_not_null, date2_null), months_between(datetime1_not_null, date2_not_null) from months_between_args"
    order_qt_empty_nullable_no_null "select months_between(date1_null, nullable(date2_not_null)), months_between(nullable(date1_not_null), date2_null), months_between(date1_null, nullable(datetime2_not_null)), months_between(nullable(datetime1_not_null), date2_null), months_between(datetime1_null, nullable(date2_not_null)), months_between(nullable(datetime1_not_null), datetime2_null) from months_between_args"

    sql "insert into months_between_args values (1, '2020-01-01', '2020-02-01', '2020-01-01 00:00:00', '2020-02-01 00:00:00', true, null, null, null, null, null)"

    order_qt_nullable "select months_between(date1_null, date2_null), months_between(date1_null, date2_null, round_off_null), months_between(datetime1_null, datetime2_null, round_off_null), months_between(date1_null, datetime2_null, round_off_null), months_between(datetime1_null, date2_null, round_off_null) from months_between_args"
    order_qt_not_nullable "select months_between(date1_not_null, date2_not_null), months_between(date1_not_null, date2_not_null, round_off_not_null), months_between(datetime1_not_null, datetime2_not_null, round_off_not_null), months_between(date1_not_null, datetime2_not_null, round_off_not_null), months_between(datetime1_not_null, date2_not_null, round_off_not_null) from months_between_args"
    order_qt_partial_nullable "select months_between(date1_null, date2_not_null), months_between(date1_not_null, date2_null), months_between(date1_not_null, date2_not_null), months_between(datetime1_null, datetime2_not_null), months_between(datetime1_not_null, date2_null), months_between(datetime1_not_null, date2_not_null) from months_between_args"
    order_qt_nullable_no_null "select months_between(date1_null, nullable(date2_not_null)), months_between(nullable(date1_not_null), date2_null), months_between(date1_null, nullable(datetime2_not_null)), months_between(nullable(datetime1_not_null), date2_null), months_between(datetime1_null, nullable(date2_not_null)), months_between(nullable(datetime1_not_null), datetime2_null) from months_between_args"

    sql "truncate table months_between_args"

    sql """
    insert into months_between_args values 
        (1, '2020-01-01', '2020-02-01', '2020-01-01 00:00:00', '2020-02-01 00:00:00', true, null, null, null, null, null),
        (2, '2020-01-01', '2020-02-01', '2020-01-01 00:00:00', '2020-02-01 00:00:00', false, '2020-01-01', '2020-02-01', '2020-01-01 00:00:00', '2020-02-01 00:00:00', false),
        (3, '2020-01-31', '2020-02-29', '2020-01-31 23:59:59', '2020-02-29 23:59:59', true, '2020-01-31', '2020-02-29', '2020-01-31 23:59:59', '2020-02-29 23:59:59', true),
        (4, '2020-12-31', '2021-01-31', '2020-12-31 00:00:00', '2021-01-31 00:00:00', false, null, '2021-01-31', null, '2021-01-31 00:00:00', false),
        (5, '1900-01-01', '2100-12-31', '1900-01-01 00:00:00', '2100-12-31 23:59:59', true, '1900-01-01', null, '1900-01-01 00:00:00', null, true),
        (6, '2020-02-29', '2020-02-29', '2020-02-29 12:00:00', '2020-02-29 12:00:00', false, '2020-02-29', '2020-02-29', null, '2020-02-29 12:00:00', null),
        (7, '2020-01-15', '2020-02-15', '2020-01-15 15:30:00', '2020-02-15 15:30:00', true, null, null, '2020-01-15 15:30:00', '2020-02-15 15:30:00', true),
        (8, '2019-12-31', '2020-01-31', '2019-12-31 23:59:59', '2020-01-31 00:00:00', false, '2019-12-31', null, null, null, null),
        (9, '2020-06-30', '2020-07-01', '2020-06-30 23:59:59', '2020-07-01 00:00:00', true, null, '2020-07-01', '2020-06-30 23:59:59', null, true),
        (10, '2020-02-28', '2021-02-28', '2020-02-28 00:00:00', '2021-02-28 00:00:00', false, '2020-02-28', '2021-02-28', null, '2021-02-28 00:00:00', false)
    """

    /// nullables
    order_qt_nullable "select months_between(date1_null, date2_null), months_between(date1_null, date2_null, round_off_null), months_between(datetime1_null, datetime2_null, round_off_null), months_between(date1_null, datetime2_null, round_off_null), months_between(datetime1_null, date2_null, round_off_null) from months_between_args"
    order_qt_not_nullable "select months_between(date1_not_null, date2_not_null), months_between(date1_not_null, date2_not_null, round_off_not_null), months_between(datetime1_not_null, datetime2_not_null, round_off_not_null), months_between(date1_not_null, datetime2_not_null, round_off_not_null), months_between(datetime1_not_null, date2_not_null, round_off_not_null) from months_between_args"
    order_qt_partial_nullable "select months_between(date1_null, date2_not_null), months_between(date1_not_null, date2_null), months_between(date1_not_null, date2_not_null), months_between(datetime1_null, datetime2_not_null), months_between(datetime1_not_null, date2_null), months_between(datetime1_not_null, date2_not_null) from months_between_args"
    order_qt_nullable_no_null "select months_between(date1_null, nullable(date2_not_null)), months_between(nullable(date1_not_null), date2_null), months_between(date1_null, nullable(datetime2_not_null)), months_between(nullable(datetime1_not_null), date2_null), months_between(datetime1_null, nullable(date2_not_null)), months_between(nullable(datetime1_not_null), datetime2_null) from months_between_args"

    /// consts. most by BE-UT
    order_qt_const_nullable "select months_between(NULL, NULL), months_between(NULL, NULL, NULL), months_between(NULL, NULL, NULL), months_between(NULL, NULL, NULL), months_between(NULL, NULL, NULL) from months_between_args"
    order_qt_partial_const_nullable "select months_between(NULL, date2_not_null), months_between(date1_not_null, NULL), months_between(NULL, datetime2_not_null, round_off_not_null), months_between(datetime1_not_null, NULL, round_off_not_null) from months_between_args"
    order_qt_const_not_nullable "select months_between('2020-01-01', '2020-02-01'), months_between('2020-01-01', '2020-02-01', true), months_between('2020-01-01 00:00:00', '2020-02-01 00:00:00', false), months_between('2020-01-01', '2020-02-01 00:00:00', true), months_between('2020-01-01 00:00:00', '2020-02-01', false) from months_between_args"
    order_qt_const_other_nullable "select months_between('2020-01-01', date2_null), months_between(date1_null, '2020-02-01'), months_between('2020-01-01', datetime2_null, round_off_null), months_between(datetime1_null, '2020-02-01', round_off_null) from months_between_args"
    order_qt_const_other_not_nullable "select months_between('2020-01-01', date2_not_null), months_between(date1_not_null, '2020-02-01'), months_between('2020-01-01', datetime2_not_null, round_off_not_null), months_between(datetime1_not_null, '2020-02-01', round_off_not_null) from months_between_args"
    order_qt_const_nullable_no_null "select months_between(nullable('2020-01-01'), nullable('2020-02-01')), months_between(nullable('2020-01-01'), nullable('2020-02-01'), nullable(true)), months_between(nullable('2020-01-01 00:00:00'), nullable('2020-02-01 00:00:00'), nullable(false)) from months_between_args"
    order_qt_const_partial_nullable_no_null "select months_between('2020-01-01', nullable('2020-02-01')), months_between(nullable('2020-01-01'), '2020-02-01'), months_between('2020-01-01', nullable('2020-02-01 00:00:00'), nullable(true)) from months_between_args"
    order_qt_const1 "select months_between('2020-01-01', date2_not_null), months_between('2020-01-01', date2_not_null, round_off_not_null) from months_between_args"
    order_qt_const12 "select months_between('2020-01-01', '2020-02-01', round_off_not_null) from months_between_args"
    order_qt_const23 "select months_between(date1_not_null, '2020-02-01', true) from months_between_args"
    order_qt_const3 "select months_between(date1_not_null, date2_not_null, true) from months_between_args"

    /// test simple cases with date and datetime combinations
    order_qt_date_date "select months_between('2020-01-01', '2020-02-01')"
    order_qt_date_datetime "select months_between('2020-01-01', '2020-02-01 00:00:00')"
    order_qt_datetime_date "select months_between('2020-01-01 00:00:00', '2020-02-01')"
    order_qt_datetime_datetime "select months_between('2020-01-01 00:00:00', '2020-02-01 00:00:00')"

    order_qt_date_date_round "select months_between('2020-01-15', '2020-02-15', true)"
    order_qt_date_datetime_round "select months_between('2020-01-15', '2020-02-15 12:00:00', true)" 
    order_qt_datetime_date_round "select months_between('2020-01-15 12:00:00', '2020-02-15', true)"
    order_qt_datetime_datetime_round "select months_between('2020-01-15 12:00:00', '2020-02-15 12:00:00', true)"

    order_qt_date_date_no_round "select months_between('2020-01-31', '2020-02-29', false)"
    order_qt_date_datetime_no_round "select months_between('2020-01-31', '2020-02-29 23:59:59', false)"
    order_qt_datetime_date_no_round "select months_between('2020-01-31 23:59:59', '2020-02-29', false)" 
    order_qt_datetime_datetime_no_round "select months_between('2020-01-31 23:59:59', '2020-02-29 23:59:59', false)"
    order_qt1 "select months_between('2020-01-01', '2020-02-01', true)"
}