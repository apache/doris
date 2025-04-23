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

suite("test_next_day") {
    sql "drop table if exists next_day_args;"
    sql """
        create table next_day_args (
            k0 int,
            a date not null,
            b date null,
            c datetime not null,
            d datetime null,
            e string not null,
            f string null,
        )
        DISTRIBUTED BY HASH(k0)
        PROPERTIES
        (
            "replication_num" = "1"
        );
    """

    order_qt_empty_nullable "select next_day(a, e), next_day(c, e) from next_day_args"
    order_qt_empty_not_nullable "select next_day(b, f), next_day(d, f) from next_day_args"
    order_qt_empty_partial_nullable "select next_day(a, f), next_day(c, f) from next_day_args"

    sql "insert into next_day_args values (1, '2025-01-01', null, '2025-01-01 00:00:00', null, 'MONDAY', null), (2, '2025-01-02', '2025-01-02', '2025-01-02 00:00:00', '2025-01-02 00:00:00', 'TUESDAY', 'TUESDAY')"
    order_qt_all_null "select next_day(b, f), next_day(d, f) from next_day_args"

    sql "truncate table next_day_args"

    sql """ 
        insert into next_day_args(k0, a, b, c, d, e, f) values 
        -- normal date
        (1, '2024-03-15', '2024-03-15', '2024-03-15 10:00:00', '2024-03-15 10:00:00', 'MON', 'MONDAY'),
        
        -- first week of 0000
        (2, '0000-01-01', '0000-01-01', '0000-01-01 00:00:00', '0000-01-01 00:00:00', 'SUN', 'SUNDAY'),
        
        -- last day of 0000
        (3, '0000-12-31', '0000-12-31', '0000-12-31 23:59:59', '0000-12-31 23:59:59', 'FRI', 'FRIDAY'),
        
        -- 0000-02-28
        (4, '0000-02-28', '0000-02-28', '0000-02-28 12:00:00', '0000-02-28 12:00:00', 'MO', 'MONDAY'),
        
        -- leap year date before and after
        (5, '2024-02-28', '2024-02-28', '2024-02-28 00:00:00', '2024-02-28 00:00:00', 'WED', 'WEDNESDAY'),
        (6, '2024-02-29', '2024-02-29', '2024-02-29 00:00:00', '2024-02-29 00:00:00', 'THU', 'THURSDAY'),

        -- non leap year date before and after
        (7, '2023-02-28', '2023-02-28', '2023-02-28 00:00:00', '2023-02-28 00:00:00', 'TUE', 'TUESDAY'),
        (8, '2023-03-01', '2023-03-01', '2023-03-01 00:00:00', '2023-03-01 00:00:00', 'WE', 'WEDNESDAY'),
        
        -- 1900 non leap year date before and after
        (9, '1900-02-28', '1900-02-28', '1900-02-28 00:00:00', '1900-02-28 00:00:00', 'WED', 'WEDNESDAY'),
        (10, '1900-03-01', '1900-03-01', '1900-03-01 00:00:00', '1900-03-01 00:00:00', 'THU', 'THURSDAY'),
                
        -- last second of 9999
        (11, '9999-12-31', '9999-12-31', '9999-12-31 23:59:59', '9999-12-31 23:59:59', 'FRI', 'FRIDAY'),
        
        -- first second of 1970
        (12, '1969-12-31', '1969-12-31', '1969-12-31 23:59:59', '1969-12-31 23:59:59', 'WED', 'WEDNESDAY'),
        (13, '1970-01-01', '1970-01-01', '1970-01-01 00:00:00', '1970-01-01 00:00:00', 'THU', 'THURSDAY');
    """

    order_qt_nullable "select next_day(b, f), next_day(d, f) from next_day_args"
    order_qt_not_nullable "select next_day(a, e), next_day(c, e) from next_day_args"
    order_qt_partial_nullable "select next_day(a, f), next_day(c, f), next_day(b, e), next_day(d, e) from next_day_args"
    order_qt_nullable_no_null "select next_day(a, nullable(e)), next_day(c, nullable(e)) from next_day_args"

    /// consts. most by BE-UT
    order_qt_const_nullable "select next_day(NULL, NULL) from next_day_args"
    order_qt_partial_const_nullable "select next_day(NULL, e) from next_day_args"
    order_qt_const_not_nullable "select next_day('2025-01-01', 'MONDAY') from next_day_args"
    order_qt_const_other_nullable "select next_day('2025-01-01', f) from next_day_args"
    order_qt_const_other_not_nullable "select next_day(a, 'FRI') from next_day_args"
    order_qt_const_nullable_no_null "select next_day(nullable('2025-01-01'), nullable('MON'))"
    order_qt_const_nullable_no_null_multirows "select next_day(nullable(c), nullable(e)) from next_day_args"
    order_qt_const_partial_nullable_no_null "select next_day('2025-01-01', nullable(e)) from next_day_args"

    /// folding
    check_fold_consistency "next_day('', 'SA')"
    check_fold_consistency "next_day(NULL, NULL)"
    check_fold_consistency "next_day(NULL, 'FRI')"
    check_fold_consistency "next_day('2025-01-01', NULL)"
    check_fold_consistency "next_day('2025-01-01', 'MONDAY')"
    check_fold_consistency "next_day(nullable('2025-01-01'), nullable('SUN'))"
    check_fold_consistency "next_day('2025-01-01', nullable('WE'))"
    check_fold_consistency "next_day(nullable('2025-01-01'), 'TH')"
    check_fold_consistency "next_day(nullable('2025-01-01 12:34:56'), 'MONDAY')"  
    // test cast date/datetime to date/datetime
    check_fold_consistency "next_day(cast('2025-01-01' as date), 'MONDAY')"
    check_fold_consistency "next_day(cast('2025-01-01' as datetime), 'MONDAY')"
    check_fold_consistency "next_day(cast('2025-01-01 12:34:56' as date), 'MONDAY')"
    check_fold_consistency "next_day(cast('2025-01-01 12:34:56' as datetime), 'MONDAY')"

    /// wrong date
    order_qt_wrong_date "select next_day('2025-02-31', 'MONDAY')"
    order_qt_wrong_date "select next_day('2025-02-29', 'MONDAY')"
    order_qt_wrong_date "select next_day('2025-03-32', 'MONDAY')"

    /// error cases
    test{
        sql """ select next_day('2025-01-01', 'SO') """
        exception "Function next_day failed to parse weekday: SO"
    }
    test{
        sql """ select next_day('2025-01-01', 'MONDDY') """
        exception "Function next_day failed to parse weekday: MONDDY"
    }
    test{
        sql """ select next_day('2025-01-01', '') """
        exception "Function next_day failed to parse weekday: "
    }
}
