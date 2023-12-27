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
test_date_or_datetime_computation_negative
suite("test_date_or_datetime_computation_negative") {
    sql """ CREATE TABLE IF NOT EXISTS test_date_or_datetime_computation_negative (
                `row_id` LARGEINT NOT NULL,
                `date` DATE NOT NULL,
                `date_null` DATE NULL,
                `dateV2` DATEV2 NOT NULL,
                `dateV2_null` DATEV2 NULL,
                `datetime` DATETIME NOT NULL,
                `datetime_null` DATETIME NULL )
            DUPLICATE KEY(`row_id`)
            DISTRIBUTED BY HASH(`row_id`) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );"""

    sql "set enable_insert_strict = false;"
    sql "set parallel_fragment_exec_instance_num = 3;"
    sql "set enable_nereids_planner = true;"

    sql """INSERT INTO test_date_or_datetime_computation_negative VALUES (1, '0000-01-01', '0000-01-01', '0000-01-01', '0000-01-01', '0000-01-01 00:00:00', '0000-01-01 00:00:00');"""
    sql """INSERT INTO test_date_or_datetime_computation_negative VALUES (2, '0000-01-01', NULL, '0000-01-01', NULL, '0000-01-01 00:00:00', NULL);"""
    sql """INSERT INTO test_date_or_datetime_computation_negative VALUES (3, '9999-12-31', '9999-12-31', '9999-12-31', '9999-12-31', '9999-12-31 23:59:59', '9999-12-31 23:59:59');"""
    sql """INSERT INTO test_date_or_datetime_computation_negative VALUES (4, '9999-12-31', NULL, '9999-12-31', NULL, '9999-12-31 23:59:59', NULL);"""

    test {
        sql """SELECT date_sub(date, interval 1 year) FROM test_date_or_datetime_computation_negative WHERE row_id=1;"""
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}

        sql """SELECT date_sub(dateV2, interval 1 year) FROM test_date_or_datetime_computation_negative WHERE row_id=1;"""
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}

        sql """SELECT date_sub(datetime, interval 1 year) FROM test_date_or_datetime_computation_negative WHERE row_id=1;"""
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}
    }
    qt_select_nullable_1 """SELECT date_sub(date_null, interval 1 year), date_sub(dateV2_null, interval 1 year), date_sub(datetime_null, interval 1 year) FROM test_date_or_datetime_computation_negative ORDER BY row_id;"""

    test {
        sql """SELECT date_sub(date, interval 1 month) FROM test_date_or_datetime_computation_negative WHERE row_id=1;"""
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}

        sql """SELECT date_sub(dateV2, interval 1 month) FROM test_date_or_datetime_computation_negative WHERE row_id=1;"""
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}

        sql """SELECT date_sub(datetime, interval 1 month) FROM test_date_or_datetime_computation_negative WHERE row_id=1;"""
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}
    }
    qt_select_nullable_2 """SELECT date_sub(date_null, interval 1 month), date_sub(dateV2_null, interval 1 month), date_sub(datetime_null, interval 1 month) FROM test_date_or_datetime_computation_negative ORDER BY row_id;"""

    test {
        sql """ SELECT date_sub(date, interval 1 week) FROM test_date_or_datetime_computation_negative WHERE row_id=1;"""
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}

        sql """ SELECT date_sub(dateV2, interval 1 week) FROM test_date_or_datetime_computation_negative WHERE row_id=1;"""
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}

        sql """ SELECT date_sub(datetime, interval 1 week) FROM test_date_or_datetime_computation_negative WHERE row_id=1; """
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}
    }

    qt_select_nullable_3 """SELECT date_sub(date_null, interval 1 week), date_sub(dateV2_null, interval 1 week), date_sub(datetime_null, interval 1 week) FROM test_date_or_datetime_computation_negative ORDER BY row_id;"""

    test {
        sql """SELECT date_sub(date, interval 1 day) FROM test_date_or_datetime_computation_negative WHERE row_id=1;"""
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}

        sql """SELECT date_sub(dateV2, interval 1 day) FROM test_date_or_datetime_computation_negative WHERE row_id=1;"""
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}

        sql """SELECT date_sub(datetime, interval 1 day) FROM test_date_or_datetime_computation_negative WHERE row_id=1;"""
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}
    }

    qt_select_nullable_4 """SELECT date_sub(date_null, interval 1 day), date_sub(dateV2_null, interval 1 day), date_sub(datetime_null, interval 1 day) FROM test_date_or_datetime_computation_negative ORDER BY row_id;"""

    test {
        sql """SELECT date_sub(date, interval 1 hour) FROM test_date_or_datetime_computation_negative WHERE row_id=1;"""
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}

        sql """SELECT date_sub(dateV2, interval 1 hour) FROM test_date_or_datetime_computation_negative WHERE row_id=1;"""
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}

        sql """SELECT date_sub(datetime, interval 1 hour) FROM test_date_or_datetime_computation_negative WHERE row_id=1;"""
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}
    }
    qt_select_nullable_5 """ SELECT date_sub(date_null, interval 1 hour), date_sub(dateV2_null, interval 1 hour), date_sub(datetime_null, interval 1 hour) FROM test_date_or_datetime_computation_negative ORDER BY row_id;"""

    test {
        sql """SELECT date_sub(date, interval 1 minute) FROM test_date_or_datetime_computation_negative WHERE row_id=1;"""
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}

        sql """SELECT date_sub(dateV2, interval 1 minute) FROM test_date_or_datetime_computation_negative WHERE row_id=1;"""
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}

        sql """SELECT date_sub(datetime, interval 1 minute) FROM test_date_or_datetime_computation_negative WHERE row_id=1;"""
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}
    }
    qt_select_nullable_6 """SELECT date_sub(date_null, interval 1 minute), date_sub(dateV2_null, interval 1 minute), date_sub(datetime_null, interval 1 minute) FROM test_date_or_datetime_computation_negative ORDER BY row_id;"""

    test {
        sql """SELECT date_sub(date, interval 1 second) FROM test_date_or_datetime_computation_negative WHERE row_id=1;"""
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}

        sql """SELECT date_sub(dateV2, interval 1 second) FROM test_date_or_datetime_computation_negative WHERE row_id=1;"""
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}

        sql """SELECT date_sub(datetime, interval 1 second) FROM test_date_or_datetime_computation_negative WHERE row_id=1;"""
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}
    }
    qt_select_nullable_7 """SELECT date_sub(date_null, interval 1 second), date_sub(dateV2_null, interval 1 second), date_sub(datetime_null, interval 1 second) FROM test_date_or_datetime_computation_negative ORDER BY row_id;"""


    test {
        sql """SELECT date_add(date, interval 1 year) FROM test_date_or_datetime_computation_negative WHERE row_id=3;"""
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}

        sql """SELECT date_add(dateV2, interval 1 year) FROM test_date_or_datetime_computation_negative WHERE row_id=3;"""
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}

        sql """SELECT date_add(datetime, interval 1 year) FROM test_date_or_datetime_computation_negative WHERE row_id=3;"""
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}
    }
    qt_select_nullable_8 """SELECT date_add(date_null, interval 1 year), date_add(dateV2_null, interval 1 year), date_add(datetime_null, interval 1 year) FROM test_date_or_datetime_computation_negative ORDER BY row_id;"""

    test {
        sql """SELECT date_add(date, interval 1 month) FROM test_date_or_datetime_computation_negative WHERE row_id=3;"""
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}

        sql """SELECT date_add(dateV2, interval 1 month) FROM test_date_or_datetime_computation_negative WHERE row_id=3;"""
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}

        sql """SELECT date_add(datetime, interval 1 month) FROM test_date_or_datetime_computation_negative WHERE row_id=3;"""
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}
    }
    qt_select_nullable_9 """SELECT date_add(date_null, interval 1 month), date_add(dateV2_null, interval 1 month), date_add(datetime_null, interval 1 month) FROM test_date_or_datetime_computation_negative ORDER BY row_id;"""

    test {
        sql """ SELECT date_add(date, interval 1 week) FROM test_date_or_datetime_computation_negative WHERE row_id=3;"""
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}

        sql """ SELECT date_add(dateV2, interval 1 week) FROM test_date_or_datetime_computation_negative WHERE row_id=3;"""
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}

        sql """ SELECT date_add(datetime, interval 1 week) FROM test_date_or_datetime_computation_negative WHERE row_id=3; """
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}
    }

    qt_select_nullable_10 """SELECT date_add(date_null, interval 1 week), date_add(dateV2_null, interval 1 week), date_add(datetime_null, interval 1 week) FROM test_date_or_datetime_computation_negative ORDER BY row_id;"""

    test {
        sql """SELECT date_add(date, interval 1 day) FROM test_date_or_datetime_computation_negative WHERE row_id=3;"""
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}

        sql """SELECT date_add(dateV2, interval 1 day) FROM test_date_or_datetime_computation_negative WHERE row_id=3;"""
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}

        sql """SELECT date_add(datetime, interval 1 day) FROM test_date_or_datetime_computation_negative WHERE row_id=3;"""
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}
    }

    qt_select_nullable_11 """SELECT date_add(date_null, interval 1 day), date_add(dateV2_null, interval 1 day), date_add(datetime_null, interval 1 day) FROM test_date_or_datetime_computation_negative ORDER BY row_id;"""

    test {
        sql """SELECT date_add(date, interval 1 hour) FROM test_date_or_datetime_computation_negative WHERE row_id=3;"""
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}

        sql """SELECT date_add(dateV2, interval 1 hour) FROM test_date_or_datetime_computation_negative WHERE row_id=3;"""
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}

        sql """SELECT date_add(datetime, interval 1 hour) FROM test_date_or_datetime_computation_negative WHERE row_id=3;"""
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}
    }
    qt_select_nullable_12 """ SELECT date_add(date_null, interval 1 hour), date_add(dateV2_null, interval 1 hour), date_add(datetime_null, interval 1 hour) FROM test_date_or_datetime_computation_negative ORDER BY row_id;"""

    test {
        sql """SELECT date_add(date, interval 1 minute) FROM test_date_or_datetime_computation_negative WHERE row_id=3;"""
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}

        sql """SELECT date_add(dateV2, interval 1 minute) FROM test_date_or_datetime_computation_negative WHERE row_id=3;"""
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}

        sql """SELECT date_add(datetime, interval 1 minute) FROM test_date_or_datetime_computation_negative WHERE row_id=3;"""
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}
    }
    qt_select_nullable_13 """SELECT date_add(date_null, interval 1 minute), date_add(dateV2_null, interval 1 minute), date_add(datetime_null, interval 1 minute) FROM test_date_or_datetime_computation_negative ORDER BY row_id;"""

    test {
        sql """SELECT date_add(date, interval 1 second) FROM test_date_or_datetime_computation_negative WHERE row_id=3;"""
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}

        sql """SELECT date_add(dateV2, interval 1 second) FROM test_date_or_datetime_computation_negative WHERE row_id=3;"""
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}

        sql """SELECT date_add(datetime, interval 1 second) FROM test_date_or_datetime_computation_negative WHERE row_id=3;"""
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}
    }
    qt_select_nullable_14 """SELECT date_add(date_null, interval 1 second), date_add(dateV2_null, interval 1 second), date_add(datetime_null, interval 1 second) FROM test_date_or_datetime_computation_negative ORDER BY row_id;"""

    // TODO:
    // nagetive test for microseconds_add/milliseconds_add/seconds_add/minutes_add/hours_add/days_add/weeks_add/months_add/years_add

    test {
        sql """SELECT hours_add(date, 24) FROM test_date_or_datetime_computation_negative WHERE row_id = 3;"""
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}

        sql """SELECT hours_add(dateV2, 24) FROM test_date_or_datetime_computation_negative WHERE row_id = 3;"""
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}

        sql """SELECT hours_add(datetime, 24) FROM test_date_or_datetime_computation_negative WHERE row_id = 3;"""
        check {result, exception, startTime, endTime ->
                assertTrue (exception != null)}
    }
    qt_select_nullable_15 """SELECT hours_add(date_null, 24), hours_add(dateV2_null, 24), hours_add(datetime_null, 24) FROM test_date_or_datetime_computation_negative ORDER BY row_id;"""

    sql "DROP TABLE test_date_or_datetime_computation_negative"
}
