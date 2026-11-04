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

suite("test_timestamptz_dst_gap") {
    // Conversion queries need no table. The partition test below uses one row.
    def cases = [
        ["America/New_York", "2024-03-10 01:59:59.999999", "2024-03-10 06:59:59.999999+00:00"],
        ["America/New_York", "2024-03-10 02:00:00", "2024-03-10 07:00:00+00:00"],
        ["America/New_York", "2024-03-10 02:30:00", "2024-03-10 07:30:00+00:00"],
        ["America/New_York", "2024-03-10 02:30:00.123456", "2024-03-10 07:30:00.123456+00:00"],
        ["America/New_York", "2024-03-10 02:59:59.999999", "2024-03-10 07:59:59.999999+00:00"],
        ["America/New_York", "2024-03-10 03:00:00", "2024-03-10 07:00:00+00:00"],
        ["America/New_York", "2024-03-10 02:59:59.9999996", "2024-03-10 07:00:00+00:00"],
        // The gap on Lord Howe is 30 minutes. The input becomes 02:45.
        ["Australia/Lord_Howe", "2024-10-06 02:15:00.123456", "2024-10-05 15:45:00.123456+00:00"]
    ]
    def originalZone = sql("SELECT @@time_zone")[0][0]
    def originalSkipFold = sql("SELECT @@debug_skip_fold_constant")[0][0]
    def originalFoldByBe = sql("SELECT @@enable_fold_constant_by_be")[0][0]
    def originalSqlCache = sql("SELECT @@enable_sql_cache")[0][0]
    try {
        // Each mode must evaluate the input. Do not reuse the first mode's result.
        sql "SET enable_sql_cache = false"
        sql "SET enable_fold_constant_by_be = false"
        for (def skipFold : [false, true]) {
            sql "SET debug_skip_fold_constant = ${skipFold}"
            for (def c : cases) {
                sql "SET time_zone = '${c[0]}'"
                order_qt_gap """
                    SELECT CAST(s AS STRING), CAST(n AS STRING), CAST(d AS STRING),
                           s = expected, n = expected, d = expected
                    FROM (SELECT CAST('${c[1]}' AS TIMESTAMPTZ(6)) AS s,
                                 CAST('${c[1]} ${c[0]}' AS TIMESTAMPTZ(6)) AS n,
                                 CAST(CAST('${c[1]}' AS DATETIMEV2(6)) AS TIMESTAMPTZ(6)) AS d,
                                 CAST('${c[2]}' AS TIMESTAMPTZ(6)) AS expected) t
                """
            }
            sql "SET time_zone = 'America/New_York'"
            order_qt_delta """
                SELECT seconds_diff(ts, CAST('2024-03-10 07:00:00+00:00' AS TIMESTAMPTZ(6))),
                       seconds_diff(ts, CAST('2024-03-10 07:30:00+00:00' AS TIMESTAMPTZ(6)))
                FROM (SELECT CAST('2024-03-10 02:30:00' AS TIMESTAMPTZ(6)) AS ts) t
            """
            order_qt_rounding """
                SELECT CAST('2024-03-10 02:59:59.999999 America/New_York' AS TIMESTAMPTZ(0)),
                       CAST(CAST('2024-03-10 02:59:59.999999' AS DATETIMEV2(6)) AS TIMESTAMPTZ(0))
            """
            // Direct string casts round once. An explicit DATETIMEV2 cast rounds twice.
            order_qt_round_once """
                SELECT CAST('2024-03-10 02:59:59.4999996' AS TIMESTAMPTZ(0)),
                       CAST('2024-03-10 02:59:59.4999996 America/New_York' AS TIMESTAMPTZ(0)),
                       CAST('2024-03-10 02:59:59.9994996' AS TIMESTAMPTZ(3)),
                       CAST('2024-03-10 02:59:59.9994996 America/New_York' AS TIMESTAMPTZ(3)),
                       CAST(CAST('2024-03-10 02:59:59.4999996' AS DATETIMEV2(6)) AS TIMESTAMPTZ(0))
            """
            order_qt_ns """
                SELECT CAST(CAST('2024-03-10 02:30:00.123456789' AS TIMESTAMP_NS) AS TIMESTAMPTZ(6))
            """
            sql "SET time_zone = '+00:00'"
            order_qt_named_zone """
                SELECT CAST('2024-03-10 02:30:00 America/New_York' AS TIMESTAMPTZ(6))
            """
            // Two scalar inputs cover a one-hour gap and a 30-minute gap.
            for (def c : [cases[3], cases[7]]) {
                sql "SET time_zone = '${c[0]}'"
                order_qt_gap_functions """
                    SELECT convert_tz(CAST('${c[1]}' AS DATETIMEV2(6)), '${c[0]}', 'UTC'),
                           convert_tz(CAST('${c[1]}789' AS TIMESTAMP_NS), '${c[0]}', 'UTC'),
                           unix_timestamp(CAST('${c[1]}' AS DATETIMEV2(6))),
                           unix_timestamp(CAST('${c[1]}789' AS TIMESTAMP_NS))
                """
                sql "SET time_zone = '+00:00'"
                order_qt_gap_named_types """
                    SELECT CAST('${c[1]} ${c[0]}' AS DATETIMEV2(6)),
                           CAST('${c[1]}789 ${c[0]}' AS TIMESTAMP_NS)
                """
            }
            order_qt_gap_named_rounding """
                SELECT CAST('2024-03-10 02:59:59.9999996 America/New_York' AS DATETIMEV2(6)),
                       CAST('2024-03-10 02:59:59.4999996 America/New_York' AS DATETIMEV2(0))
            """
            sql "SET time_zone = 'Australia/Lord_Howe'"
            // The gap candidate 02:15 becomes 02:45, after input 02:35. Select 01:15 instead.
            order_qt_gap_hour_origin """
                SELECT hour_floor(t, 1, o), hour_ceil(t, 1, o)
                FROM (SELECT CAST('2024-10-06 02:35:00.123456+11:00' AS TIMESTAMPTZ(6)) AS t,
                             CAST('2024-10-05 02:15:00.123456+10:30' AS TIMESTAMPTZ(6)) AS o) s
            """
            sql "SET time_zone = 'Africa/Monrovia'"
            // Floor selects an earlier boundary. Truncation still resets local fields, then shifts the gap.
            order_qt_gap_minute_floor """
                SELECT minute_floor(t), date_trunc(t, 'minute'), second_floor(t, 60)
                FROM (SELECT CAST('1972-01-07 00:44:40.123456Z' AS TIMESTAMPTZ(6)) AS t) s
            """
            sql "SET time_zone = '+00:00'"
            // A fixed offset has no gap. A repeated local time keeps its first occurrence.
            order_qt_controls """
                SELECT CAST('2024-03-10 02:30:00-04:00' AS TIMESTAMPTZ(6)),
                       CAST('2024-11-03 01:30:00 America/New_York' AS TIMESTAMPTZ(6))
            """
        }

        sql "SET time_zone = 'America/New_York'"
        sql "SET debug_skip_fold_constant = false"
        // The bounds both map to 07:00 UTC, but the one row maps to 07:30 UTC.
        sql "DROP TABLE IF EXISTS test_timestamptz_gap_partition"
        sql """
            CREATE TABLE test_timestamptz_gap_partition (d DATETIME(6) NOT NULL)
            DUPLICATE KEY(d)
            PARTITION BY RANGE(d) (
                PARTITION p_gap VALUES [("2024-03-10 02:00:00"), ("2024-03-10 03:00:00"))
            )
            DISTRIBUTED BY HASH(d) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        sql "INSERT INTO test_timestamptz_gap_partition VALUES ('2024-03-10 02:30:00')"
        order_qt_gap_partition """
            SELECT d FROM test_timestamptz_gap_partition
            WHERE CAST(d AS TIMESTAMPTZ(6)) = CAST('2024-03-10 07:30:00+00:00' AS TIMESTAMPTZ(6))
        """
        order_qt_gap_convert_tz_partition """
            SELECT d FROM test_timestamptz_gap_partition
            WHERE convert_tz(d, 'America/New_York', 'UTC') = '2024-03-10 07:30:00'
        """
        order_qt_gap_unix_timestamp_partition """
            SELECT d FROM test_timestamptz_gap_partition WHERE unix_timestamp(d) = 1710055800
        """
    } finally {
        sql "SET time_zone = '${originalZone}'"
        sql "SET debug_skip_fold_constant = ${originalSkipFold}"
        sql "SET enable_fold_constant_by_be = ${originalFoldByBe}"
        sql "SET enable_sql_cache = ${originalSqlCache}"
    }
}
