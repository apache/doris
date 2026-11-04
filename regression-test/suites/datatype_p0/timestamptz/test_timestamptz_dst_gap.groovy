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
    // Each query has one input. No table is needed to test the conversion paths.
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
    try {
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
            order_qt_ns """
                SELECT CAST(CAST('2024-03-10 02:30:00.123456789' AS TIMESTAMP_NS) AS TIMESTAMPTZ(6))
            """
            sql "SET time_zone = '+00:00'"
            order_qt_named_zone """
                SELECT CAST('2024-03-10 02:30:00 America/New_York' AS TIMESTAMPTZ(6))
            """
            // A fixed offset has no gap. A repeated local time keeps its first occurrence.
            order_qt_controls """
                SELECT CAST('2024-03-10 02:30:00-04:00' AS TIMESTAMPTZ(6)),
                       CAST('2024-11-03 01:30:00 America/New_York' AS TIMESTAMPTZ(6))
            """
        }
    } finally {
        sql "SET time_zone = '${originalZone}'"
        sql "SET debug_skip_fold_constant = ${originalSkipFold}"
        sql "SET enable_fold_constant_by_be = ${originalFoldByBe}"
    }
}
