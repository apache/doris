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

suite("test_timestamptz_year_interval") {
    // The session date is February 29. The stored UTC date is February 28.
    // A year change in UTC returned March 1 in this session before the fix.
    sql "SET time_zone = '+08:00'"

    // Use the exact reported value without a table. Run it with and without
    // constant folding so both the FE and the BE must return February 28.
    def originalSkipFold = sql("SELECT @@debug_skip_fold_constant")[0][0]
    try {
        for (def skipFold : [false, true]) {
            sql "SET debug_skip_fold_constant = ${skipFold}"
            order_qt_constant """
                SELECT years_add(CAST('2024-02-29 00:00:00 +08:00' AS TIMESTAMPTZ(6)), 1),
                       years_sub(CAST('2024-02-29 00:00:00 +08:00' AS TIMESTAMPTZ(6)), 1)
            """
        }
    } finally {
        sql "SET debug_skip_fold_constant = ${originalSkipFold}"
    }

    sql "DROP TABLE IF EXISTS test_timestamptz_year_interval"
    sql """
        CREATE TABLE test_timestamptz_year_interval (
            id INT NOT NULL,
            ts TIMESTAMPTZ(6) NOT NULL
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ('replication_num' = '1')
    """
    // One row is sufficient. Its fraction checks that the fix keeps microseconds.
    // Reuse id = 1 as the interval column. No second row or extra column is needed.
    sql """
        INSERT INTO test_timestamptz_year_interval VALUES
        (1, CAST('2024-02-29 00:00:00.123456 +08:00' AS TIMESTAMPTZ(6)))
    """

    // Both results must use February 28, as the DATETIME and DATE controls do.
    // Convert through DATETIME because a direct TIMESTAMPTZ-to-DATE cast is not supported.
    order_qt_column """
        SELECT ts, years_add(ts, 1), years_sub(ts, 1),
               years_add(CAST(ts AS DATETIME(6)), 1),
               years_sub(CAST(ts AS DATETIME(6)), 1),
               years_add(CAST(CAST(ts AS DATETIME(6)) AS DATE), 1),
               years_sub(CAST(CAST(ts AS DATETIME(6)) AS DATE), 1)
        FROM test_timestamptz_year_interval
    """

    // Check both column dispatch paths: time column with interval column,
    // and constant time with interval column. Both must use the session zone.
    order_qt_interval_column """
        SELECT years_add(ts, id), years_sub(ts, id),
               years_add(CAST('2024-02-29 00:00:00.123456 +08:00' AS TIMESTAMPTZ(6)), id),
               years_sub(CAST('2024-02-29 00:00:00.123456 +08:00' AS TIMESTAMPTZ(6)), id)
        FROM test_timestamptz_year_interval
    """

    // A negative interval reverses the direction. A leap-year target keeps
    // February 29. A zero interval keeps the full input value.
    order_qt_year_boundaries """
        SELECT years_add(ts, -1), years_sub(ts, -1),
               years_add(ts, 4), years_sub(ts, 4), years_add(ts, 0),
               date_add(ts, INTERVAL 1 YEAR), date_sub(ts, INTERVAL 1 YEAR)
        FROM test_timestamptz_year_interval
    """
}
