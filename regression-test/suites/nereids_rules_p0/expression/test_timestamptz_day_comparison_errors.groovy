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

suite("test_timestamptz_day_comparison_errors") {
    def originalTimeZone = sql("SELECT @@time_zone")[0][0]
    def originalFoldByBe = sql("SELECT @@enable_fold_constant_by_be")[0][0]
    sql "DROP TABLE IF EXISTS test_timestamptz_day_comparison_errors"
    sql """CREATE TABLE test_timestamptz_day_comparison_errors (
        id INT, upper_ts TIMESTAMPTZ(6), lower_ts TIMESTAMPTZ(6)
    ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES("replication_num"="1")"""
    try {
        sql "SET time_zone = 'UTC'"
        sql """INSERT INTO test_timestamptz_day_comparison_errors VALUES
            (1, '9999-12-31 23:00:00+00:00', '0000-01-01 01:00:00+00:00')"""
        [false, true].each { foldByBe ->
            sql "SET enable_fold_constant_by_be = ${foldByBe}"
            sql "SET time_zone = '+08:00'"
            // The inverse is valid and the rearranged comparison is TRUE. The original DAY
            // operation must nevertheless fail because upper_ts has local year 10000.
            "order_qt_upper_inverse_${foldByBe}"("""SELECT
                CAST(days_add(CAST('9999-12-30 12:00:00+00:00' AS TIMESTAMPTZ(6)), 1) AS STRING),
                upper_ts > days_add(CAST('9999-12-30 12:00:00+00:00' AS TIMESTAMPTZ(6)), 1)
                FROM test_timestamptz_day_comparison_errors""")
            test {
                sql "SELECT days_sub(upper_ts, 1) FROM test_timestamptz_day_comparison_errors"
                exception "out of range"
            }
            ["days_sub(upper_ts, 1)", "days_add(upper_ts, -1)"].each { arithmetic ->
                [">", ">=", "<", "<=", "=", "<=>"].each { op ->
                    test {
                        sql """SELECT id FROM test_timestamptz_day_comparison_errors
                            WHERE ${arithmetic} ${op} CAST('9999-12-30 12:00:00+00:00' AS TIMESTAMPTZ(6))"""
                        exception "out of range"
                    }
                }
            }
            explain {
                sql """SELECT id FROM test_timestamptz_day_comparison_errors
                    WHERE days_sub(upper_ts, 1) > CAST('9999-12-30 12:00:00+00:00' AS TIMESTAMPTZ(6))"""
                contains "days_sub"
            }
            // Aliases and a commuted comparison must also retain the original operation.
            ["date_sub(upper_ts, 1)", "subdate(upper_ts, 1)", "upper_ts - INTERVAL 1 DAY",
             "timestampadd(DAY, -1, upper_ts)"].each { arithmetic ->
                test {
                    sql """SELECT id FROM test_timestamptz_day_comparison_errors
                        WHERE CAST('9999-12-30 12:00:00+00:00' AS TIMESTAMPTZ(6)) < ${arithmetic}"""
                    exception "out of range"
                }
            }

            sql "SET time_zone = '-08:00'"
            // Symmetric lower-bound case: the UTC input is valid but its local year is -1.
            "order_qt_lower_inverse_${foldByBe}"("""SELECT
                CAST(days_sub(CAST('0000-01-02 12:00:00+00:00' AS TIMESTAMPTZ(6)), 1) AS STRING),
                lower_ts < days_sub(CAST('0000-01-02 12:00:00+00:00' AS TIMESTAMPTZ(6)), 1)
                FROM test_timestamptz_day_comparison_errors""")
            ["days_add(lower_ts, 1)", "days_sub(lower_ts, -1)"].each { arithmetic ->
                ["<", "<=", ">", ">=", "=", "<=>"].each { op ->
                    test {
                        sql """SELECT id FROM test_timestamptz_day_comparison_errors
                            WHERE ${arithmetic} ${op} CAST('0000-01-02 12:00:00+00:00' AS TIMESTAMPTZ(6))"""
                        exception "out of range"
                    }
                }
            }
        }
    } finally {
        sql "SET time_zone = '${originalTimeZone}'"
        sql "SET enable_fold_constant_by_be = ${originalFoldByBe}"
    }
}
