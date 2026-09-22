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

suite("test_lead_lag_large_offset") {
    // Extreme LEAD offsets used to overflow BE's exclusive frame end and crash the process.
    ["LEAD", "LAG"].each { function ->
        ["2147483648", "9223372036854775805", "9223372036854775806", "9223372036854775807",
         "9223372036854775808", "2147483647 + 1", "CAST('9223372036854775807' AS BIGINT)"].each { offset ->
            ["", ", 'z'"].each { defaultValue ->
                test {
                    sql """
                        SELECT id, ${function}(v, ${offset}${defaultValue}) OVER (ORDER BY id)
                        FROM (SELECT 1 AS id, 'a' AS v UNION ALL SELECT 2, 'b') t
                    """
                    exception "The offset parameter of ${function} must not exceed 2147483647"
                }
            }
        }

        ["-1", "CAST('-1' AS BIGINT)"].each { offset ->
            test {
                sql """
                    SELECT id, ${function}(v, ${offset}, 'z') OVER (ORDER BY id)
                    FROM (SELECT 1 AS id, 'a' AS v UNION ALL SELECT 2, 'b') t
                """
                exception "The offset parameter of ${function} must be a constant positive integer"
            }
        }

        order_qt_max_int """
            SELECT id,
                   ${function}(v, 2147483647, 'z') OVER (ORDER BY id) AS max_offset,
                   ${function}(v, 2147483647) OVER (ORDER BY id) AS null_default,
                   ${function}(v, 2147483646 + 1, 'z') OVER (ORDER BY id) AS folded_offset,
                   ${function}(v, CAST('2147483647' AS BIGINT), 'z') OVER (ORDER BY id) AS cast_offset,
                   ${function}(v, 0, 'z') OVER (ORDER BY id) AS zero_offset,
                   ${function}(v) OVER (ORDER BY id) AS implicit_offset
            FROM (SELECT 1 AS id, 'a' AS v UNION ALL SELECT 2, 'b') t
        """
    }

    test {
        sql """
            SELECT k, SUM(k) OVER (
                ORDER BY k ROWS BETWEEN 9223372036854775808 PRECEDING AND CURRENT ROW
            ) AS sum_big
            FROM (SELECT 1 AS k UNION ALL SELECT 2) t
        """
        exception "BoundOffset of ROWS WindowFrame must not exceed 9223372036854775807"
    }

    ["'abc'", "NULL", "TRUE", "DATE '2026-07-27'"].each { invalidOffset ->
        test {
            sql """
                SELECT k, SUM(k) OVER (
                    ORDER BY k ROWS BETWEEN ${invalidOffset} PRECEDING AND CURRENT ROW
                ) AS sum_invalid_offset
                FROM (SELECT 1 AS k UNION ALL SELECT 2) t
            """
            exception "BoundOffset of ROWS WindowFrame must be an Integer"
        }
    }

    test {
        sql """
            SELECT k, LAG(k, 922337203685477580.1) OVER (ORDER BY k) AS lag_big
            FROM (SELECT 1 AS k UNION ALL SELECT 2) t
        """
        exception "The offset parameter of LAG must be a constant positive integer"
    }

    test {
        sql """
            SELECT k, LEAD(k, 922337203685477580.1) OVER (ORDER BY k) AS lead_big
            FROM (SELECT 1 AS k UNION ALL SELECT 2) t
        """
        exception "The offset parameter of LEAD must be a constant positive integer"
    }
}
