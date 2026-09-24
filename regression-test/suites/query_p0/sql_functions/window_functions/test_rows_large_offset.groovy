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

suite("test_rows_large_offset") {
    // Check both boundaries before normalization can reverse or replace the frame.
    ["SUM", "FIRST_VALUE", "LAST_VALUE"].each { function ->
        ["2147483648", "9223372036854775805", "9223372036854775806",
         "9223372036854775807", "9223372036854775808"].each { offset ->
            ["BETWEEN UNBOUNDED PRECEDING AND ${offset} FOLLOWING",
             "BETWEEN CURRENT ROW AND ${offset} FOLLOWING",
             "BETWEEN ${offset} FOLLOWING AND ${offset} FOLLOWING",
             "BETWEEN ${offset} FOLLOWING AND UNBOUNDED FOLLOWING",
             "BETWEEN ${offset} PRECEDING AND CURRENT ROW",
             "BETWEEN UNBOUNDED PRECEDING AND ${offset} PRECEDING",
             "BETWEEN ${offset} PRECEDING AND ${offset} FOLLOWING",
             "BETWEEN ${offset} PRECEDING AND UNBOUNDED FOLLOWING",
             "${offset} PRECEDING"].each { frame ->
                test {
                    sql """
                        SELECT k, ${function}(k) OVER (ORDER BY k ROWS ${frame})
                        FROM (SELECT 1 AS k UNION ALL SELECT 2) t
                    """
                    exception "BoundOffset of ROWS WindowFrame must not exceed 2147483647"
                }
            }
        }
    }

    order_qt_max_rows """
        SELECT p, k,
               SUM(k) OVER (PARTITION BY p ORDER BY k
                   ROWS BETWEEN UNBOUNDED PRECEDING AND 2147483647 FOLLOWING) AS total,
               SUM(k) OVER (PARTITION BY p ORDER BY k
                   ROWS BETWEEN CURRENT ROW AND 2147483647 FOLLOWING) AS tail,
               SUM(k) OVER (PARTITION BY p ORDER BY k
                   ROWS BETWEEN 2147483647 FOLLOWING AND 2147483647 FOLLOWING) AS empty_following,
               SUM(k) OVER (PARTITION BY p ORDER BY k
                   ROWS BETWEEN 2147483647 FOLLOWING AND UNBOUNDED FOLLOWING) AS empty_tail,
               SUM(k) OVER (PARTITION BY p ORDER BY k
                   ROWS BETWEEN 2147483647 PRECEDING AND CURRENT ROW) AS head,
               SUM(k) OVER (PARTITION BY p ORDER BY k
                   ROWS BETWEEN UNBOUNDED PRECEDING AND 2147483647 PRECEDING) AS empty_head,
               SUM(k) OVER (PARTITION BY p ORDER BY k
                   ROWS BETWEEN 2147483647 PRECEDING AND 2147483647 FOLLOWING) AS spanning,
               SUM(k) OVER (PARTITION BY p ORDER BY k
                   ROWS BETWEEN 2147483647 PRECEDING AND UNBOUNDED FOLLOWING) AS reversed,
               SUM(k) OVER (PARTITION BY p ORDER BY k ROWS 2147483647 PRECEDING) AS shorthand,
               SUM(k) OVER (PARTITION BY p ORDER BY k
                   ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS current_row_only,
               SUM(k) OVER (PARTITION BY p ORDER BY k
                   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS whole_partition
        FROM (SELECT 1 AS p, 1 AS k UNION ALL SELECT 1, 2
              UNION ALL SELECT 2, 10 UNION ALL SELECT 2, 20) t
    """

    order_qt_max_rows_first_last """
        SELECT k,
               FIRST_VALUE(k) OVER (ORDER BY k
                   ROWS BETWEEN 2147483647 FOLLOWING AND UNBOUNDED FOLLOWING) AS empty_first,
               LAST_VALUE(k) OVER (ORDER BY k
                   ROWS BETWEEN UNBOUNDED PRECEDING AND 2147483647 PRECEDING) AS empty_last,
               FIRST_VALUE(k) OVER (ORDER BY k
                   ROWS BETWEEN 2147483647 PRECEDING AND UNBOUNDED FOLLOWING) AS first_in_partition,
               LAST_VALUE(k) OVER (ORDER BY k
                   ROWS BETWEEN UNBOUNDED PRECEDING AND 2147483647 FOLLOWING) AS last_in_partition
        FROM (SELECT 1 AS k UNION ALL SELECT 2) t
    """
}
