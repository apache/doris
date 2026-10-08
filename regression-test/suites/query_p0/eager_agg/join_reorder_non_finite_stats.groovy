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

suite("join_reorder_non_finite_stats") {
    sql "DROP TABLE IF EXISTS eager_non_finite_stats"
    sql "DROP TABLE IF EXISTS eager_non_finite_empty"
    sql """
        CREATE TABLE eager_non_finite_stats (k INT, v INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE TABLE eager_non_finite_empty (k INT, v INT)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql "INSERT INTO eager_non_finite_stats VALUES (1, 10), (2, 20), (NULL, 30)"
    sql "ANALYZE TABLE eager_non_finite_stats WITH SYNC"
    sql "ANALYZE TABLE eager_non_finite_empty WITH SYNC"

    // Reduced from RQG aggregate joins. Column statistics are necessary: k < 0
    // estimates zero rows, and arithmetic/null-safe equality can derive NaN.
    for (boolean reorder : [false, true]) {
        sql "set enable_join_reorder_before_eager_agg=${reorder}"
        order_qt_empty_filter """
            SELECT COUNT(*)
            FROM eager_non_finite_stats a JOIN eager_non_finite_stats b
                ON (a.k % 7) <=> (b.k % 3)
            WHERE a.k < 0
        """

        // The null-rejecting RIGHT JOIN makes an inner cluster's input empty.
        // The preserved side still has rows, so the correct result is 'right'.
        order_qt_outer_empty_input """
            SELECT 'right' AS label
            FROM eager_non_finite_stats a JOIN eager_non_finite_stats b
                ON (CASE WHEN a.k > 1 THEN b.k ELSE b.v END) <=> (b.v + 1)
            LEFT JOIN eager_non_finite_empty c ON FALSE
            RIGHT JOIN eager_non_finite_stats d ON c.k > 0
            GROUP BY label
        """

        order_qt_empty_table """
            SELECT COUNT(*)
            FROM eager_non_finite_stats a JOIN eager_non_finite_empty b
                ON (a.k % 7) <=> (b.k % 3)
        """

        // Retain nonempty results, duplicate matches and NULL-safe matches too.
        order_qt_nonempty_join """
            SELECT a.k, COUNT(*), SUM(b.v)
            FROM eager_non_finite_stats a JOIN eager_non_finite_stats b
                ON (a.k % 2) <=> (b.k % 2)
            CROSS JOIN eager_non_finite_stats c
            GROUP BY a.k
        """

        order_qt_outer_filtered_input """
            SELECT a.k, COUNT(*), COUNT(b.k), SUM(b.v)
            FROM eager_non_finite_stats a
            LEFT JOIN (
                SELECT b.k, b.v
                FROM eager_non_finite_stats b JOIN eager_non_finite_stats c
                    ON (b.k % 7) <=> (c.k % 3)
                WHERE b.k < 0
            ) b ON a.k <=> b.k
            GROUP BY a.k
        """
    }
}
