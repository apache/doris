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

suite("group_commit_mv_projection") {
    sql "DROP TABLE IF EXISTS group_commit_mv_projection"
    sql """
        CREATE TABLE group_commit_mv_projection (
            k INT NOT NULL,
            s VARCHAR(32) NULL,
            v BIGINT NULL
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "group_commit_interval_ms" = "10"
        )
    """

    // Distinct aliases add MV columns to fullSchema. The two views mix string and numeric
    // keys and aggregate expressions, so forwarding an unprojected block can misread slots.
    createMV("""
        CREATE MATERIALIZED VIEW group_commit_mv_projection_ks AS
        SELECT k AS a_k, s AS a_s, SUM(v) AS a_v
        FROM group_commit_mv_projection
        GROUP BY k, s
    """)
    createMV("""
        CREATE MATERIALIZED VIEW group_commit_mv_projection_sk AS
        SELECT s AS b_s, k AS b_k, SUM(v + 1) AS b_v
        FROM group_commit_mv_projection
        GROUP BY s, k
    """)

    sql "SET group_commit = sync_mode"
    sql """INSERT INTO group_commit_mv_projection VALUES
           (1, 'alpha', 10), (1, 'alpha', 20), (2, 'beta', 30), (3, NULL, NULL)"""
    sql """INSERT INTO group_commit_mv_projection VALUES
           (1, 'alpha', 40), (2, 'beta', 50), (3, NULL, 60)"""

    // Read the physical MV indexes explicitly so optimizer choices cannot hide bad MV data.
    check_sqls_result_equal("""
        SELECT * FROM group_commit_mv_projection INDEX group_commit_mv_projection_ks
        ORDER BY 1, 2
    """, """
        SELECT k, s, SUM(v)
        FROM group_commit_mv_projection INDEX group_commit_mv_projection
        GROUP BY k, s ORDER BY 1, 2
    """)
    check_sqls_result_equal("""
        SELECT * FROM group_commit_mv_projection INDEX group_commit_mv_projection_sk
        ORDER BY 1, 2
    """, """
        SELECT s, k, SUM(v + 1)
        FROM group_commit_mv_projection INDEX group_commit_mv_projection
        GROUP BY s, k ORDER BY 1, 2
    """)

    sql "DROP TABLE IF EXISTS group_commit_generated_middle"
    sql """
        CREATE TABLE group_commit_generated_middle (
            k INT,
            g INT AS (k + 1),
            v INT
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "group_commit_interval_ms" = "10")
    """
    // The queue contains [k, g, v]. Pruning g from the consumer scan would read g as v.
    sql "INSERT INTO group_commit_generated_middle(k, v) VALUES (1, 99), (2, 88), (3, NULL)"
    check_sqls_result_equal("""
        SELECT k, g, v FROM group_commit_generated_middle ORDER BY k
    """, """
        SELECT 1, 2, 99 UNION ALL SELECT 2, 3, 88 UNION ALL SELECT 3, 4, NULL ORDER BY 1
    """)

    createMV("""
        CREATE MATERIALIZED VIEW group_commit_generated_mv AS
        SELECT k AS mv_k, SUM(g + v) AS mv_value
        FROM group_commit_generated_middle GROUP BY k
    """)
    sql "INSERT INTO group_commit_generated_middle(k, v) VALUES (4, 77), (5, 66), (6, NULL)"
    check_sqls_result_equal("""
        SELECT k, g, v FROM group_commit_generated_middle WHERE k >= 4 ORDER BY k
    """, """
        SELECT 4, 5, 77 UNION ALL SELECT 5, 6, 66 UNION ALL SELECT 6, 7, NULL ORDER BY 1
    """)
    check_sqls_result_equal("""
        SELECT * FROM group_commit_generated_middle INDEX group_commit_generated_mv ORDER BY 1
    """, """
        SELECT k, SUM(g + v)
        FROM group_commit_generated_middle INDEX group_commit_generated_middle GROUP BY k ORDER BY k
    """)

    sql "DROP TABLE IF EXISTS group_commit_generated_time"
    sql """
        CREATE TABLE group_commit_generated_time (
            k INT,
            producer_time DATETIMEV2(6) DEFAULT CURRENT_TIMESTAMP(6),
            g DATETIMEV2(6) AS (now(6))
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "group_commit_interval_ms" = "10")
    """
    // Both values are computed with the producer's query time. The consumer must preserve g.
    sql "INSERT INTO group_commit_generated_time(k) VALUES (1), (2)"
    sql "INSERT INTO group_commit_generated_time(k) VALUES (3), (4)"
    check_sqls_result_equal("""
        SELECT k, producer_time FROM group_commit_generated_time ORDER BY k
    """, """
        SELECT k, g FROM group_commit_generated_time ORDER BY k
    """)
}
