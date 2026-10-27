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
}
