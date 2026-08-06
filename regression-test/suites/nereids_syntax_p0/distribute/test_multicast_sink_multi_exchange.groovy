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

// Regression test for computeDestIdToInstanceId picking wrong ExchangeNode
// when a fragment has multiple inputs with different partition types
// (BROADCAST + HASH_PARTITIONED). Before the fix, .iterator().next() could
// pick the BROADCAST input (1 dest per BE), producing a 1-entry shuffle map
// and causing "Rows mismatched! Data may be lost".
suite("test_multicast_sink_multi_exchange") {
    sql "DROP TABLE IF EXISTS mse_fact"
    sql "DROP TABLE IF EXISTS mse_dim"

    sql """
        CREATE TABLE mse_fact (
            k1 VARCHAR(64) NOT NULL,
            k2 INT NOT NULL,
            v1 INT NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(k1, k2)
        DISTRIBUTED BY HASH(k2) BUCKETS 8
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE TABLE mse_dim (
            k1 VARCHAR(64) NOT NULL,
            v1 INT NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(v1) BUCKETS 7
        PROPERTIES ("replication_num" = "1")
    """

    // Use numbers() to generate enough rows across buckets
    sql """
        INSERT INTO mse_dim SELECT CONCAT('key_', number), number
        FROM numbers('number' = '100')
    """
    sql """
        INSERT INTO mse_fact SELECT CONCAT('key_', number % 100), number % 10, number
        FROM numbers('number' = '500')
    """

    sql "SET enable_nereids_distribute_planner=true"
    sql "SET disable_join_reorder=true"
    sql "SET enable_local_shuffle=true"

    // CTE consumed by LEFT SEMI JOIN [broadcast] (1 dest per BE) and
    // INNER JOIN [shuffle] (N dests per BE), producing MultiCastDataSinks
    // with different partition types to the same downstream fragment.
    // Vary parallel_pipeline_task_num to change per-BE destination counts.
    for (def ppt in [1, 2, 4, 8, 16]) {
        def expected = sql """
            SELECT /*+ SET_VAR(parallel_pipeline_task_num=${ppt},enable_nereids_distribute_planner=false) */
                t2.k2, SUM(t1.v1)
            FROM (
                SELECT k1, k2 FROM mse_fact GROUP BY k1, k2
            ) t2
            LEFT SEMI JOIN mse_dim d ON t2.k1 = d.k1
            INNER JOIN mse_dim t1 ON t2.k1 = t1.k1
            GROUP BY t2.k2 ORDER BY t2.k2
        """

        test {
            sql """
                WITH /*+ SET_VAR(parallel_pipeline_task_num=${ppt}) */ dim_cte AS (
                    SELECT k1, SUM(v1) as v1 FROM mse_dim GROUP BY k1
                )
                SELECT t2.k2, SUM(t1.v1)
                FROM (
                    SELECT k1, k2 FROM mse_fact GROUP BY k1, k2
                ) t2
                LEFT SEMI JOIN [broadcast] dim_cte d ON t2.k1 = d.k1
                INNER JOIN [shuffle] dim_cte t1 ON t2.k1 = t1.k1
                GROUP BY t2.k2 ORDER BY t2.k2
            """
            result(expected)
        }
    }

    sql "DROP TABLE IF EXISTS mse_fact"
    sql "DROP TABLE IF EXISTS mse_dim"
}
