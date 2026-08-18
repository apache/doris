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

suite("test_constant_cte_partition_prune") {
    sql "DROP TABLE IF EXISTS constant_cte_prune_t"
    sql """
        CREATE TABLE constant_cte_prune_t (
            dt DATE,
            sn VARCHAR(50),
            v DOUBLE
        ) ENGINE=OLAP
        DUPLICATE KEY(dt, sn)
        PARTITION BY RANGE(dt)
        (PARTITION p20260101 VALUES [("2026-01-01"), ("2026-01-02")),
         PARTITION p20260726 VALUES [("2026-07-26"), ("2026-07-27")),
         PARTITION p20260727 VALUES [("2026-07-27"), ("2026-07-28")),
         PARTITION p20260728 VALUES [("2026-07-28"), ("2026-07-29")),
         PARTITION p20260729 VALUES [("2026-07-29"), ("2026-07-30")),
         PARTITION p20260901 VALUES [("2026-09-01"), ("2026-09-02")))
        DISTRIBUTED BY HASH(sn) BUCKETS 3
        PROPERTIES ("replication_num" = "1")
    """

    // force the constant CTE `params` to be inlined (it is referenced twice), matching the
    // reported scenario; after inlining the constants must propagate out so that predicates
    // over the CTE columns (including DATE_SUB() over them) are folded and the scans prune
    // to a single day partition
    sql "SET inline_cte_referenced_threshold=2"

    explain {
        sql """
            WITH params AS (
                SELECT CAST('2026-07-28 00:00:00' AS DATETIME) AS begin_time,
                       CAST('2026-07-28 23:59:59' AS DATETIME) AS end_time,
                       DATEDIFF(CAST('2026-07-28 23:59:59' AS DATETIME),
                                CAST('2026-07-28 00:00:00' AS DATETIME)) + 1 AS period_days
            ),
            current_data AS (
                SELECT SUM(v) AS total_value
                FROM constant_cte_prune_t JOIN params ON 1=1
                WHERE dt BETWEEN params.begin_time AND params.end_time
            ),
            last_period_data AS (
                SELECT SUM(v) AS total_value
                FROM constant_cte_prune_t JOIN params ON 1=1
                WHERE dt BETWEEN DATE_SUB(params.begin_time, INTERVAL params.period_days DAY)
                             AND DATE_SUB(params.begin_time, INTERVAL 1 DAY)
            )
            SELECT * FROM current_data, last_period_data
        """
        // current_data only needs 2026-07-28, last_period_data only needs 2026-07-27
        contains("partitions=1/6 (p20260728)")
        contains("partitions=1/6 (p20260727)")
    }
}
