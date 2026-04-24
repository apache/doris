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

suite("test_cbo_cte_inline_prune") {
    sql "DROP TABLE IF EXISTS cte_cbo_inline_tbl"
    sql """
        CREATE TABLE cte_cbo_inline_tbl (
            id INT,
            val INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql "INSERT INTO cte_cbo_inline_tbl VALUES (1, 10), (2, 20), (3, 30)"

    sql "SET cte_inline_mode=1"
    sql "SET inline_cte_referenced_threshold=1"

    explain {
        sql """
            shape plan
            WITH cte_base AS (
                SELECT id, val, 1 AS tag FROM cte_cbo_inline_tbl
            )
            SELECT * FROM cte_base WHERE tag = 2
            UNION ALL
            SELECT * FROM cte_base WHERE tag = 3
        """
        contains("PhysicalEmptyRelation")
        notContains("PhysicalCteProducer")
    }

    explain {
        sql """
            shape plan
            WITH cte_base AS (
                SELECT id, val, 1 AS tag FROM cte_cbo_inline_tbl
            ),
            cte_keep AS (
                SELECT id, val FROM cte_base WHERE tag = 1
            ),
            cte_drop AS (
                SELECT id, val FROM cte_base WHERE tag = 2
            )
            SELECT * FROM cte_keep
            UNION ALL
            SELECT * FROM cte_keep
            UNION ALL
            SELECT * FROM cte_drop WHERE 1 = 0
        """
        multiContains("PhysicalCteProducer", 0)
    }

    sql "SET cte_inline_mode=0"
    explain {
        sql """
            shape plan
            WITH cte_base AS (
                SELECT id, val, 1 AS tag FROM cte_cbo_inline_tbl
            ),
            cte_keep AS (
                SELECT id, val FROM cte_base WHERE tag = 1
            ),
            cte_drop AS (
                SELECT id, val FROM cte_base WHERE tag = 2
            ),
            cte_outer AS (
                SELECT * FROM cte_keep
                UNION ALL
                SELECT * FROM cte_drop WHERE 1 = 0
            )
            SELECT * FROM cte_outer
            UNION ALL
            SELECT * FROM cte_outer
        """
        // cte_outer is still referenced twice and remains materialized, but
        // eliminating cte_drop makes cte_base have only one live consumer.
        // The fixpoint check must revisit the kept outer CTE anchor and inline
        // cte_base; otherwise two producers would remain.
        multiContains("PhysicalCteProducer", 1)
    }
}
