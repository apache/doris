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

// Verify that when cte_inline_mode=0, CTEs used multiple times but
// NOT containing UNION ALL are NOT inlined (they remain materialized).
// Mode 0 only inlines CTEs whose body contains UNION ALL.
suite("test_cte_multiuse_no_union_not_inline") {
    sql "DROP TABLE IF EXISTS cte_multiuse_tbl"
    sql """
        CREATE TABLE cte_multiuse_tbl (
            id INT,
            val INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql "INSERT INTO cte_multiuse_tbl VALUES (1, 10), (2, 20), (3, 30)"
    sql "DROP TABLE IF EXISTS cte_multiuse_tbl2"
    sql """
        CREATE TABLE cte_multiuse_tbl2 (
            id INT,
            val INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql "INSERT INTO cte_multiuse_tbl2 VALUES (1, 10), (2, 20), (3, 30)"
    
    // --- cte_inline_mode = 0: selective mode (default) ---
    sql "SET cte_inline_mode=0"
    // Keep default inline_cte_referenced_threshold=1 so that multi-use CTEs
    // (consumers > 1) survive the early CTEInline rule and reach the optimizer.

    // Multi-use CTE with simple body (no union): must NOT be inlined
    explain {
        sql """
            shape plan
            WITH cte_simple AS (
                SELECT id, val FROM cte_multiuse_tbl WHERE val > 5
            )
            SELECT * FROM cte_simple WHERE id = 1
            UNION ALL
            SELECT * FROM cte_simple WHERE id = 2
        """
        contains("PhysicalCteProducer")
    }

    // Multi-use CTE with join body (no union): must NOT be inlined
    explain {
        sql """
            shape plan
            WITH cte_join AS (
                SELECT a.id, b.val
                FROM cte_multiuse_tbl a
                JOIN cte_multiuse_tbl b ON a.id = b.id
            )
            SELECT * FROM cte_join WHERE id = 1
            UNION ALL
            SELECT * FROM cte_join WHERE id = 2
        """
        contains("PhysicalCteProducer")
    }

    // Multi-use CTE with aggregation body (no union): must NOT be inlined
    explain {
        sql """
            shape plan
            WITH cte_agg AS (
                SELECT id, SUM(val) AS total FROM cte_multiuse_tbl GROUP BY id
            )
            SELECT * FROM cte_agg WHERE id = 1
            UNION ALL
            SELECT * FROM cte_agg WHERE id = 2
        """
        contains("PhysicalCteProducer")
    }

    // Multi-use CTE with 3 consumers (no union): must NOT be inlined
    explain {
        sql """
            shape plan
            WITH cte_three AS (
                SELECT id, val FROM cte_multiuse_tbl
            )
            SELECT * FROM cte_three WHERE id = 1
            UNION ALL
            SELECT * FROM cte_three WHERE id = 2
            UNION ALL
            SELECT * FROM cte_three WHERE id = 3
        """
        contains("PhysicalCteProducer")
    }

    // --- Contrast: multi-use CTE with UNION ALL body SHOULD be inlined ---
    // When consumer filters can eliminate some union branches (creating
    // LogicalEmptyRelation), mode=0 replaces the plan with the inlined version.
    explain {
        sql """
            shape plan
            WITH cte_union AS (
                SELECT id, val, 1 AS tag FROM cte_multiuse_tbl
                UNION ALL
                SELECT id, val, 2 AS tag FROM cte_multiuse_tbl
            )
            SELECT * FROM cte_union WHERE tag = 1
            UNION ALL
            SELECT * FROM cte_union WHERE tag = 2
        """
        notContains("PhysicalCteProducer")
    }

    explain {
        sql """
            shape plan
            WITH cte_union AS (
                SELECT id, val, 1 AS tag FROM cte_multiuse_tbl
                UNION ALL
                SELECT id, val, 2 AS tag FROM cte_multiuse_tbl
            ),
            cte2 as (
                Select id, val from cte_multiuse_tbl2
            )
            SELECT cte_union.* FROM cte_union join cte2 on cte_union.id = cte2.id WHERE cte_union.tag = 1
            UNION ALL
            SELECT cte_union.* FROM cte_union join cte2 on cte_union.id = cte2.id WHERE cte_union.tag = 2
        """
        multiContains("PhysicalCteProducer", 1)
    }
}
