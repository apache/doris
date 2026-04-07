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

// Verify that CTEs containing non-deterministic functions (rand, uuid, random)
// are never inlined regardless of cte_inline_mode setting.
// Inlining such CTEs would cause each consumer to evaluate the function
// independently, changing query semantics.
suite("test_cte_nondeterministic_not_inline") {
    sql "DROP TABLE IF EXISTS cte_nondeterministic_tbl"
    sql """
        CREATE TABLE cte_nondeterministic_tbl (
            id INT,
            val INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql "INSERT INTO cte_nondeterministic_tbl VALUES (1, 10), (2, 20), (3, 30)"

    // --- cte_inline_mode = 1: CBO comparison mode ---
    sql "SET cte_inline_mode=1"
    // high threshold so the consumer-count alone would NOT prevent inlining
    sql "SET inline_cte_referenced_threshold=10"

    def explainRand1 = sql """
        explain shape plan
        WITH cte_rand AS (
            SELECT id, val, rand() AS r FROM cte_nondeterministic_tbl
        )
        SELECT * FROM cte_rand
        UNION ALL
        SELECT * FROM cte_rand
    """
    assertTrue(explainRand1.toString().contains("PhysicalCteProducer"))

    def explainUuid1 = sql """
        explain shape plan
        WITH cte_uuid AS (
            SELECT id, val, uuid() AS u FROM cte_nondeterministic_tbl
        )
        SELECT * FROM cte_uuid
        UNION ALL
        SELECT * FROM cte_uuid
    """
    assertTrue(explainUuid1.toString().contains("PhysicalCteProducer"))

    def explainRandom1 = sql """
        explain shape plan
        WITH cte_random AS (
            SELECT id, val, random() AS r FROM cte_nondeterministic_tbl
        )
        SELECT * FROM cte_random
        UNION ALL
        SELECT * FROM cte_random
    """
    assertTrue(explainRandom1.toString().contains("PhysicalCteProducer"))

    // --- cte_inline_mode = 2: full inline mode ---
    sql "SET cte_inline_mode=2"
    sql "SET inline_cte_referenced_threshold=10"

    def explainRand2 = sql """
        explain shape plan
        WITH cte_rand AS (
            SELECT id, val, rand() AS r FROM cte_nondeterministic_tbl
        )
        SELECT * FROM cte_rand
        UNION ALL
        SELECT * FROM cte_rand
    """
    assertTrue(explainRand2.toString().contains("PhysicalCteProducer"))

    def explainUuid2 = sql """
        explain shape plan
        WITH cte_uuid AS (
            SELECT id, val, uuid() AS u FROM cte_nondeterministic_tbl
        )
        SELECT * FROM cte_uuid
        UNION ALL
        SELECT * FROM cte_uuid
    """
    assertTrue(explainUuid2.toString().contains("PhysicalCteProducer"))

    // --- cte_inline_mode = 0: default rule-based mode ---
    sql "SET cte_inline_mode=0"
    sql "SET inline_cte_referenced_threshold=10"

    def explainRand0 = sql """
        explain shape plan
        WITH cte_rand AS (
            SELECT id, val, rand() AS r FROM cte_nondeterministic_tbl
        )
        SELECT * FROM cte_rand
        UNION ALL
        SELECT * FROM cte_rand
    """
    assertTrue(explainRand0.toString().contains("PhysicalCteProducer"))

    // Single consumer: nondeterministic CTE must still be materialized
    def explainRandSingle = sql """
        explain shape plan
        WITH cte_rand_single AS (
            SELECT id, val, rand() AS r FROM cte_nondeterministic_tbl
        )
        SELECT * FROM cte_rand_single
    """
    assertTrue(explainRandSingle.toString().contains("PhysicalCteProducer"))
}
