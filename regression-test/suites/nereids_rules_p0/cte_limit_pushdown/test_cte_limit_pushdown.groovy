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

suite("test_cte_limit_pushdown") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET disable_nereids_rules='PRUNE_EMPTY_PARTITION'"

    sql "DROP TABLE IF EXISTS cte_limit_pushdown_regression_t"
    sql """
        CREATE TABLE cte_limit_pushdown_regression_t (
            k1 int NULL,
            k2 int NULL
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    sql """
        INSERT INTO cte_limit_pushdown_regression_t VALUES
        (1, 10), (2, 20), (3, 30), (4, 40), (5, 50), (6, 60)
    """

    def cteProducerFragment = { explainString ->
        int multicast = explainString.indexOf("MultiCastDataSinks")
        assert multicast >= 0
        int fragmentStart = explainString.lastIndexOf("PLAN FRAGMENT", multicast)
        assert fragmentStart >= 0
        int fragmentEnd = explainString.indexOf("PLAN FRAGMENT", multicast + 1)
        if (fragmentEnd < 0) {
            fragmentEnd = explainString.length()
        }
        return explainString.substring(fragmentStart, fragmentEnd)
    }

    def cteProducerSourceBlock = { explainString ->
        int multicast = explainString.indexOf("MultiCastDataSinks")
        assert multicast >= 0
        int scanStart = explainString.indexOf(":VOlapScanNode", multicast)
        assert scanStart >= 0
        int nextFragment = explainString.indexOf("PLAN FRAGMENT", scanStart + 1)
        if (nextFragment < 0) {
            nextFragment = explainString.length()
        }
        return explainString.substring(scanStart, nextFragment)
    }

    def hasExactLimit = { planBlock, expectedLimit ->
        return planBlock.readLines().any { line -> line.trim() == "limit: ${expectedLimit}" }
    }

    def hasAnyLimit = { planBlock ->
        return planBlock.readLines().any { line -> line.trim().startsWith("limit: ") }
    }

    def assertProducerLimit = { explainString, expectedLimit ->
        String producerFragment = cteProducerFragment(explainString)
        String producerSource = cteProducerSourceBlock(explainString)
        assert producerFragment.contains("MultiCastDataSinks")
        assert producerSource.contains("cte_limit_pushdown_regression_t")
        assert hasExactLimit(producerFragment, expectedLimit)
        assert hasExactLimit(producerSource, expectedLimit)
        return true
    }

    def assertNoProducerLimit = { explainString ->
        String producerFragment = cteProducerFragment(explainString)
        String producerSource = cteProducerSourceBlock(explainString)
        assert producerFragment.contains("MultiCastDataSinks")
        assert producerSource.contains("cte_limit_pushdown_regression_t")
        assert !hasAnyLimit(producerFragment)
        assert !hasAnyLimit(producerSource)
        return true
    }

    explain {
        sql """
            WITH cte AS (SELECT k1, k2 FROM cte_limit_pushdown_regression_t)
            (SELECT * FROM cte LIMIT 10 OFFSET 5)
            UNION ALL
            (SELECT * FROM cte LIMIT 3)
        """
        check { explainString -> assertProducerLimit(explainString, 15) }
    }

    explain {
        sql """
            WITH cte AS (SELECT k1, k2 FROM cte_limit_pushdown_regression_t)
            (SELECT k1 FROM cte LIMIT 7)
            UNION ALL
            (SELECT k1 FROM cte LIMIT 3)
        """
        check { explainString -> assertProducerLimit(explainString, 7) }
    }

    explain {
        sql """
            WITH cte AS (SELECT k1, k2 FROM cte_limit_pushdown_regression_t)
            (SELECT * FROM cte LIMIT 10)
            UNION ALL
            (SELECT * FROM cte)
        """
        check { explainString -> assertNoProducerLimit(explainString) }
    }

    explain {
        sql """
            WITH cte AS (SELECT k1, k2 FROM cte_limit_pushdown_regression_t)
            (SELECT * FROM cte WHERE k1 > 1 LIMIT 10)
            UNION ALL
            (SELECT * FROM cte LIMIT 3)
        """
        check { explainString -> assertNoProducerLimit(explainString) }
    }

    explain {
        sql """
            WITH cte AS (SELECT k1, k2 FROM cte_limit_pushdown_regression_t)
            (SELECT * FROM (SELECT * FROM cte ORDER BY k1 LIMIT 10) topn_branch)
            UNION ALL
            (SELECT * FROM cte LIMIT 3)
        """
        check { explainString -> assertNoProducerLimit(explainString) }
    }

    explain {
        sql """
            WITH cte AS (SELECT k1, k2 FROM cte_limit_pushdown_regression_t)
            (SELECT c.k1, c.k2 FROM cte c
                JOIN cte_limit_pushdown_regression_t t ON c.k1 = t.k1 LIMIT 10)
            UNION ALL
            (SELECT * FROM cte LIMIT 3)
        """
        check { explainString -> assertNoProducerLimit(explainString) }
    }

    explain {
        sql """
            WITH cte AS (SELECT k1, k2 FROM cte_limit_pushdown_regression_t)
            (SELECT k1, COUNT(*) FROM cte GROUP BY k1 LIMIT 10)
            UNION ALL
            (SELECT * FROM cte LIMIT 3)
        """
        check { explainString -> assertNoProducerLimit(explainString) }
    }

    explain {
        sql """
            WITH cte AS (SELECT k1, k2 FROM cte_limit_pushdown_regression_t)
            (SELECT k1, rn FROM (
                SELECT k1, ROW_NUMBER() OVER (ORDER BY k1) AS rn FROM cte
            ) window_branch LIMIT 10)
            UNION ALL
            (SELECT * FROM cte LIMIT 3)
        """
        check { explainString -> assertNoProducerLimit(explainString) }
    }
}
