// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

suite("test_sync_rollup_strict_sql_mode", "p0") {

    def rollupName = "rollup_by_region"

    def query = """
        SELECT region_id, SUM(sales)
        FROM test_sync_rollup_strict_sql_mode
        GROUP BY region_id
        ORDER BY region_id
    """

    // Preserve session settings modified by this test.
    def originalSqlMode = sql("SELECT @@sql_mode")[0][0].toString()
    def originalNereids = sql("SELECT @@enable_nereids_planner")[0][0]
    def originalFallback = sql("SELECT @@enable_fallback_to_original_planner")[0][0]
    def originalSqlCache = sql("SELECT @@enable_sql_cache")[0][0]

    try {
        sql "SET enable_nereids_planner = true"
        sql "SET enable_fallback_to_original_planner = false"
        sql "SET enable_sql_cache = false"

        // Create the synchronous rollup without STRICT_TRANS_TABLES.
        sql "SET sql_mode = 'ONLY_FULL_GROUP_BY'"

        sql "DROP TABLE IF EXISTS test_sync_rollup_strict_sql_mode"

        sql """
            CREATE TABLE test_sync_rollup_strict_sql_mode (
                region_id INT NOT NULL,
                store_id INT NOT NULL,
                sales BIGINT SUM NOT NULL
            )
            AGGREGATE KEY(region_id, store_id)
            DISTRIBUTED BY HASH(region_id) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """

        sql """
            INSERT INTO test_sync_rollup_strict_sql_mode VALUES
                (1, 101, 100),
                (1, 102, 200),
                (2, 201, 300),
                (2, 202, 400)
        """

        sql """
            ALTER TABLE test_sync_rollup_strict_sql_mode
            ADD ROLLUP ${rollupName} (
                region_id,
                sales
            )
        """

        // Rollup creation is asynchronous. Wait until it is usable.
        boolean rollupFinished = false

        for (int attempt = 0; attempt < 90; attempt++) {
            def jobs = sql """
                SHOW ALTER TABLE ROLLUP
                WHERE TableName = 'test_sync_rollup_strict_sql_mode'
                ORDER BY CreateTime DESC
                LIMIT 1
            """

            if (!jobs.isEmpty()) {
                String state = jobs[0][8].toString()

                if (state == "FINISHED") {
                    rollupFinished = true
                    break
                }

                assertFalse(
                    state == "CANCELLED",
                    "Rollup creation was cancelled: ${jobs[0]}"
                )
            }

            Thread.sleep(2000)
        }

        assertTrue(
            rollupFinished,
            "Rollup ${rollupName} did not finish creating"
        )

        // Baseline: matching SQL modes must allow rollup rewriting.
        explain {
            sql query
            verbose true

            check { plan ->
                boolean rewriteSucceeded =
                    plan.contains("${rollupName} chose")
                        || plan.contains("${rollupName} not chose")

                assertTrue(
                    rewriteSucceeded,
                    "Rollup rewrite failed with matching SQL mode:\n${plan}"
                )

                assertFalse(
                    plan.contains("${rollupName} fail"),
                    "Unexpected rollup rewrite failure:\n${plan}"
                )

                return true
            }
        }

        qt_normal_sql_mode(query)

        // Regression: STRICT_TRANS_TABLES alone must not
        // invalidate an otherwise compatible synchronous rollup.
        sql "SET sql_mode = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES'"

        explain {
            sql query
            verbose true

            check { plan ->
                boolean rewriteSucceeded =
                    plan.contains("${rollupName} chose")
                        || plan.contains("${rollupName} not chose")

                assertTrue(
                    rewriteSucceeded,
                    "STRICT_TRANS_TABLES prevented rollup rewriting:\n${plan}"
                )

                assertFalse(
                    plan.contains("${rollupName} fail"),
                    "Unexpected rollup rewrite failure:\n${plan}"
                )

                return true
            }
        }

        qt_strict_sql_mode(query)

    } finally {

        // Restore the original session settings.
        sql "SET sql_mode = '${originalSqlMode}'"
        sql "SET enable_sql_cache = ${originalSqlCache}"
        sql "SET enable_fallback_to_original_planner = ${originalFallback}"
        sql "SET enable_nereids_planner = ${originalNereids}"
    }
}
