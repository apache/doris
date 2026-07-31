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

// Test: SC_COMPACTION_CONFLICT retry succeeds after override is removed.
//
// Timeline:
//   1. Block SC → insert data → compaction runs freely on new tablet
//   2. Override base_max_version=6 → release SC → attempts detect cross-V1 → SC_COMPACTION_CONFLICT
//   3. Assert SC is still RUNNING (proves at least one attempt failed under override)
//   4. Disable override → FE retries → next attempt uses real V1 → SC succeeds
//
// Key assertions:
//   - SC stays RUNNING while override is on (proves cross-V1 failure occurred;
//     with only 120 rows a successful conversion finishes in <2s)
//   - SC eventually reaches FINISHED (retry works)
//   - Data integrity after SC

import org.apache.doris.regression.suite.ClusterOptions

suite('test_sc_compaction_cross_v1_retry', 'docker') {

    def options = new ClusterOptions()
    options.cloudMode = true
    options.enableDebugPoints()
    options.beConfigs += ["enable_java_support=false"]
    options.beConfigs += ["enable_new_tablet_do_compaction=true"]
    options.beConfigs += ["alter_tablet_worker_count=1"]
    options.beConfigs += ["cumulative_compaction_min_deltas=2"]
    options.beNum = 1
    options.feConfigs += ["enable_schema_change_retry=true"]
    // Use a high retry limit so the override window does not exhaust retries.
    // agent_task_resend_wait_time_ms defaults to 5s, so ~2-3 retries may fire
    // while override is active; 10 retries gives plenty of remaining budget.
    options.feConfigs += ["schema_change_max_retry_time=10"]

    docker(options) {
        def tableName = "sc_cross_v1_retry_test"

        def getJobState = { tbl ->
            def result = sql """SHOW ALTER TABLE COLUMN WHERE IndexName='${tbl}' ORDER BY createtime DESC LIMIT 1"""
            logger.info("getJobState: ${result}")
            return result[0][9]
        }

        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
            CREATE TABLE ${tableName} (
                k1 int NOT NULL,
                v1 varchar(100) NOT NULL,
                v2 int NOT NULL
            )
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            )
        """

        // Phase 1: Insert initial data (versions 2, 3, 4)
        for (int i = 0; i < 3; i++) {
            StringBuilder sb = new StringBuilder()
            sb.append("INSERT INTO ${tableName} VALUES ")
            for (int j = 0; j < 20; j++) {
                if (j > 0) sb.append(", ")
                def key = i * 20 + j + 1
                sb.append("(${key}, 'val_${key}', ${key * 10})")
            }
            sql sb.toString()
        }
        assertEquals(60L, (sql "SELECT count(*) FROM ${tableName}")[0][0])

        // Phase 2: Block SC, let compaction run freely during block
        def scBlock = 'CloudSchemaChangeJob::process_alter_tablet.block'
        def overrideDP = 'CloudSchemaChangeJob::process_alter_tablet.override_base_max_version'
        GetDebugPoint().enableDebugPointForAllBEs(scBlock)

        try {
            sql "ALTER TABLE ${tableName} MODIFY COLUMN v2 bigint"
            sleep(10000)
            assertEquals("RUNNING", getJobState(tableName))

            // Phase 3: Insert 6 batches (versions 5-10), compaction runs freely
            for (int i = 0; i < 6; i++) {
                StringBuilder sb = new StringBuilder()
                sb.append("INSERT INTO ${tableName} VALUES ")
                for (int j = 0; j < 10; j++) {
                    if (j > 0) sb.append(", ")
                    def key = 100 + i * 10 + j + 1
                    sb.append("(${key}, 'new_${key}', ${key * 10})")
                }
                sql sb.toString()
            }

            // Phase 4: Wait for compaction to merge on new tablet
            sleep(30000)

            // Phase 5: Override V1=6 so attempts trigger cross-V1 detection
            GetDebugPoint().enableDebugPointForAllBEs(overrideDP, [version: 6])

        } finally {
            // Release SC block → attempt runs with V1=6 → SC_COMPACTION_CONFLICT
            GetDebugPoint().disableDebugPointForAllBEs(scBlock)
        }

        // Phase 6: Verify SC_COMPACTION_CONFLICT was actually triggered.
        // With override on, SC cannot succeed. With only 120 rows, a successful
        // conversion would finish in <2s. So if SC is still RUNNING after 10s,
        // at least one attempt has failed due to the cross-V1 check.
        sleep(10000)
        assertEquals("RUNNING", getJobState(tableName),
                "SC should still be RUNNING with override on - cross-V1 failure expected")

        // Phase 7: Disable override → next retry uses real base_max_version → SC succeeds
        GetDebugPoint().disableDebugPointForAllBEs(overrideDP)

        // Wait for SC to finish via retry
        int maxTries = 180
        def finalState = ""
        while (maxTries-- > 0) {
            finalState = getJobState(tableName)
            if (finalState == "FINISHED" || finalState == "CANCELLED") {
                break
            }
            sleep(1000)
        }

        logger.info("SC final state after retry: ${finalState}")
        assertEquals("FINISHED", finalState)

        // Verify data integrity
        def totalRows = (sql "SELECT count(*) FROM ${tableName}")[0][0]
        assertEquals(120L, totalRows)

        // Verify schema change actually applied (v2 should be bigint now)
        def columns = sql "DESC ${tableName}"
        def v2Col = columns.find { it[0] == "v2" }
        assertTrue(v2Col[1].toString().toLowerCase().contains("bigint"),
                "v2 column should be bigint after schema change, got: ${v2Col[1]}")

        // Verify BE is still alive
        def backends = sql_return_maparray("show backends")
        assertTrue(backends.every { it.Alive.toString() == "true" },
                "BE should be alive after SC retry")
    }
}
