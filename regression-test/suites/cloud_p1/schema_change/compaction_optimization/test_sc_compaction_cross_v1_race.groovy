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

// Test: Reproduce cross-V1 compaction race that causes BE crash.
//
// Timeline:
//   SC blocked → compaction commits [5-10] on new tablet → SC runs with V1=6 (override)
//   → SC commit replaces [2,6] but [5-10] not deleted (crosses V1) → version overlap → BE crash
//
// This simulates the multi-BE scenario where the SC-executing BE has a stale
// base tablet version, causing V1 to be lower than compaction output max.

import org.apache.doris.regression.suite.ClusterOptions

suite('test_sc_compaction_cross_v1_race', 'docker') {

    def options = new ClusterOptions()
    options.cloudMode = true
    options.enableDebugPoints()
    options.beConfigs += ["enable_java_support=false"]
    options.beConfigs += ["enable_new_tablet_do_compaction=true"]
    options.beConfigs += ["alter_tablet_worker_count=1"]
    options.beConfigs += ["cumulative_compaction_min_deltas=2"]
    options.beNum = 1

    docker(options) {
        def tableName = "sc_cross_v1_test"

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

        // Phase 2: Block SC at entry, let compaction run freely during block
        def scBlock = 'CloudSchemaChangeJob::process_alter_tablet.block'
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

            // Phase 4: Wait for compaction to merge [5-10] on new tablet
            sleep(30000)

            // Phase 5: Override V1=6, then release SC
            // Compaction [5-10] already committed to meta-service (no SC job yet → success)
            // SC will run with V1=6 → SC commit replaces [2,6] → [5-10] not deleted → overlap
            GetDebugPoint().enableDebugPointForAllBEs(
                'CloudSchemaChangeJob::process_alter_tablet.override_base_max_version',
                [version: 6])

        } finally {
            GetDebugPoint().disableDebugPointForAllBEs(scBlock)
        }

        // Wait for SC to finish
        int maxTries = 120
        def finalState = ""
        while (maxTries-- > 0) {
            finalState = getJobState(tableName)
            if (finalState == "FINISHED" || finalState == "CANCELLED") {
                break
            }
            sleep(1000)
        }

        // Clean up debug point
        GetDebugPoint().disableDebugPointForAllBEs(
            'CloudSchemaChangeJob::process_alter_tablet.override_base_max_version')

        logger.info("SC final state: ${finalState}")

        // Wait for potential BE crash from overlapping rowsets
        sleep(15000)

        // Verify BE is still alive
        def backendsAfter = sql_return_maparray("show backends")
        logger.info("BE alive status after SC: ${backendsAfter.collect { it.Alive }}")
        assertTrue(backendsAfter.every { it.Alive.toString() == "true" },
            "BE crashed after SC due to cross-V1 compaction rowset overlap")
    }
}
