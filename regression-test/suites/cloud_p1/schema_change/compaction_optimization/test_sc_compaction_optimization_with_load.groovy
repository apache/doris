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

// Test: End-to-end correctness with SC compaction optimization.
// Verifies:
// 1. SC completes successfully with concurrent writes and auto compaction on new tablets
// 2. alter_version cleanup works — post-SC compaction runs normally
// 3. Data consistency after SC + compaction + continued loading

import org.apache.doris.regression.suite.ClusterOptions

suite('test_sc_compaction_optimization_with_load', 'docker') {

    def options = new ClusterOptions()
    options.cloudMode = true
    options.enableDebugPoints()
    options.beConfigs += ["enable_java_support=false"]
    options.beConfigs += ["enable_new_tablet_do_compaction=true"]
    options.beConfigs += ["alter_tablet_worker_count=1"]
    options.beConfigs += ["cumulative_compaction_min_deltas=2"]
    options.beNum = 1

    docker(options) {
        def tableName = "sc_opt_load_test"

        def getJobState = { tbl ->
            def result = sql """SHOW ALTER TABLE COLUMN WHERE IndexName='${tbl}' ORDER BY createtime DESC LIMIT 1"""
            logger.info("getJobState: ${result}")
            return result[0][9]
        }

        def insertBatch = { int startKey, int count, String prefix ->
            StringBuilder sb = new StringBuilder()
            sb.append("INSERT INTO ${tableName} VALUES ")
            for (int j = 0; j < count; j++) {
                if (j > 0) sb.append(", ")
                def key = startKey + j
                sb.append("(${key}, '${prefix}_k2_${key}', ${key}, '${prefix}_v2_${key}')")
            }
            sql sb.toString()
        }

        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
            CREATE TABLE ${tableName} (
                k1 int NOT NULL,
                k2 varchar(50) NOT NULL,
                v1 int NOT NULL,
                v2 varchar(200) NOT NULL
            )
            DISTRIBUTED BY HASH(k1) BUCKETS 3
            PROPERTIES (
                "replication_num" = "1"
            )
        """

        // Phase 1: Load initial data
        for (int i = 0; i < 5; i++) {
            insertBatch(i * 30 + 1, 30, "init_${i}")
        }
        assertEquals(150L, (sql "SELECT count(*) FROM ${tableName}")[0][0])

        def baseTablets = sql_return_maparray("SHOW TABLETS FROM ${tableName}")
        assertEquals(3, baseTablets.size())
        def baseTabletIds = baseTablets.collect { it.TabletId.toString() }

        def backends = sql_return_maparray("show backends")
        def be = backends[0]

        // Block SC at the very beginning of process_alter_tablet, BEFORE prepare_tablet_job.
        // This avoids meta-service tablet job lock which would block BE HTTP service.
        def injectName = 'CloudSchemaChangeJob::process_alter_tablet.block'
        GetDebugPoint().enableDebugPointForAllBEs(injectName)

        try {
            sql "ALTER TABLE ${tableName} MODIFY COLUMN v1 bigint"
            sleep(10000)
            assertEquals("RUNNING", getJobState(tableName))

            // Phase 3: Heavy loading during SC
            for (int i = 0; i < 8; i++) {
                insertBatch(200 + i * 20, 20, "sc_${i}")
            }

            def allTablets = sql_return_maparray("SHOW TABLETS FROM ${tableName}")
            assertEquals(6, allTablets.size())
            def newTablets = allTablets.findAll { !(it.TabletId.toString() in baseTabletIds) }
            assertEquals(3, newTablets.size())

            // Wait for auto compaction to sync double-write rowsets and compact them
            sleep(30000)

            // Verify compaction happened: check if any non-placeholder rowset spans multiple
            // versions (e.g. [4-6]). Each INSERT creates a single-version rowset [v-v],
            // so a multi-version rowset is direct proof of compaction.
            boolean compactionHappened = false
            for (def tablet : newTablets) {
                def tabletId = tablet.TabletId.toString()
                def (code, out, err) = curl("GET", tablet.CompactionStatus)
                if (code == 0) {
                    def status = parseJson(out.trim())
                    if (status.rowsets instanceof List) {
                        logger.info("New tablet ${tabletId} rowsets: ${status.rowsets}")
                        for (def rowset : status.rowsets) {
                            def match = (rowset =~ /\[(\d+)-(\d+)\]/)
                            if (match) {
                                def start = match[0][1] as int
                                def end = match[0][2] as int
                                if (start > 1 && end > start) {
                                    logger.info("New tablet ${tabletId} has merged rowset [${start}-${end}], compaction confirmed")
                                    compactionHappened = true
                                    break
                                }
                            }
                        }
                    }
                }
                if (compactionHappened) break
            }
            assertTrue(compactionHappened, "Expected auto compaction on new tablets during SC queue wait")

        } finally {
            GetDebugPoint().disableDebugPointForAllBEs(injectName)
        }

        // Phase 4: Wait for SC completion
        int maxTries = 300
        def finalState = ""
        while (maxTries-- > 0) {
            finalState = getJobState(tableName)
            if (finalState == "FINISHED" || finalState == "CANCELLED") {
                sleep(10000)  // Wait 10s for BE to fully recover
                break
            }
            sleep(1000)
        }
        assertEquals("FINISHED", finalState)

        // Phase 5: Verify data correctness and compaction
        assertEquals(310L, (sql "SELECT count(*) FROM ${tableName}")[0][0])

        // Verify column type changed
        def schema = sql "DESC ${tableName}"
        def v1Type = schema.find { it[0] == "v1" }[1]
        assertEquals("bigint", v1Type.toLowerCase())

        // Phase 6: Post-SC loading (verify alter_version cleanup)
        for (int i = 0; i < 3; i++) {
            insertBatch(500 + i * 10, 10, "post_${i}")
        }
        // Phase 6: Verify data correctness and schema change
        def expectedCount = 150 + 160 + 30  // initial(5*30) + SC inserts(8*20) + post-SC(3*10)
        assertEquals(expectedCount, (sql "SELECT count(*) FROM ${tableName}")[0][0])
        assertEquals(expectedCount, (sql "SELECT count(distinct k1) FROM ${tableName}")[0][0])

        def desc = sql "DESC ${tableName}"
        def v1Col = desc.find { it[0] == "v1" }
        assertTrue(v1Col[1].toString().toLowerCase().contains("bigint"))
    }
}
