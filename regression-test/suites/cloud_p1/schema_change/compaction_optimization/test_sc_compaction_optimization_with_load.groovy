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

// Test: Schema change with concurrent data loading and compaction on multiple tablets.
// Verifies end-to-end correctness with the optimization:
// 1. SC completes successfully with multiple tablets and concurrent writes
// 2. Compaction works on new tablets after SC completion
// 3. Data is consistent after SC + compaction + continued loading

import org.apache.doris.regression.suite.ClusterOptions

suite('test_sc_compaction_optimization_with_load', 'docker') {

    def options = new ClusterOptions()
    options.cloudMode = true
    options.enableDebugPoints()
    options.beConfigs += ["enable_java_support=false"]
    options.beConfigs += ["disable_auto_compaction=true"]
    options.beConfigs += ["enable_new_tablet_do_compaction=true"]
    options.beConfigs += ["alter_tablet_worker_count=1"]
    options.beNum = 1

    docker(options) {
        def tableName = "sc_opt_load_test"

        def getJobState = { tbl ->
            def result = sql """SHOW ALTER TABLE COLUMN WHERE IndexName='${tbl}' ORDER BY createtime DESC LIMIT 1"""
            logger.info("getJobState: ${result}")
            return result[0][9]
        }

        // Create table with 3 buckets for more realistic multi-tablet scenario
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
                "replication_num" = "1",
                "disable_auto_compaction" = "true"
            )
        """

        // Phase 1: Load initial data with multiple rowsets
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

        // Create 5 rowsets with 30 rows each = 150 rows
        for (int i = 0; i < 5; i++) {
            insertBatch(i * 30 + 1, 30, "init_${i}")
        }

        def phase1Count = sql "SELECT count(*) FROM ${tableName}"
        assertEquals(150L, phase1Count[0][0])

        // Do cumulative compaction on base tablets to consolidate initial rowsets
        trigger_and_wait_compaction(tableName, "cumulative")

        def baseTablets = sql_return_maparray("SHOW TABLETS FROM ${tableName}")
        logger.info("Base tablets: ${baseTablets}")
        assertEquals(3, baseTablets.size())
        def baseTabletIds = baseTablets.collect { it.TabletId.toString() }

        def backends = sql_return_maparray("show backends")
        def be = backends[0]

        // Phase 2: Trigger SC with blocking — simulates queue scenario
        def injectName = 'CloudSchemaChangeJob.process_alter_tablet.sleep'
        GetDebugPoint().enableDebugPointForAllBEs(injectName)

        try {
            // ALTER adds a column — triggers SC for all 3 tablets
            // With alter_tablet_worker_count=1: 1 executes (blocked), 2 queue
            sql "ALTER TABLE ${tableName} MODIFY COLUMN v1 bigint"
            sleep(10000)

            def scState = getJobState(tableName)
            assertEquals("RUNNING", scState)

            // Phase 3: Heavy loading during SC — creates many rowsets via double-write
            for (int i = 0; i < 8; i++) {
                insertBatch(200 + i * 20, 20, "sc_${i}")
            }
            // 160 more rows during SC

            // Get all tablets
            def allTablets = sql_return_maparray("SHOW TABLETS FROM ${tableName}")
            logger.info("All tablets during SC: ${allTablets}")
            assertEquals(6, allTablets.size())  // 3 base + 3 new

            def newTablets = allTablets.findAll { !(it.TabletId.toString() in baseTabletIds) }
            assertEquals(3, newTablets.size())

            // Trigger cumulative compaction on both base and new tablets
            // Base tablets: compaction on versions <= V0 (within alter_version boundary)
            // New tablets: compaction on versions > V0 (double-write data)
            def newTabletCompactionAccepted = 0
            for (def tablet : allTablets) {
                def (code, out, err) = be_run_cumulative_compaction(be.Host, be.HttpPort, tablet.TabletId)
                logger.info("Trigger cu compaction on tablet ${tablet.TabletId}: code=${code}, out=${out}")
                if (code == 0 && tablet.TabletId.toString() in newTablets.collect { it.TabletId.toString() }) {
                    def triggerResult = parseJson(out.trim())
                    def status = triggerResult.status.toLowerCase()
                    if (status == "success" || status == "already_exist") {
                        newTabletCompactionAccepted++
                    }
                }
            }
            // All new tablets should accept compaction (alter_version set by pre_submit_callback)
            logger.info("New tablet compaction accepted: ${newTabletCompactionAccepted} / ${newTablets.size()}")
            assertEquals(newTablets.size(), newTabletCompactionAccepted)

            // Wait for compactions
            for (def tablet : allTablets) {
                boolean running = true
                int maxWait = 60
                while (running && maxWait-- > 0) {
                    Thread.sleep(1000)
                    def (code, out, err) = be_get_compaction_status(be.Host, be.HttpPort, tablet.TabletId)
                    if (code == 0) {
                        def compactionStatus = parseJson(out.trim())
                        running = compactionStatus.run_status
                    } else {
                        break
                    }
                }
            }

        } finally {
            GetDebugPoint().disableDebugPointForAllBEs(injectName)
        }

        // Phase 4: Wait for SC completion
        int maxTries = 300
        def finalState = ""
        while (maxTries-- > 0) {
            finalState = getJobState(tableName)
            if (finalState == "FINISHED" || finalState == "CANCELLED") {
                sleep(3000)
                break
            }
            sleep(1000)
        }
        assertEquals("FINISHED", finalState)

        // Phase 5: Verify data after SC
        def afterScCount = sql "SELECT count(*) FROM ${tableName}"
        logger.info("Row count after SC: ${afterScCount}")
        assertEquals(310L, afterScCount[0][0])  // 150 + 160

        // Phase 6: Continue loading after SC (verifies no residual alter_version issues)
        for (int i = 0; i < 3; i++) {
            insertBatch(500 + i * 10, 10, "post_${i}")
        }

        def afterPostCount = sql "SELECT count(*) FROM ${tableName}"
        assertEquals(340L, afterPostCount[0][0])  // 310 + 30

        // Phase 7: Compaction after SC — verifies alter_version is properly cleaned up
        trigger_and_wait_compaction(tableName, "cumulative")

        // Base compaction should also work now
        trigger_and_wait_compaction(tableName, "base")

        // Final data verification
        def finalCount = sql "SELECT count(*) FROM ${tableName}"
        assertEquals(340L, finalCount[0][0])

        // Verify all distinct keys
        def distinctKeys = sql "SELECT count(distinct k1) FROM ${tableName}"
        assertEquals(340L, distinctKeys[0][0])

        // Verify column type changed successfully
        def desc = sql "DESC ${tableName}"
        logger.info("Table description: ${desc}")
        def v1Col = desc.find { it[0] == "v1" }
        assertTrue(v1Col[1].toString().toLowerCase().contains("bigint"))
    }
}
