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

// Test: During schema change with multiple tablets queued, new tablets can do
// compaction (version > V0). This verifies the core optimization:
// - BE pre_submit_callback sets alter_version (V0) before task enqueue
// - New tablets in queue can do cumulative compaction immediately

import org.apache.doris.regression.suite.ClusterOptions

suite('test_sc_compaction_optimization', 'docker') {

    def options = new ClusterOptions()
    options.cloudMode = true
    options.enableDebugPoints()
    options.beConfigs += ["enable_java_support=false"]
    options.beConfigs += ["disable_auto_compaction=true"]
    options.beConfigs += ["enable_new_tablet_do_compaction=true"]
    options.beConfigs += ["alter_tablet_worker_count=1"]  // Only 1 SC worker thread to force queuing
    options.beNum = 1

    docker(options) {
        def tableName = "sc_opt_test"

        def getJobState = { tbl ->
            def result = sql """SHOW ALTER TABLE COLUMN WHERE IndexName='${tbl}' ORDER BY createtime DESC LIMIT 1"""
            logger.info("getJobState: ${result}")
            return result[0][9]
        }

        // Create table with 2 buckets -> 2 base tablets -> 2 SC tasks
        // With alter_tablet_worker_count=1: one task executes (blocked), one task queues
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
            CREATE TABLE ${tableName} (
                k1 int NOT NULL,
                v1 varchar(100) NOT NULL,
                v2 int NOT NULL
            )
            DISTRIBUTED BY HASH(k1) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1",
                "disable_auto_compaction" = "true"
            )
        """

        // Insert initial data in multiple batches to create multiple rowsets
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

        def initialCount = sql "SELECT count(*) FROM ${tableName}"
        assertEquals(60L, initialCount[0][0])

        // Record base tablet IDs before SC
        def baseTablets = sql_return_maparray("SHOW TABLETS FROM ${tableName}")
        logger.info("Base tablets before SC: ${baseTablets}")
        assertEquals(2, baseTablets.size())
        def baseTabletIds = baseTablets.collect { it.TabletId.toString() }

        def backends = sql_return_maparray("show backends")
        def be = backends[0]

        // Block SC execution at the end of process_alter_tablet
        def injectName = 'CloudSchemaChangeJob.process_alter_tablet.sleep'
        GetDebugPoint().enableDebugPointForAllBEs(injectName)

        try {
            // Trigger schema change: 2 tablets -> 2 SC tasks submitted to BE
            // With alter_tablet_worker_count=1: 1 task dequeued and blocked at DebugPoint,
            // 1 task still in queue.
            // Optimization: pre_submit_callback sets alter_version on BOTH tablet pairs
            // before they are enqueued, so even the queued new tablet has alter_version = V0
            sql "ALTER TABLE ${tableName} MODIFY COLUMN v2 bigint"

            // Wait for SC tasks to be submitted and one to start executing (blocked)
            sleep(10000)

            // Verify SC is in RUNNING state
            def scState = getJobState(tableName)
            assertEquals("RUNNING", scState)

            // Get all tablets including new (shadow) tablets
            def allTablets = sql_return_maparray("SHOW TABLETS FROM ${tableName}")
            logger.info("All tablets during SC: ${allTablets}")
            assertEquals(4, allTablets.size())

            // Identify new (shadow) tablets
            def newTablets = allTablets.findAll { !(it.TabletId.toString() in baseTabletIds) }
            logger.info("New (shadow) tablets: ${newTablets}")
            assertEquals(2, newTablets.size())

            // Load more data during SC — double-write creates rowsets on both base and new tablets
            // These rowsets have version > V0, and should be compactable with the optimization
            for (int i = 0; i < 3; i++) {
                StringBuilder sb = new StringBuilder()
                sb.append("INSERT INTO ${tableName} VALUES ")
                for (int j = 0; j < 10; j++) {
                    if (j > 0) sb.append(", ")
                    def key = 100 + i * 10 + j + 1
                    sb.append("(${key}, 'new_${key}', ${key * 10})")
                }
                sql sb.toString()
            }

            // Key verification: trigger cumulative compaction on new tablets
            // With optimization (pre_submit_callback): alter_version = V0 -> compaction accepted
            // Without optimization: alter_version = -1 for queued tablet -> compaction rejected
            def compactionAccepted = 0
            for (def tablet : newTablets) {
                def (code, out, err) = be_run_cumulative_compaction(be.Host, be.HttpPort, tablet.TabletId)
                logger.info("Trigger cu compaction on new tablet ${tablet.TabletId}: code=${code}, out=${out}")
                if (code == 0) {
                    def triggerResult = parseJson(out.trim())
                    def status = triggerResult.status.toLowerCase()
                    if (status == "success" || status == "already_exist") {
                        compactionAccepted++
                        logger.info("Cumulative compaction accepted for new tablet ${tablet.TabletId}")
                    } else {
                        logger.info("Cumulative compaction not accepted for new tablet ${tablet.TabletId}: ${status}")
                    }
                }
            }

            // ALL new tablets should accept cumulative compaction.
            // The executing tablet has alter_version set by SC execution code;
            // the queued tablet has alter_version set by pre_submit_callback (the optimization).
            // Without the optimization, the queued tablet would have alter_version=-1 and reject compaction.
            logger.info("Compaction accepted on ${compactionAccepted} out of ${newTablets.size()} new tablet(s)")
            assertEquals(newTablets.size(), compactionAccepted)

            // Wait for compactions to finish
            for (def tablet : newTablets) {
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
            // Release SC execution block
            GetDebugPoint().disableDebugPointForAllBEs(injectName)
        }

        // Wait for SC to finish
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

        // Verify data correctness: 60 initial + 30 during SC = 90 rows
        def finalCount = sql "SELECT count(*) FROM ${tableName}"
        logger.info("Final row count: ${finalCount}")
        assertEquals(90L, finalCount[0][0])

        // Verify data integrity — all keys should be distinct
        def distinctKeys = sql "SELECT count(distinct k1) FROM ${tableName}"
        assertEquals(90L, distinctKeys[0][0])
    }
}
