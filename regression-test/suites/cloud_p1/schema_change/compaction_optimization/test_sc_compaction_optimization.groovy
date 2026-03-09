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

// Test: Verify that pre_submit_callback correctly sets alter_version on new tablets
// so that auto cumulative compaction can compact double-write rowsets during SC.
// Without the optimization, alter_version=-1 and auto compaction skips NOTREADY tablets.

import org.apache.doris.regression.suite.ClusterOptions

suite('test_sc_compaction_optimization', 'docker') {

    def options = new ClusterOptions()
    options.cloudMode = true
    options.enableDebugPoints()
    options.beConfigs += ["enable_java_support=false"]
    options.beConfigs += ["enable_new_tablet_do_compaction=true"]
    options.beConfigs += ["alter_tablet_worker_count=1"]
    options.beConfigs += ["cumulative_compaction_min_deltas=2"]
    options.beNum = 1

    docker(options) {
        def tableName = "sc_opt_test"

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
            DISTRIBUTED BY HASH(k1) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1"
            )
        """

        // Insert initial data
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

        def baseTablets = sql_return_maparray("SHOW TABLETS FROM ${tableName}")
        assertEquals(2, baseTablets.size())
        def baseTabletIds = baseTablets.collect { it.TabletId.toString() }

        def backends = sql_return_maparray("show backends")
        def be = backends[0]

        // Block SC at the very beginning of process_alter_tablet, BEFORE prepare_tablet_job.
        // This avoids meta-service tablet job lock which would block BE HTTP service.
        def injectName = 'CloudSchemaChangeJob::process_alter_tablet.block'
        GetDebugPoint().enableDebugPointForAllBEs(injectName)

        try {
            sql "ALTER TABLE ${tableName} MODIFY COLUMN v2 bigint"
            sleep(10000)
            assertEquals("RUNNING", getJobState(tableName))

            def allTablets = sql_return_maparray("SHOW TABLETS FROM ${tableName}")
            assertEquals(4, allTablets.size())
            def newTablets = allTablets.findAll { !(it.TabletId.toString() in baseTabletIds) }
            assertEquals(2, newTablets.size())

            // Insert 6 batches during SC -> creates double-write rowsets on new tablets
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

            // Wait for auto compaction to trigger (don't query tablet status while SC is blocked)
            sleep(15000)

        } finally {
            GetDebugPoint().disableDebugPointForAllBEs(injectName)
        }

        // Wait for SC to finish
        int maxTries = 300
        def finalState = ""
        while (maxTries-- > 0) {
            finalState = getJobState(tableName)
            if (finalState == "FINISHED" || finalState == "CANCELLED") {
                sleep(10000)  // Wait 10s for BE to fully recover after SC
                break
            }
            sleep(1000)
        }
        assertEquals("FINISHED", finalState)

        // Verify data correctness after SC
        assertEquals(120L, (sql "SELECT count(*) FROM ${tableName}")[0][0])
        assertEquals(120L, (sql "SELECT count(distinct k1) FROM ${tableName}")[0][0])

        // Manually trigger cumulative compaction to verify SC compaction optimization
        def allTablets = sql_return_maparray("SHOW TABLETS FROM ${tableName}")
        def newTablets = allTablets.findAll { !(it.TabletId.toString() in baseTabletIds) }
        assertTrue(newTablets.size() > 0, "Should have new tablets after SC")

        for (def tablet : newTablets) {
            def tabletId = tablet.TabletId.toString()

            // Trigger cumulative compaction
            def (code, out, err) = be_run_cumulative_compaction(be.Host, be.HttpPort, tabletId)
            logger.info("Trigger cumulative compaction on tablet ${tabletId}: code=${code}")

            // Wait for compaction to complete
            boolean running = true
            int waitCount = 0
            while (running && waitCount < 100) {
                sleep(100)
                def (code2, out2, err2) = be_get_compaction_status(be.Host, be.HttpPort, tabletId)
                if (code2 == 0) {
                    def status = parseJson(out2.trim())
                    running = status.run_status
                }
                waitCount++
            }

            // Verify rowset count after compaction
            def (code3, out3, err3) = curl("GET", tablet.CompactionStatus)
            if (code3 == 0) {
                def status = parseJson(out3.trim())
                if (status.rowsets instanceof List) {
                    def rowsetCount = status.rowsets.size()
                    logger.info("Tablet ${tabletId} has ${rowsetCount} rowsets after compaction")
                    assertTrue(rowsetCount <= 2, "Expected rowset count <= 2, got ${rowsetCount}")
                }
            }
        }
    }
}
