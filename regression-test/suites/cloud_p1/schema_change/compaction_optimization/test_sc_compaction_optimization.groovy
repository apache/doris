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

    }
}
