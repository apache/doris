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

import org.junit.Assert
import java.util.concurrent.TimeUnit
import org.awaitility.Awaitility
import org.apache.doris.regression.suite.ClusterOptions

suite("test_compaction_on_sc_new_tablet", "docker") {
    def options = new ClusterOptions()
    options.setFeNum(1)
    options.setBeNum(1)
    options.enableDebugPoints()
    options.cloudMode = false
    options.beConfigs += [
        'enable_java_support=false',
        'enable_mow_compaction_correctness_check_core=true'
    ]
    docker(options) {
        try {
            GetDebugPoint().clearDebugPointsForAllFEs()
            GetDebugPoint().clearDebugPointsForAllBEs()
            def table1 = "test_compaction_on_sc_new_tablet"
            sql "DROP TABLE IF EXISTS ${table1} FORCE;"
            sql """ CREATE TABLE IF NOT EXISTS ${table1} (
                    `k`  int,
                    `c1` int,
                    `c2` int,
                    `c3` int
                    ) UNIQUE KEY(k)
                DISTRIBUTED BY HASH(k) BUCKETS 1
                PROPERTIES (
                "disable_auto_compaction" = "true",
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true"); """

            // [2-11]
            for (int i = 1; i <= 10; i++) {
                sql "insert into ${table1} values($i,$i,$i,$i);"
            }
            qt_sql "select * from ${table1} order by k;"


            def beNodes = sql_return_maparray("show backends;")
            def tabletStats = sql_return_maparray("show tablets from ${table1};")
            logger.info("tabletStats: \n${tabletStats}")
            def tabletStat = tabletStats.get(0)
            def tabletBackendId = tabletStat.BackendId
            def tabletId = tabletStat.TabletId
            def version = tabletStat.Version
            def tabletBackend;
            for (def be : beNodes) {
                if (be.BackendId == tabletBackendId) {
                    tabletBackend = be
                    break;
                }
            }
            logger.info("tablet ${tabletId} is on backend ${tabletBackend.Host} with backendId=${tabletBackend.BackendId}, version=${version}");

            // blocking the schema change process before it gains max version
            GetDebugPoint().enableDebugPointForAllBEs("SchemaChangeJob::_do_process_alter_tablet.block")
            Thread.sleep(2000)

            sql "alter table ${table1} modify column c1 varchar(100);"

            Thread.sleep(4000)

            // double write [11-22]
            for (int i = 20; i <= 30; i++) {
                sql "insert into ${table1} values(1,9,9,9);"
            }

            tabletStats = sql_return_maparray("show tablets from ${table1};")
            logger.info("tabletStats: \n${tabletStats}")
            assertEquals(2, tabletStats.size())

            def oldTabletStat
            def newTabletStat
            for (def stat: tabletStats) {
                if (!stat.TabletId.equals(tabletId)) {
                    newTabletStat = stat
                } else {
                    oldTabletStat = stat
                }
            }
            logger.info("old tablet=[tablet_id=${oldTabletStat.TabletId}, version=${oldTabletStat.Version}]")
            logger.info("new tablet=[tablet_id=${newTabletStat.TabletId}, version=${newTabletStat.Version}]")

            
            // trigger cumu compaction on new tablet
            int start_version = 15
            int end_version = 17
            // block compaction process on new tablet
            GetDebugPoint().enableDebugPointForAllBEs("CumulativeCompaction::execute_compact.block", [tablet_id: "${newTabletStat.TabletId}"])
            // manully set cumu compaction's input rowsets on new tablet
            GetDebugPoint().enableDebugPointForAllBEs("SizeBasedCumulativeCompactionPolicy::pick_input_rowsets.set_input_rowsets",
                    [tablet_id:"${newTabletStat.TabletId}", start_version:"${start_version}", end_version:"${end_version}"])

            Thread.sleep(2000)

            logger.info("trigger compaction [15-17] on new tablet ${newTabletStat.TabletId}")
            def (code, out, err) = be_run_cumulative_compaction(tabletBackend.Host, tabletBackend.HttpPort, newTabletStat.TabletId)
            logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
            Assert.assertEquals(code, 0)
            def compactJson = parseJson(out.trim())
            Assert.assertEquals("success", compactJson.status.toLowerCase())

            // make the schema change run to complete and wait for it
            GetDebugPoint().disableDebugPointForAllBEs("SchemaChangeJob::_do_process_alter_tablet.block")
            waitForSchemaChangeDone {
                sql """ SHOW ALTER TABLE COLUMN WHERE TableName='${table1}' ORDER BY createtime DESC LIMIT 1 """
                time 2000
            }

            Thread.sleep(2000)

            // make the cumu compaction run to complete and wait for it
            GetDebugPoint().disableDebugPointForAllBEs("CumulativeCompaction::execute_compact.block")


            // BE should skip to check merged rows in cumu compaction, otherwise it will cause coredump
            // becasue [11-22] in new tablet will skip to calc delete bitmap becase tablet is in NOT_READY state
            Thread.sleep(7000)

            qt_sql "select * from ${table1} order by k;"

        } catch(Exception e) {
            logger.info(e.getMessage())
            throw e
        } finally {
            GetDebugPoint().clearDebugPointsForAllFEs()
            GetDebugPoint().clearDebugPointsForAllBEs()
        }
    }
}
