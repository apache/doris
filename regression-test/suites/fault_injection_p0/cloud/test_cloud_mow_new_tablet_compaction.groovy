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

import java.util.concurrent.TimeUnit
import org.awaitility.Awaitility

suite("test_cloud_mow_new_tablet_compaction", "nonConcurrent") {
    if (!isCloudMode()) {
        return
    }

    GetDebugPoint().clearDebugPointsForAllFEs()
    GetDebugPoint().clearDebugPointsForAllBEs()

    def customBeConfig = [
        enable_new_tablet_do_compaction : true
    ]

    setBeConfigTemporary(customBeConfig) {
        def table1 = "test_cloud_mow_new_tablet_compaction"
        sql "DROP TABLE IF EXISTS ${table1} FORCE;"
        sql """ CREATE TABLE IF NOT EXISTS ${table1} (
                `k1` int NOT NULL,
                `c1` int,
                `c2` int,
                `c3` int
                )UNIQUE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "enable_unique_key_merge_on_write" = "true",
                "disable_auto_compaction" = "true",
                "replication_num" = "1"); """

        sql "insert into ${table1} values(1,1,1,1);"
        sql "insert into ${table1} values(2,2,2,2);"
        sql "insert into ${table1} values(3,3,3,2);"
        sql "sync;"
        qt_sql "select * from ${table1} order by k1;"

        def backends = sql_return_maparray('show backends')
        def tabletStats = sql_return_maparray("show tablets from ${table1};")
        assert tabletStats.size() == 1
        def tabletId = tabletStats[0].TabletId
        def tabletBackendId = tabletStats[0].BackendId
        def tabletBackend
        for (def be : backends) {
            if (be.BackendId == tabletBackendId) {
                tabletBackend = be
                break;
            }
        }
        logger.info("tablet ${tabletId} on backend ${tabletBackend.Host} with backendId=${tabletBackend.BackendId}");

        try {
            GetDebugPoint().enableDebugPointForAllBEs("CloudSchemaChangeJob::_convert_historical_rowsets.block")
            GetDebugPoint().enableDebugPointForAllBEs("CloudSchemaChangeJob::_process_delete_bitmap.after.capture_without_lock")
            sql "alter table ${table1} modify column c1 varchar(100);"

            Thread.sleep(3000)

            tabletStats = sql_return_maparray("show tablets from ${table1};")
            def newTabletId = "-1"
            for (def stat : tabletStats) {
                if (stat.TabletId != tabletId) {
                    newTabletId = stat.TabletId
                    break
                }
            }

            logger.info("new_tablet_id=${newTabletId}")

            int start_ver = 5
            int end_ver = 4

            // these load will skip to calculate bitmaps in publish phase on new tablet because it's in NOT_READY state
            GetDebugPoint().enableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.enable_spin_wait")
            GetDebugPoint().enableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.block")
            def threads = []
            threads << Thread.start { sql "insert into ${table1} values(1,99,99,99),(3,99,99,99);"}
            ++end_ver
            Thread.sleep(2000)
            threads << Thread.start { sql "insert into ${table1} values(5,88,88,88),(1,88,88,88);" }
            ++end_ver
            Thread.sleep(2000)
            threads << Thread.start { sql "insert into ${table1} values(3,77,77,77),(5,77,77,77);" }
            ++end_ver
            Thread.sleep(2000)
            GetDebugPoint().disableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.enable_spin_wait")
            GetDebugPoint().disableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.block")
            threads.each { it.join() }


            // let sc capture these rowsets when calculating increment rowsets without lock
            GetDebugPoint().disableDebugPointForAllBEs("CloudSchemaChangeJob::_convert_historical_rowsets.block")
            Thread.sleep(1000)

            // do cumu compaction on these rowsets on new tablet
            // this can happen when enable_new_tablet_do_compaction=true
            GetDebugPoint().enableDebugPointForAllBEs("CloudSizeBasedCumulativeCompactionPolicy::pick_input_rowsets.set_input_rowsets",
                        [tablet_id:"${newTabletId}", start_version: start_ver, end_version: end_ver]);
            {
                // trigger cumu compaction, should fail
                logger.info("trigger cumu compaction on tablet=${newTabletId} BE.Host=${tabletBackend.Host} with backendId=${tabletBackend.BackendId}")
                def (code, out, err) = be_run_cumulative_compaction(tabletBackend.Host, tabletBackend.HttpPort, newTabletId)
                logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
                assert code == 0
                def compactJson = parseJson(out.trim())
                assert "success" == compactJson.status.toLowerCase()
            }

            Thread.sleep(1000)

            def (code, out, err) = be_show_tablet_status(tabletBackend.Host, tabletBackend.HttpPort, newTabletId)
            assert code == 0
            assert !out.contains("\"last cumulative failure time\": \"1970-01-01")

            GetDebugPoint().disableDebugPointForAllBEs("CloudSchemaChangeJob::_process_delete_bitmap.after.capture_without_lock")
            // wait for sc to finish
            waitForSchemaChangeDone {
                sql """ SHOW ALTER TABLE COLUMN WHERE TableName='${table1}' ORDER BY createtime DESC LIMIT 1 """
                time 1000
            }

            qt_dup_key_count "select k1,count() as cnt from ${table1} group by k1 having cnt>1;"
            order_qt_sql "select * from ${table1};"
            
        } catch(Exception e) {
            logger.info(e.getMessage())
            throw e
        } finally {
            GetDebugPoint().clearDebugPointsForAllBEs()
            GetDebugPoint().clearDebugPointsForAllFEs()
        }
    }
}
