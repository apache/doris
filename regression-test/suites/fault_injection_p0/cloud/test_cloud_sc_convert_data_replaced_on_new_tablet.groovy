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

suite("test_cloud_sc_convert_data_replaced_on_new_tablet", "nonConcurrent") {
    if (!isCloudMode()) {
        return
    }

    GetDebugPoint().clearDebugPointsForAllFEs()
    GetDebugPoint().clearDebugPointsForAllBEs()

    def customBeConfig = [
        enable_new_tablet_do_compaction : true
    ]

    setBeConfigTemporary(customBeConfig) {

        def table1 = "test_cloud_sc_convert_data_replaced_on_new_tablet"
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

        sql "insert into ${table1} values(1,1,1,1);"  // 2
        sql "insert into ${table1} values(2,2,2,2);"  // 3
        sql "insert into ${table1} values(3,3,3,2);"  // 4
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


        def trigger_and_wait_for_compaction = { tablet_id, int start, int end ->
            // trigger cumu compaction on new tablet on rowset written after alter_version to change its cumu point
            GetDebugPoint().enableDebugPointForAllBEs("CloudSizeBasedCumulativeCompactionPolicy::pick_input_rowsets.set_input_rowsets",
                        [tablet_id:"${tablet_id}", start_version: start, end_version: end]);

            {
                logger.info("trigger cumu compaction on tablet=${tablet_id} BE.Host=${tabletBackend.Host} with backendId=${tabletBackend.BackendId}")
                def (code, out, err) = be_run_cumulative_compaction(tabletBackend.Host, tabletBackend.HttpPort, tablet_id)
                logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
                assert code == 0
                def compactJson = parseJson(out.trim())
                assert "success" == compactJson.status.toLowerCase()
            }

            boolean running
            do {
                Thread.sleep(1000)
                def (code, out, err) = be_get_compaction_status(tabletBackend.Host, tabletBackend.HttpPort, tablet_id)
                logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
                assertEquals(code, 0)
                def compactionStatus = parseJson(out.trim())
                assertEquals("success", compactionStatus.status.toLowerCase())
                running = compactionStatus.run_status
            } while (running)
        }

        trigger_and_wait_for_compaction(tabletId, 3, 4)


        try {
            GetDebugPoint().enableDebugPointForAllBEs("CloudSchemaChangeJob.process_alter_tablet.sleep")
            GetDebugPoint().enableDebugPointForAllBEs("CloudSchemaChangeJob.process_alter_tablet.enter.block")
            sql "alter table ${table1} modify column c2 varchar(100);"

            Thread.sleep(1000)

            tabletStats = sql_return_maparray("show tablets from ${table1};")
            def newTabletId = "-1"
            for (def stat : tabletStats) {
                if (stat.TabletId != tabletId) {
                    newTabletId = stat.TabletId
                    break
                }
            }
            logger.info("new_tablet_id: ${newTabletId}")

            sql "insert into ${table1} values(1,10,10,10);" // 5
            sql "insert into ${table1} values(2,20,20,20);" // 6
            sql "insert into ${table1} values(3,30,30,30);" // 7
            sql "insert into ${table1} values(1,666,666,666);"    // 8

            Thread.sleep(2000)

            GetDebugPoint().disableDebugPointForAllBEs("CloudSchemaChangeJob.process_alter_tablet.enter.block")

            sql "insert into ${table1} values(10,99,99,99);" // 9
            sql "insert into ${table1} values(2,88,88,88);" // 10
            sql "insert into ${table1} values(8,8,8,8);"    // 11
            sql "insert into ${table1} values(9,9,9,9);"    // 12
            sql "insert into ${table1} values(10,10,10,10);" // 13

            Thread.sleep(2000)

            trigger_and_wait_for_compaction(newTabletId, 11, 12)

            // // trigger cumu compaction on new tablet on rowset written after alter_version to change its cumu point
            // GetDebugPoint().enableDebugPointForAllBEs("CloudSizeBasedCumulativeCompactionPolicy::pick_input_rowsets.set_input_rowsets",
            //             [tablet_id:"${newTabletId}", start_version: 11, end_version: 12]);

            // {
            //     logger.info("trigger cumu compaction on tablet=${newTabletId} BE.Host=${tabletBackend.Host} with backendId=${tabletBackend.BackendId}")
            //     def (code, out, err) = be_run_cumulative_compaction(tabletBackend.Host, tabletBackend.HttpPort, newTabletId)
            //     logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
            //     assert code == 0
            //     def compactJson = parseJson(out.trim())
            //     assert "success" == compactJson.status.toLowerCase()
            // }

            // boolean running
            // do {
            //     Thread.sleep(1000)
            //     def (code, out, err) = be_get_compaction_status(tabletBackend.Host, tabletBackend.HttpPort, newTabletId)
            //     logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
            //     assertEquals(code, 0)
            //     def compactionStatus = parseJson(out.trim())
            //     assertEquals("success", compactionStatus.status.toLowerCase())
            //     running = compactionStatus.run_status
            // } while (running)

            Thread.sleep(1000)

            GetDebugPoint().disableDebugPointForAllBEs("CloudSchemaChangeJob.process_alter_tablet.sleep")

            // // just to trigger sync_rowsets() on base tablet
            // // and let the process of converting historical rowsets happens after base tablet's max version is updated
            // // and before the base tablet's delete bitmap is updated
            // GetDebugPoint().enableDebugPointForAllBEs("CloudMetaMgr::sync_tablet_delete_bitmap.before.merge_delete_bitmap_to_local")
            // def t1 = Thread.start { sql "select * from ${table1};" }

            // // Thread.sleep(1000)
            // // alter version should be 5
            // GetDebugPoint().disableDebugPointForAllBEs("CloudSchemaChangeJob.process_alter_tablet.after.base_tablet.sync_rowsets")

            // // // let load 1 finish on new tablet
            // // Thread.sleep(1000)
            // // GetDebugPoint().disableDebugPointForAllBEs("CloudTabletCalcDeleteBitmapTask::handle.enter.block")
            // // t1.join()

            // Thread.sleep(1000)
            // GetDebugPoint().disableDebugPointForAllBEs("CloudSchemaChangeJob::_convert_historical_rowsets.block")
            waitForSchemaChangeDone {
                sql """ SHOW ALTER TABLE COLUMN WHERE TableName='${table1}' ORDER BY createtime DESC LIMIT 1 """
                time 1000
            }

            // GetDebugPoint().disableDebugPointForAllBEs("CloudMetaMgr::sync_tablet_delete_bitmap.before.merge_delete_bitmap_to_local")
            // t1.join()

            // sql "insert into ${table1} values(1,77,77,77);"

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
