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

suite("test_cloud_dup_forward_notify_be_after_txn_commit", "nonConcurrent") {
    if (!isCloudMode()) {
        return
    }

    GetDebugPoint().clearDebugPointsForAllFEs()
    GetDebugPoint().clearDebugPointsForAllBEs()

    def getTabletAndBackend = { def tableName ->
        def backends = sql_return_maparray('show backends')
        def tabletStats = sql_return_maparray("show tablets from ${tableName};")
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
        return [tabletId, tabletBackend]
    }

    def customFeConfig = [
        enable_notify_be_after_load_txn_commit: true
    ]
    def customBeConfig = [
        enable_cloud_make_rs_visible_on_be : true,
        cloud_mow_sync_rowsets_when_load_txn_begin : false,
        enable_stream_load_commit_txn_on_be : true // commit txn to MS directly on BE
    ]

    def getTabletRowsets = {def tableName ->
        def tablets = sql_return_maparray """ show tablets from ${tableName}; """
        assert tablets.size() == 1
        String compactionUrl = tablets[0]["CompactionStatus"]
        def (code, out, err) = curl("GET", compactionUrl)
        def tabletJson = parseJson(out.trim())
        assert tabletJson.rowsets instanceof List
        return tabletJson.rowsets
    }

    def executeStreamLoad = { def tableName -> 
        String data = """1,1,1"""
        streamLoad {
            table "${tableName}"
            set 'column_separator', ','
            set 'format', 'csv'
            inputStream new ByteArrayInputStream(data.getBytes())
            time 10000
        }
    }

    setFeConfigTemporary(customFeConfig) {
        setBeConfigTemporary(customBeConfig) {
            try {
                def table1 = "test_cloud_dup_forward_notify_be_after_txn_commit"
                sql "DROP TABLE IF EXISTS ${table1} FORCE;"
                sql """ CREATE TABLE IF NOT EXISTS ${table1} (
                            `k1` int NOT NULL,
                            `c1` int,
                            `c2` int
                            )duplicate KEY(k1)
                        DISTRIBUTED BY HASH(k1) BUCKETS 1
                        PROPERTIES (
                            "disable_auto_compaction" = "true",
                            "replication_num" = "1"); """
                def (tabletId, tabletBackend) = getTabletAndBackend(table1)

                executeStreamLoad(table1) // ver=2
                qt_1_1 "select * from ${table1} order by k1;"

                GetDebugPoint().enableDebugPointForAllFEs("sendMakeCloudTmpRsVisibleTasks.skip");
                // inject error to ordinary sync_rowsets calls
                GetDebugPoint().enableDebugPointForAllBEs("CloudMetaMgr::sync_tablet_rowsets.before.inject_error", ["tablet_id": tabletId])

                // 1. test that after turn on the notify feature, rowsets will be visible on BE without sync_rowsets
                executeStreamLoad(table1) // ver=3
                executeStreamLoad(table1) // ver=4
                executeStreamLoad(table1) // ver=5
                sleep(500)
                assert getTabletRowsets(table1).size() == 5
                qt_1_2 "select * from ${table1} order by k1;"
                assert getTabletRowsets(table1).size() == 5

                // 2. test the notify rpc arrived not in order
                // block the notify rpc for version 8
                GetDebugPoint().enableDebugPointForAllBEs("make_cloud_committed_rs_visible_callback.block", ["tablet_id": tabletId, "version": 8])
                executeStreamLoad(table1) // ver=6
                executeStreamLoad(table1) //ver=7
                sleep(500)
                assert getTabletRowsets(table1).size() == 7
                executeStreamLoad(table1) // ver=8
                executeStreamLoad(table1) // ver=9
                // due the miss of rowset of version 8, version 8 and version 9 will not be added to BE's tablet meta
                sleep(500)
                assert getTabletRowsets(table1).size() == 7
                GetDebugPoint().disableDebugPointForAllBEs("make_cloud_committed_rs_visible_callback.block")
                sleep(500)
                assert getTabletRowsets(table1).size() == 9
                qt_2_1 "select * from ${table1} order by k1;"

            } catch (Exception e) {
                logger.info(e.getMessage())
                throw e
            } finally {
                GetDebugPoint().clearDebugPointsForAllFEs()
                GetDebugPoint().clearDebugPointsForAllBEs()
            }
        }
    }
}
