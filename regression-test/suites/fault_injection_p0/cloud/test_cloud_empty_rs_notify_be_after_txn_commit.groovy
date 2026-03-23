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

suite("test_cloud_empty_rs_notify_be_after_txn_commit", "nonConcurrent") {
    if (!isCloudMode()) {
        return
    }

    GetDebugPoint().clearDebugPointsForAllFEs()
    GetDebugPoint().clearDebugPointsForAllBEs()

    def getTabletAndBackend = { def tableName, int index ->
        def backends = sql_return_maparray('show backends')
        def tabletStats = sql_return_maparray("show tablets from ${tableName};")
        assert tabletStats.size() > index
        def tabletId = tabletStats[index].TabletId
        def tabletBackendId = tabletStats[index].BackendId
        def tabletBackend
        for (def be : backends) {
            if (be.BackendId == tabletBackendId) {
                tabletBackend = be
                break;
            }
        }
        logger.info("tablet ${tabletId},index=${index} on backend ${tabletBackend.Host} with backendId=${tabletBackend.BackendId}");
        return [tabletId, tabletBackend]
    }

    def getTableId = {def tabletId ->
        def info = sql_return_maparray """ show tablet ${tabletId}; """
        assert info.size() == 1
        return info[0]["TableId"]
    }

    def customFeConfig = [
        enable_notify_be_after_load_txn_commit: true
    ]
    def customBeConfig = [
        enable_cloud_make_rs_visible_on_be : true,
        cloud_mow_sync_rowsets_when_load_txn_begin : false,
        skip_writing_empty_rowset_metadata : true // empty rowset opt
    ]

    def getTabletRowsets = {def tabletId ->
        def info = sql_return_maparray """ show tablet ${tabletId}; """
        assert info.size() == 1
        def detail = sql_return_maparray """${info[0]["DetailCmd"]}"""
        assert detail instanceof List
        assert detail.size() == 1
        def compactionUrl = detail[0]["CompactionStatus"]
        def (code, out, err) = curl("GET", compactionUrl)
        def tabletJson = parseJson(out.trim())
        assert tabletJson.rowsets instanceof List
        return tabletJson.rowsets
    }

    // duplicate table
    setFeConfigTemporary(customFeConfig) {
        setBeConfigTemporary(customBeConfig) {
            try {
                def table1 = "test_cloud_dup_empty_rs_notify_be_after_txn_commit"
                sql "DROP TABLE IF EXISTS ${table1} FORCE;"
                sql """ CREATE TABLE IF NOT EXISTS ${table1} (
                            `k1` int NOT NULL,
                            `c1` int,
                            `c2` int
                            )duplicate KEY(k1)
                        DISTRIBUTED BY HASH(k1) BUCKETS 2
                        PROPERTIES (
                            "disable_auto_compaction" = "true",
                            "replication_num" = "1"); """
                def (tablet1Id, tablet1Backend) = getTabletAndBackend(table1, 0)
                def (tablet2Id, tablet2Backend) = getTabletAndBackend(table1, 1)

                def tableId = getTableId(tablet1Id)

                sql "insert into ${table1} values(1,1,1);" // ver=2
                qt_1_1 "select * from ${table1} order by k1;"

                // inject error to ordinary sync_rowsets calls
                GetDebugPoint().enableDebugPointForAllBEs("CloudMetaMgr::sync_tablet_rowsets.before.inject_error", ["table_id": tableId])

                // 1. test that after turn on the notify feature, rowsets will be visible on BE without sync_rowsets
                sql "insert into ${table1} values(1,1,1);" // ver=3
                sql "insert into ${table1} values(2,2,2);" // ver=4
                sql "insert into ${table1} values(3,3,3);" // ver=5
                sql "insert into ${table1} values(3,3,3);" // ver=6
                sleep(500)
                assert getTabletRowsets(tablet1Id).size() == 6
                assert getTabletRowsets(tablet2Id).size() == 6
                qt_1_2 "select * from ${table1} order by k1;"
                assert getTabletRowsets(tablet1Id).size() == 6
                assert getTabletRowsets(tablet2Id).size() == 6


                // 2. test the notify rpc arrived not in order
                GetDebugPoint().enableDebugPointForAllBEs("make_cloud_committed_rs_visible_callback.block", ["table_id": tableId, "version": 7])
                sql "insert into ${table1} values(1,1,1);"
                sql "insert into ${table1} values(1,1,1);"
                sql "insert into ${table1} values(3,3,3);"
                sql "insert into ${table1} values(3,3,3);"
                sql "insert into ${table1} values(1,1,1);"
                sql "insert into ${table1} values(1,1,1);"
                sql "insert into ${table1} values(3,3,3);"
                sql "insert into ${table1} values(3,3,3);"
                sql "insert into ${table1} values(2,2,2);"
                sql "insert into ${table1} values(2,2,2);"
                sql "insert into ${table1} values(3,3,3);"
                sql "insert into ${table1} values(3,3,3);"
                sleep(500)
                assert getTabletRowsets(tablet1Id).size() == 6
                assert getTabletRowsets(tablet2Id).size() == 6
                GetDebugPoint().disableDebugPointForAllBEs("make_cloud_committed_rs_visible_callback.block")
                sleep(500)
                assert getTabletRowsets(tablet1Id).size() == 18
                assert getTabletRowsets(tablet2Id).size() == 18
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


    // mow table
    setFeConfigTemporary(customFeConfig) {
        setBeConfigTemporary(customBeConfig) {
            try {
                def table1 = "test_cloud_mow_empty_rs_notify_be_after_txn_commit"
                sql "DROP TABLE IF EXISTS ${table1} FORCE;"
                sql """ CREATE TABLE IF NOT EXISTS ${table1} (
                            `k1` int NOT NULL,
                            `c1` int,
                            `c2` int
                            )unique KEY(k1)
                        DISTRIBUTED BY HASH(k1) BUCKETS 2
                        PROPERTIES (
                            "disable_auto_compaction" = "true",
                            "enable_unique_key_merge_on_write" = "true",
                            "replication_num" = "1"); """
                def (tablet1Id, tablet1Backend) = getTabletAndBackend(table1, 0)
                def (tablet2Id, tablet2Backend) = getTabletAndBackend(table1, 1)

                def tableId = getTableId(tablet1Id)

                sql "insert into ${table1} values(1,1,1);" // ver=2
                qt_1_1 "select * from ${table1} order by k1;"

                // inject error to ordinary sync_rowsets calls
                GetDebugPoint().enableDebugPointForAllBEs("CloudMetaMgr::sync_tablet_rowsets.before.inject_error", ["table_id": tableId])

                // 1. test that after turn on the notify feature, rowsets will be visible on BE without sync_rowsets
                sql "insert into ${table1} values(1,1,1);" // ver=3
                sql "insert into ${table1} values(2,2,2);" // ver=4
                sql "insert into ${table1} values(3,3,3);" // ver=5
                sql "insert into ${table1} values(3,3,3);" // ver=6
                sleep(500)
                assert getTabletRowsets(tablet1Id).size() == 6
                assert getTabletRowsets(tablet2Id).size() == 6
                qt_1_2 "select * from ${table1} order by k1;"
                assert getTabletRowsets(tablet1Id).size() == 6
                assert getTabletRowsets(tablet2Id).size() == 6
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
