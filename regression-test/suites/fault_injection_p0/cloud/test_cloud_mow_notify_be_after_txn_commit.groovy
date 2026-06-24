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

suite("test_cloud_mow_notify_be_after_txn_commit", "nonConcurrent") {
    if (!isCloudMode()) {
        return
    }

    GetDebugPoint().clearDebugPointsForAllFEs()
    GetDebugPoint().clearDebugPointsForAllBEs()

    def customFeConfig = [
        enable_notify_be_after_load_txn_commit: true
    ]
    def customBeConfig = [
        enable_cloud_make_rs_visible_on_be : true,
        cloud_mow_sync_rowsets_when_load_txn_begin : false
    ]

    setFeConfigTemporary(customFeConfig) {
        setBeConfigTemporary(customBeConfig) {
            try {
                def table1 = "test_cloud_mow_notify_be_after_txn_commit"
                sql "DROP TABLE IF EXISTS ${table1} FORCE;"
                sql """ CREATE TABLE IF NOT EXISTS ${table1} (
                            `k1` int NOT NULL,
                            `c1` int,
                            `c2` int
                            )UNIQUE KEY(k1)
                        DISTRIBUTED BY HASH(k1) BUCKETS 1
                        PROPERTIES (
                            "enable_unique_key_merge_on_write" = "true",
                            "disable_auto_compaction" = "true",
                            "replication_num" = "1"); """
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

                sql "insert into ${table1} values(1,1,1),(2,1,1),(3,1,1);"
                qt_1 "select * from ${table1} order by k1;"

                // inject error to ordinary sync_rowsets calls
                GetDebugPoint().enableDebugPointForAllBEs("CloudMetaMgr::sync_tablet_rowsets.before.inject_error", ["tablet_id": tabletId])

                sql "insert into ${table1} values(1,10,10),(4,10,10);"
                sql "insert into ${table1} values(2,20,20),(5,20,20),(1,20,20);"
                sql "insert into ${table1} values(3,30,30),(6,30,30),(5,30,30);"

                qt_2 "select * from ${table1} order by k1;"

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