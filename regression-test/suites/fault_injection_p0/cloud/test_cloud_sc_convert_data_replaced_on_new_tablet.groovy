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
            "replication_num" = "1",
            "function_column.sequence_col" = "c3"
            ); """

    sql "insert into ${table1} values(1,1,1,10);"  // 2
    sql "insert into ${table1} values(2,2,2,20);"  // 3
    sql "insert into ${table1} values(3,3,3,30);"  // 4
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
        GetDebugPoint().enableDebugPointForAllBEs("CloudSchemaChangeJob::process_alter_tablet.block")
        GetDebugPoint().enableDebugPointForAllBEs("CloudSchemaChangeJob::_convert_historical_rowsets.before.commit_job")
        // GetDebugPoint().enableDebugPointForAllBEs("CloudSchemaChangeJob::_convert_historical_rowsets.before.modify_tablet_meta")
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

        sql "insert into ${table1} values(1,99,99,1);" // 5

        // let query trigger sync_rowsets to make base tablet's max version be 5
        sql "select * from ${table1} order by k1;"

        Thread.sleep(1000)
        GetDebugPoint().disableDebugPointForAllBEs("CloudSchemaChangeJob::process_alter_tablet.block")
        Thread.sleep(1000)

        // to trigger sync_rowsets on new tablet and block before modify be's tablet meta
        GetDebugPoint().enableDebugPointForAllBEs("CloudMetaMgr::sync_tablet_rowsets.before.modify_tablet_meta", [tablet_id: newTabletId])
        def t1 = Thread.start {
            sql "insert into ${table1} values(100,100,100,1);"
        }

        Thread.sleep(1000)

        // let sc modify tablet meta first
        GetDebugPoint().disableDebugPointForAllBEs("CloudSchemaChangeJob::_convert_historical_rowsets.before.commit_job")
        Thread.sleep(1000)
        // and load's sync_rowsets() on new tablet modify be's tablet meta
        GetDebugPoint().disableDebugPointForAllBEs("CloudMetaMgr::sync_tablet_rowsets.before.modify_tablet_meta")

        t1.join()

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
