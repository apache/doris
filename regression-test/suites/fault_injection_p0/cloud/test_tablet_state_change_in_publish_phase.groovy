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

import org.apache.doris.regression.suite.ClusterOptions

suite("test_tablet_state_change_in_publish_phase", "docker") {
    def options = new ClusterOptions()
    options.setFeNum(1)
    options.setBeNum(2)
    options.cloudMode = true
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'sys_log_verbose_modules=org',
        'heartbeat_interval_second=1'
    ]
    options.enableDebugPoints()

    docker(options) {
        GetDebugPoint().clearDebugPointsForAllFEs()
        GetDebugPoint().clearDebugPointsForAllBEs()

        def table1 = "test_tablet_state_change_in_publish_phase"
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

        sql "insert into ${table1} values(1,1,1);"
        sql "insert into ${table1} values(2,2,2);"
        sql "insert into ${table1} values(3,3,3);"
        sql "sync;"
        qt_sql "select * from ${table1} order by k1;"

        def beNodes = sql_return_maparray("show backends;")
        def tabletStat = sql_return_maparray("show tablets from ${table1};").get(0)
        def tabletBackendId = tabletStat.BackendId
        def tabletId = tabletStat.TabletId
        def be1
        for (def be : beNodes) {
            if (be.BackendId == tabletBackendId) {
                be1 = be
            }
        }
        logger.info("tablet ${tabletId} on backend ${be1.Host} with backendId=${be1.BackendId}");
        logger.info("backends: ${cluster.getBackends()}")
        int beIndex = 1
        for (def backend : cluster.getBackends()) {
            if (backend.host == be1.Host) {
                beIndex = backend.index
                break
            }
        }
        assert cluster.getBeByIndex(beIndex).backendId as String == tabletBackendId

        try {
            GetDebugPoint().enableDebugPointForAllBEs("CloudSchemaChangeJob::_convert_historical_rowsets.block")
            sql "alter table ${table1} modify column c1 varchar(100);"
            Thread.sleep(1000)

            cluster.stopBackends(beIndex)

            Thread.sleep(1000)

            // let tablet be on another BE
            sql "insert into ${table1} values(10,88,88);"
            qt_sql "select * from ${table1} order by k1;"
            assert sql_return_maparray("show tablets from ${table1};").get(0).BackendId as String != tabletBackendId

            // block FE's task report handler to avoid alter task re-sended to BE before we enable debug points for SC
            GetDebugPoint().enableDebugPointForAllFEs("ReportHandler.block")
            cluster.startBackends(beIndex)
            GetDebugPoint().enableDebugPointForAllBEs("CloudSchemaChangeJob::_convert_historical_rowsets.block")
            GetDebugPoint().enableDebugPointForAllBEs("CloudSchemaChangeJob.process_alter_tablet.sleep")
            GetDebugPoint().disableDebugPointForAllFEs("ReportHandler.block")

            def newThreadInDocker = { Closure actionSupplier ->
                def connInfo = context.threadLocalConn.get()
                return Thread.start {
                    connect(connInfo.username, connInfo.password, connInfo.conn.getMetaData().getURL(), actionSupplier)
                }
            }

            // let load 1 block before publish
            GetDebugPoint().enableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.enable_spin_wait")
            GetDebugPoint().enableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.block")
            def t1 = newThreadInDocker {
                // load 1 will not see any historical data when flush
                // and will skip to calculate delete bitmaps in later phase becase the tablet's state is NOT_READY
                sql "insert into ${table1} values(1,88,88);"
            }
            Thread.sleep(800)

            // let sc finish converting historical data
            GetDebugPoint().disableDebugPointForAllBEs("CloudSchemaChangeJob::_convert_historical_rowsets.block")
            Thread.sleep(1000)

            // let load 1 publish
            GetDebugPoint().disableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.enable_spin_wait")
            GetDebugPoint().disableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.block")
            t1.join()

            // load 2
            sql "insert into ${table1} values(1,77,77);"


            // let load 3 block before publish
            GetDebugPoint().enableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.enable_spin_wait")
            GetDebugPoint().enableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.block")
            def t2 = newThreadInDocker {
                sql "insert into ${table1} values(1,99,99);"
            }
            Thread.sleep(1000)

            // let sc finish
            GetDebugPoint().disableDebugPointForAllBEs("CloudSchemaChangeJob.process_alter_tablet.sleep")

            awaitUntil(30) {
                def res = sql_return_maparray """ SHOW ALTER TABLE COLUMN WHERE TableName='${table1}' ORDER BY createtime DESC LIMIT 1 """
                logger.info("alter status: ${res}")
                res[0].State as String == "FINISHED"
            }
            // tablet state has changed to NORMAL in MS

            // let load 3 publish
            GetDebugPoint().disableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.enable_spin_wait")
            GetDebugPoint().disableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.block")
            t2.join()

            qt_dup_key_count "select k1,count() as cnt from ${table1} group by k1 having cnt>1;"
            qt_sql "select * from ${table1} order by k1;"

        } catch(Exception e) {
            logger.info(e.getMessage())
            throw e
        } finally {
            GetDebugPoint().clearDebugPointsForAllFEs()
            GetDebugPoint().clearDebugPointsForAllBEs()
        }
    }
}
