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

// do compaction during commit rowset
suite("test_compact_with_seq", "nonConcurrent") {
    GetDebugPoint().clearDebugPointsForAllFEs()
    GetDebugPoint().clearDebugPointsForAllBEs()
    def tableName = "test_compact_with_seq"

    def enable_publish_spin_wait = {
        if (isCloudMode()) {
            // GetDebugPoint().enableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.enable_spin_wait")
        } else {
            GetDebugPoint().enableDebugPointForAllBEs("EnginePublishVersionTask::execute.enable_spin_wait")
        }
    }

    def enable_block_in_publish = {
        if (isCloudMode()) {
            // GetDebugPoint().enableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.block")
        } else {
            GetDebugPoint().enableDebugPointForAllBEs("EnginePublishVersionTask::execute.block")
        }
    }

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                `k1` int(11) NULL, 
                `k2` int(11) NULL, 
                `v3` int(11) NULL,
                `v4` int(11) NULL
            ) unique KEY(`k1`) 
            cluster by(`v3`, `v4`) 
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
                "function_column.sequence_col" = "v4",
                "replication_num" = "1",
                "disable_auto_compaction" = "true"
            );
    """
    sql """ INSERT INTO ${tableName} VALUES (10, 20, 39, 40),(11, 20, 38, 40),(12, 20, 37, 39),(13, 20, 36, 40),(14, 20, 35, 40); """
    sql """ INSERT INTO ${tableName} VALUES (11, 20, 38, 40),(15, 20, 39, 45),(16, 20, 33, 44),(17, 20, 39, 40),(18, 20, 36, 41); """
    sql """ INSERT INTO ${tableName} VALUES (12, 20, 37, 40),(13, 20, 34, 43),(14, 20, 34, 42),(15, 20, 36, 45),(16, 20, 34, 42); """
    sql """ INSERT INTO ${tableName} VALUES (10, 20, 36, 42),(13, 20, 39, 42),(11, 20, 38, 41),(16, 20, 31, 43),(14, 20, 31, 43); """
    sql """ INSERT INTO ${tableName} VALUES (13, 20, 35, 40),(16, 20, 32, 40),(14, 20, 36, 40),(17, 20, 32, 44),(14, 20, 32, 44); """
    sql """ INSERT INTO ${tableName} VALUES (10, 20, 36, 41),(17, 20, 34, 48),(16, 20, 36, 45),(12, 20, 33, 48),(15, 20, 35, 45); """
    sql """ INSERT INTO ${tableName} VALUES (13, 20, 35, 41),(18, 20, 37, 47),(19, 20, 35, 46),(11, 20, 34, 41),(19, 20, 33, 46); """
    order_qt_select1 """ select * from ${tableName}; """

    enable_publish_spin_wait()
    enable_block_in_publish()
    onFinish {
        GetDebugPoint().clearDebugPointsForAllFEs()
        GetDebugPoint().clearDebugPointsForAllBEs()
    }
    if (!isCloudMode()) {
        sql """ INSERT INTO ${tableName} VALUES (10, 20, 36, 39),(17, 20, 39, 46),(16, 20, 33, 44),(12, 20, 31, 49),(15, 20, 32, 39); """
        sql """ INSERT INTO ${tableName} VALUES (13, 20, 35, 49),(11, 20, 38, 39),(12, 20, 38, 39),(13, 20, 33, 41),(14, 20, 32, 38); """
    }
    order_qt_select2 """ select * from ${tableName}; """

    // trigger compaction
    // get be info
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort)
    def tablets = sql_return_maparray """ show tablets from ${tableName}; """
    for (def tablet in tablets) {
        String tablet_id = tablet.TabletId
        def backend_id = tablet.BackendId

        def (code, out, err) = be_run_cumulative_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
        logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        def compactJson = parseJson(out.trim())
        logger.info("compact json: " + compactJson)

        for (int i = 0; i < 10; i++) {
            (code, out, err) = be_show_tablet_status(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
            logger.info("loop " + i + ", Show tablet status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def json = parseJson(out.trim())
            logger.info("tablet rowsets: " + json)
            if (json.rowsets.size() <= 2) {
                break
            }
            sleep(2000)
        }
    }
    order_qt_select3 """ select * from ${tableName}; """

    GetDebugPoint().clearDebugPointsForAllFEs()
    GetDebugPoint().clearDebugPointsForAllBEs()
    if (isCloudMode()) {
        sql """ INSERT INTO ${tableName} VALUES (10, 20, 36, 39),(17, 20, 39, 46),(16, 20, 33, 44),(12, 20, 31, 49),(15, 20, 32, 39); """
        sql """ INSERT INTO ${tableName} VALUES (13, 20, 35, 49),(11, 20, 38, 39),(12, 20, 38, 39),(13, 20, 33, 41),(14, 20, 32, 38); """
    }
    // wait for publish
    for (int i = 0; i < 30; i++) {
        def result = sql "select v4 from ${tableName} where k1 = 13;"
        logger.info("v4 result: ${result}")
        if (result[0][0] == 49) {
            break
        }
        sleep(2000)
    }

    order_qt_select4 """ select * from ${tableName}; """
    // check no duplicated key
    def result = sql """ select `k1`, count(*) a from ${tableName} group by `k1` having a > 1; """
    logger.info("result: ${result}")
    assertEquals(0, result.size())
}
