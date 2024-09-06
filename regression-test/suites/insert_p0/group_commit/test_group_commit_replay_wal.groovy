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

import org.awaitility.Awaitility
import static java.util.concurrent.TimeUnit.SECONDS

suite("test_group_commit_replay_wal", "nonConcurrent") {
    def tableName = "test_group_commit_replay_wal"

    def getRowCount = { expectedRowCount ->
        Awaitility.await().atMost(30, SECONDS).pollInterval(1, SECONDS).until(
            {
                def result = sql "select count(*) from ${tableName}"
                logger.info("table: ${tableName}, rowCount: ${result}")
                return result[0][0] == expectedRowCount
            }
        )
    }

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k` int ,
            `v` int ,
        ) engine=olap
        DISTRIBUTED BY HASH(`k`) 
        BUCKETS 5 
        properties("replication_num" = "1", "group_commit_interval_ms"="2000")
    """

    // 1. load success but commit rpc timeout
    // 2. should skip replay because of fe throw LabelAlreadyUsedException and txn status is VISIBLE
    GetDebugPoint().clearDebugPointsForAllBEs()
    GetDebugPoint().clearDebugPointsForAllFEs()
    try {
        GetDebugPoint().enableDebugPointForAllBEs("LoadBlockQueue._finish_group_commit_load.commit_success_and_rpc_error")
        streamLoad {
            table "${tableName}"
            set 'column_separator', ','
            set 'group_commit', 'async_mode'
            unset 'label'
            file 'group_commit_wal_msg.csv'
            time 10000
        }
        getRowCount(5)
        // check wal count is 0
    } catch (Exception e) {
        logger.info("failed: " + e.getMessage())
        assertTrue(false)
    } finally {
        GetDebugPoint().clearDebugPointsForAllBEs()
    }

    // load fail and abort fail, wal should not be deleted and retry
    try {
        GetDebugPoint().enableDebugPointForAllBEs("LoadBlockQueue._finish_group_commit_load.load_error")
        GetDebugPoint().enableDebugPointForAllFEs("FrontendServiceImpl.loadTxnRollback.error")
        streamLoad {
            table "${tableName}"
            set 'column_separator', ','
            set 'group_commit', 'async_mode'
            unset 'label'
            file 'group_commit_wal_msg.csv'
            time 10000
        }
        getRowCount(5)
        sleep(4000) // wal replay but all failed
        getRowCount(5)
        // check wal count is 1

        GetDebugPoint().clearDebugPointsForAllFEs()
        getRowCount(10)
        // check wal count is 0
    } catch (Exception e) {
        logger.info("failed: " + e.getMessage())
        assertTrue(false)
    } finally {
        GetDebugPoint().clearDebugPointsForAllFEs()
        GetDebugPoint().clearDebugPointsForAllBEs()
    }
}