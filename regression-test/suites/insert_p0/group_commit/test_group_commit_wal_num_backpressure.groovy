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

suite("test_group_commit_wal_num_backpressure", "nonConcurrent") {
    def getRowCount = { expectedRowCount ->
        Awaitility.await().atMost(60, SECONDS).pollInterval(1, SECONDS).until(
            {
                def result = sql "select count(*) from test_group_commit_wal_num_backpressure"
                logger.info("table: test_group_commit_wal_num_backpressure, rowCount: ${result}, expectedRowCount: ${expectedRowCount}")
                return result[0][0] == expectedRowCount
            }
        )
    }

    sql """ DROP TABLE IF EXISTS test_group_commit_wal_num_backpressure """
    sql """
        CREATE TABLE IF NOT EXISTS test_group_commit_wal_num_backpressure (
            `k` int,
            `v` int
        ) engine=olap
        DISTRIBUTED BY HASH(`k`)
        BUCKETS 1
        properties(
            "replication_num" = "1",
            "group_commit_interval_ms" = "10000",
            "group_commit_data_bytes" = "1"
        )
    """

    GetDebugPoint().clearDebugPointsForAllBEs()
    GetDebugPoint().clearDebugPointsForAllFEs()
    def rowCount = 0
    try {
        setBeConfigTemporary([group_commit_max_wal_num_per_table: 5]) {
            GetDebugPoint().enableDebugPointForAllBEs("LoadBlockQueue._finish_group_commit_load.load_error")
            GetDebugPoint().enableDebugPointForAllBEs("WalTable::_handle_stream_load.fail")

            def backendIps = [:]
            def backendHttpPorts = [:]
            getBackendIpHttpPort(backendIps, backendHttpPorts)
            def backendId = backendIps.keySet()[0]
            def beHost = backendIps.get(backendId)
            def beHttpPort = backendHttpPorts.get(backendId) as int

            def streamLoadToBe = {
                streamLoad {
                    table "test_group_commit_wal_num_backpressure"
                    set 'column_separator', ','
                    set 'group_commit', 'async_mode'
                    unset 'label'
                    file 'group_commit_wal_msg.csv'
                    time 10000
                    directToBe beHost, beHttpPort
                }
                rowCount += 5
            }

            def blocked = false
            def maxAttempts = 100
            for (int i = 0; i < maxAttempts && !blocked; ++i) {
                try {
                    streamLoadToBe()
                    sleep(i < 10 ? 100 : 1000)
                } catch (Exception e) {
                    logger.info("catch expected exception: " + e.getMessage())
                    assertTrue(e.getMessage().contains("Too many group commit async WALs"))
                    assertTrue(e.getMessage().contains("limit=5"))
                    assertTrue(e.getMessage().contains("last replay wal failed reason"))
                    assertTrue(e.getMessage().contains("WalTable::_handle_stream_load.fail"))
                    blocked = true
                    break
                }
            }
            assertTrue(blocked)
        }
    } finally {
        GetDebugPoint().clearDebugPointsForAllBEs()
        GetDebugPoint().clearDebugPointsForAllFEs()
    }

    // getRowCount(rowCount)
}
