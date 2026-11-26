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

suite("test_map_agg_v2_versioned_state", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }

    def originalVersion = sql("ADMIN SHOW FRONTEND CONFIG LIKE 'be_exec_version'")[0][1]
    try {
        sql "ADMIN SET FRONTEND CONFIG ('be_exec_version' = '16')"
        sql "SET enable_agg_state = true"
        sql "DROP TABLE IF EXISTS test_map_agg_v2_versioned_state"
        sql """
            CREATE TABLE test_map_agg_v2_versioned_state (
                k INT,
                state AGG_STATE<map_agg_v2(INT, INT)> GENERIC
            ) AGGREGATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "disable_auto_compaction" = "true"
            )
        """
        for (int key = 1; key <= 4; key++) {
            sql "INSERT INTO test_map_agg_v2_versioned_state VALUES (1, map_agg_v2_state(${key}, ${key * 10}))"
        }

        // The column and its rowsets retain version 16 after the FE switches to version 17.
        sql "ADMIN SET FRONTEND CONFIG ('be_exec_version' = '17')"
        order_qt_before_compaction """
            SELECT k, map_size(map_agg_v2_merge(state))
            FROM test_map_agg_v2_versioned_state GROUP BY k ORDER BY k
        """

        def tablets = sql_return_maparray "SHOW TABLETS FROM test_map_agg_v2_versioned_state"
        assertEquals(1, tablets.size())
        def tabletId = tablets[0].TabletId
        def backendId = tablets[0].BackendId
        def backendIdToIp = [:]
        def backendIdToHttpPort = [:]
        getBackendIpHttpPort(backendIdToIp, backendIdToHttpPort)
        def beHost = backendIdToIp["${backendId}"]
        def bePort = backendIdToHttpPort["${backendId}"]
        def showTabletCompaction = {
            def (code, stdout, stderr) = be_show_tablet_status(beHost, bePort, tabletId)
            assertEquals(0, code)
            return parseJson(stdout.trim())
        }
        def countDataRowsets = { status ->
            return status.rowsets.findAll { it.contains(" DATA ") }.size()
        }
        def beforeCount = countDataRowsets(showTabletCompaction())
        assertTrue(beforeCount >= 2)

        sql "ADMIN COMPACT TABLE test_map_agg_v2_versioned_state PARTITION (test_map_agg_v2_versioned_state) WHERE type = 'full'"
        def deadline = System.currentTimeMillis() + 60000L
        def afterCompaction
        while (System.currentTimeMillis() < deadline) {
            afterCompaction = showTabletCompaction()
            if (afterCompaction["last full status"] == "[OK]" &&
                    countDataRowsets(afterCompaction) < beforeCount) {
                break
            }
            sleep(500)
        }
        assertEquals("[OK]", afterCompaction["last full status"])
        assertTrue(countDataRowsets(afterCompaction) < beforeCount)

        order_qt_after_compaction """
            SELECT k, map_size(map_agg_v2_merge(state))
            FROM test_map_agg_v2_versioned_state GROUP BY k ORDER BY k
        """
    } finally {
        sql "ADMIN SET FRONTEND CONFIG ('be_exec_version' = '${originalVersion}')"
    }
}
