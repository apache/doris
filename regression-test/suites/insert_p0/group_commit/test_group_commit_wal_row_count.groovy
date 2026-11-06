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

suite("test_group_commit_wal_row_count", "nonConcurrent") {
    sql "DROP TABLE IF EXISTS test_group_commit_wal_row_count"
    sql """
        CREATE TABLE test_group_commit_wal_row_count (
            k INT NOT NULL,
            v INT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "group_commit_interval_ms" = "2000"
        )
    """

    String loadError = "LoadBlockQueue._finish_group_commit_load.load_error"
    String replayStop = "WalTable.replay_wals.stop"
    try {
        GetDebugPoint().enableDebugPointForAllBEs(loadError)
        GetDebugPoint().enableDebugPointForAllBEs(replayStop)

        // Abort each original transaction before the next load so both WALs must be replayed.
        for (String data : ["1,10\n2,20\n3,\\N\n", "4,40\n5,50\n"]) {
            long txnId = 0
            streamLoad {
                table "test_group_commit_wal_row_count"
                set "column_separator", ","
                set "group_commit", "async_mode"
                unset "label"
                inputStream new ByteArrayInputStream(data.getBytes("UTF-8"))
                time 10000
                check { result, exception, startTime, endTime ->
                    assertNull(exception)
                    def response = parseJson(result)
                    assertEquals("Success", response.Status)
                    assertEquals(data.readLines().size(), response.NumberTotalRows as int)
                    txnId = response.TxnId as long
                    assertTrue(txnId > 0)
                }
            }
            Awaitility.await().atMost(30, SECONDS).pollInterval(1, SECONDS).until {
                def txn = sql_return_maparray "SHOW TRANSACTION WHERE ID = ${txnId}"
                return txn.size() == 1 && txn[0].TransactionStatus == "ABORTED"
            }
        }
        qt_before_replay "SELECT COUNT(*) FROM test_group_commit_wal_row_count"

        // Keep the original-load failure enabled: rows can become visible only through WAL replay.
        GetDebugPoint().disableDebugPointForAllBEs(replayStop)
        Awaitility.await().atMost(90, SECONDS).pollInterval(1, SECONDS).until {
            def rows = sql "SELECT COUNT(*) FROM test_group_commit_wal_row_count"
            return rows[0][0] == 5
        }
        qt_counts """SELECT COUNT(*), COUNT(v), SUM(k), SUM(v)
                     FROM test_group_commit_wal_row_count"""
        qt_values "SELECT k, v FROM test_group_commit_wal_row_count ORDER BY k"
        qt_filtered_count "SELECT COUNT(*) FROM test_group_commit_wal_row_count WHERE v >= 20"
    } finally {
        try {
            GetDebugPoint().disableDebugPointForAllBEs(replayStop)
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs(loadError)
        }
    }
}
