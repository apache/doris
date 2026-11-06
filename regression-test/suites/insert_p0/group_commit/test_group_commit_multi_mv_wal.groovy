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

suite("test_group_commit_multi_mv_wal", "nonConcurrent") {
    sql "DROP TABLE IF EXISTS test_group_commit_multi_mv_wal"
    sql """
        CREATE TABLE test_group_commit_multi_mv_wal (
            k INT NOT NULL,
            s VARCHAR(32) NOT NULL,
            v INT NULL
        ) DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "group_commit_interval_ms" = "2000")
    """
    createMV """CREATE MATERIALIZED VIEW multi_mv_int AS
        SELECT k AS int_key, SUM(v) AS int_sum
        FROM test_group_commit_multi_mv_wal GROUP BY k"""
    createMV """CREATE MATERIALIZED VIEW multi_mv_string AS
        SELECT s AS string_key, SUM(v) AS string_sum
        FROM test_group_commit_multi_mv_wal GROUP BY s"""
    createMV """CREATE MATERIALIZED VIEW multi_mv_count AS
        SELECT k AS count_key, COUNT(v) AS value_count
        FROM test_group_commit_multi_mv_wal GROUP BY k"""

    String loadError = "LoadBlockQueue._finish_group_commit_load.load_error"
    String replayStop = "WalTable.replay_wals.stop"
    try {
        GetDebugPoint().enableDebugPointForAllBEs(loadError)
        GetDebugPoint().enableDebugPointForAllBEs(replayStop)
        for (String data : ["1,alpha,10\n2,beta,20\n3,alpha,\\N\n", "1,alpha,40\n2,beta,50\n"]) {
            long txnId = 0
            streamLoad {
                table "test_group_commit_multi_mv_wal"
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
        qt_before_replay "SELECT COUNT(*) FROM test_group_commit_multi_mv_wal"
        // Original transactions remain forced to abort; only replay can publish these rows.
        GetDebugPoint().disableDebugPointForAllBEs(replayStop)
        Awaitility.await().atMost(90, SECONDS).pollInterval(1, SECONDS).until {
            def rows = sql "SELECT COUNT(*) FROM test_group_commit_multi_mv_wal"
            return rows[0][0] == 5
        }
        order_qt_base "SELECT * FROM test_group_commit_multi_mv_wal"
        order_qt_mv_int "SELECT * FROM test_group_commit_multi_mv_wal INDEX multi_mv_int"
        order_qt_mv_string "SELECT * FROM test_group_commit_multi_mv_wal INDEX multi_mv_string"
        order_qt_mv_count "SELECT * FROM test_group_commit_multi_mv_wal INDEX multi_mv_count"
    } finally {
        try {
            GetDebugPoint().disableDebugPointForAllBEs(replayStop)
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs(loadError)
        }
    }
}
