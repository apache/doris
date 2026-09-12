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

import java.util.concurrent.CyclicBarrier
import java.util.concurrent.TimeUnit

suite("test_point_query_rpc_batch", "p0,nonConcurrent") {
    sql "DROP TABLE IF EXISTS test_point_query_rpc_batch"
    sql """CREATE TABLE test_point_query_rpc_batch (
        k INT NOT NULL, v STRING NULL, amount DECIMAL(18, 3), a ARRAY<INT>
    ) UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 2
    PROPERTIES ("replication_num" = "1", "store_row_column" = "true",
                "enable_unique_key_merge_on_write" = "true")"""
    sql """INSERT INTO test_point_query_rpc_batch VALUES
        (1, 'first', 1.234, [1, NULL, 3]), (2, NULL, NULL, []),
        (3, repeat('x', 20000), -9.876, NULL)"""

    explain {
        sql "SELECT k, length(v), left(v, 8), amount, a FROM test_point_query_rpc_batch WHERE k = 1 ORDER BY k"
        contains "SHORT-CIRCUIT"
    }
    def url = context.config.jdbcUrl + "&useServerPrepStmts=true"
    def database = sql("SELECT DATABASE()")[0][0]
    for (def mode : [[false, false], [false, true], [true, false], [true, true]]) {
        def (enabled, lightweight) = mode
        setFeConfigTemporary([enable_point_query_rpc_batch: enabled,
                              enable_lightweight_lookup_request: lightweight,
                              point_query_rpc_batch_max_size: 8,
                              point_query_rpc_batch_max_wait_us: 1000]) {
            def barrier = new CyclicBarrier(8)
            def futures = (0..<8).collect { worker ->
                thread("point-query-batch-${worker}") {
                    connect(context.config.jdbcUser, context.config.jdbcPassword, url) {
                        sql "USE ${database}"
                        def stmt = prepareStatement """SELECT k, length(v), left(v, 8), amount, a
                            FROM test_point_query_rpc_batch WHERE k = ? ORDER BY k"""
                        try {
                            // Each connection owns a distinct prepared context. Repeat it to exercise
                            // both full-context and cached-context requests, including empty results.
                            for (int round = 0; round < 8; round++) {
                                stmt.setInt(1, (round + worker) % 4 + 1)
                                barrier.await(30, TimeUnit.SECONDS)
                                quickExecute("batch_${enabled}_${lightweight}_${worker}_${round}", stmt)
                            }
                        } finally {
                            stmt.close()
                        }
                    }
                }
            }
            futures.each { it.get(60, TimeUnit.SECONDS) }
            order_qt_before_update "SELECT * FROM test_point_query_rpc_batch WHERE k = 1"
            sql "UPDATE test_point_query_rpc_batch SET v = 'updated' WHERE k = 1"
            order_qt_after_update "SELECT * FROM test_point_query_rpc_batch WHERE k = 1"
            sql "DELETE FROM test_point_query_rpc_batch WHERE k = 2"
            order_qt_after_delete "SELECT * FROM test_point_query_rpc_batch WHERE k = 2"
            test {
                sql "SELECT missing_column FROM test_point_query_rpc_batch WHERE k = 1"
                exception "Unknown column"
            }
            // Restore data for the next configuration, keeping the table after the suite.
            sql """INSERT INTO test_point_query_rpc_batch VALUES
                (1, 'first', 1.234, [1, NULL, 3]), (2, NULL, NULL, [])"""
        }
    }
}
