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
import org.apache.doris.regression.util.WarmupMetricsUtils

// Forces exchange sinks to use the http-attachment path (normally reserved for
// requests >= 2G) via a debug point, so that the unicast (shuffle) and the
// broadcast exchange sink, in both the multi-block (default) and the single-block
// request shape, really send their borrowed block payloads as brpc attachments
// across BEs instead of only through unit tests. The brpc built-in per-method
// counter of PBackendService.transmit_block_by_http on every BE proves the HTTP
// path was taken instead of the ordinary transmit_block RPC.
suite('test_exchange_http_send_borrowed_block', 'docker') {
    def options = new ClusterOptions()
    options.beNum = 2
    options.enableDebugPoints()

    docker(options) {
        sql "DROP TABLE IF EXISTS test_exchange_http_send_borrowed_block_tbl"
        sql """
            CREATE TABLE test_exchange_http_send_borrowed_block_tbl (
                k1 INT NULL,
                k2 INT NULL
            )
            DISTRIBUTED BY HASH(k1) BUCKETS 4
            PROPERTIES ("replication_num" = "1")
        """
        sql """
            INSERT INTO test_exchange_http_send_borrowed_block_tbl VALUES
            (1, 10), (2, 20), (3, 30), (4, 40), (5, 50), (6, 60), (7, 70), (8, 80)
        """

        def backends = cluster.getAllBackends()
        assertTrue(backends.size() == 2)
        for (def be : backends) {
            be.enableDebugPoint('proto_util.enable_http_send_block.always_http', null)
        }

        // Sum of brpc's built-in request counter for transmit_block_by_http over all BEs
        // (`show backends`: Host is column 1, BrpcPort is column 5).
        def httpTransmitCount = { ->
            long total = 0
            for (def row : sql_return_maparray("show backends")) {
                total += WarmupMetricsUtils.getBrpcMetric(row.Host, row.BrpcPort,
                        'rpc_server_\\d+_doris_pbackend_service_transmit_block_by_http_count')
            }
            return total
        }

        // Join on k2, not on the distribution column k1: a self join on k1 becomes a
        // colocate join without any exchange, so no block would ever cross BEs. The counter
        // only proves that some cross-BE exchange took the http path, so pin the plan shape
        // of each query to make sure the hint was honoured and the right sink was exercised.
        def shuffleJoin = """
            SELECT a.k1, a.k2, b.k2
            FROM test_exchange_http_send_borrowed_block_tbl a INNER JOIN [shuffle] test_exchange_http_send_borrowed_block_tbl b ON a.k2 = b.k2
        """
        def broadcastJoin = """
            SELECT a.k1, a.k2, b.k2
            FROM test_exchange_http_send_borrowed_block_tbl a INNER JOIN [broadcast] test_exchange_http_send_borrowed_block_tbl b ON a.k2 = b.k2
        """
        def runJoins = { String mode ->
            explain {
                sql(shuffleJoin)
                contains "INNER JOIN(PARTITIONED)"
            }
            explain {
                sql(broadcastJoin)
                contains "INNER JOIN(BROADCAST)"
            }

            // Shuffle exchange: blocks owned by the sink's TransmitInfo.
            def before = httpTransmitCount()
            "order_qt_shuffle_${mode}"(shuffleJoin)
            def afterShuffle = httpTransmitCount()
            assertTrue(afterShuffle > before, "${mode}: shuffle join did not reach transmit_block_by_http: ${before} -> ${afterShuffle}")

            // Broadcast exchange: one BroadcastPBlockHolder block shared by every channel.
            "order_qt_broadcast_${mode}"(broadcastJoin)
            def afterBroadcast = httpTransmitCount()
            assertTrue(afterBroadcast > afterShuffle, "${mode}: broadcast join did not reach transmit_block_by_http: ${afterShuffle} -> ${afterBroadcast}")
        }

        try {
            // Default exchange_multi_blocks_byte_size (256 KiB): the sink fills the repeated
            // `blocks`, one attachment payload per block.
            runJoins('multi_blocks')

            // A negative value disables multi-block exchange: the sink fills the singular
            // `block`, the request shape older receivers expect.
            sql "SET exchange_multi_blocks_byte_size = -1"
            runJoins('single_block')
        } finally {
            for (def be : backends) {
                be.disableDebugPoint('proto_util.enable_http_send_block.always_http')
            }
        }
    }
}
