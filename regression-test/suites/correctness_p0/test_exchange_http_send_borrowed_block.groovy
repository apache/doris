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
// requests >= 2G) via a debug point, so that both the unicast (shuffle) and
// broadcast exchange sink code in proto_util.h/exchange_sink_buffer.cpp that
// move/borrow column_values into the brpc attachment actually run cross-BE,
// instead of only through unit tests. The brpc built-in per-method counter of
// PBackendService.transmit_block_by_http on every BE proves the HTTP path was
// really taken instead of the ordinary transmit_block RPC.
suite('test_exchange_http_send_borrowed_block', 'docker') {
    def options = new ClusterOptions()
    options.beNum = 2
    options.enableDebugPoints()

    docker(options) {
        def tbl = 'test_exchange_http_send_borrowed_block_tbl'

        sql "DROP TABLE IF EXISTS ${tbl}"
        sql """
            CREATE TABLE ${tbl} (
                k1 INT NULL,
                k2 INT NULL
            )
            DISTRIBUTED BY HASH(k1) BUCKETS 4
            PROPERTIES ("replication_num" = "1")
        """
        sql """
            INSERT INTO ${tbl} VALUES
            (1, 10), (2, 20), (3, 30), (4, 40), (5, 50), (6, 60), (7, 70), (8, 80)
        """

        def backends = cluster.getAllBackends()
        assertTrue(backends.size() == 2)
        for (def be : backends) {
            be.enableDebugPoint('proto_util.enable_http_send_block.always_http', null)
        }

        // enable_http_send_block() only looks at the singular `block`; with multi-block
        // exchange (default 256 KiB) the sink fills the repeated `blocks` instead and the
        // debug point is never consulted. Disable it so every cross-BE block goes through
        // the singular-block path and the forced HTTP branch.
        sql "SET exchange_multi_blocks_byte_size = -1"

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
        // colocate join without any exchange, so no block would ever cross BEs.
        try {
            // Shuffle exchange: exercises the unicast attachment path
            // (transmit_block_httpv2 / request_embed_attachment_contain_blockv2).
            def before = httpTransmitCount()
            order_qt_shuffle """
                SELECT a.k1, a.k2, b.k2
                FROM ${tbl} a INNER JOIN [shuffle] ${tbl} b ON a.k2 = b.k2
            """
            def afterShuffle = httpTransmitCount()
            assertTrue(afterShuffle > before, "shuffle join did not reach transmit_block_by_http: ${before} -> ${afterShuffle}")

            // Broadcast exchange: exercises the borrowed-block attachment path
            // (transmit_block_httpv2_with_attachment_data / request_embed_attachmentv2)
            // that this PR fixes.
            order_qt_broadcast """
                SELECT a.k1, a.k2, b.k2
                FROM ${tbl} a INNER JOIN [broadcast] ${tbl} b ON a.k2 = b.k2
            """
            def afterBroadcast = httpTransmitCount()
            assertTrue(afterBroadcast > afterShuffle, "broadcast join did not reach transmit_block_by_http: ${afterShuffle} -> ${afterBroadcast}")
        } finally {
            for (def be : backends) {
                be.disableDebugPoint('proto_util.enable_http_send_block.always_http')
            }
        }
    }
}
