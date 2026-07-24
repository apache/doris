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

suite("test_multi_segment_block_wand", "nonConcurrent") {
    def tableName = "test_multi_segment_block_wand"
    def backendIdToIp = [:]
    def backendIdToHttpPort = [:]
    def originalMaxBufferedDocs = [:]
    boolean beConfigUpdated = false
    def forceThresholdPoint = "BlockWand.multi_scorer.force_threshold"
    def prunedBlockPoint = "BlockWand.multi_scorer.pruned_block"
    def poisonScorePoint = "BlockWand.multi_scorer.poison_score_from_doc"

    getBackendIpHttpPort(backendIdToIp, backendIdToHttpPort)

    try {
        backendIdToIp.each { backendId, ip ->
            def (code, out, err) = show_be_config(ip, backendIdToHttpPort.get(backendId))
            assertEquals(code, 0)
            def configList = parseJson(out.trim())
            for (Object entry in (List) configList) {
                if (((List<String>) entry)[0] == "inverted_index_max_buffered_docs") {
                    originalMaxBufferedDocs[backendId] = ((List<String>) entry)[2]
                }
            }
        }
        assertEquals(originalMaxBufferedDocs.size(), backendIdToIp.size())

        backendIdToIp.each { backendId, ip ->
            def (code, out, err) = update_be_config(
                    ip, backendIdToHttpPort.get(backendId), "inverted_index_max_buffered_docs", "1024")
            assertEquals(code, 0)
        }
        beConfigUpdated = true

        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
            CREATE TABLE ${tableName} (
                id INT,
                body STRING,
                INDEX body_idx (body) USING INVERTED PROPERTIES(
                    "parser" = "english",
                    "support_phrase" = "true"
                )
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES(
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true",
                "inverted_index_storage_format" = "V3"
            )
        """

        // The deliberately high-frequency alpha/bravo query terms make each complete CLucene leaf
        // materialize a low-score PFOR block at local docs 511..1022. The first five documents
        // establish a higher Top-K threshold with alpha TF=2; all later matching documents have
        // alpha TF=1 and a rotating third token. The 1024th posting (local doc 1023) is CLucene's
        // metadata-free tail used to flush the preceding block, while the final short leaf has no
        // query term and cannot mask the result.
        sql """
            INSERT INTO ${tableName}
            SELECT number + 1,
                   CASE
                       WHEN number < 5 THEN 'alpha alpha bravo'
                       WHEN number < 4096 THEN
                           CASE number % 8
                               WHEN 0 THEN 'alpha bravo charlie'
                               WHEN 1 THEN 'alpha bravo delta'
                               WHEN 2 THEN 'alpha bravo echo'
                               WHEN 3 THEN 'alpha bravo foxtrot'
                               WHEN 4 THEN 'alpha bravo golf'
                               WHEN 5 THEN 'alpha bravo hotel'
                               WHEN 6 THEN 'alpha bravo india'
                               ELSE 'alpha bravo juliet'
                           END
                       ELSE 'delta echo foxtrot'
                   END
            FROM numbers("number" = "4152")
        """

        sql "SET enable_segment_limit_pushdown = true"
        sql "SET enable_inverted_index_query_cache = false"

        def topKQuery = """
            SELECT id, score()
            FROM ${tableName}
            WHERE search('alpha OR bravo',
                         '{"default_field":"body","default_operator":"or","mode":"lucene"}')
            ORDER BY score() DESC
            LIMIT 5
        """

        sql "SET enable_inverted_index_wand_query = false"
        order_qt_without_wand topKQuery

        // A non-WAND collection does not observe this point, while the multi-scorer WAND
        // collector does and therefore returns no candidate above the injected threshold.
        GetDebugPoint().enableDebugPointForAllBEs(forceThresholdPoint, [threshold: 100.0])
        order_qt_force_threshold_without_wand topKQuery
        sql "SET enable_inverted_index_wand_query = true"
        order_qt_force_threshold_with_wand topKQuery
        GetDebugPoint().disableDebugPointForAllBEs(forceThresholdPoint)

        // Poison every candidate from local doc 511 onward. CLucene must score the 1024th-posting
        // tail of each leaf because that posting has no block-max metadata. Any score in the actual
        // low-upper-bound PFOR ranges (511..1022) would precede those tails in the Top-K output;
        // their absence proves that WAND skipped the ranges after comparing their block upper bound.
        GetDebugPoint().enableDebugPointForAllBEs(prunedBlockPoint)
        GetDebugPoint().enableDebugPointForAllBEs(poisonScorePoint, [min_doc: 511])
        order_qt_wand_skips_low_upper_bound_blocks topKQuery
        GetDebugPoint().disableDebugPointForAllBEs(poisonScorePoint)
        GetDebugPoint().disableDebugPointForAllBEs(prunedBlockPoint)
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs(forceThresholdPoint)
        GetDebugPoint().disableDebugPointForAllBEs(prunedBlockPoint)
        GetDebugPoint().disableDebugPointForAllBEs(poisonScorePoint)
        if (beConfigUpdated) {
            backendIdToIp.each { backendId, ip ->
                def (code, out, err) = update_be_config(
                        ip, backendIdToHttpPort.get(backendId), "inverted_index_max_buffered_docs",
                        originalMaxBufferedDocs[backendId])
                assertEquals(code, 0)
            }
        }
    }
}
