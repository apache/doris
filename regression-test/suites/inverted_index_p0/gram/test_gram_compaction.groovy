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

// A gram index has to survive compaction, and until now nothing checked that it does.
//
// Gram indexes are written DOCS_ONLY, and the SNII index-merge fast path refuses any source
// without positions (snii/compaction/eligibility.cpp), so a compaction cannot stitch these
// indexes together -- it rebuilds each one from the merged column data instead. That rebuild
// recomputes everything the segment carries: the gram scheme, the df statistics, the high-df
// digest, and, where it is enabled, which posting lists get dropped. Every one of those is
// derived from the segment's own size, and compaction changes the segment's size, so the
// rebuilt index is not a copy of its inputs and cannot be assumed to behave like them.
//
// The suite therefore checks the property that has to hold either way: the same queries return
// the same rows before and after, and the index is still there afterwards rather than having
// been quietly dropped. Note the table deliberately does NOT set disable_auto_compaction --
// every existing benchmark and suite for this index does, which is exactly why this path had
// no coverage.
suite("test_gram_compaction", "p0") {
    def waitAnalyzerInstalled = { String name ->
        def deadline = System.currentTimeMillis() + 180_000
        Exception lastNotFound = null
        while (System.currentTimeMillis() < deadline) {
            try {
                sql """SELECT TOKENIZE('probe', '"analyzer"="${name}"')"""
                return
            } catch (Exception e) {
                if (!e.message.contains("Policy not found")) {
                    throw e
                }
                lastNotFound = e
                sleep(1000)
            }
        }
        throw new IllegalStateException("analyzer ${name} was not installed on BE", lastNotFound)
    }

    def tableName = "test_gram_compaction"

    // The table goes first: a policy still referenced by a table left behind by an earlier run
    // cannot be dropped.
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql "DROP INVERTED INDEX ANALYZER IF EXISTS gram_compact_ana"
    sql "DROP INVERTED INDEX TOKENIZER IF EXISTS gram_compact_tok"
    sql """CREATE INVERTED INDEX TOKENIZER gram_compact_tok PROPERTIES (
        "type"="ngram", "mode"="sparse", "min_gram"="3", "max_gram"="8", "density"="0.5")"""
    sql """CREATE INVERTED INDEX ANALYZER gram_compact_ana
        PROPERTIES ("tokenizer"="gram_compact_tok")"""
    waitAnalyzerInstalled("gram_compact_ana")

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """CREATE TABLE ${tableName} (
        `id` bigint NULL,
        `msg` text NULL,
        INDEX idx_msg (`msg`) USING INVERTED
            PROPERTIES('analyzer'='gram_compact_ana', 'support_phrase'='false')
    ) ENGINE=OLAP DUPLICATE KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES('replication_num'='1', 'inverted_index_storage_format'='SNII')"""

    sql "SET enable_sql_cache=false"
    sql "SET enable_condition_cache=false"

    // Several batches, so there are several rowsets for the compaction to merge. Row content
    // spans the selectivity range the gate cares about: a marker on every row, one on a few
    // hundred, and one on a handful.
    def batches = 5
    def perBatch = 800
    for (int b = 0; b < batches; b++) {
        def values = []
        for (int i = 0; i < perBatch; i++) {
            def id = b * perBatch + i
            def rare = (id % 400 == 0) ? " rare_marker_${id}" : ""
            def mid = (id % 7 == 0) ? " midfreq_token" : ""
            values.add("(${id}, 'shared_prefix_common_text filler_${id}${mid}${rare}')")
        }
        sql "INSERT INTO ${tableName} VALUES ${values.join(',')}"
    }

    def patterns = [
        "like_rare"     : "SELECT COUNT(*) FROM ${tableName} WHERE msg LIKE '%rare_marker_%'",
        "like_mid"      : "SELECT COUNT(*) FROM ${tableName} WHERE msg LIKE '%midfreq_token%'",
        "like_common"   : "SELECT COUNT(*) FROM ${tableName} WHERE msg LIKE '%shared_prefix_common%'",
        "regexp_rare"   : "SELECT COUNT(*) FROM ${tableName} WHERE msg REGEXP 'rare_marker_[0-9]+'",
        "regexp_mid"    : "SELECT COUNT(*) FROM ${tableName} WHERE msg REGEXP 'midfreq_[a-z]+'",
        "regexp_absent" : "SELECT COUNT(*) FROM ${tableName} WHERE msg REGEXP 'no_such_token_anywhere'",
        "regexp_alt"    : "SELECT COUNT(*) FROM ${tableName} WHERE msg REGEXP 'rare_marker_(0|400|800)\\\\b'",
    ]

    def runAll = { String label ->
        def out = [:]
        patterns.each { name, stmt -> out[name] = sql(stmt)[0][0] }
        logger.info("${label}: ${out}")
        return out
    }

    // Tablet statistics reach information_schema asynchronously, so a freshly loaded table can
    // report 0 index bytes for a while: poll rather than read once.
    def indexBytes = {
        def deadline = System.currentTimeMillis() + 180_000
        long bytes = 0L
        while (System.currentTimeMillis() < deadline) {
            def rows = sql """SELECT INDEX_LENGTH FROM information_schema.tables
                              WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '${tableName}'"""
            bytes = rows.isEmpty() ? 0L : (rows[0][0] as long)
            if (bytes > 0) {
                break
            }
            sleep(2000)
        }
        return bytes
    }

    // Ground truth first, with the index taken out of the picture entirely.
    sql "SET enable_inverted_index_query=false"
    def scanned = runAll("full scan")
    sql "SET enable_inverted_index_query=true"

    def before = runAll("indexed, before compaction")
    scanned.each { name, value ->
        assertEquals(value, before[name], "index and scan disagree on ${name} before compaction")
    }
    def bytesBefore = indexBytes()
    logger.info("index bytes before compaction: ${bytesBefore}")
    assertTrue(bytesBefore > 0, "the gram index must occupy bytes before compaction")

    trigger_and_wait_compaction(tableName, "full")

    def after = runAll("indexed, after compaction")
    before.each { name, value ->
        assertEquals(value, after[name], "compaction changed the answer for ${name}")
    }

    // The index must have been rebuilt, not dropped: a compaction that quietly produced an
    // index-free rowset would still answer every query above correctly, by scanning.
    def bytesAfter = indexBytes()
    logger.info("index bytes after compaction: ${bytesAfter}")
    assertTrue(bytesAfter > 0,
            "the gram index must still exist after compaction (bytes before=${bytesBefore}, after=${bytesAfter})")

    // And it must still answer correctly with the index forced off, which pins that the rows
    // themselves survived the merge rather than the index merely agreeing with a broken table.
    sql "SET enable_inverted_index_query=false"
    def scannedAfter = runAll("full scan, after compaction")
    sql "SET enable_inverted_index_query=true"
    after.each { name, value ->
        assertEquals(scannedAfter[name], value,
                "index and scan disagree on ${name} after compaction")
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql "DROP INVERTED INDEX ANALYZER IF EXISTS gram_compact_ana"
    sql "DROP INVERTED INDEX TOKENIZER IF EXISTS gram_compact_tok"
}
