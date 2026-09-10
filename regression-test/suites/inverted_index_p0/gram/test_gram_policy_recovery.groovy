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

suite("test_gram_policy_recovery", "p0") {
    sql "SET enable_sql_cache=false"
    // The condition cache must be off too: gram deliberately keeps its LIKE / REGEXP expression
    // in _common_expr_ctxs_push_down for the row-level recheck, so the segment iterator never
    // zeroes the condition cache digest, and that digest ignores enable_inverted_index_query.
    // Left on, the index-on pass fills the per-segment granule cache and the index-off pass hits
    // it, so both passes would read one and the same filter result and this parity check would
    // degenerate into a tautology.
    sql "SET enable_condition_cache=false"

    def waitAnalyzerInstalled = { int expectedTokenCount ->
        def deadline = System.currentTimeMillis() + 180_000
        Exception lastNotFound = null
        while (System.currentTimeMillis() < deadline) {
            try {
                def result = sql """SELECT JSON_LENGTH(TOKENIZE('abcdef',
                    '"analyzer"="gram_recovery_analyzer"'))"""
                // The replacement has the same name: successful lookup alone could still
                // observe the old policy before BE receives the next heartbeat update.
                if (result[0][0].toString() == expectedTokenCount.toString()) {
                    return
                }
            } catch (Exception e) {
                if (!e.message.contains("Policy not found")) {
                    throw e
                }
                lastNotFound = e
            }
            sleep(1000)
        }
        throw new IllegalStateException(
                "gram_recovery_analyzer did not produce ${expectedTokenCount} tokens on BE",
                lastNotFound)
    }

    sql "DROP TABLE IF EXISTS test_gram_policy_recovery"
    sql "DROP INVERTED INDEX ANALYZER IF EXISTS gram_recovery_analyzer"
    sql "DROP INVERTED INDEX TOKENIZER IF EXISTS gram_recovery_tokenizer"
    sql """CREATE INVERTED INDEX TOKENIZER gram_recovery_tokenizer PROPERTIES (
        "type"="ngram", "mode"="dense", "min_gram"="3")"""
    sql """CREATE INVERTED INDEX ANALYZER gram_recovery_analyzer
        PROPERTIES ("tokenizer"="gram_recovery_tokenizer")"""
    waitAnalyzerInstalled(4)
    qt_dense3_tokens """SELECT TOKENIZE('abcdef', '"analyzer"="gram_recovery_analyzer"')"""

    sql """CREATE TABLE test_gram_policy_recovery (
        id INT,
        msg VARCHAR(128),
        INDEX idx_msg (msg) USING INVERTED PROPERTIES ("analyzer"="gram_recovery_analyzer")
    ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES ("replication_num"="1", "disable_auto_compaction"="true",
                "inverted_index_storage_format"="SNII")"""
    sql """INSERT INTO test_gram_policy_recovery VALUES
        (1, 'abcdef'), (2, 'old abcdef row'), (3, 'unrelated'), (4, NULL)"""
    sql "sync"

    // Do not FORCE: the physical dense3 segment must survive in the recycle bin.
    sql "DROP TABLE test_gram_policy_recovery"
    sql "DROP INVERTED INDEX ANALYZER gram_recovery_analyzer"
    sql "DROP INVERTED INDEX TOKENIZER gram_recovery_tokenizer"
    sql """CREATE INVERTED INDEX TOKENIZER gram_recovery_tokenizer PROPERTIES (
        "type"="ngram", "mode"="dense", "min_gram"="4")"""
    sql """CREATE INVERTED INDEX ANALYZER gram_recovery_analyzer
        PROPERTIES ("tokenizer"="gram_recovery_tokenizer")"""
    waitAnalyzerInstalled(3)
    qt_dense4_tokens """SELECT TOKENIZE('abcdef', '"analyzer"="gram_recovery_analyzer"')"""
    sql "RECOVER TABLE test_gram_policy_recovery"

    [false, true].each { useIndex ->
        sql "SET enable_inverted_index_query=${useIndex}"
        "order_qt_recovered_${useIndex}_like"("""SELECT id FROM test_gram_policy_recovery
            WHERE msg LIKE '%abcdef%'""")
        "order_qt_recovered_${useIndex}_regexp"("""SELECT id FROM test_gram_policy_recovery
            WHERE msg REGEXP 'abc.*def'""")
    }

    // This writer sees dense4, while the recovered rowset retains its dense3 dictionary.
    // Disable automatic compaction above so both physical schemes remain present together.
    sql """INSERT INTO test_gram_policy_recovery VALUES
        (5, 'new abcdef row'), (6, 'abc'), (7, 'another unrelated row'), (8, NULL)"""
    sql "sync"
    [false, true].each { useIndex ->
        sql "SET enable_inverted_index_query=${useIndex}"
        "order_qt_mixed_${useIndex}_like"("""SELECT id FROM test_gram_policy_recovery
            WHERE msg LIKE '%abcdef%'""")
        "order_qt_mixed_${useIndex}_regexp"("""SELECT id FROM test_gram_policy_recovery
            WHERE msg REGEXP 'abc.*def'""")
        // Three-character literals can prune dense3 but must not eliminate dense4 rows.
        "order_qt_mixed_${useIndex}_short_like"("""SELECT id FROM test_gram_policy_recovery
            WHERE msg LIKE '%abc%'""")
        "order_qt_mixed_${useIndex}_short_regexp"("""SELECT id FROM test_gram_policy_recovery
            WHERE msg REGEXP 'abc'""")
    }
    sql "SET enable_inverted_index_query=true"
}
