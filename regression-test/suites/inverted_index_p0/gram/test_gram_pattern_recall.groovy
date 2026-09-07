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

suite("test_gram_pattern_recall", "p0") {
    sql "DROP INVERTED INDEX TOKENIZER IF EXISTS gram_recall_invalid_tokenizer"
    ['density', 'stop_gram_df'].each { property ->
        test {
            sql """CREATE INVERTED INDEX TOKENIZER gram_recall_invalid_tokenizer
                PROPERTIES ("type"="ngram", "mode"="sparse", "${property}"="0.25f")"""
            exception "${property} must be a decimal number"
        }
    }
    def sqlLiteral = { String value ->
        "'" + value.replace('\\', '\\\\').replace("'", "\\'")
                .replace('\u0000', '\\0') + "'"
    }
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

    sql "DROP TABLE IF EXISTS test_gram_pattern_recall"
    def schemes = [dense: ["dense", false], sparse: ["sparse", false],
                   dense_lc: ["dense", true], sparse_lc: ["sparse", true]]
    schemes.each { name, properties ->
        sql "DROP INVERTED INDEX ANALYZER IF EXISTS gram_recall_${name}"
        sql "DROP INVERTED INDEX TOKENIZER IF EXISTS gram_recall_${name}_tok"
        sql """CREATE INVERTED INDEX TOKENIZER gram_recall_${name}_tok PROPERTIES (
            "type"="ngram", "mode"="${properties[0]}", "min_gram"="3",
            "lower_case"="${properties[1]}")"""
        sql """CREATE INVERTED INDEX ANALYZER gram_recall_${name}
            PROPERTIES ("tokenizer"="gram_recall_${name}_tok")"""
        waitAnalyzerInstalled("gram_recall_${name}")
    }
    sql """CREATE TABLE test_gram_pattern_recall (
        id INT,
        dense VARCHAR(128), sparse VARCHAR(128), dense_lc VARCHAR(128), sparse_lc VARCHAR(128),
        INDEX idx_dense (dense) USING INVERTED PROPERTIES ("analyzer"="gram_recall_dense"),
        INDEX idx_sparse (sparse) USING INVERTED PROPERTIES ("analyzer"="gram_recall_sparse"),
        INDEX idx_dense_lc (dense_lc) USING INVERTED PROPERTIES ("analyzer"="gram_recall_dense_lc"),
        INDEX idx_sparse_lc (sparse_lc) USING INVERTED PROPERTIES ("analyzer"="gram_recall_sparse_lc")
    ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES ("replication_num"="1", "disable_auto_compaction"="true",
                "inverted_index_storage_format"="SNII")"""

    def rows = ['é', 'É', 'ask', 'aſk', 'asK', 'abÉtimeout',
                'ab\u0007cdtimeout', 'ab\fcdtimeout', 'ab\u000bcdtimeout',
                'ab\ncdtimeout', 'ab\rcdtimeout', 'abacdtimeout', 'timeout', 'timeoutx',
                'ab cdtimeout', 'ab\tcdtimeout', 'abcdeftimeout', 'abcDEFtimeout',
                'ABCdeftimeout', 'ab', 'abcccdef', 'abcctimeout', 'abcabc',
                'abéétimeout', 'atimeout', 'aaatimeout', 'prefix', 'prefixsuffix',
                'prefix\u0000suffix', 'unrelated', '', null, '%timeout%', 'a.*timeout',
                'prefix1', 'prefix1\u0000timeout']
    // Separate inserts retain multiple rowsets and exercise per-segment candidate/null bitmaps.
    rows.withIndex().collate(17).each { batch ->
        def values = batch.collect { value, id ->
            def literal = value == null ? 'NULL' : sqlLiteral(value)
            "(${id}, ${literal}, ${literal}, ${literal}, ${literal})"
        }.join(',')
        sql "INSERT INTO test_gram_pattern_recall VALUES ${values}"
        sql "sync"
    }

    def patterns = ['(?i)é', '(?i)[é]', '(?i)ask', '(?i)a[s]k', '(?i)as[k]',
                    'ab(?i)é(?-i:timeout)', 'ab(?i)[é](?-i:timeout)',
                    'ab\\acdtimeout', 'ab[\\a]cdtimeout', 'ab\\fcdtimeout',
                    'ab[\\f]cdtimeout', 'ab\\vcdtimeout', 'ab[\\v]cdtimeout',
                    'ab\\141cdtimeout', 'ab[\\141]cdtimeout', 'timeout\\Z',
                    'ab\\hcdtimeout', '(?i)((?-i)abc)DEF(?-i:timeout)',
                    '(?i)(?:(?-i)abc)DEF(?-i:timeout)',
                    '(?i)(?P<word>(?-i)abc)DEF(?-i:timeout)',
                    '\\Qabc\\E*', '\\Qabc\\E+def', '\\Qabc\\E{2}timeout',
                    '(\\Qabc\\E)+', '\\Qabé\\E{2}timeout', 'a\\Q\\E*timeout',
                    'prefix\u0000suffix', 'prefix\\x00suffix', 'prefix[0-9]\u0000timeout']
    sql "SET enable_sql_cache=false"
    // The condition cache must be off too: gram deliberately keeps its LIKE / REGEXP expression
    // in _common_expr_ctxs_push_down for the row-level recheck, so the segment iterator never
    // zeroes the condition cache digest, and that digest ignores enable_inverted_index_query.
    // Left on, the index-on pass fills the per-segment granule cache and the index-off pass hits
    // it, so both passes would read one and the same filter result and this parity check would
    // degenerate into a tautology.
    sql "SET enable_condition_cache=false"
    [false, true].each { useIndex ->
        sql "SET enable_inverted_index_query=${useIndex}"
        schemes.each { column, unused ->
            patterns.eachWithIndex { pattern, i ->
                "order_qt_${column}_${useIndex}_${i}"("""SELECT id
                    FROM test_gram_pattern_recall WHERE ${column} REGEXP ${sqlLiteral(pattern)}""")
            }
            ['%prefix\u0000%timeout%', 'prefix_\u0000%timeout%', 'prefix\u0000suffix']
                    .eachWithIndex { pattern, i ->
                "order_qt_${column}_${useIndex}_nul_like_${i}"("""SELECT id
                    FROM test_gram_pattern_recall WHERE ${column} LIKE ${sqlLiteral(pattern)}""")
            }
            "order_qt_${column}_${useIndex}_reverse_like"("""SELECT id
                FROM test_gram_pattern_recall WHERE 'abctimeout' LIKE ${column}""")
            "order_qt_${column}_${useIndex}_reverse_regexp"("""SELECT id
                FROM test_gram_pattern_recall WHERE 'abctimeout' REGEXP ${column}""")
            "order_qt_${column}_${useIndex}_null"("""SELECT id
                FROM test_gram_pattern_recall WHERE ${column} IS NULL""")
            "order_qt_${column}_${useIndex}_compound"("""SELECT id
                FROM test_gram_pattern_recall WHERE ${column} LIKE '%timeout%'
                AND (${column} REGEXP 'abc' OR ${column} IS NULL)""")
        }
    }
    sql "SET enable_inverted_index_query=true"
}
