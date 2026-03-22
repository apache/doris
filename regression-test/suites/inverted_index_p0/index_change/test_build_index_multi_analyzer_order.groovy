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

suite("test_build_index_multi_analyzer_order") {
    // Test that BUILD INDEX with multiple analyzers on the same column
    // produces consistent query results regardless of build order.
    // Regression test for: build index creates rowset schemas with
    // index order different from tablet schema, causing select_best_reader
    // to pick different indexes for different segments.

    // Cloud mode does not support BUILD INDEX with specified index name
    if (isCloudMode()) {
        return
    }

    def timeout = 60000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0

    def wait_for_latest_op_on_table_finish = { table_name, OpTimeout ->
        for (int t = delta_time; t <= OpTimeout; t += delta_time) {
            alter_res = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${table_name}" ORDER BY CreateTime DESC LIMIT 1;"""
            if (alter_res.size() > 0 && alter_res[0][9] == "FINISHED") {
                sleep(3000)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_latest_op_on_table_finish timeout")
    }

    def wait_for_build_index_finish = { table_name, OpTimeout ->
        for (int t = delta_time; t <= OpTimeout; t += delta_time) {
            alter_res = sql """SHOW BUILD INDEX WHERE TableName = "${table_name}" ORDER BY CreateTime DESC LIMIT 1;"""
            if (alter_res.size() > 0 && alter_res[0][7] == "FINISHED") {
                sleep(3000)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_build_index_finish timeout")
    }

    // Create custom analyzers
    sql """ CREATE INVERTED INDEX TOKENIZER IF NOT EXISTS edge_ngram_test_tokenizer
            PROPERTIES ("type" = "edge_ngram", "min_gram" = "3", "max_gram" = "10", "token_chars" = "digit"); """
    sql """ CREATE INVERTED INDEX ANALYZER IF NOT EXISTS edge_ngram_test
            PROPERTIES ("tokenizer" = "edge_ngram_test_tokenizer"); """
    sql """ CREATE INVERTED INDEX ANALYZER IF NOT EXISTS standard_test_default
            PROPERTIES ("tokenizer" = "standard", "token_filter" = "word_delimiter, asciifolding, lowercase"); """

    // Table 1: build index path (add idx_ch0, insert, drop, add two new indexes, build with interleaved inserts)
    def tableBuild = "test_build_idx_multi_analyzer_build"
    // Table 2: baseline (indexes at creation time)
    def tableBaseline = "test_build_idx_multi_analyzer_baseline"

    sql """ DROP TABLE IF EXISTS ${tableBuild} """
    sql """ DROP TABLE IF EXISTS ${tableBaseline} """

    // === Table 1: Dynamic index lifecycle ===
    sql """
        CREATE TABLE ${tableBuild} (
            a BIGINT NOT NULL AUTO_INCREMENT,
            ch TEXT NULL
        ) DUPLICATE KEY(a)
        DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    // Add initial plain inverted index
    sql """ ALTER TABLE ${tableBuild} ADD INDEX idx_ch0(ch) USING INVERTED; """
    wait_for_latest_op_on_table_finish(tableBuild, timeout)

    // Insert batch 1
    sql """ INSERT INTO ${tableBuild} VALUES
        (1, '1949 West German federal election'),
        (2, 'Category 1062 deaths'),
        (3, 'File Swallow Project Gutenberg eBook 11921 jpg'),
        (4, '80386DX processor'),
        (5, 'List of minor planets 49001 to 50000'),
        (6, 'HIP 57087 star'),
        (7, 'File Paul Verlaine Project Gutenberg eText 15112 png'),
        (8, 'Simple text without digits'),
        (9, '1700 in Canada'),
        (10, 'Category 1649 births');
    """

    // Drop the initial index
    sql """ ALTER TABLE ${tableBuild} DROP INDEX idx_ch0; """
    wait_for_latest_op_on_table_finish(tableBuild, timeout)

    // Add two new indexes with different analyzers (edge_ngram first, then standard)
    sql """ ALTER TABLE ${tableBuild} ADD INDEX idx_ch_edge(ch) USING INVERTED
            PROPERTIES("analyzer" = "edge_ngram_test", "support_phrase" = "true"); """
    wait_for_latest_op_on_table_finish(tableBuild, timeout)

    sql """ ALTER TABLE ${tableBuild} ADD INDEX idx_ch_std(ch) USING INVERTED
            PROPERTIES("analyzer" = "standard_test_default", "support_phrase" = "true"); """
    wait_for_latest_op_on_table_finish(tableBuild, timeout)

    // Insert batch 2 (uses new schema with both indexes)
    sql """ INSERT INTO ${tableBuild} VALUES
        (11, 'Another text 2024'),
        (12, 'No digits here');
    """

    // Build idx_ch_std FIRST (reverse order from ALTER)
    sql """ BUILD INDEX idx_ch_std ON ${tableBuild}; """
    wait_for_build_index_finish(tableBuild, timeout)

    // Insert batch 3
    sql """ INSERT INTO ${tableBuild} VALUES
        (13, 'Third batch 9999'),
        (14, 'Also no digits');
    """

    // Build idx_ch_edge SECOND
    sql """ BUILD INDEX idx_ch_edge ON ${tableBuild}; """
    wait_for_build_index_finish(tableBuild, timeout)

    // === Table 2: Baseline with indexes at creation ===
    sql """
        CREATE TABLE ${tableBaseline} (
            a BIGINT NOT NULL AUTO_INCREMENT,
            ch TEXT NULL,
            INDEX idx_ch_edge(ch) USING INVERTED PROPERTIES("analyzer" = "edge_ngram_test", "support_phrase" = "true"),
            INDEX idx_ch_std(ch) USING INVERTED PROPERTIES("analyzer" = "standard_test_default", "support_phrase" = "true")
        ) DUPLICATE KEY(a)
        DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    sql """ INSERT INTO ${tableBaseline} VALUES
        (1, '1949 West German federal election'),
        (2, 'Category 1062 deaths'),
        (3, 'File Swallow Project Gutenberg eBook 11921 jpg'),
        (4, '80386DX processor'),
        (5, 'List of minor planets 49001 to 50000'),
        (6, 'HIP 57087 star'),
        (7, 'File Paul Verlaine Project Gutenberg eText 15112 png'),
        (8, 'Simple text without digits'),
        (9, '1700 in Canada'),
        (10, 'Category 1649 births'),
        (11, 'Another text 2024'),
        (12, 'No digits here'),
        (13, 'Third batch 9999'),
        (14, 'Also no digits');
    """

    sql "sync"

    // Pin fuzzy variable: search() requires common expr pushdown to work
    sql "SET enable_common_expr_pushdown = true"

    // === Verification: regex /\d\d\d\d/ should match exactly 4-digit tokens ===
    // With edge_ngram analyzer, "11921" produces tokens: "119", "1192", "11921"
    // So "1192" (4-digit) matches /\d\d\d\d/.
    // With standard_test_default, "11921" stays as "11921" (5-digit), no 4-digit match.
    // Both tables should use the same index (edge_ngram, added first in ALTER order)
    // and return the same results.

    // Test 1: Total count with regex query
    order_qt_build_regex """ SELECT count(*) FROM ${tableBuild}
        WHERE search('/\\\\d\\\\d\\\\d\\\\d/', '{"default_operator":"OR","default_field":"ch","minimum_should_match":0,"mode":"lucene"}'); """

    order_qt_baseline_regex """ SELECT count(*) FROM ${tableBaseline}
        WHERE search('/\\\\d\\\\d\\\\d\\\\d/', '{"default_operator":"OR","default_field":"ch","minimum_should_match":0,"mode":"lucene"}'); """

    // Test 2: Batch 1 only (the data that went through build index)
    order_qt_build_batch1 """ SELECT count(*) FROM ${tableBuild}
        WHERE a <= 10 AND search('/\\\\d\\\\d\\\\d\\\\d/', '{"default_operator":"OR","default_field":"ch","minimum_should_match":0,"mode":"lucene"}'); """

    order_qt_baseline_batch1 """ SELECT count(*) FROM ${tableBaseline}
        WHERE a <= 10 AND search('/\\\\d\\\\d\\\\d\\\\d/', '{"default_operator":"OR","default_field":"ch","minimum_should_match":0,"mode":"lucene"}'); """

    // Test 3: Verify specific rows with 5+ digit tokens are found
    // These rows contain tokens like "11921", "80386", "49001", "50000", "57087", "15112"
    // which produce 4-digit edge n-grams matching /\d\d\d\d/
    order_qt_build_specific """ SELECT a FROM ${tableBuild}
        WHERE a IN (3, 4, 5, 6, 7) AND search('/\\\\d\\\\d\\\\d\\\\d/', '{"default_operator":"OR","default_field":"ch","minimum_should_match":0,"mode":"lucene"}')
        ORDER BY a; """

    order_qt_baseline_specific """ SELECT a FROM ${tableBaseline}
        WHERE a IN (3, 4, 5, 6, 7) AND search('/\\\\d\\\\d\\\\d\\\\d/', '{"default_operator":"OR","default_field":"ch","minimum_should_match":0,"mode":"lucene"}')
        ORDER BY a; """

    // Test 4: Simple term query should work on both
    order_qt_build_term """ SELECT count(*) FROM ${tableBuild}
        WHERE search('1949', '{"default_operator":"OR","default_field":"ch","minimum_should_match":0,"mode":"lucene"}'); """

    order_qt_baseline_term """ SELECT count(*) FROM ${tableBaseline}
        WHERE search('1949', '{"default_operator":"OR","default_field":"ch","minimum_should_match":0,"mode":"lucene"}'); """
}
