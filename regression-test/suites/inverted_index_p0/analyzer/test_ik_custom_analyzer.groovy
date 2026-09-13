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

suite("test_ik_custom_analyzer", "p0") {
    def pinyinFilter = "test_ik_pinyin_filter"
    def smartAnalyzer = "test_ik_smart_pinyin_analyzer"
    def maxWordAnalyzer = "test_ik_max_word_pinyin_analyzer"
    def smartOnlyAnalyzer = "test_ik_smart_only_analyzer"
    def maxWordOnlyAnalyzer = "test_ik_max_word_only_analyzer"

    sql "DROP TABLE IF EXISTS test_ik_custom_analyzer"
    sql "DROP TABLE IF EXISTS test_ik_legacy_custom_alter"
    sql "DROP TABLE IF EXISTS test_ik_legacy_custom_create"
    try_sql "DROP INVERTED INDEX ANALYZER IF EXISTS ${smartAnalyzer}"
    try_sql "DROP INVERTED INDEX ANALYZER IF EXISTS ${maxWordAnalyzer}"
    try_sql "DROP INVERTED INDEX ANALYZER IF EXISTS ${smartOnlyAnalyzer}"
    try_sql "DROP INVERTED INDEX ANALYZER IF EXISTS ${maxWordOnlyAnalyzer}"
    try_sql "DROP INVERTED INDEX TOKEN_FILTER IF EXISTS ${pinyinFilter}"

    sql """
        CREATE INVERTED INDEX TOKEN_FILTER IF NOT EXISTS ${pinyinFilter}
        PROPERTIES (
            "type" = "pinyin",
            "keep_none_chinese" = "false",
            "keep_first_letter" = "true",
            "keep_full_pinyin" = "false",
            "keep_separate_first_letter" = "false",
            "keep_original" = "true",
            "keep_joined_full_pinyin" = "true"
        )
    """
    sql """
        CREATE INVERTED INDEX ANALYZER IF NOT EXISTS ${smartAnalyzer}
        PROPERTIES (
            "tokenizer" = "ik_smart",
            "token_filter" = "${pinyinFilter}"
        )
    """
    sql """
        CREATE INVERTED INDEX ANALYZER IF NOT EXISTS ${maxWordAnalyzer}
        PROPERTIES (
            "tokenizer" = "ik_max_word",
            "token_filter" = "${pinyinFilter}"
        )
    """
    sql """
        CREATE INVERTED INDEX ANALYZER IF NOT EXISTS ${smartOnlyAnalyzer}
        PROPERTIES ("tokenizer" = "ik_smart")
    """
    sql """
        CREATE INVERTED INDEX ANALYZER IF NOT EXISTS ${maxWordOnlyAnalyzer}
        PROPERTIES ("tokenizer" = "ik_max_word")
    """

    def waitAnalyzerReady = { analyzerName ->
        int maxRetry = 30
        Exception lastException = null
        for (int i = 0; i < maxRetry; i++) {
            try {
                sql """SELECT TOKENIZE('probe', '"analyzer"="${analyzerName}"')"""
                return
            } catch (Exception e) {
                lastException = e
                sleep(1000)
            }
        }
        assertTrue(false, "Analyzer ${analyzerName} was not ready: ${lastException?.message}")
    }

    waitAnalyzerReady(smartAnalyzer)
    waitAnalyzerReady(maxWordAnalyzer)
    waitAnalyzerReady(smartOnlyAnalyzer)
    waitAnalyzerReady(maxWordOnlyAnalyzer)

    test {
        sql """
            CREATE TABLE test_ik_legacy_custom_create (
                id INT,
                content STRING,
                INDEX idx_legacy (content) USING INVERTED PROPERTIES("parser" = "ik"),
                INDEX idx_custom (content) USING INVERTED PROPERTIES("analyzer" = "${smartOnlyAnalyzer}")
            ) DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """
        exception "cannot have multiple inverted indexes"
    }

    sql """
        CREATE TABLE test_ik_legacy_custom_alter (
            id INT,
            content STRING,
            INDEX idx_legacy (content) USING INVERTED
                PROPERTIES("parser" = "ik", "parser_mode" = "ik_max_word")
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    test {
        sql """
            ALTER TABLE test_ik_legacy_custom_alter
            ADD INDEX idx_custom (content) USING INVERTED
                PROPERTIES("analyzer" = "${maxWordOnlyAnalyzer}")
        """
        exception "already exists"
    }

    qt_smart_tokenize """
        SELECT TOKENIZE('我来到北京清华大学', '"analyzer"="${smartAnalyzer}"')
    """
    qt_max_word_tokenize """
        SELECT TOKENIZE('我来到北京清华大学', '"analyzer"="${maxWordAnalyzer}"')
    """

    sql """
        CREATE TABLE test_ik_custom_analyzer (
            id INT,
            content STRING,
            INDEX idx_smart (content) USING INVERTED
                PROPERTIES("analyzer" = "${smartAnalyzer}"),
            INDEX idx_max_word (content) USING INVERTED
                PROPERTIES("analyzer" = "${maxWordAnalyzer}")
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true"
        )
    """
    sql """
        INSERT INTO test_ik_custom_analyzer VALUES
            (1, '清华大学'),
            (2, '北京大学'),
            (3, '清华园')
    """
    sql "SYNC"

    order_qt_smart_match """
        SELECT id FROM test_ik_custom_analyzer
        WHERE content MATCH 'qinghuadaxue' USING ANALYZER ${smartAnalyzer}
        ORDER BY id
    """
    order_qt_max_word_match """
        SELECT id FROM test_ik_custom_analyzer
        WHERE content MATCH 'qinghua' USING ANALYZER ${maxWordAnalyzer}
        ORDER BY id
    """
}
