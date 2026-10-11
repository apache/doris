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

// Run separately from concurrent analyzer tests that share the global policy quota.
suite("test_ik_custom_analyzer", "nonConcurrent") {
    def pinyinFilter = "test_ik_pinyin_filter"
    def smartAnalyzer = "test_ik_smart_pinyin_analyzer"
    def maxWordAnalyzer = "test_ik_max_word_pinyin_analyzer"
    def smartOnlyAnalyzer = "test_ik_smart_only_analyzer"
    def maxWordOnlyAnalyzer = "test_ik_max_word_only_analyzer"
    def paddedSmartOnlyAnalyzer = "test_ik_padded_smart_only_analyzer"
    def emptyTokenFilter = "test_ik_empty_token_filter"
    def emptyCharFilter = "test_ik_empty_char_filter"
    def emptyPaddedAnalyzer = "test_ik_empty_padded_analyzer"

    sql "DROP TABLE IF EXISTS test_ik_custom_analyzer"
    sql "DROP TABLE IF EXISTS test_ik_legacy_custom_alter"
    sql "DROP TABLE IF EXISTS test_ik_legacy_custom_create"
    sql "DROP TABLE IF EXISTS test_ik_builtin_custom_alter"
    sql "DROP TABLE IF EXISTS test_ik_builtin_custom_create"
    sql "DROP TABLE IF EXISTS test_ik_padded_custom_alter"
    sql "DROP TABLE IF EXISTS test_ik_padded_custom_create"
    sql "DROP TABLE IF EXISTS test_ik_empty_custom_alter"
    sql "DROP TABLE IF EXISTS test_ik_empty_custom_create"
    sql "DROP TABLE IF EXISTS test_ik_outer_filter_alter"
    sql "DROP TABLE IF EXISTS test_ik_outer_filter_create"
    sql "DROP TABLE IF EXISTS test_ik_lowercase_outer_filter_alter"
    sql "DROP TABLE IF EXISTS test_ik_lowercase_outer_filter_create"
    try_sql "DROP INVERTED INDEX ANALYZER IF EXISTS ${emptyPaddedAnalyzer}"
    try_sql "DROP INVERTED INDEX ANALYZER IF EXISTS ${smartAnalyzer}"
    try_sql "DROP INVERTED INDEX ANALYZER IF EXISTS ${maxWordAnalyzer}"
    try_sql "DROP INVERTED INDEX ANALYZER IF EXISTS ${smartOnlyAnalyzer}"
    try_sql "DROP INVERTED INDEX ANALYZER IF EXISTS ${maxWordOnlyAnalyzer}"
    try_sql "DROP INVERTED INDEX ANALYZER IF EXISTS ${paddedSmartOnlyAnalyzer}"
    try_sql "DROP INVERTED INDEX TOKEN_FILTER IF EXISTS ${pinyinFilter}"
    try_sql "DROP INVERTED INDEX TOKEN_FILTER IF EXISTS ${emptyTokenFilter}"
    try_sql "DROP INVERTED INDEX CHAR_FILTER IF EXISTS ${emptyCharFilter}"

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
    sql """
        CREATE INVERTED INDEX ANALYZER IF NOT EXISTS ${paddedSmartOnlyAnalyzer}
        PROPERTIES ("tokenizer" = " IK_SMART ")
    """
    sql """
        CREATE INVERTED INDEX TOKEN_FILTER IF NOT EXISTS ${emptyTokenFilter}
        PROPERTIES ("type" = "empty")
    """
    sql """
        CREATE INVERTED INDEX CHAR_FILTER IF NOT EXISTS ${emptyCharFilter}
        PROPERTIES ("type" = "empty")
    """
    sql """
        CREATE INVERTED INDEX ANALYZER IF NOT EXISTS ${emptyPaddedAnalyzer}
        PROPERTIES (
            "tokenizer" = "ik_smart",
            "token_filter" = "empty,${emptyTokenFilter}",
            "char_filter" = "${emptyCharFilter},empty"
        )
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
    waitAnalyzerReady(paddedSmartOnlyAnalyzer)
    waitAnalyzerReady(emptyPaddedAnalyzer)

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

    test {
        sql """
            CREATE TABLE test_ik_empty_custom_create (
                id INT,
                content STRING,
                INDEX idx_plain (content) USING INVERTED PROPERTIES("analyzer" = "${smartOnlyAnalyzer}"),
                INDEX idx_empty (content) USING INVERTED PROPERTIES("analyzer" = "${emptyPaddedAnalyzer}")
            ) DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """
        exception "cannot have multiple inverted indexes"
    }

    sql """
        CREATE TABLE test_ik_empty_custom_alter (
            id INT,
            content STRING,
            INDEX idx_plain (content) USING INVERTED PROPERTIES("analyzer" = "${smartOnlyAnalyzer}")
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    test {
        sql """
            ALTER TABLE test_ik_empty_custom_alter
            ADD INDEX idx_empty (content) USING INVERTED
                PROPERTIES("analyzer" = "${emptyPaddedAnalyzer}")
        """
        exception "already exists"
    }

    test {
        sql """
            CREATE TABLE test_ik_outer_filter_create (
                id INT,
                content STRING,
                INDEX idx_legacy (content) USING INVERTED
                    PROPERTIES("parser" = "ik", "parser_mode" = "ik_smart",
                        "char_filter_type" = "char_replace", "char_filter_pattern" = "-",
                        "char_filter_replacement" = " "),
                INDEX idx_filtered (content) USING INVERTED
                    PROPERTIES("analyzer" = "${smartOnlyAnalyzer}",
                        "char_filter_type" = "char_replace", "char_filter_pattern" = "-",
                        "char_filter_replacement" = " ")
            ) DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """
        exception "cannot have multiple inverted indexes"
    }

    sql """
        CREATE TABLE test_ik_outer_filter_alter (
            id INT,
            content STRING,
            INDEX idx_legacy (content) USING INVERTED
                PROPERTIES("parser" = "ik", "parser_mode" = "ik_smart",
                    "char_filter_type" = "char_replace", "char_filter_pattern" = "-",
                    "char_filter_replacement" = " ")
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    test {
        sql """
            ALTER TABLE test_ik_outer_filter_alter
            ADD INDEX idx_filtered (content) USING INVERTED
                PROPERTIES("analyzer" = "${smartOnlyAnalyzer}",
                    "char_filter_type" = "char_replace", "char_filter_pattern" = "-",
                    "char_filter_replacement" = " ")
        """
        exception "already exists"
    }

    test {
        sql """
            CREATE TABLE test_ik_lowercase_outer_filter_create (
                id INT,
                content STRING,
                INDEX idx_plain (content) USING INVERTED
                    PROPERTIES("parser" = "ik", "parser_mode" = "ik_smart"),
                INDEX idx_lowercase (content) USING INVERTED
                    PROPERTIES("analyzer" = "${smartOnlyAnalyzer}",
                        "char_filter_type" = "char_replace", "char_filter_pattern" = "AaA",
                        "char_filter_replacement" = "a")
            ) DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """
        exception "cannot have multiple inverted indexes"
    }

    sql """
        CREATE TABLE test_ik_lowercase_outer_filter_alter (
            id INT,
            content STRING,
            INDEX idx_plain (content) USING INVERTED
                PROPERTIES("parser" = "ik", "parser_mode" = "ik_smart")
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    test {
        sql """
            ALTER TABLE test_ik_lowercase_outer_filter_alter
            ADD INDEX idx_lowercase (content) USING INVERTED
                PROPERTIES("analyzer" = "${smartOnlyAnalyzer}",
                    "char_filter_type" = "char_replace", "char_filter_pattern" = "AaA",
                    "char_filter_replacement" = "a")
        """
        exception "already exists"
    }

    test {
        sql """
            CREATE TABLE test_ik_builtin_custom_create (
                id INT,
                content STRING,
                INDEX idx_builtin (content) USING INVERTED PROPERTIES("analyzer" = "ik"),
                INDEX idx_custom (content) USING INVERTED PROPERTIES("analyzer" = "${maxWordOnlyAnalyzer}")
            ) DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """
        exception "cannot have multiple inverted indexes"
    }

    sql """
        CREATE TABLE test_ik_builtin_custom_alter (
            id INT,
            content STRING,
            INDEX idx_builtin (content) USING INVERTED PROPERTIES("analyzer" = "ik")
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    test {
        sql """
            ALTER TABLE test_ik_builtin_custom_alter
            ADD INDEX idx_custom (content) USING INVERTED
                PROPERTIES("analyzer" = "${maxWordOnlyAnalyzer}")
        """
        exception "already exists"
    }

    test {
        sql """
            CREATE TABLE test_ik_padded_custom_create (
                id INT,
                content STRING,
                INDEX idx_plain (content) USING INVERTED PROPERTIES("analyzer" = "${smartOnlyAnalyzer}"),
                INDEX idx_padded (content) USING INVERTED PROPERTIES("analyzer" = "${paddedSmartOnlyAnalyzer}")
            ) DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """
        exception "cannot have multiple inverted indexes"
    }

    sql """
        CREATE TABLE test_ik_padded_custom_alter (
            id INT,
            content STRING,
            INDEX idx_plain (content) USING INVERTED PROPERTIES("analyzer" = "${smartOnlyAnalyzer}")
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    test {
        sql """
            ALTER TABLE test_ik_padded_custom_alter
            ADD INDEX idx_padded (content) USING INVERTED
                PROPERTIES("analyzer" = "${paddedSmartOnlyAnalyzer}")
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
