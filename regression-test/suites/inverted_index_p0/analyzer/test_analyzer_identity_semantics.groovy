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
suite("test_analyzer_identity_semantics", "nonConcurrent") {
    sql "DROP TABLE IF EXISTS test_identity_modes_create"
    sql "DROP TABLE IF EXISTS test_identity_modes_alter"
    sql "DROP TABLE IF EXISTS test_identity_max_word_create"
    sql "DROP TABLE IF EXISTS test_identity_max_word_alter"
    sql "DROP TABLE IF EXISTS test_identity_char_replace_create"
    sql "DROP TABLE IF EXISTS test_identity_char_replace_alter"
    sql "DROP TABLE IF EXISTS test_identity_noop_create"
    sql "DROP TABLE IF EXISTS test_identity_noop_alter"
    sql "DROP TABLE IF EXISTS test_identity_fold_create"
    sql "DROP TABLE IF EXISTS test_identity_fold_alter"
    sql "DROP TABLE IF EXISTS test_identity_outer_filter_create"
    sql "DROP TABLE IF EXISTS test_identity_outer_filter_alter"
    for (String mode : ["ik_smart", "ik_max_word"]) {
        sql "DROP TABLE IF EXISTS test_identity_ik_lowercase_create_${mode}"
        sql "DROP TABLE IF EXISTS test_identity_ik_lowercase_alter_${mode}"
    }
    for (String analyzer : ["test_identity_ab", "test_identity_ba", "test_identity_duplicates",
                            "test_identity_noop", "test_identity_plain",
                            "test_identity_plain_ik_smart", "test_identity_filtered_ik_smart",
                            "test_identity_plain_ik_max_word", "test_identity_filtered_ik_max_word",
                            "test_identity_fold_only", "test_identity_lower_then_fold"]) {
        try_sql "DROP INVERTED INDEX ANALYZER IF EXISTS ${analyzer}"
    }
    for (String filter : ["test_identity_cf_ab", "test_identity_cf_ba",
                         "test_identity_cf_duplicates", "test_identity_cf_noop", "test_identity_cf_lower_a",
                         "test_identity_cf_fold"]) {
        try_sql "DROP INVERTED INDEX CHAR_FILTER IF EXISTS ${filter}"
    }

    sql """
        CREATE TABLE test_identity_modes_create (
            id INT, content STRING,
            INDEX idx_smart (content) USING INVERTED PROPERTIES("parser"="ik", "lower_case"="false"),
            INDEX idx_max_word (content) USING INVERTED PROPERTIES("analyzer"="ik", "lower_case"="false"),
            INDEX idx_lowercase (content) USING INVERTED PROPERTIES("analyzer"="ik"),
            INDEX idx_smart_lowercase (content) USING INVERTED PROPERTIES("parser"="ik")
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_allocation"="tag.location.default: 1")
    """
    sql """
        CREATE TABLE test_identity_modes_alter (
            id INT, content STRING,
            INDEX idx_smart (content) USING INVERTED PROPERTIES("parser"="ik", "lower_case"="false")
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_allocation"="tag.location.default: 1")
    """
    test {
        sql """
            CREATE TABLE test_identity_outer_filter_create (
                id INT, content STRING,
                INDEX idx_replace_a (content) USING INVERTED PROPERTIES(
                    "analyzer"="standard", "char_filter_type"="char_replace",
                    "char_filter_pattern"="a", "char_filter_replacement"="b"),
                INDEX idx_replace_x (content) USING INVERTED PROPERTIES(
                    "analyzer"="standard", "char_filter_type"="char_replace",
                    "char_filter_pattern"="x", "char_filter_replacement"="y")
            ) DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_allocation"="tag.location.default: 1")
        """
        exception "cannot have multiple inverted indexes"
    }
    sql """
        CREATE TABLE test_identity_outer_filter_alter (
            id INT, content STRING,
            INDEX idx_replace_a (content) USING INVERTED PROPERTIES(
                "analyzer"="standard", "char_filter_type"="char_replace",
                "char_filter_pattern"="a", "char_filter_replacement"="b")
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_allocation"="tag.location.default: 1")
    """
    test {
        sql """
            ALTER TABLE test_identity_outer_filter_alter ADD INDEX idx_replace_x (content) USING INVERTED
            PROPERTIES("analyzer"="standard", "char_filter_type"="char_replace",
                       "char_filter_pattern"="x", "char_filter_replacement"="y")
        """
        exception "same analyzer selector already exists"
    }
    sql """
        ALTER TABLE test_identity_modes_alter ADD INDEX idx_max_word (content) USING INVERTED
        PROPERTIES("analyzer"="ik", "lower_case"="false")
    """
    test {
        sql """
            CREATE TABLE test_identity_max_word_create (
                id INT, content STRING,
                INDEX idx_builtin (content) USING INVERTED PROPERTIES("analyzer"="ik", "lower_case"="false"),
                INDEX idx_legacy (content) USING INVERTED
                    PROPERTIES("parser"="ik", "parser_mode"="ik_max_word", "lower_case"="false")
            ) DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_allocation"="tag.location.default: 1")
        """
        exception "cannot have multiple inverted indexes"
    }
    sql """
        CREATE TABLE test_identity_max_word_alter (
            id INT, content STRING,
            INDEX idx_builtin (content) USING INVERTED PROPERTIES("analyzer"="ik", "lower_case"="false")
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_allocation"="tag.location.default: 1")
    """
    test {
        sql """
            ALTER TABLE test_identity_max_word_alter ADD INDEX idx_legacy (content) USING INVERTED
            PROPERTIES("parser"="ik", "parser_mode"="ik_max_word", "lower_case"="false")
        """
        exception "already exists"
    }

    for (def config : [["ab", "ab"], ["ba", "ba"], ["duplicates", "aabx"], ["noop", "x"]]) {
        sql """
            CREATE INVERTED INDEX CHAR_FILTER test_identity_cf_${config[0]}
            PROPERTIES("type"="char_replace", "pattern"="${config[1]}", "replacement"="x")
        """
        sql """
            CREATE INVERTED INDEX ANALYZER test_identity_${config[0]}
            PROPERTIES("tokenizer"="keyword", "char_filter"="test_identity_cf_${config[0]}")
        """
    }
    sql """
        CREATE INVERTED INDEX ANALYZER test_identity_plain PROPERTIES("tokenizer"="keyword")
    """
    for (String analyzer : ["test_identity_ab", "test_identity_ba", "test_identity_duplicates",
                            "test_identity_noop", "test_identity_plain"]) {
        Exception lastException = null
        boolean ready = false
        for (int attempt = 0; attempt < 30; attempt++) {
            try {
                sql """SELECT TOKENIZE('probe', '"analyzer"="${analyzer}"')"""
                ready = true
                break
            } catch (Exception e) {
                lastException = e
                sleep(1000)
            }
        }
        assertTrue(ready, "Analyzer ${analyzer} was not ready: ${lastException?.message}")
    }
    sql """
        CREATE TABLE test_identity_char_replace_alter (
            id INT, content STRING,
            INDEX idx_ab (content) USING INVERTED PROPERTIES("analyzer"="test_identity_ab")
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_allocation"="tag.location.default: 1")
    """
    for (String analyzer : ["test_identity_ba", "test_identity_duplicates"]) {
        test {
            sql """
                CREATE TABLE test_identity_char_replace_create (
                    id INT, content STRING,
                    INDEX idx_ab (content) USING INVERTED PROPERTIES("analyzer"="test_identity_ab"),
                    INDEX idx_equivalent (content) USING INVERTED PROPERTIES("analyzer"="${analyzer}")
                ) DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES("replication_allocation"="tag.location.default: 1")
            """
            exception "cannot have multiple inverted indexes"
        }
        test {
            sql """
                ALTER TABLE test_identity_char_replace_alter ADD INDEX idx_equivalent (content) USING INVERTED
                PROPERTIES("analyzer"="${analyzer}")
            """
            exception "already exists"
        }
    }
    test {
        sql """
            CREATE TABLE test_identity_noop_create (
                id INT, content STRING,
                INDEX idx_plain (content) USING INVERTED PROPERTIES("analyzer"="test_identity_plain"),
                INDEX idx_noop (content) USING INVERTED PROPERTIES("analyzer"="test_identity_noop")
            ) DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_allocation"="tag.location.default: 1")
        """
        exception "cannot have multiple inverted indexes"
    }
    sql """
        CREATE TABLE test_identity_noop_alter (
            id INT, content STRING,
            INDEX idx_plain (content) USING INVERTED PROPERTIES("analyzer"="test_identity_plain")
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_allocation"="tag.location.default: 1")
    """
    test {
        sql """
            ALTER TABLE test_identity_noop_alter ADD INDEX idx_noop (content) USING INVERTED
            PROPERTIES("analyzer"="test_identity_noop")
        """
        exception "already exists"
    }

    sql """
        CREATE INVERTED INDEX CHAR_FILTER test_identity_cf_lower_a
        PROPERTIES("type"="char_replace", "pattern"="A", "replacement"="a")
    """
    sql """
        CREATE INVERTED INDEX CHAR_FILTER test_identity_cf_fold
        PROPERTIES("type"="icu_normalizer")
    """
    sql """
        CREATE INVERTED INDEX ANALYZER test_identity_fold_only
        PROPERTIES("tokenizer"="ik_smart", "char_filter"="test_identity_cf_fold")
    """
    sql """
        CREATE INVERTED INDEX ANALYZER test_identity_lower_then_fold
        PROPERTIES("tokenizer"="ik_smart",
                   "char_filter"="test_identity_cf_lower_a,test_identity_cf_fold")
    """
    test {
        sql """
            CREATE TABLE test_identity_fold_create (
                id INT, content STRING,
                INDEX idx_fold (content) USING INVERTED
                    PROPERTIES("analyzer"="test_identity_fold_only"),
                INDEX idx_redundant (content) USING INVERTED
                    PROPERTIES("analyzer"="test_identity_lower_then_fold")
            ) DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_allocation"="tag.location.default: 1")
        """
        exception "cannot have multiple inverted indexes"
    }
    sql """
        CREATE TABLE test_identity_fold_alter (
            id INT, content STRING,
            INDEX idx_fold (content) USING INVERTED
                PROPERTIES("analyzer"="test_identity_fold_only")
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_allocation"="tag.location.default: 1")
    """
    test {
        sql """
            ALTER TABLE test_identity_fold_alter
            ADD INDEX idx_redundant (content) USING INVERTED
            PROPERTIES("analyzer"="test_identity_lower_then_fold")
        """
        exception "already exists"
    }
    for (String mode : ["ik_smart", "ik_max_word"]) {
        sql """
            CREATE INVERTED INDEX ANALYZER test_identity_plain_${mode}
            PROPERTIES("tokenizer"="${mode}")
        """
        sql """
            CREATE INVERTED INDEX ANALYZER test_identity_filtered_${mode}
            PROPERTIES("tokenizer"="${mode}", "char_filter"="test_identity_cf_lower_a")
        """
        test {
            sql """
                CREATE TABLE test_identity_ik_lowercase_create_${mode} (
                    id INT, content STRING,
                    INDEX idx_plain (content) USING INVERTED
                        PROPERTIES("analyzer"="test_identity_plain_${mode}"),
                    INDEX idx_filtered (content) USING INVERTED
                        PROPERTIES("analyzer"="test_identity_filtered_${mode}")
                ) DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES("replication_allocation"="tag.location.default: 1")
            """
            exception "cannot have multiple inverted indexes"
        }
        sql """
            CREATE TABLE test_identity_ik_lowercase_alter_${mode} (
                id INT, content STRING,
                INDEX idx_plain (content) USING INVERTED
                    PROPERTIES("analyzer"="test_identity_plain_${mode}")
            ) DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_allocation"="tag.location.default: 1")
        """
        test {
            sql """
                ALTER TABLE test_identity_ik_lowercase_alter_${mode}
                ADD INDEX idx_filtered (content) USING INVERTED
                PROPERTIES("analyzer"="test_identity_filtered_${mode}")
            """
            exception "already exists"
        }
    }

    sql "INSERT INTO test_identity_modes_create VALUES (1, 'abc def'), (2, 'zzz'), (3, '清华大学'), (4, 'ABC')"
    qt_match_builtin_ik """
        SELECT id FROM test_identity_modes_create WHERE content MATCH 'abc' USING ANALYZER IK
        ORDER BY id
    """
    qt_match_builtin_ik_mode """
        SELECT id FROM test_identity_modes_create WHERE content MATCH '清华' USING ANALYZER ik
        ORDER BY id
    """
    qt_match_builtin_ik_lowercase """
        SELECT id FROM test_identity_modes_create WHERE content MATCH 'ABC' USING ANALYZER ik
        ORDER BY id
    """
    qt_match_implicit_ik_smart_mode """
        SELECT id FROM test_identity_modes_create WHERE content MATCH '清华'
        ORDER BY id
    """
    qt_match_implicit_ik_smart_uppercase """
        SELECT id FROM test_identity_modes_create WHERE content MATCH 'ABC'
        ORDER BY id
    """
    sql "INSERT INTO test_identity_noop_alter VALUES (1, 'abc def'), (2, 'zzz')"
    qt_match_keyword_implicit """
        SELECT id FROM test_identity_noop_alter WHERE content MATCH 'abc def'
        ORDER BY id
    """
    qt_match_keyword_uppercase """
        SELECT id FROM test_identity_noop_alter
        WHERE content MATCH 'abc def' USING ANALYZER TEST_IDENTITY_PLAIN
        ORDER BY id
    """
    qt_match_keyword_fragment """
        SELECT id FROM test_identity_noop_alter WHERE content MATCH 'abc'
        ORDER BY id
    """
}
