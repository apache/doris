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

suite("test_storage_format_snii", "p0, nonConcurrent") {
    sql "DROP TABLE IF EXISTS test_storage_format_snii"
    sql "DROP TABLE IF EXISTS test_storage_format_snii_array"
    sql "DROP TABLE IF EXISTS test_storage_format_snii_add_index"
    sql "DROP TABLE IF EXISTS test_storage_format_snii_build_index"
    sql "DROP TABLE IF EXISTS test_storage_format_snii_bkd"
    sql "DROP TABLE IF EXISTS test_storage_format_snii_array_bkd"
    sql "DROP TABLE IF EXISTS test_storage_format_snii_ann"
    sql "DROP TABLE IF EXISTS test_storage_format_snii_char"
    sql "DROP TABLE IF EXISTS test_storage_format_snii_array_char"

    sql """
        CREATE TABLE test_storage_format_snii (
          id INT NULL,
          body TEXT NULL,
          INDEX idx_body (`body`) USING INVERTED PROPERTIES(
            "parser" = "english",
            "support_phrase" = "true",
            "lower_case" = "true"
          ) COMMENT ''
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
          "replication_allocation" = "tag.location.default: 1",
          "disable_auto_compaction" = "true",
          "inverted_index_storage_format" = "SNII"
        );
    """

    sql """
        INSERT INTO test_storage_format_snii VALUES
          (1, 'alpha beta gamma'),
          (2, 'alpha delta'),
          (3, 'beta epsilon'),
          (4, NULL),
          (5, 'quick brown fox'),
          (6, 'quick fox'),
          (7, 'Apache Doris is a modern data warehouse for real-time analytics. It delivers lightning-fast query performance on large-scale datasets.');
    """
    sql "sync"

    order_qt_match_any """
        SELECT id FROM test_storage_format_snii
        WHERE body MATCH_ANY 'alpha'
        ORDER BY id
    """
    order_qt_match_all """
        SELECT id FROM test_storage_format_snii
        WHERE body MATCH_ALL 'alpha beta'
        ORDER BY id
    """
    order_qt_match_phrase """
        SELECT id FROM test_storage_format_snii
        WHERE body MATCH_PHRASE 'quick brown'
        ORDER BY id
    """
    order_qt_match_phrase_slop """
        SELECT id FROM test_storage_format_snii
        WHERE body MATCH_PHRASE 'modern warehouse ~3'
        ORDER BY id
    """
    order_qt_match_phrase_ordered_slop """
        SELECT id FROM test_storage_format_snii
        WHERE body MATCH_PHRASE 'modern warehouse ~3+'
        ORDER BY id
    """
    order_qt_match_phrase_transposition """
        SELECT id FROM test_storage_format_snii
        WHERE body MATCH_PHRASE 'fox quick ~2'
        ORDER BY id
    """
    order_qt_match_phrase_multi_term_slop """
        SELECT id FROM test_storage_format_snii
        WHERE body MATCH_PHRASE 'quick fox brown ~2'
        ORDER BY id
    """
    order_qt_null_bitmap """
        SELECT id FROM test_storage_format_snii
        WHERE body IS NULL
        ORDER BY id
    """
    order_qt_match_regexp_substring """
        SELECT id FROM test_storage_format_snii
        WHERE body MATCH_REGEXP 'pha'
        ORDER BY id
    """
    order_qt_match_regexp_invalid """
        SELECT id FROM test_storage_format_snii
        WHERE body MATCH_REGEXP '*pha*'
        ORDER BY id
    """

    sql """
        CREATE TABLE test_storage_format_snii_array (
          id INT NULL,
          tags ARRAY<TEXT> NULL,
          INDEX idx_tags (`tags`) USING INVERTED COMMENT ''
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
          "replication_allocation" = "tag.location.default: 1",
          "inverted_index_storage_format" = "SNII"
        );
    """

    sql """
        INSERT INTO test_storage_format_snii_array VALUES
          (1, '["alpha", "beta"]'),
          (2, '["gamma"]'),
          (3, NULL);
    """
    sql "sync"

    order_qt_array_contains """
        SELECT id FROM test_storage_format_snii_array
        WHERE array_contains(tags, 'alpha')
        ORDER BY id
    """

    sql """
        CREATE TABLE test_storage_format_snii_char (
          id INT NULL,
          value CHAR(10) NULL,
          INDEX idx_value (`value`) USING INVERTED PROPERTIES(
            "parser" = "none",
            "ignore_above" = "3"
          ) COMMENT ''
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
          "replication_allocation" = "tag.location.default: 1",
          "inverted_index_storage_format" = "SNII"
        );
    """

    sql """
        INSERT INTO test_storage_format_snii_char VALUES
          (1, 'abc'),
          (2, 'xyz');
    """
    sql "sync"

    order_qt_char_equal """
        SELECT id FROM test_storage_format_snii_char
        WHERE value = 'abc'
        ORDER BY id
    """

    sql """
        CREATE TABLE test_storage_format_snii_array_char (
          id INT NULL,
          chars ARRAY<CHAR(10)> NULL,
          INDEX idx_chars (`chars`) USING INVERTED PROPERTIES(
            "parser" = "none",
            "ignore_above" = "3"
          ) COMMENT ''
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
          "replication_allocation" = "tag.location.default: 1",
          "inverted_index_storage_format" = "SNII"
        );
    """

    sql """
        INSERT INTO test_storage_format_snii_array_char VALUES
          (1, ['abc', 'xyz']),
          (2, ['def']);
    """
    sql "sync"

    order_qt_array_char_contains """
        SELECT id FROM test_storage_format_snii_array_char
        WHERE array_contains(chars, 'xyz')
        ORDER BY id
    """

    def wait_for_latest_schema_change_finish = { table_name, timeout_ms ->
        def delta_time = 1000
        def used_time = 0
        for (int t = delta_time; t <= timeout_ms; t += delta_time) {
            def jobs = sql """
                SHOW ALTER TABLE COLUMN WHERE TableName = "${table_name}"
                ORDER BY CreateTime DESC LIMIT 1;
            """
            if (jobs.isEmpty() || jobs[0].toString().contains("FINISHED")) {
                return
            }
            used_time = t
            sleep(delta_time)
        }
        assertTrue(used_time <= timeout_ms, "wait_for_latest_schema_change_finish timeout")
    }

    def wait_for_build_index_finish = { table_name, timeout_ms ->
        def delta_time = 1000
        def used_time = 0
        for (int t = delta_time; t <= timeout_ms; t += delta_time) {
            def jobs = sql """SHOW BUILD INDEX WHERE TableName = "${table_name}";"""
            def finished = 0
            for (int i = 0; i < jobs.size(); i++) {
                logger.info(table_name + " build index job state: " + jobs[i][7])
                assertNotEquals("CANCELLED", jobs[i][7], "build index job failed: " + jobs[i])
                if (jobs[i][7] == "FINISHED") {
                    ++finished
                }
            }
            if (finished == jobs.size()) {
                break
            }
            used_time = t
            sleep(delta_time)
        }
        assertTrue(used_time <= timeout_ms, "wait_for_build_index_finish timeout")
    }

    def build_index_on = { table_name, index_name ->
        // Cloud mode builds every index of the table and takes no index name.
        if (isCloudMode()) {
            sql "BUILD INDEX ON ${table_name}"
        } else {
            sql "BUILD INDEX ${index_name} ON ${table_name}"
        }
        wait_for_build_index_finish(table_name, 300000)
    }

    // Every rowset already carries idx_body, so this build has nothing left to do
    // and must still succeed and leave the index queryable.
    build_index_on("test_storage_format_snii", "idx_body")
    order_qt_build_index_already_complete """
        SELECT id FROM test_storage_format_snii
        WHERE body MATCH_PHRASE 'quick brown'
        ORDER BY id
    """

    sql """
        CREATE TABLE test_storage_format_snii_build_index (
          id INT NOT NULL,
          body TEXT NULL,
          note TEXT NULL,
          INDEX idx_bi_body (`body`) USING INVERTED PROPERTIES(
            "parser" = "english",
            "support_phrase" = "true",
            "lower_case" = "true"
          ) COMMENT ''
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        PARTITION BY RANGE(`id`) (
          PARTITION p1 VALUES LESS THAN ("100"),
          PARTITION p2 VALUES LESS THAN ("200")
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
          "replication_allocation" = "tag.location.default: 1",
          "disable_auto_compaction" = "true",
          "inverted_index_storage_format" = "SNII"
        );
    """

    // Historical data, written before the new indexes exist.
    sql """
        INSERT INTO test_storage_format_snii_build_index VALUES
          (1, 'quick brown fox', 'alpha'),
          (2, 'quick fox', 'beta'),
          (101, 'lazy brown dog', 'alpha'),
          (102, 'lazy dog', 'gamma');
    """
    sql "sync"

    // Light index add: only new data carries the indexes, so the historical rowsets
    // above are exactly what BUILD INDEX has to backfill. Two indexes land on the
    // same column, which the build must satisfy from a single column read.
    sql "SET enable_add_index_for_new_data = true"
    sql """
        ALTER TABLE test_storage_format_snii_build_index
        ADD INDEX idx_bi_note (`note`) USING INVERTED PROPERTIES("parser" = "none") COMMENT ''
    """
    wait_for_latest_schema_change_finish("test_storage_format_snii_build_index", 300000)
    sql """
        ALTER TABLE test_storage_format_snii_build_index
        ADD INDEX idx_bi_note_tokenized (`note`) USING INVERTED PROPERTIES(
          "parser" = "english"
        ) COMMENT ''
    """
    wait_for_latest_schema_change_finish("test_storage_format_snii_build_index", 300000)
    sql "sync"

    if (isCloudMode()) {
        sql "BUILD INDEX ON test_storage_format_snii_build_index"
    } else {
        sql "BUILD INDEX idx_bi_note ON test_storage_format_snii_build_index"
        sql """
            BUILD INDEX idx_bi_note_tokenized ON test_storage_format_snii_build_index
            PARTITIONS(p1, p2)
        """
    }
    wait_for_build_index_finish("test_storage_format_snii_build_index", 300000)

    // The newly built index now answers over the historical rowsets ...
    order_qt_build_index_history """
        SELECT id FROM test_storage_format_snii_build_index
        WHERE note MATCH_ANY 'alpha'
        ORDER BY id
    """
    // ... and the index that was already there keeps answering phrase queries.
    order_qt_build_index_untouched_phrase """
        SELECT id FROM test_storage_format_snii_build_index
        WHERE body MATCH_PHRASE 'quick brown'
        ORDER BY id
    """

    // Data written after the build carries every index without a further build.
    sql """
        INSERT INTO test_storage_format_snii_build_index VALUES
          (3, 'quick lazy fox', 'alpha'),
          (103, 'brown dog', 'delta');
    """
    sql "sync"
    order_qt_build_index_new_data """
        SELECT id FROM test_storage_format_snii_build_index
        WHERE note MATCH_ANY 'alpha'
        ORDER BY id
    """
    order_qt_build_index_new_data_phrase """
        SELECT id FROM test_storage_format_snii_build_index
        WHERE body MATCH_PHRASE 'brown dog'
        ORDER BY id
    """

    // Rebuilding is idempotent: finished rowsets are skipped, results do not move.
    build_index_on("test_storage_format_snii_build_index", "idx_bi_note")
    order_qt_build_index_rerun """
        SELECT id FROM test_storage_format_snii_build_index
        WHERE note MATCH_ANY 'alpha'
        ORDER BY id
    """

    sql """
        CREATE TABLE test_storage_format_snii_add_index (
          id INT NULL,
          body TEXT NULL,
          score INT NULL,
          scores ARRAY<INT> NULL,
          embedding ARRAY<FLOAT> NOT NULL,
          INDEX idx_body_added_table (`body`) USING INVERTED COMMENT ''
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
          "replication_allocation" = "tag.location.default: 1",
          "inverted_index_storage_format" = "SNII"
        );
    """

    test {
        sql """
            ALTER TABLE test_storage_format_snii_add_index
            ADD INDEX idx_score_added (`score`) USING INVERTED COMMENT ''
        """
        exception "SNII inverted index storage format"
    }

    test {
        sql """
            ALTER TABLE test_storage_format_snii_add_index
            ADD INDEX idx_scores_added (`scores`) USING INVERTED COMMENT ''
        """
        exception "SNII inverted index storage format"
    }

    test {
        sql """
            CREATE INDEX idx_ann_added ON test_storage_format_snii_add_index (`embedding`) USING ANN PROPERTIES(
              "index_type" = "hnsw",
              "metric_type" = "l2_distance",
              "dim" = "1"
            )
        """
        exception "ANN index is not supported in index format SNII"
    }

    test {
        sql """
            CREATE TABLE test_storage_format_snii_bkd (
              id INT NULL,
              score INT NULL,
              INDEX idx_score (`score`) USING INVERTED COMMENT ''
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY RANDOM BUCKETS 1
            PROPERTIES (
              "replication_allocation" = "tag.location.default: 1",
              "inverted_index_storage_format" = "SNII"
            );
        """
        exception "SNII inverted index storage format"
    }

    test {
        sql """
            CREATE TABLE test_storage_format_snii_array_bkd (
              id INT NULL,
              scores ARRAY<INT> NULL,
              INDEX idx_scores (`scores`) USING INVERTED COMMENT ''
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY RANDOM BUCKETS 1
            PROPERTIES (
              "replication_allocation" = "tag.location.default: 1",
              "inverted_index_storage_format" = "SNII"
            );
        """
        exception "SNII inverted index storage format"
    }

    test {
        sql """
            CREATE TABLE test_storage_format_snii_ann (
              id INT NULL,
              embedding ARRAY<FLOAT> NOT NULL,
              INDEX idx_ann (`embedding`) USING ANN PROPERTIES(
                "index_type" = "hnsw",
                "metric_type" = "l2_distance",
                "dim" = "1"
              )
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY RANDOM BUCKETS 1
            PROPERTIES (
              "replication_allocation" = "tag.location.default: 1",
              "inverted_index_storage_format" = "SNII"
            );
        """
        exception "ANN index is not supported in index format SNII"
    }
}
