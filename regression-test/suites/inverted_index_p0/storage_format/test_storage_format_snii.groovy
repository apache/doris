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

    // ADD INDEX on a scalar numeric is accepted: the SNII-native BKD serves it.
    sql """
        ALTER TABLE test_storage_format_snii_add_index
        ADD INDEX idx_score_added (`score`) USING INVERTED COMMENT ''
    """

    // ADD INDEX on an ARRAY of numerics is accepted too.
    sql """
        ALTER TABLE test_storage_format_snii_add_index
        ADD INDEX idx_scores_added (`scores`) USING INVERTED COMMENT ''
    """

    // ADD INDEX on an ANN column is accepted: SNII stores it as a blob logical
    // index (kAnn), the same container mechanism the native BKD uses.
    sql """
        CREATE INDEX idx_ann_added ON test_storage_format_snii_add_index (`embedding`) USING ANN PROPERTIES(
          "index_type" = "hnsw",
          "metric_type" = "l2_distance",
          "dim" = "1"
        )
    """

    // Scalar numeric columns are served by the SNII-native BKD index. The column
    // set spans the width classes (1/4/8/16 bytes) and the composite CppTypes
    // (DECIMAL, DATETIME), because the writer walks the value array by
    // field_type_size and encodes with the matching KeyCoder -- a disagreement
    // for any one of them silently shifts every row's value.
    sql """
        CREATE TABLE test_storage_format_snii_bkd (
          id INT NULL,
          score INT NULL,
          big BIGINT NULL,
          huge LARGEINT NULL,
          tiny TINYINT NULL,
          ratio DOUBLE NULL,
          price DECIMAL(20, 4) NULL,
          ts DATETIME NULL,
          INDEX idx_score (`score`) USING INVERTED COMMENT '',
          INDEX idx_big (`big`) USING INVERTED COMMENT '',
          INDEX idx_huge (`huge`) USING INVERTED COMMENT '',
          INDEX idx_tiny (`tiny`) USING INVERTED COMMENT '',
          INDEX idx_ratio (`ratio`) USING INVERTED COMMENT '',
          INDEX idx_price (`price`) USING INVERTED COMMENT '',
          INDEX idx_ts (`ts`) USING INVERTED COMMENT ''
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
        INSERT INTO test_storage_format_snii_bkd VALUES
          (1, -100, -9223372036854775808, -170141183460469231731687303715884105728, -128, -1.5, -99999.9999, '2020-01-01 00:00:00'),
          (2, -1, -1, -1, -1, -0.5, -0.0001, '2021-06-15 12:30:45'),
          (3, 0, 0, 0, 0, 0.0, 0.0000, '2022-01-01 00:00:00'),
          (4, 1, 1, 1, 1, 0.5, 0.0001, '2023-03-08 08:08:08'),
          (5, 100, 9223372036854775807, 170141183460469231731687303715884105727, 127, 1.5, 99999.9999, '2024-12-31 23:59:59'),
          (6, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
          (7, 50, 50, 50, 50, 0.25, 12.3456, '2022-07-04 06:00:00');
    """
    sql "sync"

    // Every shape the BKD reader translates: equality, both strict and
    // non-strict one-sided bounds, and the two-sided form that arrives as a
    // conjunction of two independent predicates.
    order_qt_bkd_eq """SELECT id FROM test_storage_format_snii_bkd WHERE score = 0 ORDER BY id"""
    order_qt_bkd_lt """SELECT id FROM test_storage_format_snii_bkd WHERE score < 1 ORDER BY id"""
    order_qt_bkd_le """SELECT id FROM test_storage_format_snii_bkd WHERE score <= 1 ORDER BY id"""
    order_qt_bkd_gt """SELECT id FROM test_storage_format_snii_bkd WHERE score > 0 ORDER BY id"""
    order_qt_bkd_ge """SELECT id FROM test_storage_format_snii_bkd WHERE score >= 0 ORDER BY id"""
    order_qt_bkd_between """SELECT id FROM test_storage_format_snii_bkd WHERE score BETWEEN -1 AND 50 ORDER BY id"""
    order_qt_bkd_in """SELECT id FROM test_storage_format_snii_bkd WHERE score IN (-100, 0, 100) ORDER BY id"""

    // A NULL row owns no point, so it must not answer any comparison, and
    // IS NULL must still find it.
    order_qt_bkd_is_null """SELECT id FROM test_storage_format_snii_bkd WHERE score IS NULL ORDER BY id"""
    order_qt_bkd_is_not_null """SELECT id FROM test_storage_format_snii_bkd WHERE score IS NOT NULL ORDER BY id"""

    // The remaining width classes, each at its type's extremes.
    order_qt_bkd_bigint """SELECT id FROM test_storage_format_snii_bkd WHERE big <= -1 ORDER BY id"""
    order_qt_bkd_largeint """SELECT id FROM test_storage_format_snii_bkd WHERE huge > 0 ORDER BY id"""
    order_qt_bkd_tinyint """SELECT id FROM test_storage_format_snii_bkd WHERE tiny >= 1 ORDER BY id"""
    order_qt_bkd_double """SELECT id FROM test_storage_format_snii_bkd WHERE ratio < 0.0 ORDER BY id"""
    order_qt_bkd_decimal """SELECT id FROM test_storage_format_snii_bkd WHERE price >= 0.0001 ORDER BY id"""
    order_qt_bkd_datetime """SELECT id FROM test_storage_format_snii_bkd WHERE ts < '2022-07-04 06:00:00' ORDER BY id"""

    // The index must not change the answer. Turning it off has to produce the
    // same rows, which is what makes the assertions above about the INDEX
    // rather than about the data.
    sql "SET enable_inverted_index_query = false"
    order_qt_bkd_noindex_lt """SELECT id FROM test_storage_format_snii_bkd WHERE score < 1 ORDER BY id"""
    order_qt_bkd_noindex_between """SELECT id FROM test_storage_format_snii_bkd WHERE score BETWEEN -1 AND 50 ORDER BY id"""
    order_qt_bkd_noindex_decimal """SELECT id FROM test_storage_format_snii_bkd WHERE price >= 0.0001 ORDER BY id"""
    sql "SET enable_inverted_index_query = true"

    // ARRAY<numeric> on SNII: each element is indexed as its own point under the
    // row's id, so a row matches when ANY of its elements does.
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
          "disable_auto_compaction" = "true",
          "inverted_index_storage_format" = "SNII"
        );
    """

    sql """
        INSERT INTO test_storage_format_snii_array_bkd VALUES
          (1, [10, 20, 30]),
          (2, [20]),
          (3, []),
          (4, NULL),
          (5, [-5, 0, 5]),
          (6, [30, 30, 30]);
    """
    sql "sync"

    // 20 lives on rows 1 and 2: one row matching through several elements, and
    // one through its only element.
    order_qt_array_contains_20 """
        SELECT id FROM test_storage_format_snii_array_bkd
        WHERE array_contains(scores, 20) ORDER BY id
    """
    order_qt_array_contains_30 """
        SELECT id FROM test_storage_format_snii_array_bkd
        WHERE array_contains(scores, 30) ORDER BY id
    """
    // Absent from every row.
    order_qt_array_contains_absent """
        SELECT id FROM test_storage_format_snii_array_bkd
        WHERE array_contains(scores, 999) ORDER BY id
    """
    // An empty array is NOT null; only row 4 is.
    order_qt_array_is_null """
        SELECT id FROM test_storage_format_snii_array_bkd
        WHERE scores IS NULL ORDER BY id
    """
    // The index must not change the answer.
    sql "SET enable_inverted_index_query = false"
    order_qt_array_noindex_contains_20 """
        SELECT id FROM test_storage_format_snii_array_bkd
        WHERE array_contains(scores, 20) ORDER BY id
    """
    order_qt_array_noindex_is_null """
        SELECT id FROM test_storage_format_snii_array_bkd
        WHERE scores IS NULL ORDER BY id
    """
    sql "SET enable_inverted_index_query = true"

    // An ANN index coexisting with text and BKD indexes in ONE SNII container.
    //
    // The container format reserved LogicalIndexKind::kAnn from the start and its
    // blob logical index is a table of named opaque sub-files -- exactly what
    // faiss emits -- so ANN needs no format change, only the adapter on both ends
    // (IndexFileWriter::open + begin_close on the way in, DorisCompoundReader over
    // the blob's absolute container offsets on the way out).
    sql "DROP TABLE IF EXISTS test_storage_format_snii_ann"
    sql """
        CREATE TABLE test_storage_format_snii_ann (
          id INT NULL,
          note TEXT NULL,
          score INT NULL,
          embedding ARRAY<FLOAT> NOT NULL,
          INDEX idx_ann_note (`note`) USING INVERTED PROPERTIES("parser" = "english") COMMENT '',
          INDEX idx_ann_score (`score`) USING INVERTED COMMENT '',
          INDEX idx_ann (`embedding`) USING ANN PROPERTIES(
            "index_type" = "hnsw",
            "metric_type" = "l2_distance",
            "dim" = "4"
          )
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
          "replication_allocation" = "tag.location.default: 1",
          "inverted_index_storage_format" = "SNII"
        );
    """
    sql """
        INSERT INTO test_storage_format_snii_ann VALUES
          (1, 'alpha one',   10, [1.0, 1.0, 1.0, 1.0]),
          (2, 'beta two',    20, [2.0, 2.0, 2.0, 2.0]),
          (3, 'gamma three', 30, [3.0, 3.0, 3.0, 3.0]),
          (4, 'delta four',  40, [9.0, 9.0, 9.0, 9.0]),
          (5, 'alpha five',  50, [8.0, 8.0, 8.0, 8.0]);
    """
    sql "sync"

    // The nearest neighbour of a probe sitting on row 1's vector is row 1, and
    // the ordering out from it follows the grid the rows were laid on.
    order_qt_ann_topn_near_one """
        SELECT id FROM test_storage_format_snii_ann
        ORDER BY l2_distance_approximate(embedding, [1.0, 1.0, 1.0, 1.0])
        LIMIT 3
    """
    // A probe at the far corner must pick the far rows instead, so the answer is
    // driven by the query vector rather than by row order.
    order_qt_ann_topn_near_nine """
        SELECT id FROM test_storage_format_snii_ann
        ORDER BY l2_distance_approximate(embedding, [9.0, 9.0, 9.0, 9.0])
        LIMIT 2
    """
    // The text and BKD indexes in the SAME container must still answer: sealing
    // an ANN blob alongside them must not disturb the metadata groups.
    order_qt_ann_container_text """
        SELECT id FROM test_storage_format_snii_ann
        WHERE note MATCH_ANY 'alpha' ORDER BY id
    """
    order_qt_ann_container_bkd """
        SELECT id FROM test_storage_format_snii_ann
        WHERE score >= 30 ORDER BY id
    """
}
