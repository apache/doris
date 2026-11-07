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

suite("test_variant_tuning_properties") {
    sql """ set describe_extend_variant_column = true """

    // when extraction starts: a data file below variant_subcolumn_extraction_min_rows keeps its dynamic paths in the
    // sparse encoding, and compaction extracts them once it writes enough rows; doc mode is a separate switch
    sql "DROP TABLE IF EXISTS test_variant_deferred_subcolumns"
    sql """
        CREATE TABLE test_variant_deferred_subcolumns (
            k INT,
            payload VARIANT<PROPERTIES(
                "variant_max_subcolumns_count" = "128",
                "variant_subcolumn_extraction_min_rows" = "5",
                "variant_sparse_shard_count" = "1",
                "variant_enable_doc_mode" = "false"
            )>
        )
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true")
    """
    sql """INSERT INTO test_variant_deferred_subcolumns VALUES
        (1, parse_to_variant('{"user_id": 101, "action": "click", "campaign_a": 7}')),
        (2, parse_to_variant('{"user_id": 102, "action": "view", "os_name": "iOS"}')),
        (3, parse_to_variant('{"user_id": 103, "action": "click", "source_page": "home"}'))"""
    sql """INSERT INTO test_variant_deferred_subcolumns VALUES
        (4, parse_to_variant('{"user_id": 104, "action": "view", "tag_a": "new"}')),
        (5, parse_to_variant('{"user_id": 105, "action": "click"}')),
        (6, parse_to_variant('{"user_id": 106, "action": "view", "device_flag": true}'))"""
    qt_deferred_load "DESC test_variant_deferred_subcolumns"
    order_qt_deferred_load_read """SELECT k, payload, payload['user_id'], payload['action'], payload['os_name']
        FROM test_variant_deferred_subcolumns"""
    trigger_and_wait_compaction("test_variant_deferred_subcolumns", "full")
    qt_deferred_compacted "DESC test_variant_deferred_subcolumns"
    order_qt_deferred_compacted_read """SELECT k, payload, payload['user_id'], payload['action'], payload['os_name']
        FROM test_variant_deferred_subcolumns"""

    // doc mode keeps the whole document and still extracts up to the count, the most frequent paths first
    sql "DROP TABLE IF EXISTS test_variant_doc_mode_count"
    sql """
        CREATE TABLE test_variant_doc_mode_count (
            event_id BIGINT NOT NULL,
            payload VARIANT<PROPERTIES(
                "variant_max_subcolumns_count" = "2",
                "variant_subcolumn_extraction_min_rows" = "0",
                "variant_sparse_shard_count" = "1",
                "variant_enable_doc_mode" = "true"
            )>
        )
        DUPLICATE KEY(event_id) DISTRIBUTED BY HASH(event_id) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true")
    """
    sql """INSERT INTO test_variant_doc_mode_count VALUES
        (1, parse_to_variant('{"user_id": 101, "action": "click", "campaign_a": 7, "experiment_a": "A", "tag_a": "new"}')),
        (2, parse_to_variant('{"user_id": 102, "action": "view", "device_flag": true, "os_name": "iOS", "tag_b": "returning"}')),
        (3, parse_to_variant('{"user_id": 103, "action": "click", "experiment_b": "B", "source_page": "home", "device_model": "M1"}'))"""
    qt_doc_mode_load "DESC test_variant_doc_mode_count"
    order_qt_doc_mode_document "SELECT payload FROM test_variant_doc_mode_count WHERE CAST(payload['user_id'] AS BIGINT) = 101"
    order_qt_doc_mode_paths """SELECT event_id, payload['action'], payload['os_name'], payload['tag_a']
        FROM test_variant_doc_mode_count"""
    trigger_and_wait_compaction("test_variant_doc_mode_count", "full")
    qt_doc_mode_compacted "DESC test_variant_doc_mode_count"
    order_qt_doc_mode_compacted_read "SELECT event_id, payload FROM test_variant_doc_mode_count"

    // a count of 0 turns automatic extraction off, also when doc mode is stated off
    sql "DROP TABLE IF EXISTS test_variant_no_extraction"
    sql """
        CREATE TABLE test_variant_no_extraction (
            k INT,
            payload VARIANT<PROPERTIES(
                "variant_max_subcolumns_count" = "0",
                "variant_subcolumn_extraction_min_rows" = "0",
                "variant_sparse_shard_count" = "2",
                "variant_enable_doc_mode" = "false"
            )>
        )
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true")
    """
    sql """INSERT INTO test_variant_no_extraction VALUES
        (1, parse_to_variant('{"source": "collector-a", "metric_0001": 1}')),
        (2, parse_to_variant('{"source": "collector-b", "metric_0002": 2}'))"""
    qt_no_extraction "DESC test_variant_no_extraction"
    order_qt_no_extraction_read """SELECT k, payload['source'] FROM test_variant_no_extraction
        WHERE CAST(payload['source'] AS STRING) = 'collector-a'"""

    // the deprecated names still work and are printed as the properties that replaced them
    sql "DROP TABLE IF EXISTS test_variant_deprecated_names"
    sql """
        CREATE TABLE test_variant_deprecated_names (
            k INT,
            v VARIANT<PROPERTIES(
                "variant_max_subcolumns_count" = "2048",
                "variant_doc_materialization_min_rows" = "0",
                "variant_sparse_hash_shard_count" = "2",
                "variant_enable_doc_mode" = "false"
            )>,
            d VARIANT<PROPERTIES(
                "variant_max_subcolumns_count" = "2048",
                "variant_enable_doc_mode" = "true",
                "variant_doc_materialization_min_rows" = "10",
                "variant_doc_hash_shard_count" = "4"
            )>
        )
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    qt_deprecated_names "DESC test_variant_deprecated_names"

    test {
        sql """
            CREATE TABLE test_variant_conflicting_names (
                k INT,
                v VARIANT<PROPERTIES("variant_sparse_shard_count" = "2", "variant_sparse_hash_shard_count" = "2")>
            )
            DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        exception "variant_sparse_shard_count and variant_sparse_hash_shard_count are names of the same property"
    }
}
