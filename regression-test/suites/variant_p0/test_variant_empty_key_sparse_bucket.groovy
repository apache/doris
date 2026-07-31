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

suite("test_variant_empty_key_sparse_bucket", "nonConcurrent") {
    sql "SET default_variant_enable_doc_mode = false"
    sql "SET use_v3_storage_format = false"
    sql "SET default_variant_enable_typed_paths_to_sparse = false"
    sql "SET default_variant_sparse_hash_shard_count = 3"
    sql "SET enable_rewrite_element_at_to_slot = true"

    sql "SET default_variant_max_subcolumns_count = 0"
    sql "DROP TABLE IF EXISTS test_variant_empty_key_no_sparse"
    sql """
        CREATE TABLE test_variant_empty_key_no_sparse (
            k bigint,
            v variant
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "disable_auto_compaction" = "true");
    """

    sql """
        INSERT INTO test_variant_empty_key_no_sparse VALUES
        (1, '{"hot_a": 1, "": "BEFORE RENAME"}'),
        (2, '{"hot_a": 2, "": 42}')
    """
    sql "ALTER TABLE test_variant_empty_key_no_sparse RENAME COLUMN v Tags"
    sql """
        INSERT INTO test_variant_empty_key_no_sparse VALUES
        (3, '{"hot_a": 3, "": "AFTER RENAME"}')
    """

    trigger_and_wait_compaction("test_variant_empty_key_no_sparse", "cumulative")

    qt_empty_key_no_sparse_values """
        SELECT k, cast(Tags[''] as text)
        FROM test_variant_empty_key_no_sparse
        WHERE Tags[''] IS NOT NULL
        ORDER BY k
    """

    sql "SET default_variant_max_subcolumns_count = 3"
    sql "DROP TABLE IF EXISTS test_variant_empty_key_sparse_bucket"
    sql """
        CREATE TABLE test_variant_empty_key_sparse_bucket (
            k bigint,
            v variant
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "disable_auto_compaction" = "true");
    """

    sql """
        INSERT INTO test_variant_empty_key_sparse_bucket VALUES
        (1, '{"hot_a": 1, "hot_b": 10, "hot_c": 100, "cold_1": "a"}'),
        (2, '{"hot_a": 2, "hot_b": 20, "hot_c": 200, "cold_2": "b"}'),
        (3, '{"hot_a": 3, "hot_b": 30, "hot_c": 300, "cold_3": "c"}'),
        (4, '{"hot_a": 4, "hot_b": 40, "hot_c": 400, "cold_4": "d"}'),
        (5, '{"hot_a": 5, "hot_b": 50, "hot_c": 500, "cold_5": "e"}'),
        (6, '{"hot_a": 6, "hot_b": 60, "hot_c": 600, "cold_6": "f"}')
    """
    sql "ALTER TABLE test_variant_empty_key_sparse_bucket RENAME COLUMN v Tags"
    sql """
        INSERT INTO test_variant_empty_key_sparse_bucket VALUES
        (7, '{"hot_a": 7, "hot_b": 70, "hot_c": 700, "": "UPPER CASE"}'),
        (8, '{"hot_a": 8, "hot_b": 80, "hot_c": 800, "": 16}'),
        (9, '{"hot_a": 9, "hot_b": 90, "hot_c": 900, "": 8888888}')
    """

    trigger_and_wait_compaction("test_variant_empty_key_sparse_bucket", "cumulative")

    qt_empty_key_sparse_values """
        SELECT k, cast(Tags[''] as text)
        FROM test_variant_empty_key_sparse_bucket
        WHERE Tags[''] IS NOT NULL
        ORDER BY k
    """
}
