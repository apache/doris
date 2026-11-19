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

suite("test_variant_light_match_analyzer", "nonConcurrent") {
    // The test needs lightweight ADD and DROP with historical index files still present.
    if (isCloudMode()) {
        return
    }
    sql """SET default_variant_enable_doc_mode = false,
        default_variant_enable_nested_group = false,
        default_variant_enable_typed_paths_to_sparse = false,
        default_variant_max_subcolumns_count = 2048,
        default_variant_max_sparse_column_statistics_size = 10000,
        default_variant_sparse_hash_shard_count = 0,
        enable_fallback_on_missing_inverted_index = true"""

    def checkKeywordMatch = { String stage, String table ->
        [true, false].each { enabled ->
            sql "SET enable_inverted_index_query = ${enabled}"
            "order_qt_${stage}_token_${enabled}"("""
                SELECT id FROM ${table} WHERE v['msg'] MATCH_ANY 'hello' ORDER BY id
            """)
            "order_qt_${stage}_whole_${enabled}"("""
                SELECT id FROM ${table} WHERE v['msg'] MATCH_ANY 'hello world' ORDER BY id
            """)
        }
        sql "SET enable_inverted_index_query = true"
    }

    sql "DROP TABLE IF EXISTS variant_light_match_add"
    sql """CREATE TABLE variant_light_match_add (
        id INT, v VARIANT<MATCH_NAME 'msg':STRING>,
        INDEX idx_root(v) USING INVERTED PROPERTIES('parser'='english')
    ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES('replication_num'='1', 'disable_auto_compaction'='true', 'inverted_index_storage_format'='V2')"""
    sql """INSERT INTO variant_light_match_add VALUES
        (1, parse_to_variant('{"msg":"hello world"}')), (2, parse_to_variant('{"msg":"hello"}'))"""
    sql """ALTER TABLE variant_light_match_add ADD INDEX idx_msg(v) USING INVERTED
        PROPERTIES('field_pattern'='msg', 'parser'='none')"""
    sql """INSERT INTO variant_light_match_add VALUES
        (3, parse_to_variant('{"msg":"hello world"}')), (4, parse_to_variant('{"msg":"hello"}'))"""
    checkKeywordMatch("added", "variant_light_match_add")

    sql "DROP TABLE IF EXISTS variant_light_match_drop"
    sql """CREATE TABLE variant_light_match_drop (
        id INT, v VARIANT<MATCH_NAME 'msg':STRING>,
        INDEX idx_root(v) USING INVERTED PROPERTIES('parser'='none'),
        INDEX idx_msg(v) USING INVERTED PROPERTIES('field_pattern'='msg', 'parser'='english')
    ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES('replication_num'='1', 'disable_auto_compaction'='true', 'inverted_index_storage_format'='V2')"""
    sql """INSERT INTO variant_light_match_drop VALUES
        (1, parse_to_variant('{"msg":"hello world"}')), (2, parse_to_variant('{"msg":"hello"}'))"""
    sql "ALTER TABLE variant_light_match_drop DROP INDEX idx_msg"
    sql """INSERT INTO variant_light_match_drop VALUES
        (3, parse_to_variant('{"msg":"hello world"}')), (4, parse_to_variant('{"msg":"hello"}'))"""
    checkKeywordMatch("dropped", "variant_light_match_drop")
}
