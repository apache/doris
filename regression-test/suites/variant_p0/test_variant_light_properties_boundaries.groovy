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

suite("test_variant_light_properties_boundaries", "p0") {
    sql "SET default_variant_enable_doc_mode = false"
    sql "DROP TABLE IF EXISTS test_variant_policy_boundaries"
    sql """CREATE TABLE test_variant_policy_boundaries (id INT, v VARIANT,
        untouched VARIANT, INDEX idx_v(v) USING INVERTED)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1", "disable_auto_compaction"="true")"""
    sql """INSERT INTO test_variant_policy_boundaries VALUES
        (1, parse_to_variant('{"num_a":"001","arr":[1,null,3],"nested":{"keep":"x"}}'), parse_to_variant('{"s":"001"}')),
        (2, parse_to_variant('{"num_a":"bad","num_b":"2147483648","keep":true}'), parse_to_variant('{"s":"002"}')),
        (3, parse_to_variant('{"num_a":null,"keep":[]}'), NULL)"""
    sql """INSERT INTO test_variant_policy_boundaries VALUES
        (4, parse_to_variant('{"num_a":["1","2"],"keep":{"x":1}}'), parse_to_variant('{"s":"004"}')),
        (5, parse_to_variant('{"num_a":{"x":1},"num_b":"005"}'), parse_to_variant('{"s":"005"}')),
        (6, NULL, parse_to_variant('{"s":"006"}'))"""
    order_qt_before "SELECT id, v, untouched FROM test_variant_policy_boundaries"
    sql """ALTER TABLE test_variant_policy_boundaries MODIFY COLUMN v
        VARIANT<'num_*': INT, PROPERTIES("variant_max_subcolumns_count"="1")>"""
    order_qt_altered "SELECT id, v, untouched FROM test_variant_policy_boundaries"
    sql """INSERT INTO test_variant_policy_boundaries VALUES
        (7, parse_to_variant('{"num_a":"bad","num_b":"2147483648","keep":true}'), parse_to_variant('{"s":"007"}'))"""
    trigger_and_wait_compaction("test_variant_policy_boundaries", "full")
    order_qt_compacted "SELECT id, v, untouched FROM test_variant_policy_boundaries"
    order_qt_paths """SELECT id, v['num_a'], v['num_b'], v['arr'], v['nested'], v['keep']
        FROM test_variant_policy_boundaries"""
    order_qt_new_write "SELECT id, v, untouched FROM test_variant_policy_boundaries WHERE id IN (2,7)"
    sql "SET enable_inverted_index_query = false"
    order_qt_scan "SELECT id FROM test_variant_policy_boundaries WHERE CAST(v['num_b'] AS INT) = 5"
    sql "SET enable_inverted_index_query = true"
    order_qt_index "SELECT id FROM test_variant_policy_boundaries WHERE CAST(v['num_b'] AS INT) = 5"

    // Changing the match mode must stop converting num_a and num_b on new writes.
    sql """ALTER TABLE test_variant_policy_boundaries MODIFY COLUMN v
        VARIANT<MATCH_NAME 'num_*': INT, PROPERTIES("variant_max_subcolumns_count"="0")>"""
    sql """INSERT INTO test_variant_policy_boundaries VALUES
        (8, parse_to_variant('{"num_a":"008","num_b":"009"}'), parse_to_variant('{"s":"008"}'))"""
    trigger_and_wait_compaction("test_variant_policy_boundaries", "full")
    order_qt_exact "SELECT id, v, untouched FROM test_variant_policy_boundaries"

    sql "DROP TABLE IF EXISTS test_variant_policy_delete"
    sql """CREATE TABLE test_variant_policy_delete (id INT, v VARIANT)
        UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1", "enable_unique_key_merge_on_write"="true",
            "disable_auto_compaction"="true")"""
    sql """INSERT INTO test_variant_policy_delete VALUES
        (1, parse_to_variant('{"a":"001"}')), (2, parse_to_variant('{"a":"002"}'))"""
    sql "DELETE FROM test_variant_policy_delete WHERE id = 1"
    sql "ALTER TABLE test_variant_policy_delete MODIFY COLUMN v VARIANT<'a': INT>"
    sql """INSERT INTO test_variant_policy_delete VALUES (3, parse_to_variant('{"a":"004"}'))"""
    trigger_and_wait_compaction("test_variant_policy_delete", "full")
    order_qt_deleted "SELECT id, v FROM test_variant_policy_delete"
    sql """INSERT INTO test_variant_policy_delete VALUES (1, parse_to_variant('{"a":"003"}'))"""
    trigger_and_wait_compaction("test_variant_policy_delete", "full")
    order_qt_reinserted "SELECT id, v FROM test_variant_policy_delete"
    sql "SET default_variant_enable_doc_mode = true"
    sql "DROP TABLE IF EXISTS test_variant_policy_doc"
    sql """CREATE TABLE test_variant_policy_doc (id INT, v VARIANT)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1", "disable_auto_compaction"="true")"""
    sql """INSERT INTO test_variant_policy_doc VALUES
        (1, parse_to_variant('{"a":"001","keep":[1,null,3]}'))"""
    sql """INSERT INTO test_variant_policy_doc VALUES
        (2, parse_to_variant('{"a":"bad","keep":{"x":1}}'))"""
    sql "ALTER TABLE test_variant_policy_doc MODIFY COLUMN v VARIANT<'a': INT>"
    sql """INSERT INTO test_variant_policy_doc VALUES (4, parse_to_variant('{"a":"004"}'))"""
    trigger_and_wait_compaction("test_variant_policy_doc", "full")
    // Doc mode preserves source JSON and applies templates to the materialized copy.
    order_qt_doc_root "SELECT id, v FROM test_variant_policy_doc"
    order_qt_doc_paths "SELECT id, v['a'], v['keep'] FROM test_variant_policy_doc"
    sql """INSERT INTO test_variant_policy_doc VALUES
        (3, parse_to_variant('{"a":"003","keep":[4]}'))"""
    order_qt_doc_new_write "SELECT id, v['a'], v['keep'] FROM test_variant_policy_doc"
    sql "ALTER TABLE test_variant_policy_doc MODIFY COLUMN v VARIANT<'a': STRING>"
    sql """INSERT INTO test_variant_policy_doc VALUES (5, parse_to_variant('{"a":"005"}'))"""
    trigger_and_wait_compaction("test_variant_policy_doc", "full")
    order_qt_doc_changed_root "SELECT id, v FROM test_variant_policy_doc"
    order_qt_doc_changed_paths "SELECT id, v['a'], v['keep'] FROM test_variant_policy_doc"

}
