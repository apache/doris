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

suite("test_variant_light_properties", "p0") {
    sql "SET default_variant_enable_doc_mode = false"
    sql "DROP TABLE IF EXISTS test_variant_light_properties"
    sql """CREATE TABLE test_variant_light_properties (
        id INT,
        v VARIANT<'a': STRING, PROPERTIES("variant_max_subcolumns_count"="1")>,
        INDEX idx_v(v) USING INVERTED
    ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES("replication_num"="1", "disable_auto_compaction"="true")"""
    sql """INSERT INTO test_variant_light_properties VALUES
        (1, parse_to_variant('{"a":"001","b":"002","keep":7}')), (2, parse_to_variant('{"a":"003","keep":8}'))"""
    sql """INSERT INTO test_variant_light_properties VALUES
        (3, parse_to_variant('{"b":"004","keep":9}')), (4, NULL)"""
    order_qt_before "SELECT id, v FROM test_variant_light_properties"

    sql """ALTER TABLE test_variant_light_properties MODIFY COLUMN v
        VARIANT<'a': INT, 'b': INT, PROPERTIES("variant_max_subcolumns_count"="0")>"""
    // No new load: compaction must obtain the altered policy independently of input rowsets.
    order_qt_after_alter "SELECT id, v FROM test_variant_light_properties"
    trigger_and_wait_compaction("test_variant_light_properties", "full")
    order_qt_after_compaction "SELECT id, v FROM test_variant_light_properties"
    order_qt_paths """SELECT id, v['a'], v['b'], v['keep']
        FROM test_variant_light_properties"""
    order_qt_filtered """SELECT id FROM test_variant_light_properties
        WHERE CAST(v['a'] AS INT) = 1 OR CAST(v['b'] AS INT) = 4"""

    sql """INSERT INTO test_variant_light_properties VALUES
        (5, parse_to_variant('{"a":"005","b":"006","keep":10}'))"""
    order_qt_new_write "SELECT id, v FROM test_variant_light_properties"
    sql """ALTER TABLE test_variant_light_properties MODIFY COLUMN v
        VARIANT<PROPERTIES("variant_max_subcolumns_count"="1")>"""
    trigger_and_wait_compaction("test_variant_light_properties", "full")
    order_qt_removed_template "SELECT id, v FROM test_variant_light_properties"

    sql """ALTER TABLE test_variant_light_properties MODIFY COLUMN v
        VARIANT<'a': STRING, PROPERTIES("variant_max_subcolumns_count"="3")>"""
    sql """INSERT INTO test_variant_light_properties VALUES
        (6, parse_to_variant('{"a":6,"b":7,"keep":11}'))"""
    order_qt_mixed_versions "SELECT id, v FROM test_variant_light_properties"
    trigger_and_wait_compaction("test_variant_light_properties", "full")
    order_qt_final "SELECT id, v FROM test_variant_light_properties"

    test {
        sql """ALTER TABLE test_variant_light_properties MODIFY COLUMN v
            VARIANT<PROPERTIES("variant_max_sparse_column_statistics_size"="1")>"""
        exception "Can not change variant max sparse column statistics size"
    }

    sql "DROP TABLE IF EXISTS test_variant_light_properties_mow"
    sql """CREATE TABLE test_variant_light_properties_mow (
        id INT,
        v VARIANT<'a': STRING, PROPERTIES("variant_max_subcolumns_count"="1")>,
        INDEX idx_v(v) USING INVERTED
    ) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES("replication_num"="1", "disable_auto_compaction"="true",
        "enable_unique_key_merge_on_write"="true")"""
    sql """INSERT INTO test_variant_light_properties_mow VALUES
        (1, parse_to_variant('{"a":"001","keep":1}')), (2, parse_to_variant('{"a":"002","keep":2}'))"""
    sql """INSERT INTO test_variant_light_properties_mow VALUES
        (1, parse_to_variant('{"a":"003","keep":3}')), (3, parse_to_variant('{"a":"004","keep":4}'))"""
    order_qt_mow_before "SELECT id, v FROM test_variant_light_properties_mow"
    sql """ALTER TABLE test_variant_light_properties_mow MODIFY COLUMN v
        VARIANT<'a': INT, PROPERTIES("variant_max_subcolumns_count"="0")>"""
    trigger_and_wait_compaction("test_variant_light_properties_mow", "full")
    order_qt_mow_compacted "SELECT id, v FROM test_variant_light_properties_mow"
    order_qt_mow_filtered """SELECT id FROM test_variant_light_properties_mow
        WHERE CAST(v['a'] AS INT) = 1 OR CAST(v['a'] AS INT) = 3"""
    sql """INSERT INTO test_variant_light_properties_mow VALUES
        (2, parse_to_variant('{"a":"005","keep":5}'))"""
    trigger_and_wait_compaction("test_variant_light_properties_mow", "full")
    order_qt_mow_updated "SELECT id, v FROM test_variant_light_properties_mow"

    sql "DROP TABLE IF EXISTS test_variant_light_properties_rowstore"
    sql """CREATE TABLE test_variant_light_properties_rowstore (
        id INT,
        v VARIANT<PROPERTIES("variant_max_subcolumns_count"="1")>
    ) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES("replication_num"="1", "disable_auto_compaction"="true",
        "store_row_column"="true", "enable_unique_key_merge_on_write"="true")"""
    sql """INSERT INTO test_variant_light_properties_rowstore VALUES (1, parse_to_variant('{"a":"001"}'))"""
    sql """INSERT INTO test_variant_light_properties_rowstore VALUES (2, parse_to_variant('{"a":"002"}'))"""
    sql """ALTER TABLE test_variant_light_properties_rowstore MODIFY COLUMN v
        VARIANT<PROPERTIES("variant_max_subcolumns_count"="0")>"""
    trigger_and_wait_compaction("test_variant_light_properties_rowstore", "full")
    sql "SET enable_short_circuit_query = false"
    order_qt_rowstore_scan "SELECT id, v FROM test_variant_light_properties_rowstore WHERE id = 1"
    sql "SET enable_short_circuit_query = true"
    // Variant tables currently fall back to a normal scan even with this option enabled.
    order_qt_rowstore_short_circuit_enabled """SELECT id, v
        FROM test_variant_light_properties_rowstore WHERE id = 1"""
    test {
        sql """ALTER TABLE test_variant_light_properties_rowstore MODIFY COLUMN v VARIANT<'a': INT>"""
        exception "Can not change variant schema templates on a row-store table"
    }
}
