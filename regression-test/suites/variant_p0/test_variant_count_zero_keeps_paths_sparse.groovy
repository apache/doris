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

suite("test_variant_count_zero_keeps_paths_sparse") {
    sql " set default_variant_enable_doc_mode = false "
    sql """ set default_variant_enable_typed_paths_to_sparse = false """
    sql """ set default_variant_max_sparse_column_statistics_size = 10 """
    sql """ set default_variant_sparse_hash_shard_count = 0 """
    sql """ set describe_extend_variant_column = true """

    // a count of 0 keeps every dynamic path sparse, while template paths and a positive count give subcolumns
    sql "DROP TABLE IF EXISTS test_variant_count_zero"
    sql """
        CREATE TABLE test_variant_count_zero (
            k INT,
            zero VARIANT<PROPERTIES("variant_max_subcolumns_count" = "0")>,
            typed VARIANT<'t' : BIGINT, PROPERTIES("variant_max_subcolumns_count" = "0")>,
            counted VARIANT<PROPERTIES("variant_max_subcolumns_count" = "2048")>
        )
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true")
    """
    sql """INSERT INTO test_variant_count_zero VALUES
        (1, parse_to_variant('{"a": 1, "b": "x"}'), parse_to_variant('{"t": 1, "a": 1}'), parse_to_variant('{"a": 1}'))"""
    sql """INSERT INTO test_variant_count_zero VALUES
        (2, parse_to_variant('{"a": 2, "c": {"d": 2}}'), parse_to_variant('{"t": 2, "b": "y"}'), parse_to_variant('{"b": 2}'))"""
    qt_desc_load "DESC test_variant_count_zero"
    order_qt_read_load """SELECT k, zero, zero['a'], zero['c']['d'], typed['t'], typed['b'], counted['a']
        FROM test_variant_count_zero"""

    trigger_and_wait_compaction("test_variant_count_zero", "full")
    qt_desc_compacted "DESC test_variant_count_zero"
    order_qt_read_compacted """SELECT k, zero, zero['a'], zero['c']['d'], typed['t'], typed['b'], counted['a']
        FROM test_variant_count_zero"""

    // a column added to the table has the same meaning, and a table created from it too
    sql """ALTER TABLE test_variant_count_zero
        ADD COLUMN added VARIANT<PROPERTIES("variant_max_subcolumns_count" = "0")>"""
    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE TableName = 'test_variant_count_zero' ORDER BY CreateTime DESC LIMIT 1"""
        time 600
    }
    sql """INSERT INTO test_variant_count_zero VALUES
        (3, parse_to_variant('{"a": 3}'), parse_to_variant('{"t": 3}'), parse_to_variant('{"a": 3}'), parse_to_variant('{"e": 3}'))"""
    trigger_and_wait_compaction("test_variant_count_zero", "full")
    qt_desc_added "DESC test_variant_count_zero"
    order_qt_read_added "SELECT k, added, added['e'] FROM test_variant_count_zero"

    sql "DROP TABLE IF EXISTS test_variant_count_zero_ctas"
    sql """
        CREATE TABLE test_variant_count_zero_ctas
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1") AS SELECT * FROM test_variant_count_zero
    """
    qt_desc_ctas "DESC test_variant_count_zero_ctas"
    order_qt_read_ctas "SELECT k, zero['a'], typed['t'], added['e'] FROM test_variant_count_zero_ctas"

    // the serialize type behind this is internal
    test {
        sql """
            CREATE TABLE test_variant_serialize_type_rejected (
                k INT,
                v VARIANT<PROPERTIES("variant_serialize_type" = "v1")>
            )
            DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        exception "only support for"
    }
}
