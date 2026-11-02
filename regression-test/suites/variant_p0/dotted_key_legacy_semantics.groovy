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

suite("dotted_key_legacy_semantics", "p0") {
    // Master rejects doc_mode=false together with explicit subcolumn properties.
    // Pin this one default here; each column still fixes all sparse layout options.
    sql "set default_variant_enable_doc_mode = false"
    sql "drop table if exists dotted_key_legacy_read"
    sql """
        create table dotted_key_legacy_read (
            id int, v variant<properties(
                "variant_max_subcolumns_count"="0",
                "variant_enable_typed_paths_to_sparse"="false",
                "variant_max_sparse_column_statistics_size"="10000",
                "variant_sparse_hash_shard_count"="1",
                "variant_enable_nested_group"="false")>,
            sparse_v variant<properties(
                "variant_max_subcolumns_count"="1",
                "variant_enable_typed_paths_to_sparse"="false",
                "variant_max_sparse_column_statistics_size"="10000",
                "variant_sparse_hash_shard_count"="1",
                "variant_enable_nested_group"="false")>
        ) duplicate key(id) distributed by hash(id) buckets 1
        properties("replication_num"="1", "disable_auto_compaction"="true")
    """
    sql """insert into dotted_key_legacy_read values
        (1, parse_to_variant('{"a.b":1}'), parse_to_variant('{"a.b":1,"hot":9}')),
        (2, parse_to_variant('{"a":5,"a.b":6}'), parse_to_variant('{"a":5,"a.b":6,"hot":9}'))
    """

    // A: legacy storage splits a literal dotted key. Constant subpath pruning can
    // read a.b; disabling only VARIANT_SUB_PATH_PRUNING evaluates the constant-key
    // element_at on the assembled object. Variant element_at rejects non-constant keys.
    logger.info("Pruned dotted-key plan: ${sql("explain select v['a.b'] from dotted_key_legacy_read where id=1")}")
    logger.info("In-memory dotted-key plan: ${sql("explain select /*+SET_VAR(disable_nereids_rules='VARIANT_SUB_PATH_PRUNING')*/ element_at(v, 'a.b') from dotted_key_legacy_read where id=1")}")
    order_qt_dotted_read """select id, v['a.b'], v['a']['b'] from dotted_key_legacy_read where id=1"""
    order_qt_dotted_in_memory """select /*+SET_VAR(disable_nereids_rules='VARIANT_SUB_PATH_PRUNING')*/
        id, v, element_at(v, 'a.b') from dotted_key_legacy_read where id=1"""
    order_qt_dotted_sparse """select id, sparse_v['a.b'] from dotted_key_legacy_read where id=1"""

    order_qt_dotted_sparse_in_memory """select /*+SET_VAR(disable_nereids_rules='VARIANT_SUB_PATH_PRUNING')*/
        id, sparse_v, element_at(sparse_v, 'a.b') from dotted_key_legacy_read where id=1"""

    sql "set describe_extend_variant_column = true"
    order_qt_physical_schema "desc dotted_key_legacy_read"

    // C: whole-object assembly prefers the descendant. A typed subpath read probes
    // the ancestor but returns NULL for the object. Extended DESC still lists v.a.
    logger.info("Ancestor cast plan: ${sql("explain select cast(v['a'] as int) from dotted_key_legacy_read where id=2")}")
    order_qt_ancestor_read """select id, cast(v['a'] as int), v['a.b'] from dotted_key_legacy_read where id=2"""
    order_qt_ancestor_in_memory """select /*+SET_VAR(disable_nereids_rules='VARIANT_SUB_PATH_PRUNING')*/
        id, v, element_at(v, 'a') from dotted_key_legacy_read where id=2"""
    order_qt_ancestor_sparse """select id, cast(sparse_v['a'] as int), sparse_v['a.b']
        from dotted_key_legacy_read where id=2"""

    sql "drop table if exists dotted_key_legacy_index"
    sql """create table dotted_key_legacy_index (
        id int, v variant<properties(
                "variant_max_subcolumns_count"="0",
                "variant_enable_typed_paths_to_sparse"="false",
                "variant_max_sparse_column_statistics_size"="10000",
                "variant_sparse_hash_shard_count"="1",
                "variant_enable_nested_group"="false")>,
        index idx_v(v) using inverted
    ) duplicate key(id) distributed by hash(id) buckets 1
    properties("replication_num"="1", "disable_auto_compaction"="true",
               "inverted_index_storage_format"="V2")"""

    // D: legacy index suffix escaping maps both paths to a%2Eb. Publishing both
    // physical subcolumns in one segment opens the same index directory twice.
    test {
        sql """insert into dotted_key_legacy_index values
            (0, parse_to_variant('{"a":{"b":11},"a%2Eb":22}'))"""
        exception "IndexFileWriter::open attempted to insert a duplicate dir"
    }
    // Separate segments avoid a write-time collision. Check both predicates with
    // and without inverted-index evaluation, including the failed row's absence.
    // Value 11 appears under different paths in different segments, exposing aliasing.
    sql """insert into dotted_key_legacy_index values
        (1, parse_to_variant('{"a":{"b":11}}')),
        (3, parse_to_variant('{"a":{"b":33}}'))"""
    sql """insert into dotted_key_legacy_index values
        (2, parse_to_variant('{"a%2Eb":22}')),
        (4, parse_to_variant('{"a%2Eb":11}'))"""
    sql "set enable_condition_cache = false"
    sql "set enable_inverted_index_query = true"
    order_qt_index_on_dotted "select /* dotted_key_legacy_index_on_dotted */ id from dotted_key_legacy_index where cast(v['a.b'] as bigint)=11"
    order_qt_index_on_percent "select /* dotted_key_legacy_index_on_percent */ id from dotted_key_legacy_index where cast(v['a%2Eb'] as bigint)=11"
    sql "set enable_inverted_index_query = false"
    order_qt_index_off_dotted "select /* dotted_key_legacy_index_off_dotted */ id from dotted_key_legacy_index where cast(v['a.b'] as bigint)=11"
    order_qt_index_off_percent "select /* dotted_key_legacy_index_off_percent */ id from dotted_key_legacy_index where cast(v['a%2Eb'] as bigint)=11"
    order_qt_index_rows "select id, v from dotted_key_legacy_index"
}
