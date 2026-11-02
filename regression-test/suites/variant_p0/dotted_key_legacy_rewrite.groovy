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

suite("dotted_key_legacy_rewrite", "p0,nonConcurrent") {
    // Master rejects doc_mode=false together with explicit subcolumn properties.
    // Pin this one default here; each column still fixes all sparse layout options.
    sql "set default_variant_enable_doc_mode = false"
    sql "drop table if exists dotted_key_legacy_source"
    sql """create table dotted_key_legacy_source (
        id int, v variant<properties(
                "variant_max_subcolumns_count"="0",
                "variant_enable_typed_paths_to_sparse"="false",
                "variant_max_sparse_column_statistics_size"="10000",
                "variant_sparse_hash_shard_count"="1",
                "variant_enable_nested_group"="false")>, padding int default '0'
    ) duplicate key(id) distributed by hash(id) buckets 1
    properties("replication_num"="1", "disable_auto_compaction"="true",
               "light_schema_change"="true")"""
    sql """insert into dotted_key_legacy_source(id,v) values
        (1, parse_to_variant('{"a":5,"a.b":6}'))"""
    sql """insert into dotted_key_legacy_source(id,v) values
        (2, parse_to_variant('{"a":5,"a.b":6}'))"""
    sql "drop table if exists dotted_key_legacy_copy"
    sql """create table dotted_key_legacy_copy (
        id int, v variant<properties(
                "variant_max_subcolumns_count"="0",
                "variant_enable_typed_paths_to_sparse"="false",
                "variant_max_sparse_column_statistics_size"="10000",
                "variant_sparse_hash_shard_count"="1",
                "variant_enable_nested_group"="false")>, padding int default '0'
    ) duplicate key(id) distributed by hash(id) buckets 1
    properties("replication_num"="1", "disable_auto_compaction"="true",
               "light_schema_change"="true")"""
    sql "drop table if exists dotted_key_legacy_heavy"
    sql """create table dotted_key_legacy_heavy (
        id int, v variant<properties(
                "variant_max_subcolumns_count"="0",
                "variant_enable_typed_paths_to_sparse"="false",
                "variant_max_sparse_column_statistics_size"="10000",
                "variant_sparse_hash_shard_count"="1",
                "variant_enable_nested_group"="false")>, padding int default '0'
    ) duplicate key(id) distributed by hash(id) buckets 1
    properties("replication_num"="1", "disable_auto_compaction"="true",
               "light_schema_change"="true")"""
    sql """insert into dotted_key_legacy_heavy(id,v) values
        (1, parse_to_variant('{"a":5,"a.b":6}'))"""
    sql """insert into dotted_key_legacy_heavy(id,v) values
        (2, parse_to_variant('{"a":5,"a.b":6}'))"""
    sql "drop table if exists dotted_key_legacy_vertical"
    sql """create table dotted_key_legacy_vertical (
        id int, v variant<properties(
                "variant_max_subcolumns_count"="0",
                "variant_enable_typed_paths_to_sparse"="false",
                "variant_max_sparse_column_statistics_size"="10000",
                "variant_sparse_hash_shard_count"="1",
                "variant_enable_nested_group"="false")>, padding int default '0'
    ) duplicate key(id) distributed by hash(id) buckets 1
    properties("replication_num"="1", "disable_auto_compaction"="true",
               "light_schema_change"="true")"""
    sql """insert into dotted_key_legacy_vertical(id,v) values
        (1, parse_to_variant('{"a":5,"a.b":6}'))"""
    sql """insert into dotted_key_legacy_vertical(id,v) values
        (1, parse_to_variant('{"a":5,"a.b":6}'))"""
    sql "drop table if exists dotted_key_legacy_whole"
    sql """create table dotted_key_legacy_whole (
        id int, v variant<properties(
                "variant_max_subcolumns_count"="0",
                "variant_enable_typed_paths_to_sparse"="false",
                "variant_max_sparse_column_statistics_size"="10000",
                "variant_sparse_hash_shard_count"="1",
                "variant_enable_nested_group"="false")>, padding int default '0'
    ) duplicate key(id) distributed by hash(id) buckets 1
    properties("replication_num"="1", "disable_auto_compaction"="true",
               "light_schema_change"="true")"""
    sql """insert into dotted_key_legacy_whole(id,v) values
        (1, parse_to_variant('{"a":5,"a.b":6}'))"""
    sql """insert into dotted_key_legacy_whole(id,v) values
        (1, parse_to_variant('{"a":5,"a.b":6}'))"""

    order_qt_source_whole "select id, v from dotted_key_legacy_source"

    sql "set describe_extend_variant_column = true"
    order_qt_source_schema "desc dotted_key_legacy_source"
    order_qt_heavy_before_schema "desc dotted_key_legacy_heavy"
    order_qt_vertical_before_schema "desc dotted_key_legacy_vertical"
    order_qt_whole_before_schema "desc dotted_key_legacy_whole"

    // C: typed pushdown and extended DESC independently probe the ancestor.
    // Hierarchical reads may mask its scalar even when DESC still lists it.
    order_qt_source """select id, cast(v['a'] as int), v['a.b']
        from dotted_key_legacy_source"""
    order_qt_heavy_before """select id, cast(v['a'] as int), v['a.b']
        from dotted_key_legacy_heavy"""
    order_qt_vertical_before """select id, cast(v['a'] as int), v['a.b']
        from dotted_key_legacy_vertical"""
    order_qt_whole_before """select id, cast(v['a'] as int), v['a.b']
        from dotted_key_legacy_whole"""

    // INSERT SELECT consumes the assembled whole value and permanently drops a.
    // Extended DESC loses v.a, while v.a.b remains.
    sql "insert into dotted_key_legacy_copy select * from dotted_key_legacy_source"
    order_qt_insert_select_schema "desc dotted_key_legacy_copy"
    order_qt_insert_select """select id, cast(v['a'] as int), v['a.b']
        from dotted_key_legacy_copy"""

    // Changing a value column INT to BIGINT requires a heavy COLUMN job even on
    // a light-schema-change-enabled table (Variant requires that table setting).
    // The rewrite permanently drops v.a. Log job metadata without ids/times in .out.
    sql "alter table dotted_key_legacy_heavy modify column padding bigint default '0'"
    waitForSchemaChangeDone {
        sql """show alter table column where TableName='dotted_key_legacy_heavy'
            order by CreateTime desc limit 1"""
        time 600
    }
    def jobs = sql_return_maparray """show alter table column
        where TableName='dotted_key_legacy_heavy' order by CreateTime desc limit 1"""
    logger.info("Heavy schema change evidence: ${jobs}")
    assertEquals("FINISHED", jobs[0].State)
    order_qt_heavy_after_schema "desc dotted_key_legacy_heavy"
    order_qt_heavy_after """select id, cast(v['a'] as int), v['a.b']
        from dotted_key_legacy_heavy"""

    // Overlapping duplicate keys in the two rowsets prevent ordered linking.
    // The master default is true. Pin it temporarily so unrelated BE settings do
    // not change this fixture: vertical per-path compaction preserves both leaves.
    setBeConfigTemporary([enable_vertical_compaction: true,
                          enable_vertical_compact_variant_subcolumns: true]) {
        trigger_and_wait_compaction("dotted_key_legacy_vertical", "full")
    }
    order_qt_vertical_after_schema "desc dotted_key_legacy_vertical"
    order_qt_vertical_after """select id, cast(v['a'] as int), v['a.b']
        from dotted_key_legacy_vertical"""

    // Disabling per-path compaction materializes the assembled value and permanently
    // drops v.a. DESC detects the loss even though root JSON and casts look unchanged.
    setBeConfigTemporary([enable_vertical_compaction: true,
                          enable_vertical_compact_variant_subcolumns: false]) {
        trigger_and_wait_compaction("dotted_key_legacy_whole", "full")
    }
    order_qt_whole_after_schema "desc dotted_key_legacy_whole"
    order_qt_whole_after """select id, cast(v['a'] as int), v['a.b']
        from dotted_key_legacy_whole"""
}
