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

suite("dotted_key_escape_rewrite", "p0,nonConcurrent") {
    sql "set default_variant_enable_doc_mode=false"
    sql "set describe_extend_variant_column=true"
    // The model and column-group width are the variables under test.
    ["dup", "mow"].each { model ->
        [1, 2].each { width ->
            String name = "dotted_key_escape_${model}_${width}"
            sql "drop table if exists ${name}"
            sql """create table ${name}(id int, v variant<properties("variant_max_subcolumns_count"="1", "variant_enable_typed_paths_to_sparse"="false",
            "variant_max_sparse_column_statistics_size"="10000",
            "variant_sparse_hash_shard_count"="3", "variant_enable_nested_group"="false")>, padding int default '0',
                index idx_v(v) using inverted)
                ${model == 'mow' ? 'unique' : 'duplicate'} key(id)
                distributed by hash(id) buckets 1 properties("variant_escape_dots_in_keys"="true", "replication_num"="1",
                "disable_auto_compaction"="true", "light_schema_change"="true",
                "inverted_index_storage_format"="V2"
                ${model == 'mow' ? ', "enable_unique_key_merge_on_write"="true"' : ''})"""
            sql """insert into ${name} values
                (1,parse_to_variant('{"a.b":1,"a":{"b":2},"a%2Eb":3,"50%":4}'),0),
                (2,parse_to_variant('{"a.b":10,"hot":99}'),0),
                (3,parse_to_variant('{"a.b":11,"hot":99}'),0)"""
            // Repeating the hot dotted path makes it materialized; the nested path is sparse.
            sql """insert into ${name} values
                (4,parse_to_variant('{"a.b":12,"hot":99}'),0)"""
            if (model == "mow") {
                def oldPartialUpdate = sql("show variables like 'enable_unique_key_partial_update'")[0][1]
                def oldInsertStrict = sql("show variables like 'enable_insert_strict'")[0][1]
                try {
                    sql "set enable_unique_key_partial_update=true"
                    sql "set enable_insert_strict=false"
                    // Reading the omitted v must preserve every key through a whole-value rebuild.
                    sql "insert into ${name}(id,padding) values (1,7)"
                } finally {
                    sql "set enable_unique_key_partial_update=${oldPartialUpdate}"
                    sql "set enable_insert_strict=${oldInsertStrict}"
                }
            }
            order_qt_before "select id,v,v['a.b'],v['a']['b'],v['a%2Eb'],v['50%'],padding from ${name}"
            order_qt_before_schema "desc ${name}"
            setBeConfigTemporary([enable_vertical_compaction:true,
                    enable_vertical_compact_variant_subcolumns:true,
                    vertical_compaction_num_columns_per_group:width]) {
                trigger_and_wait_compaction(name,"full")
            }
            order_qt_vertical "select id,v,v['a.b'],v['a']['b'],v['a%2Eb'],v['50%'],padding from ${name}"
            order_qt_vertical_schema "desc ${name}"
            sql "set enable_condition_cache=false"
            sql "set enable_inverted_index_query=true"
            order_qt_compacted_index_on "select id from ${name} where cast(v['a.b'] as bigint)=1"
            order_qt_compacted_nested_on "select id from ${name} where cast(v['a']['b'] as bigint)=2"
            sql "set enable_inverted_index_query=false"
            order_qt_compacted_index_off "select id from ${name} where cast(v['a.b'] as bigint)=1"
            order_qt_compacted_nested_off "select id from ${name} where cast(v['a']['b'] as bigint)=2"
            sql "set enable_inverted_index_query=true"
        }
    }
    sql "drop table if exists dotted_key_escape_source"
    sql """create table dotted_key_escape_source(id int,v variant<properties("variant_max_subcolumns_count"="0", "variant_enable_typed_paths_to_sparse"="false",
            "variant_max_sparse_column_statistics_size"="10000",
            "variant_sparse_hash_shard_count"="3", "variant_enable_nested_group"="false")>,padding int default '0')
        duplicate key(id) distributed by hash(id) buckets 1
        properties("variant_escape_dots_in_keys"="true", "replication_num"="1", "disable_auto_compaction"="true", "light_schema_change"="true")"""
    sql """insert into dotted_key_escape_source values
        (1,parse_to_variant('{"a.b":1}'),0),
        (2,parse_to_variant('{"a":5,"a.b":6}'),0),
        (3,parse_to_variant('{"a.b":1,"a":{"b":2}}'),0),
        (4,parse_to_variant('{"a%2Eb":1,"x.y":2}'),0),
        (5,parse_to_variant('{"50%":1}'),0)"""
    sql "drop table if exists dotted_key_escape_ctas"
    sql """create table dotted_key_escape_ctas distributed by hash(id) buckets 1
        properties("replication_num"="1") as select * from dotted_key_escape_source"""
    // CTAS must carry the physical column property with its inferred Variant type.
    order_qt_ctas_schema "desc dotted_key_escape_ctas"
    order_qt_ctas_values "select id,v from dotted_key_escape_ctas"
    sql "drop table if exists dotted_key_escape_copy"
    sql """create table dotted_key_escape_copy(id int,v variant<properties("variant_max_subcolumns_count"="0", "variant_enable_typed_paths_to_sparse"="false",
            "variant_max_sparse_column_statistics_size"="10000",
            "variant_sparse_hash_shard_count"="3", "variant_enable_nested_group"="false")>,padding int default '0')
        duplicate key(id) distributed by hash(id) buckets 1
        properties("variant_escape_dots_in_keys"="true", "replication_num"="1", "disable_auto_compaction"="true", "light_schema_change"="true")"""
    sql "drop table if exists dotted_key_escape_heavy"
    sql """create table dotted_key_escape_heavy(id int,v variant<properties("variant_max_subcolumns_count"="0", "variant_enable_typed_paths_to_sparse"="false",
            "variant_max_sparse_column_statistics_size"="10000",
            "variant_sparse_hash_shard_count"="3", "variant_enable_nested_group"="false")>,padding int default '0')
        duplicate key(id) distributed by hash(id) buckets 1
        properties("variant_escape_dots_in_keys"="true", "replication_num"="1", "disable_auto_compaction"="true", "light_schema_change"="true")"""
    sql """insert into dotted_key_escape_heavy values
        (1,parse_to_variant('{"a.b":1}'),0),
        (2,parse_to_variant('{"a":5,"a.b":6}'),0),
        (3,parse_to_variant('{"a.b":1,"a":{"b":2}}'),0),
        (4,parse_to_variant('{"a%2Eb":1,"x.y":2}'),0),
        (5,parse_to_variant('{"50%":1}'),0)"""
    sql "drop table if exists dotted_key_escape_whole"
    sql """create table dotted_key_escape_whole(id int,v variant<properties("variant_max_subcolumns_count"="0", "variant_enable_typed_paths_to_sparse"="false",
            "variant_max_sparse_column_statistics_size"="10000",
            "variant_sparse_hash_shard_count"="3", "variant_enable_nested_group"="false")>,padding int default '0')
        duplicate key(id) distributed by hash(id) buckets 1
        properties("variant_escape_dots_in_keys"="true", "replication_num"="1", "disable_auto_compaction"="true", "light_schema_change"="true")"""
    sql """insert into dotted_key_escape_whole values
        (1,parse_to_variant('{"a.b":1}'),0),
        (2,parse_to_variant('{"a":5,"a.b":6}'),0),
        (3,parse_to_variant('{"a.b":1,"a":{"b":2}}'),0),
        (4,parse_to_variant('{"a%2Eb":1,"x.y":2}'),0),
        (5,parse_to_variant('{"50%":1}'),0)"""
    sql """insert into dotted_key_escape_whole values (1,parse_to_variant('{"a.b":1}'),0)"""
    sql "drop table if exists dotted_key_escape_doc"
    sql """create table dotted_key_escape_doc(id int,v variant<properties("variant_enable_nested_group"="false",
            "variant_enable_doc_mode"="true", "variant_doc_materialization_min_rows"="100",
            "variant_doc_hash_shard_count"="3")>,padding int default '0')
        duplicate key(id) distributed by hash(id) buckets 1
        properties("variant_escape_dots_in_keys"="true", "replication_num"="1", "disable_auto_compaction"="true", "light_schema_change"="true")"""
    sql """insert into dotted_key_escape_doc values
        (1,parse_to_variant('{"a.b":1}'),0),
        (2,parse_to_variant('{"a":5,"a.b":6}'),0),
        (3,parse_to_variant('{"a.b":1,"a":{"b":2}}'),0),
        (4,parse_to_variant('{"a%2Eb":1,"x.y":2}'),0),
        (5,parse_to_variant('{"50%":1}'),0)"""
    sql """insert into dotted_key_escape_doc values (1,parse_to_variant('{"a.b":1}'),0)"""
    sql "insert into dotted_key_escape_copy select * from dotted_key_escape_source"
    order_qt_copy "select id,v from dotted_key_escape_copy"
    order_qt_copy_schema "desc dotted_key_escape_copy"
    order_qt_heavy_before "select id,v from dotted_key_escape_heavy"
    order_qt_heavy_before_schema "desc dotted_key_escape_heavy"
    sql "alter table dotted_key_escape_heavy modify column padding bigint default '0'"
    waitForSchemaChangeDone {
        sql """show alter table column where TableName='dotted_key_escape_heavy'
            order by CreateTime desc limit 1"""
        time 600
    }
    def heavyJobs = sql_return_maparray """show alter table column
        where TableName='dotted_key_escape_heavy' order by CreateTime desc limit 1"""
    logger.info("Escaped heavy schema change evidence: ${heavyJobs}")
    order_qt_heavy "select id,v from dotted_key_escape_heavy"
    order_qt_heavy_schema "desc dotted_key_escape_heavy"
    ["whole", "doc"].each { layout ->
        String name = "dotted_key_escape_${layout}"
        order_qt_rebuild_before "select id,v from ${name}"
        order_qt_rebuild_before_schema "desc ${name}"
        // Doc mode exercises its per-bucket snapshot rebuild iterator.
        setBeConfigTemporary([enable_vertical_compaction:layout == "doc",
                enable_vertical_compact_variant_subcolumns:layout == "doc"]) {
            trigger_and_wait_compaction(name,"full")
        }
        order_qt_rebuild_after "select id,v from ${name}"
        order_qt_rebuild_after_schema "desc ${name}"
    }
}
