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

suite("dotted_key_escape_semantics", "p0") {
    sql "set default_variant_enable_doc_mode = false"
    sql "set describe_extend_variant_column = true"
    sql "drop table if exists dotted_key_escape_gate"
    sql "drop table if exists dotted_key_escape_values"
    sql """create table dotted_key_escape_values (
        id int, v variant<properties("variant_max_subcolumns_count"="0", "variant_enable_typed_paths_to_sparse"="false",
            "variant_max_sparse_column_statistics_size"="10000",
            "variant_sparse_hash_shard_count"="3", "variant_enable_nested_group"="false")>,
        index idx_v(v) using inverted properties("parser"="english")
    ) duplicate key(id) distributed by hash(id) buckets 1
    properties("variant_escape_dots_in_keys"="true", "replication_num"="1", "disable_auto_compaction"="true",
               "inverted_index_storage_format"="V2")"""
    // A single INSERT writes all collision shapes into one segment with a parent index.
    sql """insert into dotted_key_escape_values values
        (1, parse_to_variant('{"a.b":1}')),
        (2, parse_to_variant('{"a":5,"a.b":6}')),
        (3, parse_to_variant('{"a.b":1,"a":{"b":2},"a%2Eb":3}')),
        (4, parse_to_variant('{"a%2Eb":1,"x.y":2}')),
        (5, parse_to_variant('{"50%":1}')),
        (6, parse_to_variant('{"s.t":"hello","s":{"t":"nested"},"pct%":"percent"}'))"""
    def valueTablet = sql_return_maparray("show tablets from dotted_key_escape_values")[0].TabletId
    order_qt_one_segment """select sum(NUM_SEGMENTS),sum(ROWSET_NUM_ROWS)
        from information_schema.rowsets where TABLET_ID=${valueTablet}"""
    order_qt_values "select id,v from dotted_key_escape_values"
    order_qt_paths """select id,v['a'],v['a.b'],v['a']['b'],v.a.b,v['a%2Eb'],v['50%']
        from dotted_key_escape_values"""
    order_qt_in_memory """select /*+SET_VAR(disable_nereids_rules='VARIANT_SUB_PATH_PRUNING')*/
        id,element_at(v,'a.b'),element_at(v,'a%2Eb') from dotted_key_escape_values"""
    order_qt_storage_schema "desc dotted_key_escape_values"
    // Lazy fetch must translate logical subpaths at the rowid read entry as well.
    def lazyQuery = """select /*+SET_VAR(topn_lazy_materialization_threshold=1024,
        topn_lazy_materialization_using_index=false,enable_segment_limit_pushdown=false)*/ id,v,v['a.b'],v['a']['b'],v['a%2Eb']
        from dotted_key_escape_values where abs(id % 3) in (0,1) order by id desc limit 3"""
    explain {
        sql lazyQuery
        contains("MaterializeNode")
    }
    order_qt_lazy_fetch lazyQuery
    sql "set enable_condition_cache=false"
    sql "set enable_inverted_index_query=true"
    order_qt_index_on_dotted "select id from dotted_key_escape_values where cast(v['a.b'] as bigint)=1"
    order_qt_index_on_nested "select id from dotted_key_escape_values where cast(v['a']['b'] as bigint)=2"
    order_qt_index_on_percent "select id from dotted_key_escape_values where cast(v['a%2Eb'] as bigint)=1"
    sql "set enable_inverted_index_query=false"
    order_qt_index_off_dotted "select id from dotted_key_escape_values where cast(v['a.b'] as bigint)=1"
    order_qt_index_off_nested "select id from dotted_key_escape_values where cast(v['a']['b'] as bigint)=2"
    order_qt_index_off_percent "select id from dotted_key_escape_values where cast(v['a%2Eb'] as bigint)=1"
    sql "set enable_inverted_index_query=true"
    order_qt_search_dotted "select id from dotted_key_escape_values where search('v.s%2Et:hello')"
    order_qt_search_nested "select id from dotted_key_escape_values where search('v.s.t:nested')"
    order_qt_search_percent "select id from dotted_key_escape_values where search('v.pct%25:percent')"

    // LIKE and SHOW CREATE carry the table policy; final NestedGroup must remain disabled.
    def oldNestedGroup = sql("show variables like 'default_variant_enable_nested_group'")[0][1]
    try {
        sql "set default_variant_enable_nested_group=false"
        sql "drop table if exists dotted_key_escape_like"
        sql "drop table if exists dotted_key_escape_show_create"
        sql "create table dotted_key_escape_like like dotted_key_escape_values"
        def sourceDdl = sql("show create table dotted_key_escape_values")[0][1]
        sql sourceDdl.replace('CREATE TABLE `dotted_key_escape_values`',
                'CREATE TABLE `dotted_key_escape_show_create`')
        ["dotted_key_escape_like", "dotted_key_escape_show_create"].each { name ->
            def copiedDdl = sql("show create table ${name}")[0][1]
            assertTrue(copiedDdl.contains('"variant_escape_dots_in_keys" = "true"'))
            sql """insert into ${name} values
                (1,parse_to_variant('{"a.b":1,"a":{"b":2},"a%2Eb":3}'))"""
        }
        order_qt_like_roundtrip """select id,v,v['a.b'],v['a']['b'],v['a%2Eb']
            from dotted_key_escape_like"""
        order_qt_show_create_roundtrip """select id,v,v['a.b'],v['a']['b'],v['a%2Eb']
            from dotted_key_escape_show_create"""
    } finally {
        sql "set default_variant_enable_nested_group=${oldNestedGroup}"
    }

    // Phase 1 forbids both directions; the remaining properties stay identical.
    test {
        sql """alter table dotted_key_escape_values modify column v variant<properties("variant_escape_dots_in_keys"="false",
            "variant_max_subcolumns_count"="0", "variant_enable_typed_paths_to_sparse"="false",
            "variant_max_sparse_column_statistics_size"="10000",
            "variant_sparse_hash_shard_count"="3", "variant_enable_nested_group"="false")>"""
        exception "variant_escape_dots_in_keys is a table property"
    }
    sql "drop table if exists dotted_key_escape_legacy"
    sql """create table dotted_key_escape_legacy(id int,v variant<properties("variant_max_subcolumns_count"="0", "variant_enable_typed_paths_to_sparse"="false",
            "variant_max_sparse_column_statistics_size"="10000",
            "variant_sparse_hash_shard_count"="3", "variant_enable_nested_group"="false")>)
        duplicate key(id) distributed by hash(id) buckets 1 properties("replication_num"="1")"""
    test {
        sql """alter table dotted_key_escape_legacy modify column v variant<properties("variant_escape_dots_in_keys"="true",
            "variant_max_subcolumns_count"="0", "variant_enable_typed_paths_to_sparse"="false",
            "variant_max_sparse_column_statistics_size"="10000",
            "variant_sparse_hash_shard_count"="3", "variant_enable_nested_group"="false")>"""
        exception "variant_escape_dots_in_keys is a table property"
    }
    // Only a non-conflicting row is copied: a legacy sink intentionally flattens the key.
    sql "insert into dotted_key_escape_legacy select id,v from dotted_key_escape_values where id=1"
    order_qt_legacy_sink "select id,v from dotted_key_escape_legacy"

    test {
        sql """create table dotted_key_escape_gate(id int,
            v variant<properties("variant_enable_nested_group"="true")>)
            duplicate key(id) distributed by hash(id) buckets 1 properties("variant_escape_dots_in_keys"="true", "replication_num"="1")"""
        exception "Escaped tables do not support NestedGroup"
    }
    sql "drop table if exists dotted_key_escape_template"
    sql """create table dotted_key_escape_template(id int,
        v variant<MATCH_NAME 'a%2Eb':string, MATCH_NAME 'a.b':string, properties("variant_max_subcolumns_count"="0", "variant_enable_typed_paths_to_sparse"="false",
            "variant_max_sparse_column_statistics_size"="10000",
            "variant_sparse_hash_shard_count"="3", "variant_enable_nested_group"="false")>,
        index idx_dotted(v) using inverted properties("field_pattern"="a%2Eb","parser"="english"),
        index idx_nested(v) using inverted properties("field_pattern"="a.b","parser"="english")
    ) duplicate key(id) distributed by hash(id) buckets 1
    properties("variant_escape_dots_in_keys"="true", "replication_num"="1", "inverted_index_storage_format"="V2")"""
    sql """insert into dotted_key_escape_template values
        (1,parse_to_variant('{"a.b":"dotted","a":{"b":"nested"}}'))"""
    order_qt_template "select id,v,v['a.b'],v['a']['b'] from dotted_key_escape_template"
    order_qt_template_search "select id from dotted_key_escape_template where search('v.a%2Eb:dotted')"
    order_qt_template_schema "desc dotted_key_escape_template"
    test {
        sql """create index idx_bad on dotted_key_escape_template(v) using inverted
            properties("field_pattern"="a%2e")"""
        exception "Can not create index with field pattern"
    }
    test {
        sql "select id from dotted_key_escape_values where search('v.bad%20:value')"
        exception "Invalid escaped Variant path component"
    }
    test {
        sql """create table dotted_key_escape_gate(id int,
            v variant<MATCH_NAME 'bad%':int,properties("variant_max_subcolumns_count"="0", "variant_enable_typed_paths_to_sparse"="false",
            "variant_max_sparse_column_statistics_size"="10000",
            "variant_sparse_hash_shard_count"="3", "variant_enable_nested_group"="false")>) duplicate key(id)
            distributed by hash(id) buckets 1 properties("variant_escape_dots_in_keys"="true", "replication_num"="1")"""
        exception "Invalid escaped Variant path component"
    }
    // A new escaped column is legal when old segments have no physical root for it.
    sql """alter table dotted_key_escape_legacy add column e variant<properties("variant_max_subcolumns_count"="0", "variant_enable_typed_paths_to_sparse"="false",
            "variant_max_sparse_column_statistics_size"="10000",
            "variant_sparse_hash_shard_count"="3", "variant_enable_nested_group"="false")>"""
    order_qt_absent_root "select id,e from dotted_key_escape_legacy"
}
