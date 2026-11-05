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

suite("dotted_key_escape_added_column_rewrite", "p0,nonConcurrent") {
    sql "set default_variant_enable_doc_mode=false"
    sql "set describe_extend_variant_column=true"
    sql "drop table if exists dotted_key_escape_added_column"
    sql """create table dotted_key_escape_added_column(id int,
        v variant<properties("variant_enable_nested_group"="false", "variant_max_subcolumns_count"="2048",
            "variant_sparse_hash_shard_count"="1")>,padding int default '0')
        duplicate key(id) distributed by hash(id) buckets 1 properties("replication_num"="1",
        "disable_auto_compaction"="true", "light_schema_change"="true")"""
    sql """insert into dotted_key_escape_added_column values
        (1,parse_to_variant('{"a.b":101,"a%2Eb":301}'),0),
        (3,parse_to_variant('{"a":{"b":203},"a%2Eb":303}'),0)"""
    sql """alter table dotted_key_escape_added_column add column e variant<properties(
        "variant_escape_dots_in_keys"="true", "variant_enable_nested_group"="false",
        "variant_max_subcolumns_count"="0", "variant_sparse_hash_shard_count"="1")>"""
    sql """insert into dotted_key_escape_added_column(id,v,padding,e) values
        (2,parse_to_variant('{"a.b":102,"a%2Eb":302}'),0,
           parse_to_variant('{"a.b":112,"a":{"b":212},"a%2Eb":312}')),
        (4,parse_to_variant('{"a":{"b":204},"a%2Eb":304}'),0,
           parse_to_variant('{"a.b":114,"a":{"b":214},"a%2Eb":314}'))"""
    order_qt_added_before """select id,v,v['a.b'],v['a']['b'],v['a%2Eb'],e is null,e,
        e['a.b'],e['a']['b'],e['a%2Eb'] from dotted_key_escape_added_column"""
    setBeConfigTemporary([enable_vertical_compaction:true,enable_vertical_compact_variant_subcolumns:true,
            vertical_compaction_num_columns_per_group:1]) {
        trigger_and_wait_compaction("dotted_key_escape_added_column", "full")
    }
    order_qt_added_compacted """select id,v,v['a.b'],v['a']['b'],v['a%2Eb'],e is null,e,
        e['a.b'],e['a']['b'],e['a%2Eb'] from dotted_key_escape_added_column"""
    order_qt_added_schema "desc dotted_key_escape_added_column"
    sql "alter table dotted_key_escape_added_column modify column padding bigint default '0'"
    waitForSchemaChangeDone {
        sql """show alter table column where TableName='dotted_key_escape_added_column'
            order by CreateTime desc limit 1"""
        time 600
    }
    order_qt_added_heavy """select id,v,v['a.b'],v['a']['b'],v['a%2Eb'],e is null,e,
        e['a.b'],e['a']['b'],e['a%2Eb'] from dotted_key_escape_added_column"""
}
