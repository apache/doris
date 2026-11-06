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

suite("dotted_key_escape_doc_threshold", "p0,nonConcurrent") {
    sql "set describe_extend_variant_column=true"
    sql "drop table if exists dotted_key_escape_doc_threshold"
    sql """create table dotted_key_escape_doc_threshold(id int, v variant<properties(
        "variant_enable_nested_group"="false",
        "variant_enable_doc_mode"="true", "variant_doc_materialization_min_rows"="4",
        "variant_doc_hash_shard_count"="3")>)
        duplicate key(id) distributed by hash(id) buckets 1
        properties("variant_escape_dots_in_keys"="true", "replication_num"="1", "disable_auto_compaction"="true")"""
    sql """insert into dotted_key_escape_doc_threshold values
        (1,parse_to_variant('{"a.b":101,"a":{"b":201},"a%2Eb":301,"50%":401}')),
        (3,parse_to_variant('{"a.b":103,"a":{"b":203},"a%2Eb":303}')),
        (5,parse_to_variant('{"a.b":105,"a":{"b":205},"a%2Eb":305}'))"""
    sql """insert into dotted_key_escape_doc_threshold values
        (2,parse_to_variant('{"a.b":102,"a":{"b":202},"a%2Eb":302}')),
        (4,parse_to_variant('{"a.b":104,"a":{"b":204},"a%2Eb":304}')),
        (6,parse_to_variant('{"a.b":106,"a":{"b":206},"a%2Eb":306}'))"""
    order_qt_doc_only_schema "desc dotted_key_escape_doc_threshold"
    order_qt_doc_only "select id,v,v['a.b'],v['a']['b'],v['a%2Eb'] from dotted_key_escape_doc_threshold"
    setBeConfigTemporary([enable_vertical_compaction:true,enable_vertical_compact_variant_subcolumns:true]) {
        trigger_and_wait_compaction("dotted_key_escape_doc_threshold", "full")
        order_qt_hybrid_schema "desc dotted_key_escape_doc_threshold"
        order_qt_hybrid "select id,v,v['a.b'],v['a']['b'],v['a%2Eb'] from dotted_key_escape_doc_threshold"
        sql """insert into dotted_key_escape_doc_threshold values
            (0,parse_to_variant('{"a.b":100,"a":{"b":200},"a%2Eb":300}'))"""
        trigger_and_wait_compaction("dotted_key_escape_doc_threshold", "full")
        order_qt_mixed_schema "desc dotted_key_escape_doc_threshold"
        order_qt_mixed "select id,v,v['a.b'],v['a']['b'],v['a%2Eb'] from dotted_key_escape_doc_threshold"
    }
}
