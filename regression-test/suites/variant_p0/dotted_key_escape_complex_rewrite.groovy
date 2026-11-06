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

suite("dotted_key_escape_complex_rewrite", "p0,nonConcurrent") {
    sql "set default_variant_enable_doc_mode=false"
    sql "set describe_extend_variant_column=true"
    sql "drop table if exists dotted_key_escape_complex"
    // One shard deliberately puts all special paths in the same sorted sparse map.
    // The frequent 'hot' field occupies the sole materialized-column slot.
    sql """create table dotted_key_escape_complex(id int, v variant<properties(
        "variant_enable_nested_group"="false",
        "variant_max_subcolumns_count"="1", "variant_sparse_hash_shard_count"="1",
        "variant_max_sparse_column_statistics_size"="10000")>)
        duplicate key(id) distributed by hash(id) buckets 1
        properties("variant_escape_dots_in_keys"="true", "replication_num"="1", "disable_auto_compaction"="true")"""
    sql """insert into dotted_key_escape_complex values
        (1,parse_to_variant('{"hot":1,"a.b":{"c.d":11,"c%2Ed":12},"a":{"b":{"c.d":21,"c%2Ed":22}},"a%2Eb":{"c.d":51},"a%2e":61,"a%20":62,"a%":63}')),
        (3,parse_to_variant('{"hot":3}')),(5,parse_to_variant('{"hot":5}'))"""
    sql """insert into dotted_key_escape_complex values
        (2,parse_to_variant('{"hot":2,"a.b":[{"c.d":31,"c%2Ed":32},null,{}],"a":{"b":[41,42]}}')),
        (4,parse_to_variant('{"hot":4}')),(6,parse_to_variant('{"hot":6}'))"""
    order_qt_complex_before """select id,v,v['a.b'],v['a']['b'],v['a%2Eb'],
        v['a.b']['c.d'],v['a.b']['c%2Ed'],v['a']['b']['c.d'],
        v['a%2e'],v['a%20'],v['a%'] from dotted_key_escape_complex"""
    setBeConfigTemporary([enable_vertical_compaction:true,enable_vertical_compact_variant_subcolumns:true,
            vertical_compaction_num_columns_per_group:1]) {
        trigger_and_wait_compaction("dotted_key_escape_complex", "full")
    }
    order_qt_complex_after """select id,v,v['a.b'],v['a']['b'],v['a%2Eb'],
        v['a.b']['c.d'],v['a.b']['c%2Ed'],v['a']['b']['c.d'],
        v['a%2e'],v['a%20'],v['a%'] from dotted_key_escape_complex"""
    order_qt_complex_schema "desc dotted_key_escape_complex"
}
