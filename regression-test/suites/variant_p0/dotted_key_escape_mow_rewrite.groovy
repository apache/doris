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

suite("dotted_key_escape_mow_rewrite", "p0,nonConcurrent") {
    sql "set default_variant_enable_doc_mode=false"
    sql "drop table if exists dotted_key_escape_mow_versions"
    sql """create table dotted_key_escape_mow_versions(id int, v variant<properties(
        "variant_enable_nested_group"="false",
        "variant_max_subcolumns_count"="1", "variant_sparse_hash_shard_count"="3",
        "variant_max_sparse_column_statistics_size"="10000")>,padding int default '0')
        unique key(id) distributed by hash(id) buckets 1 properties("variant_escape_dots_in_keys"="true", "replication_num"="1",
        "disable_auto_compaction"="true", "enable_unique_key_merge_on_write"="true")"""
    sql """insert into dotted_key_escape_mow_versions values
        (1,parse_to_variant('{"a.b":101,"a":{"b":201},"a%2Eb":301}'),0),
        (3,parse_to_variant('{"a.b":103,"a":{"b":203},"a%2Eb":303}'),0),
        (5,parse_to_variant('{"a.b":105}'),0)"""
    sql """insert into dotted_key_escape_mow_versions values
        (1,parse_to_variant('{"a.b":111,"a":{"b":211},"a%2Eb":311}'),1)"""
    sql "delete from dotted_key_escape_mow_versions where id=3"
    setBeConfigTemporary([enable_vertical_compaction:true,enable_vertical_compact_variant_subcolumns:true,
            vertical_compaction_num_columns_per_group:1]) {
        trigger_and_wait_compaction("dotted_key_escape_mow_versions", "full")
        order_qt_latest "select id,v,v['a.b'],v['a']['b'],v['a%2Eb'],padding from dotted_key_escape_mow_versions"
        def oldPartial = sql("show variables like 'enable_unique_key_partial_update'")[0][1]
        def oldStrict = sql("show variables like 'enable_insert_strict'")[0][1]
        try {
            sql "set enable_unique_key_partial_update=true"
            sql "set enable_insert_strict=false"
            sql "insert into dotted_key_escape_mow_versions(id,padding) values (1,7)"
        } finally {
            sql "set enable_unique_key_partial_update=${oldPartial}"
            sql "set enable_insert_strict=${oldStrict}"
        }
        trigger_and_wait_compaction("dotted_key_escape_mow_versions", "full")
        order_qt_partial "select id,v,v['a.b'],v['a']['b'],v['a%2Eb'],padding from dotted_key_escape_mow_versions"
    }
}
