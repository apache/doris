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

suite("dotted_key_escape_layout_switch", "p0,nonConcurrent") {
    sql "set default_variant_enable_doc_mode=false"
    sql "set describe_extend_variant_column=true"
    [1, 2].each { width ->
        sql "drop table if exists dotted_key_escape_switch"
        sql """create table dotted_key_escape_switch(id int, v variant<properties(
            "variant_enable_nested_group"="false",
            "variant_max_subcolumns_count"="1", "variant_sparse_hash_shard_count"="3",
            "variant_max_sparse_column_statistics_size"="10000")>)
            duplicate key(id) distributed by hash(id) buckets 1
            properties("variant_escape_dots_in_keys"="true", "replication_num"="1", "disable_auto_compaction"="true")"""
        // Unequal frequencies select dot, then percent, then dot again. Overlapping key
        // ranges force rewriting, and the second merge consumes the first merge's output.
        sql """insert into dotted_key_escape_switch values
            (1,parse_to_variant('{"a.b":101,"a":{"b":201},"a%2Eb":301,"50%":401}')),
            (8,parse_to_variant('{"a.b":108}')),(16,parse_to_variant('{"a.b":116}'))"""
        order_qt_initial_schema "desc dotted_key_escape_switch"
        order_qt_initial "select id,v,v['a.b'],v['a']['b'],v['a%2Eb'],v['50%'] from dotted_key_escape_switch"
        sql """insert into dotted_key_escape_switch values
            (2,parse_to_variant('{"a%2Eb":302}')),(4,parse_to_variant('{"a%2Eb":304}')),
            (6,parse_to_variant('{"a%2Eb":306}')),(10,parse_to_variant('{"a%2Eb":310}')),
            (14,parse_to_variant('{"a%2Eb":314}'))"""
        setBeConfigTemporary([enable_vertical_compaction:true,
                enable_vertical_compact_variant_subcolumns:true,
                vertical_compaction_num_columns_per_group:width]) {
            trigger_and_wait_compaction("dotted_key_escape_switch", "full")
            order_qt_percent_schema "desc dotted_key_escape_switch"
            order_qt_percent "select id,v,v['a.b'],v['a']['b'],v['a%2Eb'],v['50%'] from dotted_key_escape_switch"
            def tablet = sql_return_maparray("show tablets from dotted_key_escape_switch")[0].TabletId
            order_qt_first_output """select count(*),sum(NUM_SEGMENTS),sum(ROWSET_NUM_ROWS)
                from information_schema.rowsets where TABLET_ID=${tablet} and NUM_SEGMENTS>0"""
            sql """insert into dotted_key_escape_switch values
                (3,parse_to_variant('{"a.b":103}')),(5,parse_to_variant('{"a.b":105}')),
                (7,parse_to_variant('{"a.b":107}')),(9,parse_to_variant('{"a.b":109}')),
                (11,parse_to_variant('{"a.b":111}')),(12,parse_to_variant('{"a.b":112}')),
                (13,parse_to_variant('{"a.b":113}')),(15,parse_to_variant('{"a.b":115}'))"""
            trigger_and_wait_compaction("dotted_key_escape_switch", "full")
            order_qt_dot_schema "desc dotted_key_escape_switch"
            order_qt_dot "select id,v,v['a.b'],v['a']['b'],v['a%2Eb'],v['50%'] from dotted_key_escape_switch"
            order_qt_second_output """select count(*),sum(NUM_SEGMENTS),sum(ROWSET_NUM_ROWS)
                from information_schema.rowsets where TABLET_ID=${tablet} and NUM_SEGMENTS>0"""
        }
    }
    // Zero means unlimited. The finite-cap branch must retain a sparse path too.
    [0, 2].each { cap ->
        [1, 2].each { width ->
            sql "drop table if exists dotted_key_escape_cap"
            sql """create table dotted_key_escape_cap(id int, v variant<properties(
                "variant_enable_nested_group"="false",
                "variant_max_subcolumns_count"="${cap}", "variant_sparse_hash_shard_count"="3",
                "variant_max_sparse_column_statistics_size"="10000")>)
                duplicate key(id) distributed by hash(id) buckets 1
                properties("variant_escape_dots_in_keys"="true", "replication_num"="1", "disable_auto_compaction"="true")"""
            sql """insert into dotted_key_escape_cap values
                (1,parse_to_variant('{"a.b":101,"a":{"b":201},"a%2Eb":301}')),
                (3,parse_to_variant('{"a.b":103,"a":{"b":203}}'))"""
            sql """insert into dotted_key_escape_cap values (2,parse_to_variant('{"a.b":102}'))"""
            setBeConfigTemporary([enable_vertical_compaction:true,
                    enable_vertical_compact_variant_subcolumns:true,
                    vertical_compaction_num_columns_per_group:width]) {
                trigger_and_wait_compaction("dotted_key_escape_cap", "full")
            }
            order_qt_cap_schema "desc dotted_key_escape_cap"
            order_qt_cap "select id,v,v['a.b'],v['a']['b'],v['a%2Eb'] from dotted_key_escape_cap"
        }
    }
}
