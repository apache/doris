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

suite("dotted_key_escape_index_rewrite", "p0,nonConcurrent") {
    sql "set default_variant_enable_doc_mode=false"
    ["V2", "V3"].each { format ->
        sql "drop table if exists dotted_key_escape_index_rewrite"
        sql """create table dotted_key_escape_index_rewrite(id int, v variant<
            MATCH_NAME 'a%2Eb':string, MATCH_NAME 'a.b':string, MATCH_NAME 'a%252Eb':string,
            properties(
            "variant_enable_nested_group"="false",
            "variant_max_subcolumns_count"="0")>,
            index idx_dot(v) using inverted properties("field_pattern"="a%2Eb","parser"="english"),
            index idx_nested(v) using inverted properties("field_pattern"="a.b","parser"="english"),
            index idx_percent(v) using inverted properties("field_pattern"="a%252Eb","parser"="english"))
            duplicate key(id) distributed by hash(id) buckets 1
            properties("variant_escape_dots_in_keys"="true", "replication_num"="1", "disable_auto_compaction"="true",
            "inverted_index_storage_format"="${format}")"""
        sql """insert into dotted_key_escape_index_rewrite values
            (1,parse_to_variant('{"a.b":"hit","a":{"b":"miss"},"a%2Eb":"miss"}')),
            (3,parse_to_variant('{"a.b":"miss","a":{"b":"miss"},"a%2Eb":"hit"}'))"""
        sql """insert into dotted_key_escape_index_rewrite values
            (2,parse_to_variant('{"a.b":"miss","a":{"b":"hit"},"a%2Eb":"miss"}'))"""
        order_qt_index_before_dot "select id from dotted_key_escape_index_rewrite where search('v.a%2Eb:hit')"
        order_qt_index_before_nested "select id from dotted_key_escape_index_rewrite where search('v.a.b:hit')"
        order_qt_index_before_percent "select id from dotted_key_escape_index_rewrite where search('v.a%252Eb:hit')"
        def tablet = sql_return_maparray("show tablets from dotted_key_escape_index_rewrite")[0].TabletId
        order_qt_index_inputs """select count(*),sum(NUM_SEGMENTS),sum(ROWSET_NUM_ROWS)
            from information_schema.rowsets where TABLET_ID=${tablet} and NUM_SEGMENTS>0"""
        setBeConfigTemporary([enable_vertical_compaction:true,enable_vertical_compact_variant_subcolumns:true,
                vertical_compaction_num_columns_per_group:1]) {
            trigger_and_wait_compaction("dotted_key_escape_index_rewrite", "full")
        }
        order_qt_index_output """select count(*),sum(NUM_SEGMENTS),sum(ROWSET_NUM_ROWS)
            from information_schema.rowsets where TABLET_ID=${tablet} and NUM_SEGMENTS>0"""
        order_qt_index_after_dot "select id from dotted_key_escape_index_rewrite where search('v.a%2Eb:hit')"
        order_qt_index_after_nested "select id from dotted_key_escape_index_rewrite where search('v.a.b:hit')"
        order_qt_index_after_percent "select id from dotted_key_escape_index_rewrite where search('v.a%252Eb:hit')"
        order_qt_index_values "select id,v,v['a.b'],v['a']['b'],v['a%2Eb'] from dotted_key_escape_index_rewrite"
    }
}
