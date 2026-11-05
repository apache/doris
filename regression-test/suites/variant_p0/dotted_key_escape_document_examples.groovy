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

suite("dotted_key_escape_document_examples", "p0,nonConcurrent") {
    sql "set default_variant_enable_doc_mode=false"
    def oldNested = sql("show variables like 'default_variant_enable_nested_group'")[0][1]
    try {
        [false, true].each { nestedDefault ->
            sql "set default_variant_enable_nested_group=${nestedDefault}"
            sql "drop table if exists dotted_key_escape_document"
            sql """create table dotted_key_escape_document(id int, v variant<
                MATCH_NAME 'a%2Eb':string, MATCH_NAME 'a.b':string, properties(
                "variant_escape_dots_in_keys"="true", "variant_enable_nested_group"="false")>,
                index idx_dot(v) using inverted properties("field_pattern"="a%2Eb","parser"="english"),
                index idx_nested(v) using inverted properties("field_pattern"="a.b","parser"="english"))
                duplicate key(id) distributed by hash(id) buckets 1
                properties("replication_num"="1", "inverted_index_storage_format"="V2")"""
            sql """insert into dotted_key_escape_document values
                (1,parse_to_variant('{"a.b":"dotted","a":{"b":"nested"},"a%2Eb":"percent"}')),
                (2,'{"a.b":"dotted","a":{"b":"nested"},"a%2Eb":"percent"}')"""
            order_qt_document_object_and_string """select id,v,v['a.b'],v['a']['b'],v['a%2Eb']
                from dotted_key_escape_document"""
            order_qt_document_types "select id,json_type(v,'\$') from dotted_key_escape_document"
            order_qt_document_dot "select id from dotted_key_escape_document where search('v.a%2Eb:dotted')"
            order_qt_document_nested "select id from dotted_key_escape_document where search('v.a.b:nested')"
            order_qt_document_cross_dot "select id from dotted_key_escape_document where search('v.a%2Eb:nested')"
            order_qt_document_cross_nested "select id from dotted_key_escape_document where search('v.a.b:dotted')"
        }
        sql "drop table if exists dotted_key_escape_single_index"
        sql """create table dotted_key_escape_single_index(id int, v variant<
                MATCH_NAME 'a%2Eb':string, MATCH_NAME 'a.b':string, properties(
            "variant_escape_dots_in_keys"="true", "variant_enable_nested_group"="false")>,
            index idx_dot(v) using inverted properties("field_pattern"="a%2Eb","parser"="english"))
            duplicate key(id) distributed by hash(id) buckets 1
            properties("replication_num"="1", "inverted_index_storage_format"="V2")"""
        sql """insert into dotted_key_escape_single_index values
            (1,parse_to_variant('{"a.b":"dotted","a":{"b":"nested"}}'))"""
        order_qt_single_index_dot "select id from dotted_key_escape_single_index where search('v.a%2Eb:dotted')"
        test {
            sql "select id from dotted_key_escape_single_index where search('v.a.b:nested')"
            exception "SEARCH index unavailable"
        }
        sql "drop table if exists dotted_key_escape_glob"
        sql """create table dotted_key_escape_glob(id int, v variant<
            MATCH_NAME_GLOB 'a%2Eb.*':bigint, MATCH_NAME_GLOB 'a.b.*':string,
            properties("variant_escape_dots_in_keys"="true", "variant_enable_nested_group"="false",
                "variant_max_subcolumns_count"="2048", "variant_sparse_hash_shard_count"="1")>)
            duplicate key(id) distributed by hash(id) buckets 1
            properties("replication_num"="1")"""
        sql """insert into dotted_key_escape_glob values
            (1,parse_to_variant('{"a.b":{"c.d":"11"},"a":{"b":{"c.d":"nested"}}}'))"""
        order_qt_glob "select id,v,v['a.b']['c.d'],v['a']['b']['c.d'] from dotted_key_escape_glob"
        sql "set describe_extend_variant_column=true"
        order_qt_glob_schema "desc dotted_key_escape_glob"
    } finally {
        sql "set default_variant_enable_nested_group=${oldNested}"
    }
}
