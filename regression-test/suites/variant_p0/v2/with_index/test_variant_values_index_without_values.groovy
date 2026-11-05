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

suite("test_variant_values_index_without_values", "p0,nonConcurrent") {
    sql "SET default_variant_enable_doc_mode=false"
    sql "SET default_variant_enable_nested_group=false"
    sql "SET default_variant_max_subcolumns_count=10"
    sql "SET enable_match_without_inverted_index=false"
    sql "SET enable_sql_cache=false"
    sql "SET enable_query_cache=false"
    sql "SET enable_condition_cache=false"
    sql "SET enable_inverted_index_query_cache=false"
    for (boolean values : [true,false]) {
        String table = values ? "vvi_control_values" : "vvi_control_paths"
        sql "DROP TABLE IF EXISTS ${table}"
        String rootIndex = values ? 'INDEX values_idx(v) USING INVERTED PROPERTIES("variant_index_scope"="values","parser"="english"),' : ''
        sql """CREATE TABLE ${table}(id INT,v VARIANT<'tag*':STRING>,
            ${rootIndex}
            INDEX english_idx(v) USING INVERTED PROPERTIES("parser"="english","lower_case"="false"),
            INDEX unicode_idx(v) USING INVERTED PROPERTIES("parser"="unicode"),
            INDEX tag_idx(v) USING INVERTED PROPERTIES("field_pattern"="tag*","parser"="chinese"))
            DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_num"="1","disable_auto_compaction"="true","inverted_index_storage_format"="SNII")"""
        sql """INSERT INTO ${table} VALUES
            (1,parse_to_variant('{"s":"HELLO world","tagA":"alpha"}')),
            (2,parse_to_variant('{"s":"hello world","tagA":"beta"}'))"""
        sql """INSERT INTO ${table} VALUES
            (3,parse_to_variant('{"s":"world HELLO","tagB":"alpha"}')),(4,NULL)"""
        for (String phase : ["before","after"]) {
            if (phase == "after") { trigger_and_wait_compaction(table,"full",1800) }
            quickTest("${values}_${phase}_values", "SELECT id,v FROM ${table} ORDER BY id", true)
            quickTest("${values}_${phase}_english", "SELECT id FROM ${table} WHERE v['s'] MATCH_ANY 'HELLO' USING ANALYZER english ORDER BY id", true)
            quickTest("${values}_${phase}_phrase", "SELECT id FROM ${table} WHERE v['s'] MATCH_PHRASE 'HELLO world' USING ANALYZER english ORDER BY id", true)
            quickTest("${values}_${phase}_unicode", "SELECT id FROM ${table} WHERE v['s'] MATCH_PHRASE 'hello world' USING ANALYZER unicode ORDER BY id", true)
            quickTest("${values}_${phase}_search", "SELECT id FROM ${table} WHERE SEARCH('v.s:HELLO') ORDER BY id", true)
            quickTest("${values}_${phase}_template", "SELECT id FROM ${table} WHERE v['tagA'] MATCH_ANY 'alpha' ORDER BY id", true)
            quickTest("${values}_${phase}_template_absent", "SELECT id FROM ${table} WHERE v['tagA'] MATCH_ANY 'missing' ORDER BY id", true)
            if (values) {
                quickTest("${phase}_root", "SELECT id FROM ${table} WHERE v MATCH_ALL 'hello world' ORDER BY id", true)
            } else {
                test {
                    sql "SELECT id FROM ${table} WHERE v MATCH_ANY 'hello' ORDER BY id"
                    exception "VARIANT root column does not support MATCH predicates"
                }
            }
        }
    }
}
