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

suite("test_variant_values_index_routes", "p0,nonConcurrent") {
    sql "SET enable_sql_cache=false"
    sql "SET enable_query_cache=false"
    sql "SET enable_condition_cache=false"
    sql "SET enable_inverted_index_query_cache=false"
    sql "SET default_variant_enable_doc_mode=false"
    sql "SET default_variant_enable_nested_group=false"
    sql "SET default_variant_max_subcolumns_count=10"
    for (int position : [0,1,2]) {
        def ordinary = [
            'INDEX english_idx(v) USING INVERTED PROPERTIES("parser"="english","lower_case"="false")',
            'INDEX unicode_idx(v) USING INVERTED PROPERTIES("parser"="unicode")'
        ]
        ordinary.add(position, 'INDEX values_idx(v) USING INVERTED PROPERTIES("variant_index_scope"="values","parser"="english")')
        setBeConfigTemporary([
            enable_vertical_compaction: position != 2,
            enable_vertical_compact_variant_subcolumns: position != 1,
            vertical_compaction_num_columns_per_group: 1,
            enable_ordered_data_compaction: false,
            inverted_index_compaction_enable: position != 1
        ]) {
            sql "DROP TABLE IF EXISTS vvi_routes"
            sql """CREATE TABLE vvi_routes(id INT,v VARIANT<'exact':STRING,'tag*':STRING,'inherit':STRING>,
                ${ordinary.join(',')},
                INDEX exact_idx(v) USING INVERTED PROPERTIES("field_pattern"="exact","parser"="english"),
                INDEX glob_idx(v) USING INVERTED PROPERTIES("field_pattern"="tag*","parser"="chinese"))
                DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES("replication_num"="1","disable_auto_compaction"="true","inverted_index_storage_format"="SNII")"""
            sql """INSERT INTO vvi_routes VALUES
                (1,parse_to_variant('{"s":"HELLO world","inherit":"HELLO world","exact":"EXACT","tagA":"alpha","tagB":"beta"}')),
                (2,parse_to_variant('{"s":"hello world","inherit":"hello world","exact":"other","tagA":"beta","tagB":"alpha"}'))"""
            sql """INSERT INTO vvi_routes VALUES
                (3,parse_to_variant('{"s":"other","inherit":"other","exact":"EXACT","tagA":"gamma","tagB":"delta"}')),
                (4,NULL)"""
            sql "SET enable_match_without_inverted_index=false"
            for (String phase : ["before", "after"]) {
                if (phase == "after") { trigger_and_wait_compaction("vvi_routes", "full", 1800) }
                def queries = [
                    root: "SELECT id FROM vvi_routes WHERE v MATCH_ALL 'hello world' ORDER BY id",
                    english: "SELECT id FROM vvi_routes WHERE v['s'] MATCH_PHRASE 'HELLO world' USING ANALYZER english ORDER BY id",
                    english_excluded: "SELECT id FROM vvi_routes WHERE v['s'] MATCH_PHRASE 'hello world' USING ANALYZER english ORDER BY id",
                    unicode: "SELECT id FROM vvi_routes WHERE v['s'] MATCH_PHRASE 'hello world' USING ANALYZER unicode ORDER BY id",
                    typed_inherit: "SELECT id FROM vvi_routes WHERE v['inherit'] MATCH_ANY 'HELLO' USING ANALYZER english ORDER BY id",
                    exact: "SELECT id FROM vvi_routes WHERE v['exact'] MATCH_ANY 'exact' ORDER BY id",
                    glob_a: "SELECT id FROM vvi_routes WHERE v['tagA'] MATCH_ANY 'alpha' ORDER BY id",
                    glob_b: "SELECT id FROM vvi_routes WHERE v['tagB'] MATCH_ANY 'alpha' ORDER BY id",
                    glob_absent: "SELECT id FROM vvi_routes WHERE v['tagA'] MATCH_ANY 'delta' ORDER BY id",
                    search: "SELECT id FROM vvi_routes WHERE SEARCH('v.s:HELLO') ORDER BY id",
                    values: "SELECT id,v FROM vvi_routes ORDER BY id"
                ]
                queries.each { tag, query -> quickTest("${position}_${phase}_${tag}", query, true) }
                test {
                    sql "SELECT id FROM vvi_routes WHERE v MATCH_ANY 'hello' USING ANALYZER unicode ORDER BY id"
                    exception "analyzer"
                }
            }
        }
    }
    // M3: predefined-field order selects an overlapping exact/glob template.
    for (boolean globFirst : [true,false]) {
        def fields = globFirst ? "'tag*':STRING,'tagA':STRING" : "'tagA':STRING,'tag*':STRING"
        sql "DROP TABLE IF EXISTS vvi_overlap"
        sql """CREATE TABLE vvi_overlap(id INT,v VARIANT<${fields}>,
            INDEX values_idx(v) USING INVERTED PROPERTIES("variant_index_scope"="values","parser"="english"),
            INDEX path_idx(v) USING INVERTED PROPERTIES("parser"="english"),
            INDEX glob_idx(v) USING INVERTED PROPERTIES("field_pattern"="tag*","parser"="chinese","lower_case"="false"),
            INDEX exact_idx(v) USING INVERTED PROPERTIES("field_pattern"="tagA","parser"="unicode"))
            DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_num"="1","disable_auto_compaction"="true","inverted_index_storage_format"="SNII")"""
        sql """INSERT INTO vvi_overlap VALUES(1,parse_to_variant('{"tagA":"ALPHA","tagB":"alpha","tagC":"beta"}'))"""
        sql """INSERT INTO vvi_overlap VALUES(2,parse_to_variant('{"tagA":"alpha","tagB":"beta","tagC":"alpha"}'))"""
        for (String phase : ["before","after"]) {
            if (phase == "after") { trigger_and_wait_compaction("vvi_overlap", "full", 1800) }
            quickTest("overlap_${globFirst}_${phase}_a", "SELECT id FROM vvi_overlap WHERE v['tagA'] MATCH_ANY 'ALPHA' ORDER BY id", true)
            quickTest("overlap_${globFirst}_${phase}_b", "SELECT id FROM vvi_overlap WHERE v['tagB'] MATCH_ANY 'alpha' ORDER BY id", true)
            quickTest("overlap_${globFirst}_${phase}_c", "SELECT id FROM vvi_overlap WHERE v['tagC'] MATCH_ANY 'alpha' ORDER BY id", true)
            quickTest("overlap_${globFirst}_${phase}_root", "SELECT id FROM vvi_overlap WHERE v MATCH_ANY 'alpha' ORDER BY id", true)
            quickTest("overlap_${globFirst}_${phase}_values", "SELECT id,v FROM vvi_overlap ORDER BY id", true)
        }
    }

}
