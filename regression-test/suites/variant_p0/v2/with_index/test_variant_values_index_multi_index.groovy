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

suite("test_variant_values_index_multi_index", "p0,nonConcurrent") {
    sql "SET enable_sql_cache=false"
    sql "SET enable_query_cache=false"
    sql "SET enable_condition_cache=false"
    sql "SET enable_inverted_index_query_cache=false"
    sql "SET default_variant_enable_doc_mode=false"
    sql "SET default_variant_enable_nested_group=false"
    sql "SET default_variant_max_subcolumns_count=1"
    sql "SET default_variant_enable_typed_paths_to_sparse=false"
    def baseline = [:]
    for (boolean nativeMerge : [true, false]) {
        for (boolean vertical : [true, false]) {
            setBeConfigTemporary([
                enable_vertical_compaction: vertical,
                enable_ordered_data_compaction: false,
                inverted_index_compaction_enable: nativeMerge
            ]) {
                sql "DROP TABLE IF EXISTS vvi_multi_index"
                // Scope/analyzer selection must distinguish root, inherited path and template indexes.
                sql """CREATE TABLE vvi_multi_index (id INT,
                    v VARIANT<'n':INT, 's':STRING, 'tag*':STRING>, other VARIANT,
                    INDEX values_idx(v) USING INVERTED PROPERTIES("variant_index_scope"="values","parser"="english"),
                    INDEX paths_idx(v) USING INVERTED PROPERTIES("parser"="english","lower_case"="false"),
                    INDEX number_idx(v) USING INVERTED PROPERTIES("field_pattern"="n"),
                    INDEX tags_idx(v) USING INVERTED PROPERTIES("field_pattern"="tag*","parser"="unicode"),
                    INDEX other_values(other) USING INVERTED PROPERTIES("variant_index_scope"="values","parser"="unicode","lower_case"="false"))
                    DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                    PROPERTIES("replication_num"="1","disable_auto_compaction"="true",
                               "inverted_index_storage_format"="SNII")"""
                sql """INSERT INTO vvi_multi_index VALUES
                    (1,parse_to_variant('{"s":"HELLO","n":"00042","tagA":"café","obj":{"s":"WORLD"}}'),parse_to_variant('{"s":"UPPER"}')),
                    (2,parse_to_variant('{"s":"other","n":7,"tagB":"other"}'),parse_to_variant('{"s":"hello"}'))"""
                sql """INSERT INTO vvi_multi_index VALUES
                    (3,parse_to_variant('{"s":"hello","n":42,"tagA":"café"}'),NULL),
                    (4,NULL,parse_to_variant('{"s":"UPPER"}'))"""
                sql "SYNC"
                def queries = [
                    root: "SELECT id FROM vvi_multi_index WHERE v MATCH_ANY 'hello' ORDER BY id",
                    cross_leaf: "SELECT id FROM vvi_multi_index WHERE v MATCH_ALL 'hello world' ORDER BY id",
                    converted: "SELECT id FROM vvi_multi_index WHERE v MATCH_ANY '42' ORDER BY id",
                    raw_input: "SELECT id FROM vvi_multi_index WHERE v MATCH_ANY '00042' ORDER BY id",
                    path_upper: "SELECT id FROM vvi_multi_index WHERE v['s'] MATCH_ANY 'HELLO' ORDER BY id",
                    path_lower: "SELECT id FROM vvi_multi_index WHERE v['s'] MATCH_ANY 'hello' ORDER BY id",
                    nested: "SELECT id FROM vvi_multi_index WHERE v['obj']['s'] MATCH_ANY 'WORLD' ORDER BY id",
                    template: "SELECT id FROM vvi_multi_index WHERE v['tagA'] MATCH_ANY 'café' ORDER BY id",
                    other_upper: "SELECT id FROM vvi_multi_index WHERE other MATCH_ANY 'UPPER' ORDER BY id",
                    other_lower: "SELECT id FROM vvi_multi_index WHERE other MATCH_ANY 'upper' ORDER BY id",
                    both: "SELECT id FROM vvi_multi_index WHERE (v MATCH_ANY 'hello') AND (other MATCH_ANY 'UPPER') ORDER BY id",
                    cte: "WITH c AS (SELECT id,v AS a,other AS b FROM vvi_multi_index) SELECT id FROM c WHERE (a MATCH_ANY 'hello') OR (b MATCH_ANY 'UPPER') ORDER BY id",
                    left_join: "SELECT a.id FROM vvi_multi_index a LEFT JOIN vvi_multi_index b ON a.id=b.id AND b.id<3 WHERE (b.v MATCH_ANY 'hello') OR b.id IS NULL ORDER BY a.id"
                ]
                for (String phase : ["before", "after"]) {
                    if (phase == "after") {
                        trigger_and_wait_compaction("vvi_multi_index", "full", 1800)
                    }
                    queries.each { tag, query ->
                        sql "SET enable_inverted_index_query=true"
                        sql "SET enable_match_without_inverted_index=false"
                        def indexed = sql(query)
                        sql "SET enable_inverted_index_query=false"
                        sql "SET enable_match_without_inverted_index=true"
                        def scanned = sql(query)
                        assertEquals(indexed, scanned)
                        if (!baseline.containsKey(tag)) {
                            // Root matches [1,3]; case-sensitive paths separately match [1] and [3].
                            quickTest(tag, query, true)
                            baseline[tag] = scanned
                        } else {
                            assertEquals(baseline[tag], indexed)
                        }
                    }
                    sql "SET enable_inverted_index_query=true"
                    sql "SET enable_match_without_inverted_index=false"
                    // Numeric template indexes use equality rather than text MATCH.
                    quickTest("number_${nativeMerge}_${vertical}_${phase}",
                        "SELECT id,CAST(v['n'] AS INT) FROM vvi_multi_index WHERE CAST(v['n'] AS INT)=42 ORDER BY id", true)
                }
            }
        }
    }
    // Adding/dropping an ordinary index must retain the existing values/template index metadata.
    test {
        sql 'ALTER TABLE vvi_multi_index ADD INDEX duplicate_paths(v) USING INVERTED PROPERTIES("parser"="unicode")'
        exception "with the same analyzer selector already exists"
    }
    test {
        sql 'ALTER TABLE vvi_multi_index ADD INDEX alias_paths(v) USING INVERTED PROPERTIES("parser"="standard")'
        exception "analyzer identity 'standard' already exists"
    }
    sql 'ALTER TABLE vvi_multi_index ADD INDEX chinese_paths(v) USING INVERTED PROPERTIES("parser"="chinese")'
    waitForSchemaChangeDone {
        sql "SHOW ALTER TABLE COLUMN WHERE TableName='vvi_multi_index' ORDER BY CreateTime DESC LIMIT 1"
        time 120
    }
    sql """INSERT INTO vvi_multi_index VALUES
        (5,parse_to_variant('{"s":"HELLO","n":"00042","tagC":"café"}'),parse_to_variant('{"s":"UPPER"}'))"""
    sql "SYNC"
    sql "SET enable_inverted_index_query=true"
    sql "SET enable_match_without_inverted_index=false"
    qt_added_index_root "SELECT id FROM vvi_multi_index WHERE v MATCH_ANY 'hello' ORDER BY id"
    qt_added_index_new_path "SELECT id FROM vvi_multi_index WHERE id=5 AND v['s'] MATCH_ANY 'hello' USING ANALYZER chinese ORDER BY id"
    sql "SET enable_match_without_inverted_index=true"
    qt_added_index_historical_path "SELECT id FROM vvi_multi_index WHERE v['s'] MATCH_ANY 'hello' USING ANALYZER chinese ORDER BY id"
    trigger_and_wait_compaction("vvi_multi_index", "full", 1800)
    sql "SET enable_match_without_inverted_index=false"
    qt_added_index_compacted_root "SELECT id FROM vvi_multi_index WHERE v MATCH_ANY 'hello' ORDER BY id"
    qt_added_index_compacted_path "SELECT id FROM vvi_multi_index WHERE v['s'] MATCH_ANY 'hello' USING ANALYZER chinese ORDER BY id"
    sql "DROP INDEX chinese_paths ON vvi_multi_index"
    waitForSchemaChangeDone {
        sql "SHOW ALTER TABLE COLUMN WHERE TableName='vvi_multi_index' ORDER BY CreateTime DESC LIMIT 1"
        time 120
    }
    qt_dropped_index_root "SELECT id FROM vvi_multi_index WHERE v MATCH_ANY 'hello' ORDER BY id"
    qt_dropped_index_original_path "SELECT id FROM vvi_multi_index WHERE v['s'] MATCH_ANY 'HELLO' ORDER BY id"

}
