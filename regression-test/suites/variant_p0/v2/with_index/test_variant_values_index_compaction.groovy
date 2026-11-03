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

// BE configuration/debug points are global: run this suite with -parallel 1 when selected alone.
suite("test_variant_values_index_compaction", "p0,nonConcurrent") {
    sql "SET enable_match_without_inverted_index=true"
    sql "SET enable_inverted_index_query=true"
    sql "SET enable_sql_cache=false"
    sql "SET enable_query_cache=false"
    sql "SET enable_condition_cache=false"
    sql "SET enable_inverted_index_query_cache=false"
    sql "SET enable_prune_nested_column=false"
    sql "SET enable_common_expr_pushdown=true"
    sql "SET default_variant_enable_doc_mode=false"
    sql "SET default_variant_max_subcolumns_count=0"
    sql "SET default_variant_enable_typed_paths_to_sparse=false"
    sql "SET default_variant_enable_nested_group=false"
    sql "SET default_variant_max_subcolumns_count=1"
    def baseline = null
    for (boolean nativeMerge : [true, false]) {
        for (boolean vertical : [true, false]) {
            // The helper restores each BE's original values in finally, including assertion failures.
            setBeConfigTemporary([
                enable_vertical_compaction: true,
                enable_ordered_data_compaction: false,
                inverted_index_compaction_enable: nativeMerge,
                enable_vertical_compact_variant_subcolumns: vertical
            ]) {
                sql "DROP TABLE IF EXISTS vvi_compaction"
                sql """CREATE TABLE vvi_compaction (id INT, v VARIANT, plain VARIANT,
                    INDEX values_idx(v) USING INVERTED PROPERTIES("variant_index_scope"="values","parser"="english"))
                    DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                    PROPERTIES("replication_num"="1","disable_auto_compaction"="true",
                               "inverted_index_storage_format"="SNII")"""
                // Separate inserts create multiple rowsets. n promotes INT -> DOUBLE -> STRING;
                // stable wins the first rowset's one-subcolumn budget; n wins the second.
                // Rare paths also change across rowsets, exercising mixed sparse/materialized input.
                // No bool/numeric or >2^53 integer/double mixing (accepted native-merge deviations).
                sql """INSERT INTO vvi_compaction VALUES
                    (1,parse_to_variant('{"stable":"hello","n":7,"rare1":"first"}'),
                       parse_to_variant('{"stable":"plain first","n":7,"rare1":"first"}')),
                    (2,parse_to_variant('{"stable":"other"}'),NULL)"""
                sql """INSERT INTO vvi_compaction VALUES
                    (3,parse_to_variant('{"stable":"hello world","n":7.5,"rare2":["second"]}'),
                       parse_to_variant('{"stable":"plain second","n":7.5,"rare2":["second"]}')),
                    (4,parse_to_variant('{"n":9.5,"spare":"other"}'),parse_to_variant('{}'))"""
                sql """INSERT INTO vvi_compaction VALUES
                    (5,parse_to_variant('{"stable":"hello","n":"ten","rare3":{"s":"third"}}'),
                       parse_to_variant('{"stable":"plain third","n":"ten","rare3":{"s":"third"}}')),
                    (6,NULL,parse_to_variant('{"only_plain":"kept"}')),
                    (7,parse_to_variant('[]'),parse_to_variant('[]'))"""
                sql "SYNC"
                def queries = [
                    """SELECT 'any' AS kind,id FROM vvi_compaction WHERE v MATCH_ANY 'hello'
                       UNION ALL SELECT 'all',id FROM vvi_compaction WHERE v MATCH_ALL 'hello world'
                       UNION ALL SELECT 'rare',id FROM vvi_compaction WHERE v MATCH_ANY 'first second third ten'
                       ORDER BY kind,id""",
                    // Stable leaves in both roots survive sparse/materialized layout changes.
                    "SELECT id,CAST(v['stable'] AS STRING),CAST(plain['stable'] AS STRING),CAST(v['rare1'] AS STRING),CAST(v['rare2'] AS STRING),CAST(v['rare3'] AS STRING),CAST(plain['rare1'] AS STRING),CAST(plain['rare2'] AS STRING),CAST(plain['rare3'] AS STRING) FROM vvi_compaction ORDER BY id",
                    "SELECT count(*) FROM vvi_compaction WHERE v MATCH_ANY 'hello'"
                ]
                def check = { String phase, List before ->
                    def rows = []
                    queries.eachWithIndex { query, i ->
                        sql "SET enable_inverted_index_query=true"
                        def indexed = sql(query)
                        sql "SET enable_inverted_index_query=false"
                        assertEquals(indexed, sql(query))
                        if (phase == "after") {
                            assertEquals(before[i], indexed)
                        }
                        rows.add(indexed)
                        if (baseline == null && phase == "before") {
                            quickTest("before_${i}", query, true)
                        }
                    }
                    sql "SET enable_inverted_index_query=true"
                    rows
                }
                def before = check("before", [])
                if (baseline == null) {
                    baseline = before
                } else {
                    assertEquals(baseline, before)
                }
                trigger_and_wait_compaction("vvi_compaction", "full", 1800)
                assertEquals(baseline, check("after", before))
                sql "DROP TABLE IF EXISTS vvi_compaction_paths"
                sql """CREATE TABLE vvi_compaction_paths (id INT,v VARIANT,
                    INDEX values_idx(v) USING INVERTED PROPERTIES("variant_index_scope"="values","parser"="english"),
                    INDEX path_idx(v) USING INVERTED PROPERTIES("parser"="english"))
                    DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                    PROPERTIES("replication_num"="1","disable_auto_compaction"="true",
                               "inverted_index_storage_format"="SNII")"""
                sql """INSERT INTO vvi_compaction_paths VALUES (1,parse_to_variant('{"s":"hello"}'))"""
                sql """INSERT INTO vvi_compaction_paths VALUES (2,parse_to_variant('{"s":"other"}'))"""
                sql """INSERT INTO vvi_compaction_paths VALUES (3,parse_to_variant('{"s":"hello world"}'))"""
                // A missing path index must fail instead of using residual MATCH.
                sql "SET enable_match_without_inverted_index=false"
                try {
                    for (boolean compacted : [false, true]) {
                        if (compacted) {
                            trigger_and_wait_compaction("vvi_compaction_paths", "full", 1800)
                        }
                        assertEquals([[1], [3]], sql("SELECT id FROM vvi_compaction_paths WHERE v['s'] MATCH_ANY 'hello' ORDER BY id"))
                    }
                } finally {
                    sql "SET enable_match_without_inverted_index=true"
                }
            }
        }
    }
}
