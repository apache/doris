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

suite("test_variant_values_index_dotted_keys", "p0,nonConcurrent") {
    sql "SET enable_sql_cache=false"
    sql "SET enable_query_cache=false"
    sql "SET enable_condition_cache=false"
    sql "SET enable_inverted_index_query_cache=false"
    sql "SET debug_variant_enable_nested_group=false"
    sql "SET debug_variant_enable_doc_mode=false"
    for (boolean doc : [false, true]) {
        for (int layoutCase : [0, 1]) {
            // ORDINARY uses unlimited/finite subcolumns; DOC uses doc-only/hybrid materialization.
            def storageProperties = doc
                    ? """
                        "variant_enable_doc_mode"="true", "variant_doc_hash_shard_count"="3",
                        "variant_doc_materialization_min_rows"="${layoutCase == 0 ? 100 : 1}"
                      """
                    : """
                        "variant_max_subcolumns_count"="${layoutCase}",
                        "variant_enable_typed_paths_to_sparse"="false", "variant_sparse_hash_shard_count"="3"
                      """
            for (boolean nativeMerge : [false, true]) {
                setBeConfigTemporary([
                    enable_vertical_compaction: true,
                    enable_vertical_compact_variant_subcolumns: nativeMerge,
                    vertical_compaction_num_columns_per_group: 1,
                    enable_ordered_data_compaction: false,
                    inverted_index_compaction_enable: nativeMerge
                ]) {
                    sql "DROP TABLE IF EXISTS vvi_dotted_keys"
                    sql """CREATE TABLE vvi_dotted_keys(id INT, v VARIANT<
                        MATCH_NAME 'a%2Eb':STRING, MATCH_NAME 'a.b':STRING,
                        MATCH_NAME 'a%252Eb':STRING, MATCH_NAME 'n%2Ex':INT,
                        MATCH_NAME 'n.x':INT, PROPERTIES(
                            ${storageProperties},
                            "variant_enable_nested_group"="false")>,
                        INDEX values_idx(v) USING INVERTED PROPERTIES(
                            "variant_index_scope"="values","parser"="english"),
                        INDEX path_idx(v) USING INVERTED PROPERTIES("parser"="english"))
                        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                        PROPERTIES("replication_num"="1","disable_auto_compaction"="true",
                            "variant_escape_dots_in_keys"="true",
                            "inverted_index_storage_format"="SNII")"""
                    // These three paths must remain distinct in both postings and assembled values.
                    sql """INSERT INTO vvi_dotted_keys VALUES
                        (1,parse_to_variant('{"a.b":"literal","a":{"b":"nested"},"a%2Eb":"percent"}')),
                        (2,parse_to_variant('{"a":"ancestor","a.b":"sibling","50%":"kept"}')),
                        (3,parse_to_variant('{"items":[{"x.y":"arraydot","x":{"y":"arraynested"},"x%2Ey":"arraypercent"}]}'))"""
                    sql """INSERT INTO vvi_dotted_keys VALUES
                        (4,parse_to_variant('{"a.b":"nested","a":{"b":"percent"},"a%2Eb":"literal"}')),
                        (5,parse_to_variant('{"a.b":null,"a":{"b":null},"a%2Eb":null}')),
                        (6,NULL), (7,parse_to_variant('{}')),
                        (8,parse_to_variant('{"n.x":"0042","n":{"x":7}}'))"""
                    sql "SYNC"
                    def rootQueries = [
                        literal: "SELECT id FROM vvi_dotted_keys WHERE v MATCH_ANY 'literal' ORDER BY id",
                        cross_leaf: "SELECT id FROM vvi_dotted_keys WHERE v MATCH_ALL 'literal nested percent' ORDER BY id",
                        ancestor: "SELECT id FROM vvi_dotted_keys WHERE v MATCH_ALL 'ancestor sibling kept' ORDER BY id",
                        array: "SELECT id FROM vvi_dotted_keys WHERE v MATCH_ALL 'arraydot arraynested arraypercent' ORDER BY id",
                        numeric: "SELECT id FROM vvi_dotted_keys WHERE v MATCH_ANY '42' ORDER BY id",
                        numeric_raw: "SELECT id FROM vvi_dotted_keys WHERE v MATCH_ANY '0042' ORDER BY id",
                        numeric_nested: "SELECT id FROM vvi_dotted_keys WHERE v MATCH_ANY '7' ORDER BY id",
                        // Object keys are never values-index terms, including their escaped spellings.
                        keys: "SELECT id FROM vvi_dotted_keys WHERE v MATCH_ANY 'a b a%2Eb items x y x%2Ey 50%' ORDER BY id",
                        negated: "SELECT id FROM vvi_dotted_keys WHERE NOT(v MATCH_ANY 'literal') ORDER BY id"
                    ]
                    def pathQueries = [
                        dotted: "SELECT id FROM vvi_dotted_keys WHERE v['a.b'] MATCH_ANY 'literal' ORDER BY id",
                        nested: "SELECT id FROM vvi_dotted_keys WHERE v['a']['b'] MATCH_ANY 'nested' ORDER BY id",
                        percent: "SELECT id FROM vvi_dotted_keys WHERE v['a%2Eb'] MATCH_ANY 'literal' ORDER BY id",
                        numeric_paths: "SELECT id,CAST(v['n.x'] AS INT),CAST(v['n']['x'] AS INT) FROM vvi_dotted_keys WHERE CAST(v['n.x'] AS INT)=42 ORDER BY id",
                        projection: "SELECT id,v MATCH_ANY 'ancestor',v MATCH_ALL 'literal nested percent',v MATCH_ANY 'arraydot' FROM vvi_dotted_keys ORDER BY id",
                        values: "SELECT id,v,v['a.b'],v['a']['b'],v['a%2Eb'],v['50%'] FROM vvi_dotted_keys ORDER BY id"
                    ]
                    def check = { String phase ->
                        (rootQueries + pathQueries).each { tag, query ->
                            sql "SET enable_inverted_index_query=true"
                            // A successful root predicate with fallback disabled proves index binding.
                            sql "SET enable_match_without_inverted_index=${!rootQueries.containsKey(tag)}"
                            def indexed = sql(query)
                            sql "SET enable_match_without_inverted_index=true"
                            sql "SET enable_inverted_index_query=false"
                            assertEquals(indexed, sql(query))
                            quickTest("${doc}_${layoutCase}_${nativeMerge}_${phase}_${tag}", query, true)
                        }
                        sql "SET enable_inverted_index_query=true"
                    }
                    check("before")
                    trigger_and_wait_compaction("vvi_dotted_keys", "full", 1800)
                    check("after")
                }
            }
        }
    }
}
