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

suite("test_variant_values_index_mow", "p0,nonConcurrent") {
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
    sql "SET enable_unique_key_partial_update=false"
    sql "SET enable_insert_strict=true"
    sql "DROP TABLE IF EXISTS vvi_mow"
    sql """CREATE TABLE vvi_mow (id INT, v VARIANT, note STRING DEFAULT 'initial',
        INDEX values_idx(v) USING INVERTED PROPERTIES("variant_index_scope"="values","parser"="english"))
        UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1","disable_auto_compaction"="true",
                   "enable_unique_key_merge_on_write"="true","inverted_index_storage_format"="SNII")"""
    def check = { String phase, List queries, boolean emit = true ->
        sql "SYNC"
        def results = []
        queries.eachWithIndex { query, i ->
            sql "SET enable_inverted_index_query=true"
            def indexed = sql(query)
            sql "SET enable_inverted_index_query=false"
            assertEquals(indexed, sql(query))
            if (emit) {
                quickTest("${phase}_${i}", query, true)
            }
            results.add(indexed)
        }
        sql "SET enable_inverted_index_query=true"
        results
    }
    sql """INSERT INTO vvi_mow VALUES
        (1,parse_to_variant('{"s":"old","a":["hello","world"]}'),'one'),
        (2,parse_to_variant('{"s":"deleted"}'),'two'),
        (3,parse_to_variant('{"s":"retained hello world"}'),'three'),
        (4,NULL,'four')"""
    check("insert", [
        "SELECT id,v MATCH_ANY 'old deleted',v MATCH_ALL 'hello world' FROM vvi_mow ORDER BY id",
        "SELECT count(*) FROM vvi_mow WHERE v MATCH_ANY 'deleted'"])
    sql """INSERT INTO vvi_mow VALUES (1,parse_to_variant('{"s":"new","a":["hello","world"]}'),'upsert')"""
    check("upsert", [
        "SELECT id,v MATCH_ANY 'old',v MATCH_ANY 'new',note FROM vvi_mow WHERE id=1",
        "SELECT count(*) FROM vvi_mow WHERE v MATCH_ANY 'old new'"])
    sql "DELETE FROM vvi_mow WHERE id=2"
    check("delete", [
        "SELECT id FROM vvi_mow WHERE v MATCH_ANY 'deleted' ORDER BY id",
        "SELECT count(*) FROM vvi_mow WHERE v MATCH_ANY 'deleted'"])
    try {
        sql "SET enable_unique_key_partial_update=true"
        sql "SET enable_insert_strict=false"
        sql "INSERT INTO vvi_mow(id,note) VALUES (3,'note changed')"
        check("partial_note", [
            "SELECT id,note,v MATCH_ALL 'hello world' FROM vvi_mow WHERE id=3",
            "SELECT id FROM vvi_mow WHERE v MATCH_ANY 'retained' ORDER BY id"])
        sql """INSERT INTO vvi_mow(id,v) VALUES (1,parse_to_variant('{"s":"replacement"}'))"""
        check("partial_variant", [
            "SELECT id,note,v MATCH_ANY 'replacement',v MATCH_ALL 'hello world' FROM vvi_mow WHERE id=1",
            "SELECT count(*) FROM vvi_mow WHERE v MATCH_ANY 'new replacement'"])
    } finally {
        sql "SET enable_unique_key_partial_update=false"
        sql "SET enable_insert_strict=true"
    }
    def finalQueries = [
        "SELECT id,v MATCH_ANY 'replacement retained',note FROM vvi_mow ORDER BY id",
        "SELECT count(*) FROM vvi_mow WHERE v MATCH_ANY 'old new deleted'"]
    def before = check("before_compaction", finalQueries)
    trigger_and_wait_compaction("vvi_mow", "full", 1800)
    assertEquals(before, check("after_compaction", finalQueries, false))
}
