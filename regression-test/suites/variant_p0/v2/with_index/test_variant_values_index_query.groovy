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

suite("test_variant_values_index_query", "p0,nonConcurrent") {
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
    for (String name : ["vvi_query", "vvi_query_unicode", "vvi_query_no_values", "vvi_query_case"]) {
        sql "DROP TABLE IF EXISTS ${name}"
    }
    def baseline = [:]
    def typedBaselines = [(false): [:], (true): [:]]
    def compare = { String tag, String query, Map reference, boolean emit ->
        sql "SET enable_inverted_index_query=true"
        def indexed = sql(query)
        sql "SET enable_inverted_index_query=false"
        assertEquals(indexed, sql(query))
        if (reference.containsKey(tag)) {
            assertEquals(reference[tag], indexed)
        } else {
            reference[tag] = indexed
        }
        if (emit) {
            quickTest(tag, query, true)
        }
        sql "SET enable_inverted_index_query=true"
    }
    for (boolean doc : [false, true]) {
        for (int limit : [0, 1]) {
            sql "SET default_variant_enable_doc_mode=${doc}"
            sql "SET default_variant_max_subcolumns_count=${limit}"
            sql "DROP TABLE IF EXISTS vvi_query"
            // Doc mode rejects declared DATETIME and DECIMAL paths; those values keep their input types.
            def typedTime = doc ? "" : ", 'typed_time':DATETIME, 'price':DECIMAL(10,2)"
            sql """CREATE TABLE vvi_query (id INT,
                v VARIANT<'typed_int':INT, 'typed_string':STRING${typedTime}, 'ratio':FLOAT, 'arr':ARRAY<STRING>>,
                INDEX values_idx(v) USING INVERTED PROPERTIES("variant_index_scope"="values","parser"="english"),
                INDEX path_idx(v) USING INVERTED PROPERTIES("parser"="english"))
                DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES("replication_num"="1","disable_auto_compaction"="true",
                           "inverted_index_storage_format"="SNII")"""
            sql """INSERT INTO vvi_query VALUES
                (1,parse_to_variant('{"s":"HELLO world","obj":{"s":"nested"},"n":42,"ok":true}')),
                (2,parse_to_variant('["hello",["world"],{"s":"arrayleaf"},null]')),
                (3,parse_to_variant('{"keyonly":"other","obj":"hello"}')),
                (4,parse_to_variant('{}')),
                (5,parse_to_variant('[]')),
                (6,parse_to_variant('{"s":null}')),
                (7,NULL),
                (8,parse_to_variant('{"n":18446744073709551615}')),
                (9,parse_to_variant('{"typed_int":"123","typed_string":456,"typed_time":"2026-01-02 03:04:05","price":"19.90","ratio":0.1}')),
                (10,parse_to_variant('{"a":"hidden","a.b":"visible"}')),
                (11,parse_to_variant('"scalarroot"')),
                (12,parse_to_variant('{"arr":["apache","doris"],"s":"apache doris"}')),
                (13,parse_to_variant('{"fraction":0.125}')),
                (14,parse_to_variant('{"integral":42.0}')),
                (15,parse_to_variant('{"zero":-0.0}')),
                (16,parse_to_variant('{"ok":false}')),
                (17,parse_to_variant('{"signed":-17}'))"""
            sql "SYNC"
            def queries = [
                any: "SELECT id FROM vvi_query WHERE v MATCH 'hello world' ORDER BY id",
                all: "SELECT id FROM vvi_query WHERE v MATCH_ALL 'hello world' ORDER BY id",
                containers: "SELECT id FROM vvi_query WHERE v MATCH_ANY 'nested arrayleaf scalarroot' ORDER BY id",
                // Separate numeric predicates prevent another matching leaf from hiding a missing token.
                integer: "SELECT id FROM vvi_query WHERE v MATCH_ANY '42' ORDER BY id",
                negative: "SELECT id FROM vvi_query WHERE v MATCH_ANY '-17' ORDER BY id",
                decimal: "SELECT id FROM vvi_query WHERE v MATCH_ANY '18446744073709551615' ORDER BY id",
                fraction: "SELECT id FROM vvi_query WHERE v MATCH_ANY '0.125' ORDER BY id",
                zero: "SELECT id FROM vvi_query WHERE v MATCH_ANY '0' ORDER BY id",
                booleans: "SELECT id,v MATCH_ANY 'true',v MATCH_ANY 'false' FROM vvi_query WHERE id IN (1,16) ORDER BY id",
                nulls: "SELECT id,v MATCH_ANY 'hello',NOT(v MATCH_ANY 'hello'),v MATCH_ANY '',v MATCH_ALL '',v MATCH_ANY NULL FROM vvi_query WHERE id IN (1,4,5,6,7) ORDER BY id",
                visibility: "SELECT id,v MATCH_ANY 'keyonly',v MATCH_ANY 'hidden',v MATCH_ANY 'visible' FROM vvi_query WHERE id IN (3,10) ORDER BY id",
                count: "SELECT count(*) FROM vvi_query WHERE v MATCH_ALL 'hello world'",
                alias: "SELECT id FROM (SELECT id,v AS root_alias FROM vvi_query) s WHERE root_alias MATCH_ANY 'hello' ORDER BY id",
                subpath: "SELECT id,v['obj'] MATCH_ANY 'hello',CAST(v['obj'] AS STRING) MATCH_ANY 'hello' FROM vvi_query WHERE id IN (1,3) ORDER BY id",
                explode: "SELECT id,element MATCH_ANY 'apache',CAST(element AS STRING) MATCH_ANY 'apache' FROM vvi_query LATERAL VIEW explode_variant_array(v['arr']) e AS element ORDER BY id,CAST(element AS STRING)",
                path_phrase: "SELECT id FROM vvi_query WHERE v['s'] MATCH_PHRASE 'apache doris' ORDER BY id",
                analyzer: "SELECT id FROM vvi_query WHERE v MATCH_ANY 'hello' USING ANALYZER english ORDER BY id",
                typed: "SELECT id,v MATCH_ANY '123',v MATCH_ANY '456',v MATCH_ALL '0.1',CAST(v['typed_int'] AS INT),CAST(v['typed_string'] AS STRING),CAST(v['typed_time'] AS DATETIME) FROM vvi_query WHERE id=9"
            ]
            boolean first = !doc && limit == 0
            queries.each { tag, query -> compare(tag, query, baseline, first) }
            // WHERE exercises index filtering; hidden ancestors must also stay hidden under NOT.
            ["v MATCH_ANY 'hidden'": [], "v MATCH_ANY 'visible'": [[10]],
             "NOT(v MATCH_ANY 'hidden')": [[10]]].each { predicate, expected ->
                def query = "SELECT id FROM vvi_query WHERE id=10 AND ${predicate} ORDER BY id"
                for (boolean indexed : [true, false]) {
                    sql "SET enable_inverted_index_query=${indexed}"
                    assertEquals(expected, sql(query))
                }
            }
            sql "SET enable_inverted_index_query=true"
            // Only these two declared types differ in DOC mode; still compare both limits per mode.
            compare("typed_doc_${doc}",
                    "SELECT id,v MATCH_ALL '19.9',v MATCH_ANY '2026' FROM vvi_query WHERE id=9",
                    typedBaselines[doc], limit == 0)
            // SEARCH cannot run without inverted indexes; compare it across all four layouts.
            def search = "SELECT id FROM vvi_query WHERE SEARCH('v.s:hello') ORDER BY id"
            if (first) {
                baseline.search = sql(search)
                quickTest("search", search, true)
            } else {
                assertEquals(baseline.search, sql(search))
            }
            for (String op : ["MATCH_PHRASE", "MATCH_PHRASE_PREFIX", "MATCH_PHRASE_EDGE", "MATCH_REGEXP"]) {
                test {
                    sql "SELECT id FROM vvi_query WHERE v ${op} 'hello world'"
                    exception "VARIANT root column supports only MATCH, MATCH_ANY, and MATCH_ALL."
                }
            }
            test {
                sql "SELECT id FROM vvi_query WHERE v MATCH_ANY 'hello' USING ANALYZER unicode"
                exception "VARIANT root MATCH requires a values index matching its analyzer"
            }
            test {
                sql "SELECT id,score() FROM vvi_query WHERE v MATCH_ANY 'hello'"
                exception "score() is not supported with a MATCH served by a VARIANT values index"
            }
            for (String query : [
                "SELECT id FROM vvi_query WHERE CAST(v AS STRING) MATCH_ANY 'hello'",
                "SELECT id FROM (SELECT id,v FROM vvi_query UNION ALL SELECT id,v FROM vvi_query) s WHERE v MATCH_ANY 'hello'"]) {
                test {
                    sql query
                    exception "VARIANT root column does not support MATCH predicates."
                }
            }
        }
    }
    sql "SET default_variant_enable_doc_mode=false"
    sql "SET default_variant_max_subcolumns_count=0"
    sql """CREATE TABLE vvi_query_unicode (id INT, v VARIANT,
        INDEX values_idx(v) USING INVERTED PROPERTIES("variant_index_scope"="values","parser"="unicode"))
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1","disable_auto_compaction"="true","inverted_index_storage_format"="SNII")"""
    // A single insert/segment makes the cumulative debug-point filtered-row count deterministic.
    sql """INSERT INTO vvi_query_unicode VALUES
        (1,parse_to_variant('{"s":"HELLO café"}')),(2,parse_to_variant('{"s":"other"}'))"""
    sql "SYNC"
    compare("unicode_all", "SELECT id FROM vvi_query_unicode WHERE v MATCH_ALL 'hello café' ORDER BY id", [:], true)
    def point = "segment_iterator.inverted_index.filtered_rows"
    try {
        GetDebugPoint().enableDebugPointForAllBEs(point, [filtered_rows: 1])
        qt_index_participation "SELECT id FROM vvi_query_unicode WHERE v MATCH_ANY 'hello' ORDER BY id"
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs(point)
    }
    // Explicit user casing still applies, independently of the ordinary path-index scope.
    sql """CREATE TABLE vvi_query_case (id INT,v VARIANT,
        INDEX values_idx(v) USING INVERTED PROPERTIES("variant_index_scope"="values","parser"="english","lower_case"="false"),
        INDEX path_idx(v) USING INVERTED PROPERTIES("parser"="english","lower_case"="false"))
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1","inverted_index_storage_format"="SNII")"""
    sql """INSERT INTO vvi_query_case VALUES (1,parse_to_variant('{"obj":{"s":"HELLO"}}'))"""
    compare("explicit_case", "SELECT id,v MATCH_ANY 'hello',v MATCH_ANY 'HELLO',v['obj']['s'] MATCH_ANY 'HELLO' FROM vvi_query_case ORDER BY id", [:], true)
    // SEARCH requires ordinary path indexes; a values index alone cannot serve it.
    test {
        sql "SELECT id FROM vvi_query_unicode WHERE SEARCH('v.s:hello')"
        exception "has no inverted index"
    }
    sql """CREATE TABLE vvi_query_no_values (id INT,v VARIANT,
        INDEX path_idx(v) USING INVERTED PROPERTIES("parser"="english"))
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1","inverted_index_storage_format"="SNII")"""
    sql "INSERT INTO vvi_query_no_values SELECT id,v FROM vvi_query"
    def searchQuery = "SELECT id FROM vvi_query WHERE SEARCH('v.s:hello') ORDER BY id"
    def ordinarySearch = "SELECT id FROM vvi_query_no_values WHERE SEARCH('v.s:hello') ORDER BY id"
    assertEquals(sql(searchQuery), sql(ordinarySearch))
    assertEquals(sql("SELECT id,v MATCH_ANY 'hello' FROM vvi_query_no_values ORDER BY id"),
                 sql("SELECT id,CAST(v AS STRING) MATCH_ANY 'hello' FROM vvi_query_no_values ORDER BY id"))
    test {
        sql "SELECT id FROM vvi_query_no_values WHERE v MATCH_ANY 'hello'"
        exception "VARIANT root column does not support MATCH predicates."
    }
    sql "DROP TABLE IF EXISTS vvi_query_subpath_alias"
    sql """CREATE TABLE vvi_query_subpath_alias (id INT,v VARIANT,
        INDEX values_idx(v) USING INVERTED PROPERTIES("variant_index_scope"="values","parser"="english"),
        INDEX path_idx(v) USING INVERTED PROPERTIES("parser"="none"))
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1","inverted_index_storage_format"="SNII")"""
    sql """INSERT INTO vvi_query_subpath_alias VALUES
        (1,parse_to_variant('{"obj":{"s":"hello"}}')),(2,parse_to_variant('{"obj":"hello"}'))"""
    for (boolean prune : [false, true]) {
        sql "SET enable_prune_nested_column=${prune}"
        for (boolean indexed : [true, false]) {
            sql "SET enable_inverted_index_query=${indexed}"
            def direct = sql "SELECT id FROM vvi_query_subpath_alias WHERE v['obj'] MATCH_ANY 'hello' ORDER BY id"
            assertEquals([[2]], direct)
            assertEquals(direct, sql("SELECT id FROM (SELECT id,v['obj'] AS o FROM vvi_query_subpath_alias) s WHERE o MATCH_ANY 'hello' ORDER BY id"))
        }
    }
}
