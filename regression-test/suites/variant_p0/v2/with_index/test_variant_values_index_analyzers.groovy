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

suite("test_variant_values_index_analyzers", "p0,nonConcurrent") {
    sql "SET enable_sql_cache=false"
    sql "SET enable_query_cache=false"
    sql "SET enable_condition_cache=false"
    sql "SET enable_inverted_index_query_cache=false"
    sql "SET default_variant_enable_doc_mode=false"
    sql "SET default_variant_enable_nested_group=false"
    for (String parser : ["english", "unicode", "chinese", "standard"]) {
        sql "DROP TABLE IF EXISTS vvi_analyzers"
        sql """CREATE TABLE vvi_analyzers (id INT,v VARIANT,
            INDEX values_idx(v) USING INVERTED PROPERTIES("variant_index_scope"="values","parser"="${parser}",
                "char_filter_type"="char_replace","char_filter_pattern"="_","char_filter_replacement"="x"))
            DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_num"="1","inverted_index_storage_format"="SNII")"""
        sql """INSERT INTO vvi_analyzers VALUES
            (1,parse_to_variant('{"a":"HELLO_WORLD","b":"中文分词"}')),
            (2,parse_to_variant('["hello",{"s":"world"}]')),
            (3,parse_to_variant('{"s":"hello"}')),(4,NULL)"""
        sql "SYNC"
        ["v MATCH_ALL 'hello world'", "v MATCH_ALL 'hello hello world'",
         "v MATCH_ANY 'hello_world'", "v MATCH_ANY '中文'"].eachWithIndex { predicate, i ->
            def query = "SELECT id FROM vvi_analyzers WHERE ${predicate} ORDER BY id"
            sql "SET enable_inverted_index_query=true"
            sql "SET enable_match_without_inverted_index=false"
            def indexed = sql(query)
            quickTest("${parser}_indexed_${i}", query, true)
            sql "SET enable_inverted_index_query=false"
            sql "SET enable_match_without_inverted_index=true"
            quickTest("${parser}_scan_${i}", query, true)
            assertEquals(indexed, sql(query))
        }
    }
}
