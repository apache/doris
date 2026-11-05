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

suite("test_variant_values_index_null_predicates", "p0,nonConcurrent") {
    sql "SET enable_sql_cache=false"
    sql "SET enable_query_cache=false"
    sql "SET enable_condition_cache=false"
    sql "SET enable_inverted_index_query_cache=false"
    sql "SET default_variant_enable_nested_group=false"
    sql "DROP TABLE IF EXISTS vvi_null_predicates"
    sql """CREATE TABLE vvi_null_predicates (id INT,v VARIANT,
        INDEX values_idx(v) USING INVERTED PROPERTIES("variant_index_scope"="values","parser"="unicode"))
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1","inverted_index_storage_format"="SNII")"""
    sql """INSERT INTO vvi_null_predicates VALUES
        (1,parse_to_variant('{"s":"hello"}')),(2,parse_to_variant('{"s":"other"}')),
        (3,NULL),(4,parse_to_variant('{}')),
        (5,parse_to_variant('[]')),(6,parse_to_variant('null')),(7,parse_to_variant('{"s":"the"}'))"""
    sql "SYNC"
    qt_null_projection "SELECT id,v,v IS NULL,v MATCH_ANY 'hello',NOT(v MATCH_ANY 'hello') FROM vvi_null_predicates ORDER BY id"
    // Unicode removes the English stopword; TOKENIZE makes the empty-token premise visible.
    qt_stopword_tokens """SELECT TOKENIZE('the', '\"parser\"=\"unicode\"')"""
    // SQL NULL can be rescued by OR TRUE and by NOT(UNKNOWN AND FALSE).
    def predicates = [
        "NOT(v MATCH_ANY 'hello')",
        "(v MATCH_ANY 'hello') OR id=3",
        "NOT((v MATCH_ANY 'hello') AND id=1)",
        "(v MATCH_ANY 'hello') OR NOT(v MATCH_ANY 'hello')"
    ]
    for (String op : ["MATCH_ANY", "MATCH_ALL"]) {
        for (String text : ["", "   ", "!!!", "the"]) {
            predicates.addAll([
                "v ${op} '${text}'",
                "NOT(v ${op} '${text}')",
                "(v ${op} '${text}') OR id=3",
                "(v ${op} '${text}') AND id=3",
                "NOT((v ${op} '${text}') AND id=1)",
                "(v ${op} '${text}') OR NOT(v ${op} '${text}')"
            ])
        }
    }
    def differences = []
    predicates.eachWithIndex { predicate, i ->
        def query = "SELECT id FROM vvi_null_predicates WHERE ${predicate} ORDER BY id"
        sql "SET enable_inverted_index_query=true"
        // Compound predicates may materialize a common MATCH expression for residual evaluation.
        sql "SET enable_match_without_inverted_index=true"
        def indexed = sql(query)
        quickTest("indexed_${i}", query, true)
        sql "SET enable_inverted_index_query=false"
        sql "SET enable_match_without_inverted_index=true"
        def scanned = sql(query)
        quickTest("scan_${i}", query, true)
        if (indexed != scanned) {
            differences.add([predicate, indexed, scanned])
        }
    }
    assertEquals([], differences)
}
