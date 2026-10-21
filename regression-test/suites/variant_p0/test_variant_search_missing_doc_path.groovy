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

// Different rowsets have different doc paths. Unrelated doc values must not make a
// proven-missing path unavailable, and NOT must complement the missing leaf before union.
suite("test_variant_search_missing_doc_path", "p0") {
    // V1 INSERT parses JSON with the table's properties; parse_to_variant returns Variant(0),
    // which branch-4.2 cannot cast to the table's Variant(2048). V2 needs explicit JSON parsing.
    def variantV2Function = getFeConfig("enable_variant_v2").toBoolean() ? "parse_to_variant" : ""
    sql "DROP TABLE IF EXISTS test_variant_search_missing_doc_path"
    sql """
        CREATE TABLE test_variant_search_missing_doc_path (
            id INT,
            v VARIANT<PROPERTIES(
                "variant_enable_doc_mode"="true",
                "variant_doc_materialization_min_rows"="0"
            )>,
            INDEX idx_v(v) USING INVERTED PROPERTIES("parser"="unicode")
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES(
            "replication_num"="1",
            "inverted_index_storage_format"="V2",
            "disable_auto_compaction"="true"
        )
    """
    sql """INSERT INTO test_variant_search_missing_doc_path VALUES
        (1, ${variantV2Function}('{"a":"hit"}')), (2, NULL), (3, ${variantV2Function}('{}'))"""
    sql """INSERT INTO test_variant_search_missing_doc_path VALUES
        (4, ${variantV2Function}('{"b":"hit"}'))"""

    ["standard", "lucene"].each { mode ->
        [false, true].each { cache ->
            sql "SET enable_inverted_index_query_cache = ${cache}"
            // Repeating with cache enabled checks both its first fill and its warm read.
            ["first", "repeat"].each { round ->
                def prefix = "${mode}_${cache}_${round}"
                def options = '{"mode":"' + mode + '","default_operator":"OR"}'
                [
                    "a": "v.a:hit",
                    "b": "v.b:hit",
                    "or": "v.a:hit OR v.b:hit",
                    "and_not": "v.a:hit AND NOT v.b:hit",
                    "not_b": "NOT v.b:hit",
                    "missing": "v.missing:hit",
                    "not_missing": "NOT v.missing:hit",
                    "prefix_b": "v.b:h*"
                ].each { label, dsl ->
                    quickTest("${prefix}_${label}", """
                        SELECT id FROM test_variant_search_missing_doc_path
                        WHERE SEARCH('${dsl}', '${options}') ORDER BY id
                    """)
                }
                quickTest("${prefix}_scored_or", """
                    SELECT id, s FROM (
                        SELECT id, score() AS s FROM test_variant_search_missing_doc_path
                        WHERE SEARCH('v.a:hit OR v.b:hit', '${options}')
                        ORDER BY s DESC LIMIT 100
                    ) scored ORDER BY id
                """)
                quickTest("${prefix}_external_not_b", """
                    SELECT id FROM test_variant_search_missing_doc_path
                    WHERE NOT SEARCH('v.b:hit', '${options}') ORDER BY id
                """)
            }
        }
    }

    // This field exists only in doc storage because its row count is below the materialization
    // threshold. It has no executable leaf index and must error instead of returning no matches.
    sql "DROP TABLE IF EXISTS test_variant_search_unindexed_doc_path"
    sql """
        CREATE TABLE test_variant_search_unindexed_doc_path (
            id INT,
            v VARIANT<PROPERTIES(
                "variant_enable_doc_mode"="true",
                "variant_doc_materialization_min_rows"="1000"
            )>,
            INDEX idx_v(v) USING INVERTED PROPERTIES("parser"="unicode")
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES(
            "replication_num"="1",
            "inverted_index_storage_format"="V2"
        )
    """
    sql """INSERT INTO test_variant_search_unindexed_doc_path VALUES
        (1, ${variantV2Function}('{"present":"hit"}'))"""
    ["v.present:hit", "NOT v.present:hit"].each { dsl ->
        test {
            sql "SELECT id FROM test_variant_search_unindexed_doc_path WHERE SEARCH('${dsl}')"
            exception "SEARCH index unavailable for Variant field 'v.present'"
        }
    }
}
