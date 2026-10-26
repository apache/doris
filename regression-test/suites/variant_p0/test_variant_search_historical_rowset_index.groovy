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

// CREATE INDEX changes table metadata without building an index for historical rowsets.
// Verify the stored document kinds and extracted values so JSON-looking scalar strings
// cannot satisfy the historical subpath fixture.
suite("test_variant_search_historical_rowset_index", "p0") {
    // V1 INSERT parses JSON with the table's properties; parse_to_variant returns Variant(0),
    // which this release branch cannot cast to the table's Variant(2048). V2 needs explicit JSON parsing.
    def variantV2Function = getFeConfig("enable_variant_v2").toBoolean() ? "parse_to_variant" : ""
    // Cloud requires this flag and a non-tokenized index for metadata-only creation.
    // A tokenized index uses full schema change and rebuilds the historical rowsets.
    sql "SET enable_add_index_for_new_data = true"
    sql "DROP TABLE IF EXISTS test_variant_search_historical_rowset_index"
    sql """
        CREATE TABLE test_variant_search_historical_rowset_index (
            id INT,
            title TEXT,
            v VARIANT<PROPERTIES(
                "variant_enable_doc_mode"="false"
            )>,
            INDEX idx_title(title) USING INVERTED
                PROPERTIES("parser"="unicode", "support_phrase"="true")
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES(
            "replication_num"="1",
            "inverted_index_storage_format"="V2",
            "disable_auto_compaction"="true"
        )
    """
    sql """INSERT INTO test_variant_search_historical_rowset_index VALUES
        (1, 'miss', ${variantV2Function}('{"missing":"hit"}')),
        (2, 'hit', ${variantV2Function}('{}')),
        (3, 'miss', ${variantV2Function}('{"missing":"miss"}')),
        (4, 'hit', ${variantV2Function}('{"missing":"hit"}'))"""
    sql "SYNC"
    sql """CREATE INDEX idx_v ON test_variant_search_historical_rowset_index(v)
        USING INVERTED PROPERTIES("parser"="none")"""
    sql "SYNC"

    // V1 variant_type reports a field-type schema, while V2 reports the value kind.
    // Inspect the JSON value kind in both modes, preserving the object-versus-string guard.
    qt_historical_values """
        SELECT id, LOWER(JSON_TYPE(CAST(v AS JSON), '\$')), CAST(v['missing'] AS STRING)
        FROM test_variant_search_historical_rowset_index ORDER BY id
    """
    qt_historical_row_matches """
        SELECT id FROM test_variant_search_historical_rowset_index
        WHERE CAST(v['missing'] AS STRING) = 'hit' ORDER BY id
    """
    qt_historical_title """
        SELECT id FROM test_variant_search_historical_rowset_index
        WHERE SEARCH('title:hit') ORDER BY id
    """

    def checkUnavailable = {
        ["standard", "lucene"].each { mode ->
            def options = '{"mode":"' + mode + '","default_operator":"OR"}'
            [false, true].each { cache ->
                sql "SET enable_inverted_index_query_cache = ${cache}"
                // A failed evaluation must not leave a partial result in the cache.
                ["cold", "repeat"].each { round ->
                    ["v.missing:hit", "title:hit OR v.missing:hit",
                     "NOT v.missing:hit", "v.missing:h*"].each { dsl ->
                        test {
                            sql """SELECT id FROM test_variant_search_historical_rowset_index
                                WHERE SEARCH('${dsl}', '${options}') ORDER BY id"""
                            exception "SEARCH index unavailable for Variant field 'v.missing'"
                        }
                    }
                    ["v.missing:hit", "title:hit OR v.missing:hit"].each { dsl ->
                        test {
                            sql """SELECT id, score() AS s
                                FROM test_variant_search_historical_rowset_index
                                WHERE SEARCH('${dsl}', '${options}')
                                ORDER BY s DESC LIMIT 20"""
                            exception "SEARCH index unavailable for Variant field 'v.missing'"
                        }
                    }
                }
            }
        }
    }
    checkUnavailable()

    sql """INSERT INTO test_variant_search_historical_rowset_index VALUES
        (5, 'miss', ${variantV2Function}('{"missing":"hit"}')),
        (6, 'miss', ${variantV2Function}('{"missing":"miss"}'))"""
    sql "SYNC"
    qt_mixed_values """
        SELECT id, LOWER(JSON_TYPE(CAST(v AS JSON), '\$')), CAST(v['missing'] AS STRING)
        FROM test_variant_search_historical_rowset_index ORDER BY id
    """
    qt_mixed_row_matches """
        SELECT id FROM test_variant_search_historical_rowset_index
        WHERE title = 'hit' OR CAST(v['missing'] AS STRING) = 'hit' ORDER BY id
    """
    checkUnavailable()

    ["standard", "lucene"].each { mode ->
        def options = '{"mode":"' + mode + '","default_operator":"OR"}'
        [false, true].each { cache ->
            sql "SET enable_inverted_index_query_cache = ${cache}"
            ["cold", "repeat"].each { round ->
                def prefix = "${mode}_${cache}_${round}"
                // Pruning the historical rowset leaves the newly written, executable index.
                quickTest("${prefix}_new_rows", """
                    SELECT id FROM test_variant_search_historical_rowset_index
                    WHERE id >= 5 AND SEARCH('v.missing:hit', '${options}') ORDER BY id
                """)
                quickTest("${prefix}_new_rows_scored", """
                    SELECT id FROM (
                        SELECT id, score() AS s
                        FROM test_variant_search_historical_rowset_index
                        WHERE id >= 5 AND SEARCH('v.missing:hit', '${options}')
                        ORDER BY s DESC LIMIT 20
                    ) scored ORDER BY id
                """)
                // An absent path remains a valid empty match, including in the old rowset.
                quickTest("${prefix}_absent", """
                    SELECT id FROM test_variant_search_historical_rowset_index
                    WHERE SEARCH('v.absent:hit', '${options}') ORDER BY id
                """)
                quickTest("${prefix}_not_absent", """
                    SELECT id FROM test_variant_search_historical_rowset_index
                    WHERE SEARCH('NOT v.absent:hit', '${options}') ORDER BY id
                """)
            }
        }
    }
}
