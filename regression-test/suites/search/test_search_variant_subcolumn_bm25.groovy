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

/**
 * Test score() over SEARCH on a VARIANT subcolumn (DORIS-29489).
 *
 * A scored SEARCH clause is executed against an inverted index, but BM25 needs the collection
 * statistics the scan collects before the read starts. The collector sees only the clause's
 * field name (e.g. "v.content"), which is not a column of the tablet schema while the VARIANT
 * path is not materialized, so it has to resolve the subcolumn to its parent VARIANT column and
 * to the index that serves the path. When that resolution is missing, statistics are collected
 * for no field at all and the read fails with
 * "Index statistics collection failed: Not such column <uid>.<parent>.<path>".
 *
 * The suite covers both ways a VARIANT path gets its index:
 *   1. a plain index on the parent VARIANT column, inherited by the path, and
 *   2. a field_pattern index declared for the path in the table DDL.
 */
suite("test_search_variant_subcolumn_bm25", "p0") {
    def variantV2Function = "parse_to_variant"

    sql """ set enable_common_expr_pushdown = true """
    sql """ set enable_match_without_inverted_index = false """
    sql """ set default_variant_enable_typed_paths_to_sparse = false """
    sql """ set default_variant_enable_doc_mode = false """

    sql "DROP TABLE IF EXISTS test_search_variant_subcolumn_bm25"
    sql """
        CREATE TABLE test_search_variant_subcolumn_bm25 (
            id INT NOT NULL,
            content TEXT NULL,
            v VARIANT NULL,
            INDEX idx_content(content) USING INVERTED PROPERTIES(
                "parser" = "english"
            ),
            INDEX idx_v(v) USING INVERTED PROPERTIES(
                "parser" = "english",
                "support_phrase" = "true"
            )
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true"
        )
    """

    sql """ INSERT INTO test_search_variant_subcolumn_bm25 VALUES
            (1, 'Apple Apple Banana Banana Banana', ${variantV2Function}('{"content":"Apple Apple Banana Banana Banana"}')),
            (2, 'Apple Banana', ${variantV2Function}('{"content":"Apple Banana"}')),
            (3, 'Apple Orange', ${variantV2Function}('{"content":"Apple Orange"}')),
            (4, 'Green Apple Fresh Banana', ${variantV2Function}('{"content":"Green Apple Fresh Banana"}')),
            (5, 'Banana Orange', ${variantV2Function}('{"content":"Banana Orange"}')),
            (6, 'Pear Grape', ${variantV2Function}('{"content":"Pear Grape"}'))
    """
    sql "sync"

    // Terms equal in both rows carry the same BM25 score, so read the scores by document id
    // instead of relying on the order among tied documents.
    def scoresById = { rows ->
        def result = [:]
        for (row in rows) {
            result[row[0] as int] = Double.parseDouble(row[1].toString())
        }
        return result
    }

    // A scored SEARCH on the VARIANT path. This is the query reported as
    // "Index statistics collection failed: Not such column" before the collector resolved the
    // VARIANT subcolumn through its parent index.
    def variantRows = sql """
        SELECT id, score() AS bm25_score FROM test_search_variant_subcolumn_bm25
        WHERE search('v.content:apple', '{"default_operator":"AND","mode":"lucene","minimum_should_match":0}')
        ORDER BY bm25_score DESC LIMIT 100
    """
    assertEquals(4, variantRows.size())
    def variantScores = scoresById(variantRows)
    assertEquals([1, 2, 3, 4], variantScores.keySet().sort())
    assertTrue(variantScores.values().every { it > 0.0 })
    def orderedScores = variantRows.collect { Double.parseDouble(it[1].toString()) }
    assertEquals(orderedScores.sort(false).reverse(), orderedScores)

    // The same analyzer serves the plain TEXT column, so the collection statistics, and
    // therefore the BM25 scores, must be the same for the same documents.
    def textRows = sql """
        SELECT id, score() AS bm25_score FROM test_search_variant_subcolumn_bm25
        WHERE search('content:apple', '{"mode":"lucene"}')
        ORDER BY bm25_score DESC LIMIT 100
    """
    assertEquals(variantScores, scoresById(textRows))

    // A phrase clause is scored from the VARIANT path's positions and norms, which also asks
    // the collector for that path's statistics.
    def phraseRows = sql """
        SELECT id, score() AS bm25_score FROM test_search_variant_subcolumn_bm25
        WHERE search('v.content:"apple banana"', '{"mode":"lucene"}')
        ORDER BY bm25_score DESC LIMIT 100
    """
    assertEquals(2, phraseRows.size())
    assertEquals([1, 2], scoresById(phraseRows).keySet().sort())
    assertTrue(scoresById(phraseRows).values().every { it > 0.0 })

    // A VARIANT path whose index comes from a field_pattern declaration is collected through
    // the schema template instead of through the parent index.
    sql "DROP TABLE IF EXISTS test_search_variant_subcolumn_bm25_field_pattern"
    sql """
        CREATE TABLE test_search_variant_subcolumn_bm25_field_pattern (
            id INT NOT NULL,
            v VARIANT<
                MATCH_NAME 'host' : text,
                PROPERTIES("variant_max_subcolumns_count" = "0")
            > NULL,
            INDEX idx_v_host (v) USING INVERTED PROPERTIES(
                "parser" = "english",
                "support_phrase" = "true",
                "field_pattern" = "host"
            )
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true"
        )
    """

    sql """ INSERT INTO test_search_variant_subcolumn_bm25_field_pattern VALUES
            (1, ${variantV2Function}('{"host":"alpha database server"}')),
            (2, ${variantV2Function}('{"host":"beta server cluster"}')),
            (3, ${variantV2Function}('{"other":"alpha"}'))
    """
    sql "sync"

    def fieldPatternRows = sql """
        SELECT id, score() AS bm25_score FROM test_search_variant_subcolumn_bm25_field_pattern
        WHERE search('v.host:alpha', '{"mode":"lucene"}')
        ORDER BY bm25_score DESC LIMIT 100
    """
    assertEquals(1, fieldPatternRows.size())
    assertEquals(1, fieldPatternRows[0][0] as int)
    assertTrue(Double.parseDouble(fieldPatternRows[0][1].toString()) > 0.0)
}
