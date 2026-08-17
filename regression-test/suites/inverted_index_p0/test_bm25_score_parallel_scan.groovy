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

// BM25 idf and avg_dl are properties of the whole collection, so score() must not depend on how
// many scanners the read source was divided into. Collection statistics used to be collected from
// each scanner's own rs_splits, which made the scores drift as soon as parallel scan produced more
// than one scanner per tablet.
suite("test_bm25_score_parallel_scan", "p0") {
    def tableName = "test_bm25_score_parallel_scan"

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            id INT NOT NULL,
            body STRING NULL,
            INDEX idx_body (body) USING INVERTED PROPERTIES (
                "parser" = "english",
                "support_phrase" = "true"
            )
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true",
            "inverted_index_storage_format" = "V3"
        )
    """

    // Several separate loads so the tablet ends up with several rowsets. One rowset per INSERT is
    // what lets the parallel scanner builder hand different rowsets to different scanners -- with a
    // single rowset every scanner would see the whole collection and the bug would stay hidden.
    def rareDocs = (0..<8).collect { "(${it}, 'alpha rare_term beta')" }.join(", ")
    sql "INSERT INTO ${tableName} VALUES ${rareDocs}"
    for (int batch = 0; batch < 6; batch++) {
        def filler = (0..<200).collect {
            def docId = 1000 + batch * 200 + it
            "(${docId}, 'alpha common filler document number ${docId}')"
        }.join(", ")
        sql "INSERT INTO ${tableName} VALUES ${filler}"
    }
    sql "SYNC"

    def scoreRows = { ->
        sql """
            SELECT id, ROUND(s, 6) FROM (
                SELECT id, score() AS s FROM ${tableName}
                WHERE body MATCH_ANY 'rare_term alpha'
                ORDER BY s DESC LIMIT 20
            ) ranked ORDER BY s DESC, id
        """
    }

    // Capture rather than hardcode: the defaults for these have changed before, and restoring a
    // wrong value would leak into whatever runs next on this connection.
    def savedVars = ["enable_parallel_scan", "parallel_scan_max_scanners_count",
                     "parallel_scan_min_rows_per_scanner"].collectEntries { name ->
        [(name): sql("SHOW VARIABLES LIKE '${name}'")[0][1].toString()]
    }

    try {
        // Baseline: one scanner per tablet, so its slice already is the whole collection.
        sql "SET enable_parallel_scan = false"
        def serialScores = scoreRows()
        assertFalse(serialScores.isEmpty(), "serial scan must return rows")

        // Force many scanners over the same data. Every scanner has to derive idf from the whole
        // collection, not from the rowsets it happens to own.
        sql "SET enable_parallel_scan = true"
        sql "SET parallel_scan_max_scanners_count = 16"
        sql "SET parallel_scan_min_rows_per_scanner = 16"
        def parallelScores = scoreRows()

        assertEquals(serialScores, parallelScores,
                "score() changed when the read source was split across scanners, which means "
                        + "collection statistics were collected per scanner instead of per collection")
    } finally {
        savedVars.each { name, value -> sql "SET ${name} = ${value}" }
    }
}
