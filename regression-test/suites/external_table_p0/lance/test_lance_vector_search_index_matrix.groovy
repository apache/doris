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

suite("test_lance_vector_search_index_matrix", "p0,external") {
    /*
     * Plan-level coverage for every element type x metric x algorithm cell that the other
     * Lance vector suites do not carry.
     *
     * The sibling suites are the depth tier: one table per representative cell, each with
     * goldens, a closed-form distance ladder where the data shape allows one, and a
     * discriminator proving the search parameter reached the index. That is what shows a cell
     * returns the *right* rows, and it costs roughly 190KB of committed binary per cell.
     *
     * This suite is the breadth tier. doris.vs_index_matrix is a single 64-row table with one
     * vector column per remaining cell, each carrying exactly one index. One column per cell,
     * never several indexes on one column, because only the first index built on a column is
     * reachable: Lance answers the other one with a silent brute-force scan, and Doris gets
     * there differently but ends up the same, since LanceScanNode.selectIndexSegments keeps
     * only the segments of the first index it finds for the column's field id and
     * metricMatches then rejects the query whose metric that index does not carry.
     *
     * What this proves: Doris plans an indexed split for the cell, and a refined indexed
     * search returns exactly the rows an exhaustive scan returns. That is the same equality
     * the IVF_FLAT suites assert, reached here by reranking with refine_factor so a
     * quantizing or graph index can be held to it as well.
     *
     * What it does not prove, and the documentation must not claim: that the distances are
     * right in absolute terms. Both sides of the comparison are computed by the same backend,
     * so a distance kernel that is wrong in both directions passes here. The suites listed
     * above are what pin absolute values, against a closed-form ladder and goldens.
     *
     * There are no goldens on purpose. Every assertion below is a property, so this suite
     * costs nothing to regenerate when the fixture is rebuilt, and the cells it covers can
     * grow without a golden churn.
     *
     * See lance_build_preinstalled_catalog.py (BREADTH_TABLE and check_breadth_table) for how
     * the table is generated and what the generator verifies before committing it.
     */
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Lance index matrix test because the Iceberg MinIO environment is disabled.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_lance_vector_search_index_matrix"
    String tableName = "${catalogName}.doris.vs_index_matrix"
    int topK = 5

    sql """DROP CATALOG IF EXISTS `${catalogName}`"""
    sql """
        CREATE CATALOG `${catalogName}` PROPERTIES (
            "type" = "lance",
            "lance.catalog.type" = "filesystem",
            "warehouse" = "s3://warehouse/lance",
            "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.region" = "us-east-1",
            "use_path_style" = "true"
        )
    """
    sql """SET enable_file_scanner_v2 = true"""

    // The cells under test are read from the table's own schema rather than listed here. A
    // hardcoded list of 44 would be a second copy of the fixture's matrix, free to drift away
    // from it the moment a cell is added or removed; deriving them means a cell that leaves
    // the fixture leaves this suite with it, and one that arrives is picked up automatically.
    // The generator names each column emb__<type>__<metric>__<algorithm>, with a double
    // underscore between the parts because an algorithm name contains single ones.
    def describe = sql """DESC `${catalogName}`.`doris`.`vs_index_matrix`"""
    def cells = []
    for (row : describe) {
        String column = row[0]
        if (!column.startsWith("emb__")) {
            continue
        }
        def parts = column.split("__")
        assertEquals(4, parts.length,
                "${column}: expected emb__<type>__<metric>__<algorithm>, so exactly four "
                + "double-underscore parts, got ${parts.length}")
        cells.add([column: column, elementType: parts[1], metric: parts[2],
                   algorithm: parts[3]])
    }
    // A schema that silently lost vector columns would make every loop below a no-op and the
    // suite would pass having tested nothing. Pinned exactly rather than as a lower bound: the
    // count only changes when buildable_combos() does, which happens when the embedded Lance
    // widens or narrows what it accepts - and that is a thing to notice, not to tolerate.
    assertEquals(44, cells.size(),
            "expected the breadth matrix to carry 44 indexed cells, found " + cells.size()
            + ". Either the fixture was not rebuilt after buildable_combos() moved, or it is "
            + "not the fixture this suite was written for.")

    // The total alone would not notice one cell being swapped for another, so pin the shape of
    // the matrix too: how many cells each element type, metric and algorithm contributes. That
    // is three short maps rather than a 44-entry list, so it still does not duplicate the
    // fixture, but a cell replaced by a different one moves two of these counts.
    def tally = { String axis -> cells.groupBy { it[axis] }.collectEntries { k, v -> [k, v.size()] } }
    assertEquals([f32: 9, f64: 17, f16: 17, u8: 1], tally("elementType"),
            "breadth matrix element-type shape changed: " + tally("elementType"))
    assertEquals([l2: 11, cosine: 15, dot: 17, hamming: 1], tally("metric"),
            "breadth matrix metric shape changed: " + tally("metric"))
    assertEquals([ivf_flat: 5, ivf_sq: 8, ivf_pq: 6,
                  ivf_hnsw_flat: 9, ivf_hnsw_sq: 8, ivf_hnsw_pq: 8], tally("algorithm"),
            "breadth matrix algorithm shape changed: " + tally("algorithm"))
    logger.info("Lance index matrix: ${cells.size()} cells under test")

    // Query vectors are read back out of the table rather than written here. Each element
    // type has its own shape and width - the generator sizes the uint8 thermometer to this
    // table's row count, so hardcoding it would silently desync the moment the table grows -
    // and none of the assertions below need the query to be any particular vector, only to be
    // one the table actually stores.
    def vectorCache = [:]
    def queryVectorOf = { Map cell, int row ->
        String key = "${cell.column}#${row}"
        if (vectorCache.containsKey(key)) {
            return vectorCache[key]
        }
        def rows = sql """SELECT `${cell.column}` FROM ${tableName} WHERE row_id = ${row}"""
        assertEquals(1, rows.size(),
                "${cell.column}: expected exactly one row with row_id ${row}")
        String literal = rows[0][0].toString().replaceAll("\\s+", "")
        assertTrue(literal.startsWith("[") && literal.endsWith("]"),
                "${cell.column}: row ${row} did not come back as a vector literal: ${literal}")
        vectorCache[key] = literal
        return literal
    }

    def search = { Map cell, String query, String nprobes = "4", String extraArgs = '' ->
        String ef = cell.algorithm.contains("hnsw") ? ', "ef"="50"' : ''
        """vector_search("table"="${tableName}", "column"="${cell.column}", """ +
        """"query_vector"="${query}", "top_k"="${topK}", "metric"="${cell.metric}", """ +
        """"nprobes"="${nprobes}", "use_index"="true"${ef}${extraArgs})"""
    }
    def flatSearch = { Map cell, String query ->
        """vector_search("table"="${tableName}", "column"="${cell.column}", """ +
        """"query_vector"="${query}", "top_k"="${topK}", """ +
        """"metric"="${cell.metric}", "use_index"="false")"""
    }

    // Every cell must reach an indexed plan. lanceSearchIndexSegments is the FE's own report
    // of how many physical index segments it planned, so a non-zero count is what separates
    // "the metric was accepted and the index was chosen" from "the query silently ran a flat
    // scan and returned plausible rows anyway".
    // lanceSearchUnindexedFragments=0 is the one thing only this check covers: an index that
    // reached only one of the two fragments would still return correct rows, because the
    // fragment Doris scans without an index contributes correct rows too, and it would still
    // respond to nprobes through the fragment that is indexed. Both checks below would pass
    // while half the column was being scanned unindexed.
    for (Map cell : cells) {
        explain {
            sql("""SELECT row_id, _distance
                   FROM ${search(cell, queryVectorOf(cell, 1))}
                   ORDER BY _distance, row_id""")
            contains "externalSearchType=VECTOR"
            contains "lanceMetric=${cell.metric}"
            contains "lanceSearchFragments=2"
            contains "lanceSearchUnindexedFragments=0"
            contains "lanceSearchIndexSegments=1"
        }
    }

    // Execution-level proof that the index is what answered, which the plan above cannot give
    // and the result check below cannot either. A flat scan has no partitions, so nprobes
    // means nothing to it and cannot change its answer: observing a difference proves the
    // parameter reached partition-based code. The converse does not hold - the true top-k can
    // simply all live in the one probed partition - so try several query rows, one per
    // partition-sized stretch of the table, and take the first that discriminates.
    List<Integer> probeRows = [1, 17, 33, 49]
    for (Map cell : cells) {
        boolean discriminated = false
        for (int row : probeRows) {
            String query = queryVectorOf(cell, row)
            def narrow = sql """SELECT row_id FROM ${search(cell, query, "1")}
                                ORDER BY _distance, row_id"""
            def wide = sql """SELECT row_id FROM ${search(cell, query, "4")}
                              ORDER BY _distance, row_id"""
            if (narrow.collect { it[0] } != wide.collect { it[0] }) {
                discriminated = true
                break
            }
        }
        assertTrue(discriminated,
                "${cell.column}: nprobes=1 returns what nprobes=4 returns at every row in "
                + probeRows + ", so nothing here proves the search reached the index rather "
                + "than falling back to a scan. Check the data shape first: a column whose "
                + "dimensions are largely constant gives kmeans nothing to separate, and then "
                + "no nprobes can restrict anything.")
    }

    // Planning the split is not the same as answering it correctly. Every cell must return
    // exactly the rows an exhaustive scan returns - the same equality the IVF_FLAT suites
    // assert, reached here by reranking with refine_factor so that a quantizing or graph
    // index is held to it too. Without refinement about 18 of these cells legitimately
    // differ, which is a property of the algorithm rather than a defect.
    //
    // refine_factor is 10 here, the widest the generator tries, because that is the value
    // whose margin survives a retrain. The generator measures how narrow each cell could
    // actually go and records it; pinning that tightest value here would pin a zero margin,
    // and it already broke once - 5 held on one set of indexes and failed on the next.
    //
    // No goldens: this is a property, so it survives a fixture rebuild untouched, which is
    // what makes covering this many cells affordable at all. What it does not establish is
    // that the distances are right in absolute terms - both sides are computed by the same
    // backend. The suites listed at the top of this file are what pin absolute values,
    // against a closed-form ladder and committed goldens.
    for (Map cell : cells) {
        String query = queryVectorOf(cell, 1)
        def indexed = sql """
            SELECT row_id, _distance
            FROM ${search(cell, query, "4", ', "refine_factor"="10"')}
            ORDER BY _distance, row_id
        """
        def flat = sql """
            SELECT row_id, _distance
            FROM ${flatSearch(cell, query)}
            ORDER BY _distance, row_id
        """
        assertEquals(topK, indexed.size(),
                "${cell.column}: indexed search returned ${indexed.size()} rows, expected "
                + topK + ". The index segment was planned but did not answer it.")
        assertEquals(flat.collect { it[0] }, indexed.collect { it[0] },
                "${cell.column}: a refined indexed search disagrees with an exhaustive scan. "
                + "indexed=" + indexed + " flat=" + flat)
    }
}
