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

suite("test_lance_vector_search_partial_index", "p0,external") {
    /*
     * A quantized vector index covering the first of two fragments. FE plans one split per index
     * segment plus a flat split for the uncovered fragment, and the Doris TopN above the scan
     * merges them by _distance. An IVF_PQ segment scores its candidates from the compressed
     * codes, so those candidates are rescored against the stored vectors; otherwise the merge
     * would compare them with the exact distances the flat split reports.
     *
     * Fixture: partial_vector_index.lance (see lance_build_partial_vector_index.py). row_id
     * 1..512 in fragment 0 with vec_idx (IVF_PQ, l2, 2 partitions, 2 sub-vectors) over it, then
     * row_id 513..640 appended as fragment 1, which no index covers. vec = [row_id, row_id + 1,
     * row_id + 2, row_id + 3], so the squared L2 distance between rows r and n is 4 * (n - r)^2
     * and every expected distance below is an analytic value, not a recall comparison.
     *
     * The query sits a quarter step past row 512, which puts the true neighbours alternately in
     * the two arms: 512 (0.25), 511 (6.25) and 510 (20.25) are indexed, 513 (2.25), 514 (12.25)
     * and 515 (30.25) are not. The recorded distances are what pins the fix: the qt_partial_mixed
     * and qt_partial_unrefined blocks below hold the same query with the rescoring on and off, so
     * the .out itself carries both scales. On this codebook the unrefined merge puts row 510 at
     * 30.2798 instead of its analytic 20.25, which pushes it past the exactly scored 515, while
     * rows 511 and 512 happen to reconstruct exactly from their codes. Which indexed rows the
     * error reaches, and by how much, is a property of the trained codebook rather than of the
     * data shape; the rescored side is always these analytic constants, which is why that is the
     * side the fix is pinned against. nprobes equals the partition count, so the indexed arm
     * probes every partition and its candidate set does not depend on where kmeans drew its edges.
     */
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Lance partial vector index test because the Iceberg MinIO environment is disabled.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_lance_vector_search_partial_index"
    String tableName = "${catalogName}.`default`.partial_vector_index"
    String query = "[512.25,513.25,514.25,515.25]"
    def search = { String options ->
        """vector_search("table"="${tableName}", "column"="vec", "query_vector"="${query}", """ +
        """"top_k"="6", "metric"="l2"${options})"""
    }
    String originalScannerV2 = (sql """SHOW VARIABLES LIKE 'enable_file_scanner_v2'""")[0][1].toString()
    String originalRefinement = (sql """
        SHOW VARIABLES LIKE 'enable_lance_vector_distance_refinement'""")[0][1].toString()

    sql """DROP CATALOG IF EXISTS `${catalogName}`"""
    try {
        sql """SET enable_file_scanner_v2 = true"""
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

        // The plan is mixed, and FE reports that it rescores the segment's candidates.
        explain {
            sql("SELECT row_id FROM ${search(', "nprobes"="2"')}")
            contains "lanceVectorIndexStatus=USED"
            contains "lanceVersion=3"
            contains "lanceSearchIndexSegments=1"
            contains "lanceSearchIndexFragments=1"
            contains "lanceSearchUnindexedFragments=1"
            contains "lanceSearchRefineDistances=true"
        }

        // The recorded distances are the analytic 4 * (512.25 - row_id)^2 values, which only
        // stored-vector rescoring produces, and the two arms interleave in that order.
        qt_partial_mixed """
            SELECT row_id, _distance FROM ${search(', "nprobes"="2"')}
            ORDER BY _distance, row_id
        """

        // An explicit refine_factor=1 asks for exactly what FE already forced, so this must
        // record the same rows and distances. Removing the forcing breaks the pair.
        qt_partial_refine_one """
            SELECT row_id, _distance FROM ${search(', "nprobes"="2", "refine_factor"="1"')}
            ORDER BY _distance, row_id
        """

        // Recorded as a diagnostic, not as the proof: refinement rescores the candidates the
        // index already chose and does not widen that choice, so an exact scan is not required
        // to return the same rows.
        qt_partial_flat """
            SELECT row_id, _distance FROM ${search(', "use_index"="false"')}
            ORDER BY _distance, row_id
        """

        // Turning the rescoring off is what this merge looked like before it was forced: the
        // indexed arm reports the distances its codes produce while the appended fragment still
        // reports exact ones, so these recorded values come from two different scales. On this
        // codebook that reaches row 510, which lands at 30.2798 rather than 20.25 and so sorts
        // behind the exactly scored 515 instead of ahead of it; the rows whose codes happen to
        // reconstruct exactly keep their analytic values. The index keeps selecting the same
        // candidates, because refinement rescores that selection rather than widening it.
        sql """SET enable_lance_vector_distance_refinement = false"""
        explain {
            sql("SELECT row_id FROM ${search(', "nprobes"="2"')}")
            contains "lanceSearchUnindexedFragments=1"
            contains "lanceSearchRefineDistances=false"
        }
        qt_partial_unrefined """
            SELECT row_id, _distance FROM ${search(', "nprobes"="2"')}
            ORDER BY _distance, row_id
        """
        sql """SET enable_lance_vector_distance_refinement = true"""

        // A quantized index covering every fragment in one segment has no foreign scale to be
        // comparable with. Several physical segments would reintroduce one, which the FE unit
        // tests cover; no preinstalled dataset has a multi-segment vector index.
        explain {
            sql("""SELECT row_id FROM vector_search("table"="${catalogName}.`default`.legacy_vector_index",
                   "column"="vec", "query_vector"="[100.25,101.25,102.25,103.25]", "top_k"="3",
                   "metric"="l2", "nprobes"="2")""")
            contains "lanceVectorIndexStatus=USED"
            contains "lanceSearchIndexSegments=1"
            contains "lanceSearchUnindexedFragments=0"
            contains "lanceSearchRefineDistances=false"
        }

        // The decision reads the index type the manifest reports. search_snapshot_evolved was
        // written as IVF_FLAT, which stores full vectors, but its manifest does not identify the
        // type to the planner - the pre-existing two-sided distance-range optimisation, which
        // keeps the index only for IVF_FLAT, also falls back on it. An unidentified type is
        // rescored rather than trusted, so this mixed plan refines as well. The exemption for a
        // type that is identified is covered by LanceScanNodeTest, which builds the segment
        // metadata directly.
        explain {
            sql("""SELECT row_id FROM vector_search("table"="${catalogName}.`default`.search_snapshot_evolved",
                   "column"="embedding", "query_vector"="[2.25,3.25,4.25,5.25]", "top_k"="3",
                   "metric"="l2", "nprobes"="2")""")
            contains "lanceVectorIndexStatus=USED"
            contains "lanceSearchUnindexedFragments=1"
            contains "lanceSearchRefineDistances=true"
        }
    } finally {
        sql """SET enable_lance_vector_distance_refinement = ${originalRefinement}"""
        sql """SET enable_file_scanner_v2 = ${originalScannerV2}"""
        sql """DROP CATALOG IF EXISTS `${catalogName}`"""
    }
}
