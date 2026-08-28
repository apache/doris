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

suite("test_lance_vector_search", "p0,external") {
    /*
     * vector_search() exposes Lance vector search as a relation:
     *
     *   SELECT <source columns>, _distance
     *   FROM vector_search(
     *       "table"="<catalog>.<database>.<table>",
     *       "column"="<vector column>",
     *       "query_vector"="[<number>, ...]",
     *       "top_k"="<positive integer>",
     *       ...
     *   )
     *   ORDER BY _distance;
     *
     * The result contains all columns from the source Lance table plus a generated FLOAT column
     * named _distance. SQL does not guarantee relation order, so callers should use an explicit
     * ORDER BY _distance (and preferably a stable tie-breaker) when result order matters. The
     * vector column itself does not have to be projected.
     *
     * Required properties:
     *   table         Fully qualified catalog.database.table name. It must resolve to a Lance
     *                 external table and the user must have SELECT privilege on it.
     *   column        Lance fixed_size_list<float16|float32|float64|uint8|int8> vector column.
     *   query_vector  JSON numeric array. Its dimension must match column, and its values must be
     *                 representable by the vector element type.
     *
     * Optional logical properties:
     *   top_k         Number of rows returned after offset; default 10 and must be positive.
     *   offset        Number of nearest rows skipped inside vector search; default 0. The sum of
     *                 top_k and offset must fit in an unsigned 32-bit integer.
     *   metric        l2, cosine, dot (dot_product is an alias), or hamming. If omitted, Lance
     *                 selects its default metric.
     *   filter        Lance SQL predicate evaluated before candidate selection (prefilter).
     *
     * Optional Lance index tuning properties:
     *   nprobes       Positive IVF partition probe count.
     *   refine_factor Positive candidate refinement/reranking factor.
     *   ef            Positive graph-search candidate width.
     *   use_index     true allows index search; false explicitly requests a flat search.
     *
     * An outer WHERE is deliberately different from the TVF filter property: it is evaluated by
     * Doris after Lance returns Top-K and can therefore reduce the final result below top_k.
     * The current implementation pins one Lance dataset version, uses physical index segments as
     * indexed-search splits, retains uncovered fragments as flat-search splits, and lets Doris
     * merge the split-local candidates with a global Top-N.
     *
     * Fixture: doris.vs_ivf_pq_f32 is generated offline by
     * docker/thirdparties/docker-compose/iceberg/scripts/lance_build_preinstalled_catalog.py.
     * It holds 1024 rows in two fragments; embedding[j] = (row_id - 1) + j with dimension 16,
     * covered by a real IVF_PQ index (4 partitions, 4-bit PQ) whose creation is verified by the
     * generator self-check. For a query equal to the vector of row r, the exact squared L2
     * distance of row n is 16 * (n - r)^2.
     *
     * IVF_PQ is lossy: raw PQ distances are approximations, so every indexed query here uses
     * refine_factor to rerank candidates with exact distances. The agreement between indexed and
     * flat results below is an observed property of this frozen fixture and the pinned Lance
     * version, not an IVF_PQ algorithm guarantee. IVF_FLAT is covered by
     * test_lance_vector_search_ivf_flat and the remaining algorithms by
     * test_lance_vector_search_index_types; the other vector element types and distance metrics
     * are follow-up work for #66495.
     */
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Lance vector search test because the Iceberg MinIO environment is disabled.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_lance_vector_search"
    String tableName = "${catalogName}.doris.vs_ivf_pq_f32"
    // headQuery is exactly row 1's vector and tailQuery is row 1024's, so distances are the
    // deterministic ladder 0, 16, 64, 144, ... with no ties.
    String headQuery = "[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]"
    String tailQuery = "[1023,1024,1025,1026,1027,1028,1029,1030,1031,1032,1033,1034,1035,1036,1037,1038]"
    // boundaryQuery is row 257's vector. On this frozen index row 257 lies next to an IVF
    // partition edge, so part of its true neighbourhood sits in an adjacent partition
    // (pinned by the generator self-check); see the discriminator block below.
    String boundaryQuery = "[256,257,258,259,260,261,262,263,264,265,266,267,268,269,270,271]"
    String indexedTopFive = """vector_search("table"="${tableName}", "column"="embedding", "query_vector"="${headQuery}", "top_k"="5", "metric"="l2", "nprobes"="4", "refine_factor"="10", "use_index"="true")"""
    String flatTopFive = """vector_search("table"="${tableName}", "column"="embedding", "query_vector"="${headQuery}", "top_k"="5", "metric"="l2", "use_index"="false")"""
    String indexedTopTwo = """vector_search("table"="${tableName}", "column"="embedding", "query_vector"="${headQuery}", "top_k"="2", "metric"="l2", "nprobes"="4", "refine_factor"="10", "use_index"="true")"""
    String flatTopTwo = """vector_search("table"="${tableName}", "column"="embedding", "query_vector"="${headQuery}", "top_k"="2", "metric"="l2", "use_index"="false")"""
    String indexedOffset = """vector_search("table"="${tableName}", "column"="embedding", "query_vector"="${headQuery}", "top_k"="2", "offset"="1", "metric"="l2", "nprobes"="4", "refine_factor"="10", "use_index"="true")"""
    String indexedPrefilter = """vector_search("table"="${tableName}", "column"="embedding", "query_vector"="${headQuery}", "top_k"="3", "filter"="category = 'odd'", "metric"="l2", "nprobes"="4", "refine_factor"="10", "use_index"="true")"""
    String indexedTail = """vector_search("table"="${tableName}", "column"="embedding", "query_vector"="${tailQuery}", "top_k"="3", "metric"="l2", "nprobes"="4", "refine_factor"="10", "use_index"="true")"""

    sql """DROP CATALOG IF EXISTS `${catalogName}`"""
    try {
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

        order_qt_vector_table_desc """DESC `${catalogName}`.`doris`.`vs_ivf_pq_f32`"""
        qt_vector_table_rows """
            SELECT count(*), count(DISTINCT row_id), min(row_id), max(row_id)
            FROM `${catalogName}`.`doris`.`vs_ivf_pq_f32`
        """

        // EXPLAIN reports the searched vector column, distance metric, fixed snapshot and Doris
        // physical split plan, while lance-c does not expose its selected index at runtime.
        explain {
            sql("""SELECT row_id, label, _distance FROM ${indexedTopFive} ORDER BY _distance, row_id""")
            contains "externalSearchType=VECTOR"
            contains "lanceVectorColumn=embedding"
            contains "lanceMetric=l2"
            contains "lanceVersion="
            contains "lanceSearchFragments=2"
            contains "lanceSearchUnindexedFragments=0"
            contains "lanceSearchIndexSegments=1"
            contains "lanceSearchIndexFragments=2"
            // The raw query vector must not be echoed into the plan output.
            notContains "[0,1,2,3"
        }

        // nprobes=4 covers all four IVF partitions and refine_factor reranks candidates with
        // exact distances. On this frozen fixture the result equals the flat search.
        qt_indexed_l2_topk """
            SELECT row_id, label, _distance
            FROM ${indexedTopFive}
            ORDER BY _distance, row_id
        """

        // Disable the index explicitly for the exact flat baseline.
        qt_flat_l2_topk """
            SELECT row_id, label, _distance
            FROM ${flatTopFive}
            ORDER BY _distance, row_id
        """

        // Search the opposite end of the dataset. This catches an accidental
        // fragment-scoped search that happens to contain only the first rows.
        qt_indexed_tail """
            SELECT row_id, label, _distance
            FROM ${indexedTail}
            ORDER BY _distance, row_id
        """

        // candidate_k is top_k + offset. The offset is part of vector_search,
        // rather than an outer SQL OFFSET.
        qt_indexed_offset """
            SELECT row_id, label, _distance
            FROM ${indexedOffset}
            ORDER BY _distance, row_id
        """

        // The TVF filter is evaluated before Lance chooses Top-K. Odd-category rows are the
        // even row ids, so the nearest eligible rows are row_id 2, 4 and 6.
        qt_indexed_pre_search_filter """
            SELECT row_id, category, _distance
            FROM ${indexedPrefilter}
            ORDER BY _distance, row_id
        """

        // With use_index=true, one physical index segment covers both fragments and returns the
        // snapshot's two nearest candidates, rows 1 and 2. The Doris postfilter keeps only row 2;
        // it does not ask Lance for a replacement candidate.
        explain {
            sql """SELECT row_id, category, _distance FROM ${indexedTopTwo}
                    WHERE category = 'odd' ORDER BY _distance, row_id"""
            contains "lanceSearchUnindexedFragments=0"
            contains "lanceSearchIndexSegments=1"
            contains "lanceSearchIndexFragments=2"
        }
        qt_post_search_filter_use_index_true """
            SELECT row_id, category, _distance
            FROM ${indexedTopTwo}
            WHERE category = 'odd'
            ORDER BY _distance, row_id
        """

        // With use_index=false, Doris creates one flat-search split per fragment. The two splits
        // contribute rows 1..2 and 513..514; postfilter keeps rows 2 and 514 before global TopN.
        explain {
            sql """SELECT row_id, category, _distance FROM ${flatTopTwo}
                    WHERE category = 'odd' ORDER BY _distance, row_id"""
            contains "lanceSearchUnindexedFragments=2"
            contains "lanceSearchIndexSegments=0"
            contains "lanceSearchIndexFragments=0"
        }
        qt_post_search_filter_use_index_false """
            SELECT row_id, category, _distance
            FROM ${flatTopTwo}
            WHERE category = 'odd'
            ORDER BY _distance, row_id
        """

        // Silent-fallback discriminator. Row 257's true nearest neighbours straddle an IVF
        // partition edge, so a genuine single-partition probe must miss the ones on the far
        // side and differ from flat search even after exact reranking. A pipeline that ignores use_index/nprobes and silently scans
        // flat fails this assertion: on an unindexed table Lance ignores nprobes and
        // returns exactly the flat rows.
        // top_k is 9, not 10: distances here come in symmetric pairs (rows 256-d and
        // 256+d tie), and 9 is the last cut that lands on a complete pair. At 10 the tie
        // group at distance 400 (rows 251 and 261) is split and only one of them fits, so
        // the golden would pin an arbitrary choice and could flip on any change to Lance's
        // top-k selection.
        def boundarySingleProbe = sql """
            SELECT row_id, _distance
            FROM vector_search("table"="${tableName}", "column"="embedding", "query_vector"="${boundaryQuery}", "top_k"="9", "metric"="l2", "nprobes"="1", "refine_factor"="10", "use_index"="true")
            ORDER BY _distance, row_id
        """
        def boundaryFlat = sql """
            SELECT row_id, _distance
            FROM vector_search("table"="${tableName}", "column"="embedding", "query_vector"="${boundaryQuery}", "top_k"="9", "metric"="l2", "use_index"="false")
            ORDER BY _distance, row_id
        """
        assertEquals(9, boundarySingleProbe.size())
        assertEquals(9, boundaryFlat.size())
        // Compare distance sequences rather than row ids: the boundary query is symmetric, so
        // rows r-d and r+d tie at the same distance and either may fill the last slot. Only a
        // genuinely missed neighbour changes the distances.
        def singleProbeDistances = boundarySingleProbe.collect { it[1] }
        def flatDistances = boundaryFlat.collect { it[1] }
        assertFalse(singleProbeDistances.equals(flatDistances),
                "nprobes=1 produced the same distance sequence as the flat search, so the "
                + "single-partition restriction had no effect: the IVF_PQ index was not used "
                + "(silent flat fallback or ignored nprobes). "
                + "nprobes=1 distances=" + singleProbeDistances + " flat=" + flatDistances)

        qt_boundary_single_probe """
            SELECT row_id, label, _distance
            FROM vector_search("table"="${tableName}", "column"="embedding", "query_vector"="${boundaryQuery}", "top_k"="9", "metric"="l2", "nprobes"="1", "refine_factor"="10", "use_index"="true")
            ORDER BY _distance, row_id
        """
        qt_boundary_flat """
            SELECT row_id, label, _distance
            FROM vector_search("table"="${tableName}", "column"="embedding", "query_vector"="${boundaryQuery}", "top_k"="9", "metric"="l2", "use_index"="false")
            ORDER BY _distance, row_id
        """

        // Note on vector-column pruning: no query in this suite projects embedding, so Lance
        // searching an unprojected vector column is exercised by every block above.

        test {
            sql("""SELECT row_id FROM vector_search("table"="${tableName}", "column"="embedding", "query_vector"="[0,0,0]", "top_k"="1")""")
            exception "dimension"
        }

        test {
            sql("""SELECT row_id FROM vector_search("table"="${tableName}", "column"="embedding", "query_vector"="${headQuery}", "top_k"="0")""")
            exception "top_k"
        }
    } finally {
        // sql """DROP CATALOG IF EXISTS `${catalogName}`"""
    }
}
