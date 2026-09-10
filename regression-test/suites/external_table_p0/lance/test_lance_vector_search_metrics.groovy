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

suite("test_lance_vector_search_metrics", "p0,external") {
    /*
     * vector_search() against indexes built with cosine and dot. The other vector suites are
     * all L2, and an L2 index cannot stand in for these: Doris plans an indexed split only
     * when the requested metric equals the metric the index was built with
     * (LanceScanNode.metricMatches), so the cosine and dot branches of that comparison have
     * never run against a real index. That is what this suite covers - it is a Doris
     * planning path, not only a Lance capability claim.
     *
     * Three tables, all 1024 rows in two fragments with 16-dim Float32 embeddings:
     *
     *   vs_ivf_flat_f32_cosine   IVF_FLAT, cosine - exact, so full probe must equal flat
     *   vs_ivf_pq_f32_cosine     IVF_PQ, cosine   - lossy, queried with refine_factor
     *   vs_ivf_pq_f32_dot        IVF_PQ, dot      - lossy, queried with refine_factor
     *
     * These tables do NOT hold the collinear embedding[j] = (row_id - 1) + j data the L2
     * suites use, because that shape is degenerate under both of these metrics: under dot
     * every query returns the same highest-norm rows regardless of the query, and under
     * cosine all row directions converge so the top distances collapse to 0.0 and the
     * ranking becomes arbitrary tie-breaking. Instead each row sits on a quasi-uniform
     * direction with an independently varying norm, which the fixture generator verifies is
     * tie-free and ranks differently under l2, cosine and dot. So there is no closed-form
     * distance ladder here; the goldens record what the frozen fixture produces, and the
     * assertions below check properties rather than hand-derived numbers.
     *
     * See lance_build_preinstalled_catalog.py for how the data is generated and what the
     * generator verifies about each index before it is committed.
     */
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Lance vector metric test because the Iceberg MinIO environment is disabled.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_lance_vector_search_metrics"

    // Row 1's stored vector, and row 256's. Both are the generator's rounded values, so a
    // query equal to one of them puts that row at distance 0 under cosine.
    String headQuery = "[0.5212,-1.0089,1.0115,-0.8052,0.9277,-0.6251,0.7094,0.7868,-0.9736,0.6707,-0.4194,0.5045,0.5806,-0.3585,-0.7997,0.9973]"
    String boundaryQuery = "[0.1835,0.4286,0.3099,0.7048,0.2626,0.1009,-0.072,-0.5285,-0.7581,-0.4567,0.6234,0.704,0.7249,-0.7313,0.2246,-0.7363]"

    // One complete record per table rather than maps keyed by the same table names: with
    // parallel maps, a table added to one and forgotten in another interpolates a null
    // straight into the vector_search argument list below and fails as an opaque parse error
    // instead of as the missing coverage it is. The guard below makes a partial record fail
    // by name.
    //
    // refine carries the refinement clause: only the quantized tables need it, because
    // IVF_FLAT already answers from the original vectors and adding refine_factor there would
    // hide what the exactness check proves.
    Map<String, Map<String, String>> tables = [
            "vs_ivf_flat_f32_cosine": [metric: "cosine", refine: ""],
            "vs_ivf_pq_f32_cosine"  : [metric: "cosine", refine: ', "refine_factor"="10"'],
            "vs_ivf_pq_f32_dot"     : [metric: "dot", refine: ', "refine_factor"="10"'],
    ]
    tables.each { String table, Map<String, String> spec ->
        for (String field : ["metric", "refine"]) {
            assertNotNull(spec[field],
                    "${table}: incomplete test record, ${field} is missing. Every table in "
                    + "this suite must carry both, or the queries below silently interpolate "
                    + "a null.")
        }
    }

    def search = { String table, String query, String topK, String nprobes, String metric, String extra ->
        """vector_search("table"="${catalogName}.doris.${table}", "column"="embedding", "query_vector"="${query}", "top_k"="${topK}", "metric"="${metric}", "nprobes"="${nprobes}", "use_index"="true"${extra})"""
    }
    def flatSearch = { String table, String query, String topK, String metric ->
        """vector_search("table"="${catalogName}.doris.${table}", "column"="embedding", "query_vector"="${query}", "top_k"="${topK}", "metric"="${metric}", "use_index"="false")"""
    }
    // Deliberately omits metric, to pin what Doris does with an unset one.
    def defaultMetricSearch = { String table, String query, String topK ->
        """vector_search("table"="${catalogName}.doris.${table}", "column"="embedding", "query_vector"="${query}", "top_k"="${topK}", "use_index"="true")"""
    }

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

    order_qt_ivf_flat_cosine_desc """DESC `${catalogName}`.`doris`.`vs_ivf_flat_f32_cosine`"""
    order_qt_ivf_pq_cosine_desc """DESC `${catalogName}`.`doris`.`vs_ivf_pq_f32_cosine`"""
    order_qt_ivf_pq_dot_desc """DESC `${catalogName}`.`doris`.`vs_ivf_pq_f32_dot`"""

    // A metric that matches the index has to reach an indexed plan. lanceSearchIndexSegments
    // is the FE's own report of how many physical index segments it planned, so a non-zero
    // count here is what proves metricMatches accepted a non-L2 metric - without it the
    // queries below would still return correct rows, from a flat scan, and prove nothing.
    for (Map.Entry<String, Map<String, String>> entry : tables.entrySet()) {
        String table = entry.getKey()
        Map<String, String> spec = entry.getValue()
        String metric = spec.metric
        explain {
            sql("""SELECT row_id, _distance
                   FROM ${search(table, headQuery, "5", "4", metric, spec.refine)}
                   ORDER BY _distance, row_id""")
            contains "externalSearchType=VECTOR"
            contains "lanceMetric=${metric}"
            contains "lanceSearchFragments=2"
            contains "lanceSearchUnindexedFragments=0"
            contains "lanceSearchIndexSegments=1"
        }
    }

    // The other side of the same comparison: asking a cosine index for l2 must not use it.
    // Doris declines the index and plans flat splits rather than passing the mismatch down
    // to Lance, which would answer from a brute-force scan without saying so.
    explain {
        sql("""SELECT row_id, _distance
               FROM ${search("vs_ivf_flat_f32_cosine", headQuery, "5", "4", "l2", "")}
               ORDER BY _distance, row_id""")
        contains "lanceMetric=l2"
        contains "lanceSearchIndexSegments=0"
    }

    // An omitted metric is treated as l2 during index selection, so a cosine index is not
    // selected either. This pins current behaviour deliberately: the user-facing default
    // documented for `metric` is the metric of the matching index, which is not what index
    // selection does. If that is ever reconciled, this assertion is the one that has to be
    // updated, and it should be updated knowingly rather than discovered by a golden diff.
    explain {
        sql("""SELECT row_id, _distance
               FROM ${defaultMetricSearch("vs_ivf_flat_f32_cosine", headQuery, "5")}
               ORDER BY _distance, row_id""")
        contains "lanceSearchIndexSegments=0"
    }

    // Silent-fallback discriminator, per table because each trains its own IVF clustering.
    // Row 256's true neighbours are spread across partitions here rather than sitting next
    // to one edge - these directions do not cluster into contiguous row ranges the way the
    // collinear data does - so a genuine single-partition probe must miss some of them.
    // Unlike the L2 suites this compares row ids, not distances: this data was verified
    // tie-free, so the rows are the exact quantity, while the distances differ in the last
    // few bits between the indexed and flat paths and would make any two results "differ".
    for (Map.Entry<String, Map<String, String>> entry : tables.entrySet()) {
        String table = entry.getKey()
        Map<String, String> spec = entry.getValue()
        String metric = spec.metric
        def singleProbe = sql """
            SELECT row_id, _distance
            FROM ${search(table, boundaryQuery, "9", "1", metric, spec.refine)}
            ORDER BY _distance, row_id
        """
        def flat = sql """
            SELECT row_id, _distance
            FROM ${flatSearch(table, boundaryQuery, "9", metric)}
            ORDER BY _distance, row_id
        """
        assertEquals(9, singleProbe.size())
        assertEquals(9, flat.size())
        assertFalse(singleProbe.collect { it[0] }.equals(flat.collect { it[0] }),
                "${table}: nprobes=1 returned the same rows as the flat search, so the "
                + "single-partition restriction had no effect and the index was not used. "
                + "nprobes=1=" + singleProbe + " flat=" + flat)
    }

    // IVF_FLAT stores the original vectors, so probing every partition is an exhaustive
    // scan by another name and must reproduce the flat result exactly. This is the
    // algorithm's guarantee and holds for cosine just as it does for L2. Row ids only: the
    // distances are equal to within float rounding, not bit for bit.
    def fullProbe = sql """
        SELECT row_id
        FROM ${search("vs_ivf_flat_f32_cosine", headQuery, "10", "4", "cosine", "")}
        ORDER BY _distance, row_id
    """
    def flatCosine = sql """
        SELECT row_id
        FROM ${flatSearch("vs_ivf_flat_f32_cosine", headQuery, "10", "cosine")}
        ORDER BY _distance, row_id
    """
    assertEquals(10, fullProbe.size())
    assertEquals(flatCosine.collect { it[0] }, fullProbe.collect { it[0] },
            "vs_ivf_flat_f32_cosine: probing every partition did not reproduce the flat "
            + "cosine search, which IVF_FLAT guarantees it must. indexed=" + fullProbe
            + " flat=" + flatCosine)

    qt_ivf_flat_cosine """
        SELECT row_id, label, _distance
        FROM ${search("vs_ivf_flat_f32_cosine", headQuery, "5", "4", "cosine", "")}
        ORDER BY _distance, row_id
    """
    qt_ivf_pq_cosine """
        SELECT row_id, label, _distance
        FROM ${search("vs_ivf_pq_f32_cosine", headQuery, "5", "4", "cosine", tables["vs_ivf_pq_f32_cosine"].refine)}
        ORDER BY _distance, row_id
    """
    // dot is not a proper metric: a longer vector scores better than a closer one, so in
    // general the nearest row need not be the row the query vector was taken from. On this
    // data row 1 does come back first - its norm happens to be large enough to win - but that
    // is a coincidence of the fixture rather than something to rely on, which is why the
    // generator skips the "the query's own row is at distance 0" assertion whenever the metric
    // is dot. Lance reports the score as a negated inner product, so ORDER BY _distance ASC
    // still puts the best match first and every distance below is negative.
    qt_ivf_pq_dot """
        SELECT row_id, label, _distance
        FROM ${search("vs_ivf_pq_f32_dot", headQuery, "5", "4", "dot", tables["vs_ivf_pq_f32_dot"].refine)}
        ORDER BY _distance, row_id
    """

    // Flat baselines for the same queries, so a golden diff shows immediately whether a
    // change moved the index path, the flat path, or both.
    qt_flat_cosine """
        SELECT row_id, label, _distance
        FROM ${flatSearch("vs_ivf_flat_f32_cosine", headQuery, "5", "cosine")}
        ORDER BY _distance, row_id
    """
    qt_flat_dot """
        SELECT row_id, label, _distance
        FROM ${flatSearch("vs_ivf_pq_f32_dot", headQuery, "5", "dot")}
        ORDER BY _distance, row_id
    """
}
