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

suite("test_lance_vector_search_index_types", "p0,external") {
    /*
     * Query coverage for the remaining Lance ANN algorithms: IVF_SQ, IVF_HNSW_FLAT,
     * IVF_HNSW_SQ and IVF_HNSW_PQ, all Float32 + L2. See test_lance_vector_search for the
     * vector_search() property reference, test_lance_vector_search_ivf_flat for the exact
     * algorithm, and lance_build_preinstalled_catalog.py for how the fixtures are built and
     * what the generator verifies about each physical index before it is committed.
     *
     * Every table here holds the same 1024 rows in two fragments with 16-dim Float32
     * embedding[j] = (row_id - 1) + j, so a query equal to row r's vector puts row n at
     * exact squared L2 distance 16 * (n - r)^2. They differ only in the index built over
     * that column.
     *
     * None of these four is exact. SQ and PQ answer from quantized codes; the HNSW variants
     * reach candidates through a graph that may miss a true neighbour even when the stored
     * vectors are exact. So the normal queries below pass refine_factor=10, which reranks a
     * wider candidate set with exact distances, and the goldens record what this frozen
     * fixture and the pinned Lance version produce - they are not algorithm guarantees. The
     * graph indexes additionally need ef, Lance's candidate width; a refined query needs
     * ef >= top_k * refine_factor, and a smaller ef is an error rather than a silent
     * degradation, which the last test pins.
     */
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Lance vector index type test because the Iceberg MinIO environment is disabled.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_lance_vector_search_index_types"
    String headQuery = "[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]"
    // Row 256's vector, next to an IVF partition edge on every one of these frozen indexes.
    String boundaryQuery = "[255,256,257,258,259,260,261,262,263,264,265,266,267,268,269,270]"
    // Row 512's vector, in the middle of the data, where the graph traversal has room to
    // settle for a worse neighbour when its candidate width is narrow. The generator pins
    // this row for the ef discriminator below.
    String midQuery = "[511,512,513,514,515,516,517,518,519,520,521,522,523,524,525,526]"

    // ef is what a graph index cannot be searched without; refine_factor is what makes a
    // lossy index comparable to an exact distance. Both stay out of the discriminators.
    Map<String, String> refineOptions = [
            "vs_ivf_sq_f32"       : ', "refine_factor"="10"',
            "vs_ivf_hnsw_flat_f32": ', "refine_factor"="10", "ef"="100"',
            "vs_ivf_hnsw_sq_f32"  : ', "refine_factor"="10", "ef"="100"',
            "vs_ivf_hnsw_pq_f32"  : ', "refine_factor"="10", "ef"="100"',
    ]

    def search = { String table, String query, String topK, String nprobes, String extra ->
        """vector_search("table"="${catalogName}.doris.${table}", "column"="embedding", "query_vector"="${query}", "top_k"="${topK}", "metric"="l2", "nprobes"="${nprobes}", "use_index"="true"${extra})"""
    }
    def flatSearch = { String table, String query, String topK ->
        """vector_search("table"="${catalogName}.doris.${table}", "column"="embedding", "query_vector"="${query}", "top_k"="${topK}", "metric"="l2", "use_index"="false")"""
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

    order_qt_ivf_sq_desc """DESC `${catalogName}`.`doris`.`vs_ivf_sq_f32`"""
    order_qt_ivf_hnsw_flat_desc """DESC `${catalogName}`.`doris`.`vs_ivf_hnsw_flat_f32`"""
    order_qt_ivf_hnsw_sq_desc """DESC `${catalogName}`.`doris`.`vs_ivf_hnsw_sq_f32`"""
    order_qt_ivf_hnsw_pq_desc """DESC `${catalogName}`.`doris`.`vs_ivf_hnsw_pq_f32`"""

    // Silent-fallback discriminator, per table because each table trains its own IVF
    // clustering. Row 256's true neighbours straddle a partition edge, so a genuine
    // single-partition probe must miss the ones on the far side; a pipeline that ignored
    // use_index/nprobes would return exactly the flat rows and fail here. top_k is 9 so the
    // cut lands on a complete symmetric tie pair, and the comparison is on distances, not
    // row ids, because either member of a tie may fill the last slot.
    for (Map.Entry<String, String> entry : refineOptions.entrySet()) {
        String table = entry.getKey()
        def singleProbe = sql """
            SELECT row_id, _distance
            FROM ${search(table, boundaryQuery, "9", "1", entry.getValue())}
            ORDER BY _distance, row_id
        """
        def flat = sql """
            SELECT row_id, _distance
            FROM ${flatSearch(table, boundaryQuery, "9")}
            ORDER BY _distance, row_id
        """
        assertEquals(9, singleProbe.size())
        assertEquals(9, flat.size())
        assertFalse(singleProbe.collect { it[1] }.equals(flat.collect { it[1] }),
                "${table}: nprobes=1 produced the same distance sequence as the flat search, "
                + "so the single-partition restriction had no effect and the index was not "
                + "used. nprobes=1=" + singleProbe + " flat=" + flat)
    }

    qt_ivf_sq_l2 """
        SELECT row_id, label, _distance
        FROM ${search("vs_ivf_sq_f32", headQuery, "5", "4", refineOptions["vs_ivf_sq_f32"])}
        ORDER BY _distance, row_id
    """
    qt_ivf_hnsw_flat_l2 """
        SELECT row_id, label, _distance
        FROM ${search("vs_ivf_hnsw_flat_f32", headQuery, "5", "4", refineOptions["vs_ivf_hnsw_flat_f32"])}
        ORDER BY _distance, row_id
    """
    qt_ivf_hnsw_sq_l2 """
        SELECT row_id, label, _distance
        FROM ${search("vs_ivf_hnsw_sq_f32", headQuery, "5", "4", refineOptions["vs_ivf_hnsw_sq_f32"])}
        ORDER BY _distance, row_id
    """
    qt_ivf_hnsw_pq_l2 """
        SELECT row_id, label, _distance
        FROM ${search("vs_ivf_hnsw_pq_f32", headQuery, "5", "4", refineOptions["vs_ivf_hnsw_pq_f32"])}
        ORDER BY _distance, row_id
    """

    // The graph counterpart of the nprobes discriminator: with ef=5 the traversal has to
    // settle for a worse fifth neighbour than with ef=50, so ef demonstrably reached the
    // index instead of being dropped on the way. Both queries probe all four partitions and
    // neither reranks, because refine_factor with exact distances is precisely what would
    // hide the effect. IVF_HNSW_SQ is the table the fixture generator pins this on: on 1024
    // collinear vectors the FLAT and PQ graphs still return the exact rows at ef=5, so only
    // this one can carry the assertion.
    def narrowEf = sql """
        SELECT row_id, _distance
        FROM ${search("vs_ivf_hnsw_sq_f32", midQuery, "5", "4", ', "ef"="5"')}
        ORDER BY _distance, row_id
    """
    def wideEf = sql """
        SELECT row_id, _distance
        FROM ${search("vs_ivf_hnsw_sq_f32", midQuery, "5", "4", ', "ef"="50"')}
        ORDER BY _distance, row_id
    """
    assertEquals(5, narrowEf.size())
    assertEquals(5, wideEf.size())
    assertFalse(narrowEf.collect { it[1] }.equals(wideEf.collect { it[1] }),
            "IVF_HNSW_SQ returned the same distances for ef=5 and ef=50, so ef never reached "
            + "the graph search. ef=5=" + narrowEf + " ef=50=" + wideEf)

    qt_ivf_hnsw_sq_ef_5 """
        SELECT row_id, label, _distance
        FROM ${search("vs_ivf_hnsw_sq_f32", midQuery, "5", "4", ', "ef"="5"')}
        ORDER BY _distance, row_id
    """
    qt_ivf_hnsw_sq_ef_50 """
        SELECT row_id, label, _distance
        FROM ${search("vs_ivf_hnsw_sq_f32", midQuery, "5", "4", ', "ef"="50"')}
        ORDER BY _distance, row_id
    """

    // Lance reranks a refined query over top_k * refine_factor candidates, so ef has to be
    // at least that wide. Too small an ef is a diagnosable error rather than a quietly
    // truncated result, and that is worth keeping true: here 10 * 10 > 50.
    test {
        sql """
            SELECT row_id
            FROM ${search("vs_ivf_hnsw_sq_f32", headQuery, "10", "4", ', "refine_factor"="10", "ef"="50"')}
        """
        exception "ef must be greater than or equal to k"
    }
}
