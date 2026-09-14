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

suite("test_lance_vector_search_ivf_flat", "p0,external") {
    /*
     * IVF_FLAT query coverage. See test_lance_vector_search for the vector_search()
     * property reference; this suite only covers what is specific to IVF_FLAT.
     *
     * IVF_FLAT is the one ANN algorithm in the fixture that stores the original vectors
     * instead of a quantization of them, so probing every partition visits every candidate
     * with its true distance and MUST reproduce the flat search exactly - no refine_factor
     * involved. That equality is an algorithm guarantee rather than an observed property,
     * which is why this suite asserts it programmatically instead of only freezing goldens.
     *
     * Fixture: doris.vs_ivf_flat_f32, generated offline by
     * docker/thirdparties/docker-compose/iceberg/scripts/lance_build_preinstalled_catalog.py.
     * 1024 rows in two fragments, 16-dim Float32 embedding[j] = (row_id - 1) + j, one
     * IVF_FLAT index over 4 partitions with metric L2. For a query equal to the vector of
     * row r the exact squared L2 distance of row n is 16 * (n - r)^2.
     */
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Lance IVF_FLAT vector search test because the Iceberg MinIO environment is disabled.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_lance_vector_search_ivf_flat"
    String tableName = "${catalogName}.doris.vs_ivf_flat_f32"
    // headQuery is exactly row 1's vector and tailQuery is row 1024's, so distances are the
    // deterministic ladder 0, 16, 64, 144, ... with no ties.
    String headQuery = "[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]"
    String tailQuery = "[1023,1024,1025,1026,1027,1028,1029,1030,1031,1032,1033,1034,1035,1036,1037,1038]"
    // boundaryQuery is row 257's vector; it sits next to an IVF partition edge on this
    // frozen index (pinned by the generator self-check). See the discriminator below.
    String boundaryQuery = "[256,257,258,259,260,261,262,263,264,265,266,267,268,269,270,271]"

    def indexedSearch = { String query, String topK ->
        """vector_search("table"="${tableName}", "column"="embedding", "query_vector"="${query}", "top_k"="${topK}", "metric"="l2", "nprobes"="4", "use_index"="true")"""
    }
    def flatSearch = { String query, String topK ->
        """vector_search("table"="${tableName}", "column"="embedding", "query_vector"="${query}", "top_k"="${topK}", "metric"="l2", "use_index"="false")"""
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

    order_qt_ivf_flat_desc """DESC `${catalogName}`.`doris`.`vs_ivf_flat_f32`"""
    qt_ivf_flat_rows """
        SELECT count(*), count(DISTINCT row_id), min(row_id), max(row_id)
        FROM `${catalogName}`.`doris`.`vs_ivf_flat_f32`
    """

    // The IVF_FLAT guarantee, asserted rather than described: probing all four partitions
    // returns exactly what the flat search returns, at both ends of the dataset. A backend
    // that silently truncated the candidate set, searched a single fragment, or reranked
    // with wrong distances would break this even though its rows still look plausible.
    for (String query : [headQuery, tailQuery]) {
        def indexed = sql """
            SELECT row_id, _distance
            FROM ${indexedSearch(query, "10")}
            ORDER BY _distance, row_id
        """
        def flat = sql """
            SELECT row_id, _distance
            FROM ${flatSearch(query, "10")}
            ORDER BY _distance, row_id
        """
        assertEquals(10, indexed.size())
        assertEquals(flat, indexed,
                "IVF_FLAT full-partition probe must reproduce the flat search exactly; "
                + "indexed=" + indexed + " flat=" + flat)
    }

    // No refine_factor anywhere in this suite: IVF_FLAT holds the original vectors, so the
    // distances below are already exact - 0, 16, 64, 144, 256 is 16 * (n - 1)^2.
    qt_indexed_l2_topk """
        SELECT row_id, label, _distance
        FROM ${indexedSearch(headQuery, "5")}
        ORDER BY _distance, row_id
    """
    qt_flat_l2_topk """
        SELECT row_id, label, _distance
        FROM ${flatSearch(headQuery, "5")}
        ORDER BY _distance, row_id
    """
    // Search the opposite end of the dataset. This catches an accidental fragment-scoped
    // search that happens to contain only the first rows.
    qt_indexed_tail """
        SELECT row_id, label, _distance
        FROM ${indexedSearch(tailQuery, "3")}
        ORDER BY _distance, row_id
    """

    // Silent-fallback discriminator. Row 257's true nearest neighbours straddle an IVF
    // partition edge, so a genuine single-partition probe must miss the ones on the far side
    // and differ from the flat search. A pipeline that ignores use_index/nprobes and scans
    // flat fails this: on an unindexed table Lance ignores nprobes and returns exactly the
    // flat rows.
    // top_k is 9, not 10: distances here come in symmetric pairs (rows 256-d and 256+d tie),
    // and 9 is the last cut that lands on a complete pair, so no golden pins an arbitrary
    // tie winner. The fixture generator checks the same property at the same k.
    def boundarySingleProbe = sql """
        SELECT row_id, _distance
        FROM vector_search("table"="${tableName}", "column"="embedding", "query_vector"="${boundaryQuery}", "top_k"="9", "metric"="l2", "nprobes"="1", "use_index"="true")
        ORDER BY _distance, row_id
    """
    def boundaryFlat = sql """
        SELECT row_id, _distance
        FROM ${flatSearch(boundaryQuery, "9")}
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
            + "single-partition restriction had no effect: the IVF_FLAT index was not used "
            + "(silent flat fallback or ignored nprobes). "
            + "nprobes=1 distances=" + singleProbeDistances + " flat=" + flatDistances)

    qt_boundary_single_probe """
        SELECT row_id, label, _distance
        FROM vector_search("table"="${tableName}", "column"="embedding", "query_vector"="${boundaryQuery}", "top_k"="9", "metric"="l2", "nprobes"="1", "use_index"="true")
        ORDER BY _distance, row_id
    """
    qt_boundary_flat """
        SELECT row_id, label, _distance
        FROM ${flatSearch(boundaryQuery, "9")}
        ORDER BY _distance, row_id
    """
}
