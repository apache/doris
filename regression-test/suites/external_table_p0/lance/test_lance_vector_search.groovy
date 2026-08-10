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
     * The current implementation pins one Lance dataset version and searches its entire snapshot
     * with one scanner. Multi-scanner search plus global Top-K merging remains future work.
     */
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Lance vector search test because the Iceberg MinIO environment is disabled.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_lance_vector_search"
    String tableName = "${catalogName}.doris.vector_search"
    String indexedTopFive = """vector_search("table"="${tableName}", "column"="embedding", "query_vector"="[0,0,0,0]", "top_k"="5", "metric"="l2", "nprobes"="4", "refine_factor"="10", "use_index"="true")"""
    String flatTopFive = """vector_search("table"="${tableName}", "column"="embedding", "query_vector"="[0,0,0,0]", "top_k"="5", "metric"="l2", "use_index"="false")"""
    String indexedTopTwo = """vector_search("table"="${tableName}", "column"="embedding", "query_vector"="[0,0,0,0]", "top_k"="2", "metric"="l2", "nprobes"="4", "refine_factor"="10", "use_index"="true")"""
    String indexedOffset = """vector_search("table"="${tableName}", "column"="embedding", "query_vector"="[0,0,0,0]", "top_k"="2", "offset"="1", "metric"="l2", "nprobes"="4", "refine_factor"="10", "use_index"="true")"""
    String indexedPrefilter = """vector_search("table"="${tableName}", "column"="embedding", "query_vector"="[0,0,0,0]", "top_k"="3", "filter"="category = 'odd'", "metric"="l2", "nprobes"="4", "refine_factor"="10", "use_index"="true")"""
    String indexedTail = """vector_search("table"="${tableName}", "column"="embedding", "query_vector"="[1023,2046,3069,4092]", "top_k"="3", "metric"="l2", "nprobes"="4", "refine_factor"="10", "use_index"="true")"""

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

        order_qt_vector_table_desc """DESC `${catalogName}`.`doris`.`vector_search`"""
        qt_vector_table_rows """
            SELECT count(*), count(DISTINCT row_id), min(row_id), max(row_id)
            FROM `${catalogName}`.`doris`.`vector_search`
        """

        explain {
            sql("""SELECT row_id, label, _distance FROM ${indexedTopFive} ORDER BY _distance, row_id""")
            contains "externalSearchType=VECTOR"
            contains "lanceVectorColumn=embedding"
            contains "lanceTopK=5"
            contains "lanceOffset=0"
            contains "lanceMetric=l2"
            contains "lanceSearchScanners=1"
            notContains "[0,0,0,0]"
        }

        // The fixture has an IVF_PQ index over embedding. nprobes covers all
        // four IVF partitions, and refine_factor reranks physical candidates.
        qt_indexed_l2_topk """
            SELECT row_id, label, _distance
            FROM ${indexedTopFive}
            ORDER BY _distance, row_id
        """

        // Disable the index explicitly. Indexed and flat search must agree for
        // these deterministic nearest rows.
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

        // The TVF filter is evaluated before Lance chooses Top-K. The nearest
        // eligible rows are row_id 2, 4 and 6.
        qt_indexed_pre_search_filter """
            SELECT row_id, category, _distance
            FROM ${indexedPrefilter}
            ORDER BY _distance, row_id
        """

        // An outer WHERE remains a Doris post-search predicate. Search first
        // selects row_id 1 and 2; filtering for odd retains only row_id 2.
        qt_post_search_filter """
            SELECT row_id, category, _distance
            FROM ${indexedTopTwo}
            WHERE category = 'odd'
            ORDER BY _distance, row_id
        """

        // Lance can search embedding even when Doris does not project it.
        qt_pruned_vector_column """
            SELECT row_id, label, _distance
            FROM ${indexedTopFive}
            ORDER BY _distance, row_id
        """

        test {
            sql("""SELECT row_id FROM vector_search("table"="${tableName}", "column"="embedding", "query_vector"="[0,0,0]", "top_k"="1")""")
            exception "dimension"
        }

        test {
            sql("""SELECT row_id FROM vector_search("table"="${tableName}", "column"="embedding", "query_vector"="[0,0,0,0]", "top_k"="0")""")
            exception "top_k"
        }
    } finally {
        // sql """DROP CATALOG IF EXISTS `${catalogName}`"""
    }
}
