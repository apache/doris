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

suite("test_lance_vector_search_legacy_index", "p0,external") {
    /*
     * A vector index without index details, as Lance writers before 0.20 produced. Lance infers
     * the missing details from the index files when it loads the index, so the FE plans the
     * index like any other and the BE searches it through the planned segment. A split without
     * index segments always searches flat, so a legacy index the FE skipped would lose ANN.
     *
     * Fixture: legacy_vector_index.lance (see lance_build_legacy_vector_index.py), written by
     * pylance 0.18.2: row_id 1..512 in fragment 0, vec = [row_id, row_id + 1, row_id + 2,
     * row_id + 3], and vec_idx (IVF_PQ, l2, 2 partitions, 2 sub-vectors) over fragment 0. The
     * squared L2 distance between rows r and n is 4 * (n - r)^2; refine_factor recomputes it
     * from the stored vectors, so the indexed search returns the flat search's rows.
     */
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Lance legacy vector index test because the Iceberg MinIO environment is disabled.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_lance_vector_search_legacy_index"
    String tableName = "${catalogName}.`default`.legacy_vector_index"
    // The query sits a quarter step past row 100, so no two rows tie.
    String query = "[100.25,101.25,102.25,103.25]"
    def search = { String options ->
        """vector_search("table"="${tableName}", "column"="vec", "query_vector"="${query}", "top_k"="3", "metric"="l2"${options})"""
    }
    String originalScannerV2 = (sql """SHOW VARIABLES LIKE 'enable_file_scanner_v2'""")[0][1].toString()

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

        explain {
            sql("SELECT row_id FROM ${search(', "nprobes"="2", "refine_factor"="4"')}")
            contains "lanceVectorIndexStatus=USED"
            contains "lanceVersion=2"
            contains "lanceSearchIndexSegments=1"
            contains "lanceSearchIndexFragments=1"
            contains "lanceSearchUnindexedFragments=0"
        }
        qt_legacy_indexed """
            SELECT row_id, _distance FROM ${search(', "nprobes"="2", "refine_factor"="4"')}
            ORDER BY _distance, row_id
        """
        qt_legacy_flat """
            SELECT row_id, _distance FROM ${search(', "use_index"="false"')}
            ORDER BY _distance, row_id
        """
        // Before the index was built, the same search plans no index.
        explain {
            sql("SELECT row_id FROM ${search(', "version"="1"')}")
            contains "lanceVectorIndexStatus=NO_MATCH"
            contains "lanceSearchUnindexedFragments=1"
        }
    } finally {
        sql """SET enable_file_scanner_v2 = ${originalScannerV2}"""
        sql """DROP CATALOG IF EXISTS `${catalogName}`"""
    }
}
