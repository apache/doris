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

suite("test_lance_full_text_search", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Lance full-text search test because the Iceberg MinIO environment is disabled.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_lance_full_text_search"
    String fullTable = "${catalogName}.doris.full_text_search"
    String partialTable = "${catalogName}.doris.full_text_search_partial"

    def search = { String table, String query, String topK, String offset = "0",
            String coverageMode = "strict", String extra = "" ->
        """full_text_search(
                "table"="${table}",
                "column"="body",
                "query"="${query}",
                "top_k"="${topK}",
                "offset"="${offset}",
                "coverage_mode"="${coverageMode}"${extra})"""
    }

    sql "DROP CATALOG IF EXISTS `${catalogName}`"
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
        sql "SET enable_file_scanner_v2 = true"

        // The full corpus is indexed across multiple fragments. The plan must use FTS index
        // segment splits, and strict coverage must see no unindexed fragment.
        explain {
            sql """SELECT row_id, _score
                    FROM ${search(fullTable, "lance", "4")}
                    ORDER BY _score DESC, row_id"""
            contains "externalSearchType=FULL_TEXT"
            contains "lanceFullTextColumn=body"
            contains "lanceFtsCoverageMode=STRICT"
            contains "lanceSearchUnindexedFragments=0"
            contains "lanceSearchIndexSegments="
        }

        qt_fts_ranked """
            SELECT row_id, _score
            FROM ${search(fullTable, "lance", "4")}
            ORDER BY _score DESC, row_id
        """

        // OFFSET belongs to the snapshot-wide TopN. It must not be applied independently by
        // every physical FTS index split.
        qt_fts_offset """
            SELECT row_id
            FROM ${search(fullTable, "lance", "2", "1")}
            ORDER BY _score DESC, row_id
        """

        // The TVF filter runs inside Lance before FTS candidates are selected.
        qt_fts_filter """
            SELECT row_id
            FROM ${search(fullTable, "lance", "4", "0", "strict",
                    ', "filter"="category = \'storage\'"')}
            ORDER BY _score DESC, row_id
        """

        // The second table has a committed FTS index plus one fragment containing two rows
        // appended afterwards. STRICT is the safe default and rejects the incomplete snapshot.
        test {
            sql """SELECT row_id
                    FROM ${search(partialTable, "lance", "10")}
                    ORDER BY _score DESC, row_id"""
            exception "unindexed fragments"
        }

        // INDEX_ONLY searches exactly the fragments covered by the selected FTS segment. The
        // two appended rows also contain "lance" but must not leak into the result.
        explain {
            sql """SELECT row_id
                    FROM ${search(partialTable, "lance", "10", "0", "index_only")}
                    ORDER BY _score DESC, row_id"""
            contains "externalSearchType=FULL_TEXT"
            contains "lanceFtsCoverageMode=INDEX_ONLY"
            contains "lanceSearchUnindexedFragments=1"
        }
        qt_fts_index_only """
            SELECT row_id
            FROM ${search(partialTable, "lance", "10", "0", "index_only")}
            ORDER BY _score DESC, row_id
        """
    } finally {
        // sql "DROP CATALOG IF EXISTS `${catalogName}`"
    }
}
