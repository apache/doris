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

suite("test_lance_s3_tvf", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Lance S3 TVF test because the Iceberg MinIO environment is disabled.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String lanceTvf = """
        s3(
            "uri" = "s3://warehouse/lance/all_types.lance",
            "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.region" = "us-east-1",
            "use_path_style" = "true",
            "format" = "lance"
        )
    """

    def scannerV2Rows = sql """SHOW VARIABLES LIKE 'enable_file_scanner_v2'"""
    assertEquals(1, scannerV2Rows.size())
    String originalScannerV2 = scannerV2Rows[0][1].toString()
    String originalTimeZone = sql("""SELECT @@session.time_zone""")[0][0].toString()

    try {
        sql """SET enable_file_scanner_v2 = true"""
        sql """SET time_zone = '+08:00'"""

        // S3 TVF should expose the same schema and values as the Lance Catalog.
        order_qt_desc """DESC FUNCTION ${lanceTvf}"""

        // FE pins one Lance version and creates one split per fragment. These
        // aggregates catch missing fragments and duplicate whole-dataset scans.
        qt_scan_cardinality """
            SELECT count(*), count(DISTINCT row_id), min(row_id), max(row_id), sum(row_id)
            FROM ${lanceTvf}
        """

        qt_integer_boundaries """
            SELECT row_id, int8_col, uint8_col, int64_col, uint64_col
            FROM ${lanceTvf}
            WHERE row_id IN (2, 3, 10, 11, 12)
            ORDER BY row_id
        """

        qt_representative_types """
            SELECT
                bool_col,
                float16_col,
                decimal256_col,
                hex(binary_col),
                cast(time32_ms_col AS string),
                cast(time64_us_col AS string),
                timestamp_us_col,
                timestamp_us_utc_col,
                fixed_size_list_uint8_col,
                map_col
            FROM ${lanceTvf}
            WHERE row_id = 1
        """

        qt_additional_types """
            SELECT
                null_col IS NULL AS null_is_null,
                duration_s_col,
                duration_ms_col,
                duration_us_col,
                duration_ns_col,
                blob_col.kind AS blob_kind,
                blob_col.size AS blob_size,
                CAST(json_col AS STRING) AS json_col,
                bfloat16_vector_col
            FROM ${lanceTvf}
            WHERE row_id = 1
        """
    } finally {
        sql """SET enable_file_scanner_v2 = ${originalScannerV2}"""
        sql """SET time_zone = '${originalTimeZone}'"""
    }
}
