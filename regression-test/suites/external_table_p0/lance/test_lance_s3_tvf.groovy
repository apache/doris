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

        // all_types deliberately contains both supported and unsupported Arrow
        // types. S3 TVF should expose the same schema as the Lance Catalog and
        // allow a query to project only supported columns.
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
                CAST(json_col AS STRING) AS json_col,
                bfloat16_vector_col
            FROM ${lanceTvf}
            WHERE row_id = 1
        """
        // "uris": several locations named in one invocation. Each case below
        // compares its row count with the "uri" scan above.
        String urisOptions = """
                "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
                "s3.access_key" = "admin",
                "s3.secret_key" = "password",
                "s3.region" = "us-east-1",
                "use_path_style" = "true",
                "format" = "lance"
        """
        def uriRowCount = (sql """SELECT count(*) FROM ${lanceTvf}""")[0][0]

        // A single-element "uris" names one dataset and reads what "uri" reads.
        def urisOneRowCount = (sql """
            SELECT count(*) FROM s3(
                "uris" = '["s3://warehouse/lance/all_types.lance"]',
                ${urisOptions}
            )
        """)[0][0]
        assertEquals(uriRowCount, urisOneRowCount)

        // A location named twice is read once: deduplication is by location.
        def urisTwiceRowCount = (sql """
            SELECT count(*) FROM s3(
                "uris" = '["s3://warehouse/lance/all_types.lance", "s3://warehouse/lance/all_types.lance"]',
                ${urisOptions}
            )
        """)[0][0]
        assertEquals(uriRowCount, urisTwiceRowCount)

        // "uri" and "uris" are mutually exclusive; supplying both is an error.
        test {
            sql """
                SELECT count(*) FROM s3(
                    "uri" = "s3://warehouse/lance/all_types.lance",
                    "uris" = '["s3://warehouse/lance/all_types.lance"]',
                    ${urisOptions}
                )
            """
            exception "are mutually exclusive"
        }

        // The array shape is refused at parse time, before any storage is touched.
        test {
            sql """SELECT count(*) FROM s3("uris" = '[]', ${urisOptions})"""
            exception "must name at least one location"
        }
        test {
            sql """SELECT count(*) FROM s3("uris" = 's3://warehouse/lance/all_types.lance', ${urisOptions})"""
            exception "is not valid JSON"
        }

        // A dataset that cannot be opened fails the whole query.
        test {
            sql """
                SELECT count(*) FROM s3(
                    "uris" = '["s3://warehouse/lance/all_types.lance",
                                "s3://warehouse/lance/does_not_exist.lance"]',
                    ${urisOptions}
                )
            """
            exception "Failed to open Lance dataset 's3://warehouse/lance/does_not_exist.lance'"
        }

        // Several datasets are one table only when their schemas are exactly equal.
        test {
            sql """
                SELECT count(*) FROM s3(
                    "uris" = '["s3://warehouse/lance/all_types.lance",
                                "s3://warehouse/lance/nested_null.lance"]',
                    ${urisOptions}
                )
            """
            exception "Lance dataset 's3://warehouse/lance/nested_null.lance' does not have the same schema as 's3://warehouse/lance/all_types.lance'"
        }

        // Four locations with strictly equal schemas but DIFFERENT data: each fixture
        // is all_types.lance with row_id shifted by a distinct offset (lance_1/a +100000,
        // lance_1/b +200000, lance_2/a +300000, all_types unchanged). The cases below
        // prove the merged table really contains every location's rows.
        String urisFour = """
            "uris" = '["s3://warehouse/lance_1/a.lance",
                        "s3://warehouse/lance_2/a.lance",
                        "s3://warehouse/lance_1/b.lance",
                        "s3://warehouse/lance/all_types.lance"]',
            ${urisOptions}
        """

        // 1) cardinality: every location is read once.
        def urisMultiRowCount = (sql """
            SELECT count(*) FROM s3(${urisFour})
        """)[0][0]
        assertEquals(uriRowCount * 4, urisMultiRowCount)

        // 2) dataset behaviour over the whole merged table: the min/max/sum pin down
        //    that all four ranges (1..12, 100001..100012, 200001..200012, 300001..300012)
        //    are present and combined correctly.
        def mergedAgg = (sql """
            SELECT min(row_id), max(row_id), sum(row_id) FROM s3(${urisFour})
        """)[0]
        assertEquals(1, mergedAgg[0])
        assertEquals(300012, mergedAgg[1])
        assertEquals(7200312, mergedAgg[2])

        // 3) per-location value correctness: a row pulled from each offset path must
        //    carry the same column values as the all_types row it was copied from.
        def vAllTypes = (sql """SELECT int8_col, uint8_col FROM s3(${urisFour}) WHERE row_id = 3""")[0]
        assertEquals(-128, vAllTypes[0]); assertEquals(0, vAllTypes[1])

        def vLance1A = (sql """SELECT int8_col, uint8_col FROM s3(${urisFour}) WHERE row_id = 100003""")[0]
        assertEquals(-128, vLance1A[0]); assertEquals(0, vLance1A[1])

        def vLance1B = (sql """SELECT int8_col, uint8_col FROM s3(${urisFour}) WHERE row_id = 200010""")[0]
        assertEquals(100, vLance1B[0]); assertEquals(128, vLance1B[1])

        def vLance2A = (sql """SELECT int8_col, uint8_col FROM s3(${urisFour}) WHERE row_id = 300011""")[0]
        assertEquals(127, vLance2A[0]); assertEquals(255, vLance2A[1])

        // 4) whole-table behaviour: project every supported column over all 48
        //    rows, ordered by row_id, and compare the merged table row-by-row
        //    against the .out. blob_col is UNSUPPORTED and must stay excluded;
        //    binary/time columns are hex/cast to keep their text form stable.
        qt_merged_full_table """
            SELECT * REPLACE(
                hex(binary_col) AS binary_col,
                hex(large_binary_col) AS large_binary_col,
                hex(fixed_size_binary_col) AS fixed_size_binary_col,
                cast(time32_s_col AS string) AS time32_s_col,
                cast(time32_ms_col AS string) AS time32_ms_col,
                cast(time64_us_col AS string) AS time64_us_col,
                cast(time64_ns_col AS string) AS time64_ns_col
            ) EXCEPT(
                null_col,
                duration_s_col, duration_ms_col, duration_us_col, duration_ns_col,
                dictionary_col,
                blob_col,
                json_col,
                bfloat16_vector_col
            )
            FROM s3(${urisFour})
            ORDER BY row_id
        """
    } finally {
        sql """SET enable_file_scanner_v2 = ${originalScannerV2}"""
        sql """SET time_zone = '${originalTimeZone}'"""
    }
}
