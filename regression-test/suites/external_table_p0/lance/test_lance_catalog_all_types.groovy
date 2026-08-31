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

suite("test_lance_catalog_all_types","p0,external") {
    // The Lance fixture is preinstalled in the MinIO container of the Iceberg
    // external environment, so this suite deliberately shares its switch.
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Lance test because the Iceberg MinIO environment is disabled.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_lance_catalog_all_types"
    String databaseName = "default"
    String tableName = "all_types"
    def scannerV2Rows = sql """SHOW VARIABLES LIKE 'enable_file_scanner_v2'"""
    assertEquals(1, scannerV2Rows.size())
    String originalScannerV2 = scannerV2Rows[0][1].toString()

    /*
     * Lance/Arrow to Doris type mapping exercised by this fixture:
     *
     * `UNSUPPORTED` below means `unknown type: UNSUPPORTED_TYPE` in DESC.
     *
     * | Lance / Arrow type | Doris DESC type | Status / notes |
     * |---|---|---|
     * | null | null_type | Supported; every value is SQL NULL |
     * | bool | boolean | Supported |
     * | int8 | tinyint | Supported |
     * | uint8 | smallint | Supported by lossless widening |
     * | int16 | smallint | Supported |
     * | uint16 | int | Supported by lossless widening |
     * | int32 | int | Supported |
     * | uint32 | bigint | Supported by lossless widening |
     * | int64 | bigint | Supported |
     * | uint64 | largeint | Supported by lossless widening |
     * | float16 | float | Supported by lossless widening |
     * | float32 | float | Supported |
     * | float64 | double | Supported |
     * | decimal128(38,10) | decimal(38,10) | Supported |
     * | decimal256(76,38) | decimal(76,38) | Supported up to precision 76 |
     * | utf8 / large_utf8 | text | Supported |
     * | binary / large_binary | varbinary(2147483647) | Supported up to the Doris VARBINARY limit |
     * | fixed_size_binary(16) | varbinary(16) | Supported with the fixed byte width preserved |
     * | date32(day) | date | Supported |
     * | date64(ms) | date | Supported when values contain whole days |
     * | time32(s) | time(0) | Supported |
     * | time32(ms) | time(3) | Supported |
     * | time64(us/ns) | time(6) | Nanoseconds are truncated to microseconds |
     * | timestamp(s) | datetime | Supported |
     * | timestamp(ms) | datetime(3) | Supported |
     * | timestamp(us/ns) | datetime(6) | Nanoseconds are truncated to microseconds |
     * | timestamp(us, UTC) | timestamptz(6) | UTC instant; rendered in the Doris session timezone |
     * | duration(s/ms/us/ns) | bigint | Exact signed count in the Arrow field's declared unit |
     * | struct | struct<...> | Child types are converted recursively |
     * | list / large_list / fixed_size_list | array<...> | Item type is converted recursively |
     * | fixed_size_list<uint8> | array<smallint> | Item type is widened recursively |
     * | fixed_size_list<float16> | array<float> | Item type is widened recursively |
     * | map | map<...,...> | Key/value types are converted recursively |
     * | dictionary<int16, utf8> | smallint | SDK currently exposes only the physical index type |
     * | Lance Blob v2 | struct<kind,position,size,blob_id,blob_uri> | Descriptor metadata only; the payload is never materialized |
     * | Arrow JSON extension | json | Returned as Doris JSON |
     * | fixed_size_list<Lance BFloat16> | array<float> | BFloat16 values are widened exactly |
     *
     * Dataset.getSchema() preserves extension metadata for Blob, JSON, and BFloat16,
     * allowing Doris to validate each physical storage type before mapping it.
     * The current Lance Java SDK erases Dictionary metadata from getSchema(), and
     * getLanceSchema() fails on a schema containing Dictionary, so Dictionary remains
     * a physical fallback until the SDK conversion is fixed.
     */

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

        sql """ use `${catalogName}`.`${databaseName}`; """
        order_qt_sql_1 """desc `${tableName}`;"""
        sql """SET time_zone = '+08:00'"""
        qt_select """
            select * replace(
                hex(binary_col) as binary_col,
                hex(large_binary_col) as large_binary_col,
                hex(fixed_size_binary_col) as fixed_size_binary_col,
                cast(time32_s_col as string) as time32_s_col,
                cast(time32_ms_col as string) as time32_ms_col,
                cast(time64_us_col as string) as time64_us_col,
                cast(time64_ns_col as string) as time64_ns_col
            ) except(
                row_id,
                null_col,
                duration_s_col,
                duration_ms_col,
                duration_us_col,
                duration_ns_col,
                dictionary_col,
                blob_col,
                json_col,
                bfloat16_vector_col
            )
            from all_types
            where row_id = 1;
        """

        // Verify both schema mappings and values for the additional Lance types.
        // Blob v2 is exposed as its descriptor struct, so read the descriptor fields instead of
        // the payload. size is the byte length of the stored Blob; kind/position/blob_id/blob_uri
        // describe where it lives and depend on how the fixture stored it.
        qt_additional_lance_types """
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
            FROM `${catalogName}`.`${databaseName}`.`${tableName}`
            WHERE row_id = 1
        """

        def timestampRows = sql """
            SELECT timestamp_us_col, timestamp_us_utc_col
            FROM `${catalogName}`.`${databaseName}`.`${tableName}`
            WHERE row_id = 1
        """
        assertEquals(1, timestampRows.size())
        assertEquals("2026-07-28T12:34:56.123456", timestampRows[0][0].toString())
        assertEquals("2026-07-28 20:34:56.123456+08:00", timestampRows[0][1].toString())

        sql """SET time_zone = 'UTC'"""
        timestampRows = sql """
            SELECT timestamp_us_col, timestamp_us_utc_col
            FROM `${catalogName}`.`${databaseName}`.`${tableName}`
            WHERE row_id = 1
        """
        assertEquals("2026-07-28T12:34:56.123456", timestampRows[0][0].toString())
        assertEquals("2026-07-28 12:34:56.123456+00:00", timestampRows[0][1].toString())

    } finally {
        sql """SET enable_file_scanner_v2 = ${originalScannerV2}"""
        // sql """DROP CATALOG IF EXISTS `${catalogName}`"""
    }
}
