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

// Verifies that Doris can write (and read back) Iceberg Parquet tables for every
// compression codec it claims to support, including LZ4. LZ4 is emitted as the
// Hadoop-framed Parquet "LZ4" codec (not LZ4_RAW) so the output stays readable by
// Spark/Iceberg/Trino, matching what those engines write for
// `write.parquet.compression-codec=lz4`.
suite("test_iceberg_write_parquet_compression", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "test_iceberg_write_parquet_compression"
    String db = catalog_name + "_db"

    sql """drop catalog if exists ${catalog_name}"""
    sql """
    CREATE CATALOG ${catalog_name} PROPERTIES (
        'type'='iceberg',
        'iceberg.catalog.type'='rest',
        'uri' = 'http://${externalEnvIp}:${rest_port}',
        "s3.access_key" = "admin",
        "s3.secret_key" = "password",
        "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
        "s3.region" = "us-east-1"
    );"""

    sql """ switch ${catalog_name} """
    sql """ drop database if exists ${db} force """
    sql """ create database ${db} """

    def expected = [["doris0", 0L], ["doris1", 1L], ["doris2", 2L]]

    // Codecs Doris is expected to support for Parquet writes.
    for (String codec : ["lz4", "snappy", "zstd", "uncompressed"]) {
        String tbl = "${db}.tbl_${codec}"
        sql """ drop table if exists ${tbl} """
        sql """
        CREATE TABLE ${tbl} (a STRING, b BIGINT) PROPERTIES (
            'write-format' = 'parquet',
            'write.format.default' = 'parquet',
            'format-version' = '2',
            'write.parquet.compression-codec' = '${codec}'
        )"""

        sql """ INSERT INTO ${tbl} VALUES ('doris0', 0), ('doris1', 1), ('doris2', 2) """

        def rows = sql """ SELECT a, b FROM ${tbl} ORDER BY b """
        assertEquals(expected.size(), rows.size())
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected[i][0], rows[i][0])
            assertEquals(expected[i][1], rows[i][1] as Long)
        }
        logger.info("iceberg parquet write+read round-trip OK for codec=${codec}")
    }

    // GZIP is intentionally not yet supported by the Doris Parquet writer; the INSERT must
    // fail explicitly rather than silently fall back. Remove this block when GZIP support is
    // added (and update the cross-engine product test expectation in tandem).
    sql """ drop table if exists ${db}.tbl_gzip """
    sql """
    CREATE TABLE ${db}.tbl_gzip (a STRING, b BIGINT) PROPERTIES (
        'write-format' = 'parquet',
        'write.format.default' = 'parquet',
        'format-version' = '2',
        'write.parquet.compression-codec' = 'gzip'
    )"""
    test {
        sql """ INSERT INTO ${db}.tbl_gzip VALUES ('doris0', 0) """
        exception "Unsupported compress type GZ with parquet"
    }

    sql """ drop database if exists ${db} force """
    sql """drop catalog if exists ${catalog_name}"""
}
