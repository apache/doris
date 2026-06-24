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

// Verifies that Doris writes Iceberg Parquet LZ4 as Hadoop-framed Parquet "LZ4"
// (not LZ4_RAW) and that Iceberg ORC/delete-file LZ4 paths are reachable.
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

        def q_data = "order_qt_iceberg_parquet_${codec}_data"
        "${q_data}" """ SELECT a, b FROM ${tbl} ORDER BY b """
    }

    def lz4DataFiles = sql """ SELECT file_path FROM ${db}.tbl_lz4\$files ORDER BY file_path """
    String lz4DataFile = lz4DataFiles[0][0].toString()
    order_qt_iceberg_parquet_lz4_footer_codec """
        SELECT DISTINCT compression
        FROM parquet_meta(
            "uri" = "${lz4DataFile}",
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
            "s3.region" = "us-east-1",
            "mode" = "parquet_metadata"
        )
        ORDER BY compression
    """

    sql """ DROP TABLE IF EXISTS ${db}.tbl_orc_lz4 """
    sql """
    CREATE TABLE ${db}.tbl_orc_lz4 (a STRING, b BIGINT) PROPERTIES (
        'write-format' = 'orc',
        'write.format.default' = 'orc',
        'format-version' = '2',
        'write.orc.compression-codec' = 'lz4'
    )"""
    sql """ INSERT INTO ${db}.tbl_orc_lz4 VALUES ('doris0', 0), ('doris1', 1), ('doris2', 2) """
    order_qt_iceberg_orc_lz4_data """ SELECT a, b FROM ${db}.tbl_orc_lz4 ORDER BY b """
    order_qt_iceberg_orc_lz4_files """
        SELECT lower(file_format), sum(record_count)
        FROM ${db}.tbl_orc_lz4\$files
        GROUP BY lower(file_format)
        ORDER BY 1
    """

    sql """ DROP TABLE IF EXISTS ${db}.tbl_lz4_delete """
    sql """
    CREATE TABLE ${db}.tbl_lz4_delete (id INT, name STRING) PROPERTIES (
        'write-format' = 'parquet',
        'write.format.default' = 'parquet',
        'format-version' = '2',
        'write.parquet.compression-codec' = 'lz4',
        'write.delete.mode' = 'merge-on-read',
        'write.update.mode' = 'merge-on-read',
        'write.merge.mode' = 'merge-on-read'
    )"""
    sql """ INSERT INTO ${db}.tbl_lz4_delete VALUES (1, 'a'), (2, 'b'), (3, 'c') """
    qt_iceberg_parquet_lz4_delete """ DELETE FROM ${db}.tbl_lz4_delete WHERE id = 2 """
    order_qt_iceberg_parquet_lz4_delete_data """
        SELECT id, name FROM ${db}.tbl_lz4_delete ORDER BY id
    """
    order_qt_iceberg_parquet_lz4_delete_files """
        SELECT lower(file_format), sum(record_count)
        FROM ${db}.tbl_lz4_delete\$delete_files
        GROUP BY lower(file_format)
        ORDER BY 1
    """
    def lz4DeleteFiles = sql """
        SELECT file_path
        FROM ${db}.tbl_lz4_delete\$delete_files
        ORDER BY file_path
    """
    String lz4DeleteFile = lz4DeleteFiles[0][0].toString()
    order_qt_iceberg_parquet_lz4_delete_footer_codec """
        SELECT DISTINCT compression
        FROM parquet_meta(
            "uri" = "${lz4DeleteFile}",
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
            "s3.region" = "us-east-1",
            "mode" = "parquet_metadata"
        )
        ORDER BY compression
    """

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
}
