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

suite("test_iceberg_system_table_projection", "p0,external,iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String catalogName = "test_iceberg_system_table_projection"
    String dbName = "test_iceberg_system_table_projection_db"
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    sql """drop catalog if exists ${catalogName}"""
    sql """
    CREATE CATALOG ${catalogName} PROPERTIES (
        'type'='iceberg',
        'iceberg.catalog.type'='rest',
        'uri' = 'http://${externalEnvIp}:${restPort}',
        "s3.access_key" = "admin",
        "s3.secret_key" = "password",
        "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
        "s3.region" = "us-east-1"
    );"""

    sql """switch ${catalogName}"""
    sql """create database if not exists ${dbName}"""
    sql """use ${dbName}"""

    def verifySystemTableProjection = { String tableName, String writeFormat ->
        sql """DROP TABLE IF EXISTS ${tableName}"""
        sql """
        CREATE TABLE ${tableName} (
            id int,
            t_map_boolean map<boolean, boolean>,
            dt int
        ) ENGINE=iceberg
        PARTITION BY LIST (dt) ()
        PROPERTIES (
            "write-format" = "${writeFormat}"
        );
        """
        sql """
        INSERT INTO ${tableName}
        VALUES
            (1, MAP(true, false), 20260702);
        """

        List<List<Object>> dataFiles = sql """
            SELECT file_size_in_bytes
            FROM ${tableName}\$data_files
            ORDER BY file_size_in_bytes;
        """
        assertEquals(1, dataFiles.size())
        assertTrue(((Number) dataFiles[0][0]).longValue() > 0)

        List<List<Object>> filesSize = sql """
            SELECT file_size_in_bytes
            FROM ${tableName}\$files
            ORDER BY file_size_in_bytes;
        """
        assertEquals(1, filesSize.size())
        assertTrue(((Number) filesSize[0][0]).longValue() > 0)

        List<List<Object>> files = sql """
            SELECT file_path, record_count
            FROM ${tableName}\$files
            ORDER BY file_path;
        """
        assertEquals(1, files.size())
        assertTrue(files[0][0].toString().contains(tableName))
        assertEquals(1L, ((Number) files[0][1]).longValue())
    }

    def verifyReadableMetricsProjection = { String tableName, String writeFormat ->
        sql """DROP TABLE IF EXISTS ${tableName}"""
        sql """
        CREATE TABLE ${tableName} (
            id int,
            name string,
            dt int
        ) ENGINE=iceberg
        PARTITION BY LIST (dt) ()
        PROPERTIES (
            "write-format" = "${writeFormat}"
        );
        """
        sql """
        INSERT INTO ${tableName}
        VALUES
            (1, 'alice', 20260702),
            (2, 'bob', 20260702);
        """

        List<List<Object>> files = sql """
            SELECT readable_metrics
            FROM ${tableName}\$files
            ORDER BY file_path;
        """
        assertEquals(1, files.size())
        String readableMetrics = files[0][0].toString()
        assertTrue(readableMetrics.contains("\"id\""))
        assertTrue(readableMetrics.contains("\"name\""))
        assertTrue(readableMetrics.contains("\"lower_bound\""))
        assertTrue(readableMetrics.contains("\"upper_bound\""))
    }

    verifySystemTableProjection("test_iceberg_system_table_projection_orc", "orc")
    verifySystemTableProjection("test_iceberg_system_table_projection_parquet", "parquet")
    verifyReadableMetricsProjection("test_iceberg_system_table_projection_readable_metrics", "orc")
}
