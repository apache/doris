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

suite("test_iceberg_spark_doris_metrics_consistency", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String catalogName = "test_iceberg_spark_doris_metrics_consistency"
    String dbName = "test_iceberg_spark_doris_metrics_consistency_db"
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    spark_iceberg_multi """
        CREATE DATABASE IF NOT EXISTS demo.${dbName};
        DROP TABLE IF EXISTS demo.${dbName}.spark_doris_orc_map_bool_metrics;
        CREATE TABLE demo.${dbName}.spark_doris_orc_map_bool_metrics (
            id INT,
            name STRING,
            score BIGINT,
            t_map_boolean MAP<BOOLEAN, BOOLEAN>,
            dt INT
        ) USING iceberg
        PARTITIONED BY (dt)
        TBLPROPERTIES (
            'write.format.default'='orc',
            'write.orc.compression-codec'='zlib'
        );
        INSERT INTO demo.${dbName}.spark_doris_orc_map_bool_metrics
        VALUES (1, 'alice', 7000000000, MAP(true, false), 20260702);

        DROP TABLE IF EXISTS demo.${dbName}.spark_doris_parquet_map_bool_metrics;
        CREATE TABLE demo.${dbName}.spark_doris_parquet_map_bool_metrics (
            id INT,
            name STRING,
            score BIGINT,
            t_map_boolean MAP<BOOLEAN, BOOLEAN>,
            dt INT
        ) USING iceberg
        PARTITIONED BY (dt)
        TBLPROPERTIES (
            'write.format.default'='parquet',
            'write.parquet.compression-codec'='zstd'
        );
        INSERT INTO demo.${dbName}.spark_doris_parquet_map_bool_metrics
        VALUES (1, 'alice', 7000000000, MAP(true, false), 20260702);
    """

    sql """drop catalog if exists ${catalogName}"""
    sql """
        CREATE CATALOG ${catalogName} PROPERTIES (
            'type'='iceberg',
            'iceberg.catalog.type'='rest',
            'uri'='http://${externalEnvIp}:${restPort}',
            's3.access_key'='admin',
            's3.secret_key'='password',
            's3.endpoint'='http://${externalEnvIp}:${minioPort}',
            's3.region'='us-east-1',
            'enable.mapping.varbinary'='true'
        )
    """

    sql """switch ${catalogName}"""
    sql """create database if not exists ${dbName}"""
    sql """use ${dbName}"""

    def assertSparkDorisMetricBoundsEqual = { String tableName, String complexFieldId ->
        sql """refresh table ${dbName}.${tableName}"""
        List<List<Object>> metricRows = sql """
            SELECT record_count, lower_bounds, upper_bounds
            FROM ${tableName}\$files
        """
        assertEquals(2, metricRows.size(), "${tableName} should have one Spark file and one Doris file")
        Set<String> metricSignatures = metricRows.collect { row -> row.toString() } as Set
        assertEquals(1, metricSignatures.size(),
                "${tableName} should have identical Spark and Doris bounds metrics: ${metricRows}")
        metricRows.each { row ->
            assertTrue(!String.valueOf(row[1]).contains("${complexFieldId}:"),
                    "${tableName} lower_bounds should not contain complex field ${complexFieldId}: ${row[1]}")
            assertTrue(!String.valueOf(row[2]).contains("${complexFieldId}:"),
                    "${tableName} upper_bounds should not contain complex field ${complexFieldId}: ${row[2]}")
        }

        sql """SELECT * FROM ${tableName}\$data_files"""
    }

    sql """
        INSERT INTO spark_doris_orc_map_bool_metrics
        VALUES (1, 'alice', 7000000000, MAP(true, false), 20260702)
    """
    assertSparkDorisMetricBoundsEqual("spark_doris_orc_map_bool_metrics", "4")

    sql """
        INSERT INTO spark_doris_parquet_map_bool_metrics
        VALUES (1, 'alice', 7000000000, MAP(true, false), 20260702)
    """
    assertSparkDorisMetricBoundsEqual("spark_doris_parquet_map_bool_metrics", "4")
}
