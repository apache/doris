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

suite("test_iceberg_spark_doris_consistency_demo", "p2,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String catalogName = "test_iceberg_spark_doris_consistency_demo"
    String dbName = "iceberg_spark_doris_consistency_demo_db"
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    def expectedRows = [
        [1, "alice", 10],
        [2, "bob", 20],
        [3, "cindy", null],
        [4, "doris", 40]
    ]
    def expectedAggRows = [[4L, 70L]]

    // Example: execute multiple Spark Iceberg statements in one JDBC connection.
    spark_iceberg_multi """
        CREATE DATABASE IF NOT EXISTS demo.${dbName};
        DROP TABLE IF EXISTS demo.${dbName}.spark_written_iceberg_demo;
        CREATE TABLE demo.${dbName}.spark_written_iceberg_demo (
            id INT,
            name STRING,
            score INT
        ) USING iceberg;
        INSERT INTO demo.${dbName}.spark_written_iceberg_demo VALUES
            (1, 'alice', 10),
            (2, 'bob', 20),
            (3, 'cindy', NULL);
    """

    // Example: write one more Iceberg row through Spark SQL.
    spark_iceberg """
        INSERT INTO demo.${dbName}.spark_written_iceberg_demo VALUES
            (4, 'doris', 40);
    """

    sql """drop catalog if exists ${catalogName}"""
    sql """
        CREATE CATALOG ${catalogName} PROPERTIES (
            'type'='iceberg',
            'iceberg.catalog.type'='rest',
            'uri' = 'http://${externalEnvIp}:${restPort}',
            's3.access_key' = 'admin',
            's3.secret_key' = 'password',
            's3.endpoint' = 'http://${externalEnvIp}:${minioPort}',
            's3.region' = 'us-east-1'
        );
    """

    sql """switch ${catalogName}"""

    def sparkRows = spark_iceberg """
        SELECT id, name, score
        FROM demo.${dbName}.spark_written_iceberg_demo
        ORDER BY id
    """
    // Example 1: compare Spark Iceberg query result with explicit expected values.
    assertEquals(expectedRows, sparkRows)

    def dorisRows = sql """
        SELECT id, name, score
        FROM ${dbName}.spark_written_iceberg_demo
        ORDER BY id
    """
    // Example 1: compare Doris Iceberg query result with explicit expected values.
    assertEquals(expectedRows, dorisRows)

    // Example 2: compare Doris and Spark query results.
    assertSparkDorisResultEquals(sparkRows, dorisRows)

    def sparkAggRows = spark_iceberg """
        SELECT count(*), sum(score)
        FROM demo.${dbName}.spark_written_iceberg_demo
    """
    // Compare Spark Iceberg aggregate result with explicit expected values.
    assertEquals(expectedAggRows, sparkAggRows)

    def dorisAggRows = sql """
        SELECT count(*), sum(score)
        FROM ${dbName}.spark_written_iceberg_demo
    """
    assertSparkDorisResultEquals(sparkAggRows, dorisAggRows)
}
