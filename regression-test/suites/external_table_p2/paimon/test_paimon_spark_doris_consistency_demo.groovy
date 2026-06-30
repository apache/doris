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

suite("test_paimon_spark_doris_consistency_demo", "p2,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String catalogName = "test_paimon_spark_doris_consistency_demo"
    String dbName = "paimon_spark_doris_consistency_demo_db"
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    def expectedRows = [
        [1, "alice", 10],
        [2, "bob", 20],
        [3, "cindy", null],
        [4, "doris", 40]
    ]
    def expectedAggRows = [[4L, 70L]]

    // Example: execute multiple Spark Paimon statements in one JDBC connection.
    spark_paimon_multi """
        CREATE DATABASE IF NOT EXISTS paimon.${dbName};
        DROP TABLE IF EXISTS paimon.${dbName}.spark_written_paimon_demo;
        CREATE TABLE paimon.${dbName}.spark_written_paimon_demo (
            id INT,
            name STRING,
            score INT
        ) USING paimon;
        INSERT INTO paimon.${dbName}.spark_written_paimon_demo VALUES
            (1, 'alice', 10),
            (2, 'bob', 20),
            (3, 'cindy', NULL);
    """

    // Example: write one more Paimon row through Spark SQL.
    spark_paimon """
        INSERT INTO paimon.${dbName}.spark_written_paimon_demo VALUES
            (4, 'doris', 40);
    """

    sql """drop catalog if exists ${catalogName}"""
    sql """
        CREATE CATALOG ${catalogName} PROPERTIES (
            'type' = 'paimon',
            'warehouse' = 's3://warehouse/wh',
            's3.endpoint' = 'http://${externalEnvIp}:${minioPort}',
            's3.access_key' = 'admin',
            's3.secret_key' = 'password',
            's3.path.style.access' = 'true'
        );
    """

    sql """switch ${catalogName}"""

    def sparkRows = spark_paimon """
        SELECT id, name, score
        FROM paimon.${dbName}.spark_written_paimon_demo
        ORDER BY id
    """
    // Example 1: compare Spark Paimon query result with explicit expected values.
    assertEquals(expectedRows, sparkRows)

    def dorisRows = sql """
        SELECT id, name, score
        FROM ${dbName}.spark_written_paimon_demo
        ORDER BY id
    """
    // Example 1: compare Doris Paimon query result with explicit expected values.
    assertEquals(expectedRows, dorisRows)

    // Example 2: compare Doris and Spark query results.
    assertSparkDorisResultEquals(sparkRows, dorisRows)

    def sparkAggRows = spark_paimon """
        SELECT count(*), sum(score)
        FROM paimon.${dbName}.spark_written_paimon_demo
    """
    // Compare Spark Paimon aggregate result with explicit expected values.
    assertEquals(expectedAggRows, sparkAggRows)

    def dorisAggRows = sql """
        SELECT count(*), sum(score)
        FROM ${dbName}.spark_written_paimon_demo
    """
    assertSparkDorisResultEquals(sparkAggRows, dorisAggRows)
}
