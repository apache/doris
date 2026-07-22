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

suite("test_paimon_write_append_only", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")

    String catalogName = "test_pw_ao_catalog"
    String dbName = "test_pw_ao_db"

    // Tables are created via Spark because Doris does not yet support
    // Paimon DDL (CREATE TABLE ... engine=paimon).
    spark_paimon_multi """
        CREATE DATABASE IF NOT EXISTS paimon.${dbName};
        DROP TABLE IF EXISTS paimon.${dbName}.t_append;
        CREATE TABLE paimon.${dbName}.t_append (
            id INT, name STRING, score DOUBLE
        ) USING paimon;

        DROP TABLE IF EXISTS paimon.${dbName}.t_append_part;
        CREATE TABLE paimon.${dbName}.t_append_part (
            id INT, name STRING, score DOUBLE, region STRING
        ) USING paimon
        PARTITIONED BY (region)
       ;

        DROP TABLE IF EXISTS paimon.${dbName}.t_append_empty;
        CREATE TABLE paimon.${dbName}.t_append_empty (
            id INT, name STRING
        ) USING paimon;

        DROP TABLE IF EXISTS paimon.${dbName}.t_append_default;
        CREATE TABLE paimon.${dbName}.t_append_default (
            id INT, name STRING NOT NULL DEFAULT 'unknown'
        ) USING paimon;
    """

    sql """drop catalog if exists ${catalogName}"""
    sql """
        CREATE CATALOG ${catalogName} PROPERTIES (
            'type' = 'paimon',
            'paimon.catalog.type' = 'filesystem',
            'warehouse' = 's3://warehouse/wh',
            's3.endpoint' = 'http://${externalEnvIp}:${minioPort}',
            's3.access_key' = 'admin',
            's3.secret_key' = 'password',
            's3.path.style.access' = 'true'
        );
    """
    sql """switch ${catalogName}"""
    sql """use ${dbName}"""

    try {
        def assertTableEquals = { String tableName, String orderBy ->
            def sparkRows = spark_paimon """SELECT * FROM paimon.${dbName}.${tableName} ${orderBy}"""
            def dorisRows = sql """SELECT * FROM ${tableName} ${orderBy}"""
            assertSparkDorisResultEquals(sparkRows, dorisRows)
        }

        // FT-001: Append-only table — basic INSERT
        sql """INSERT INTO t_append VALUES (1, 'alice', 95.5)"""
        sql """INSERT INTO t_append VALUES (2, 'bob', 87.0), (3, 'charlie', 92.3)"""
        order_qt_ao_basic """SELECT * FROM t_append ORDER BY id"""

        sql """INSERT INTO t_append VALUES (4, 'diana', 88.0), (5, 'eve', 91.0)"""
        // Full-column and partial-column writes with columns in non-schema order
        sql """INSERT INTO t_append (score, name, id) VALUES (93.0, 'frank', 6)"""
        sql """INSERT INTO t_append (name, id) VALUES ('grace', 7)"""
        assertTableEquals("t_append", "ORDER BY id")

        // FT-002: Partitioned append-only
        sql """INSERT INTO t_append_part VALUES (1, 'alice', 95.5, 'east'), (2, 'bob', 87.0, 'west')"""
        sql """INSERT INTO t_append_part VALUES (3, 'charlie', 92.3, 'east'), (4, 'diana', 88.0, 'north')"""
        // Keep the partition column away from its schema position in both full and partial writes
        sql """INSERT INTO t_append_part (region, score, name, id)
            VALUES ('south', 86.5, 'erin', 5)"""
        sql """INSERT INTO t_append_part (region, id) VALUES ('east', 6)"""
        order_qt_ao_part """SELECT * FROM t_append_part ORDER BY id"""
        assertTableEquals("t_append_part", "ORDER BY id")

        // FT-014: Empty INSERT — should succeed with 0 rows
        sql """INSERT INTO t_append_empty SELECT 1, 'test' WHERE 1 = 0"""
        sql """INSERT INTO t_append_empty (id) VALUES (1)"""
        sql """INSERT INTO t_append_empty (name, id) VALUES ('reordered', 2)"""
        assertTableEquals("t_append_empty", "ORDER BY id")

        // FT-043: Omitted NOT NULL columns use Paimon schema defaults.
        sql """INSERT INTO t_append_default (id) VALUES (1)"""
        order_qt_ao_default_value """SELECT id, name FROM t_append_default ORDER BY id"""
        assertTableEquals("t_append_default", "ORDER BY id")

        // FT-044: Duplicate target columns are rejected case-insensitively.
        test {
            sql """INSERT INTO t_append (id, ID) VALUES (8, 9)"""
            exception "Duplicate column"
        }
    } finally {
        sql """drop catalog if exists ${catalogName}"""
    }
}
