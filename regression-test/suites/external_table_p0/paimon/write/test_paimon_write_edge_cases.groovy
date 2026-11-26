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

suite("test_paimon_write_edge_cases", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")

    String catalogName = "test_pw_edge_catalog"
    String dbName = "test_pw_edge_db"

    spark_paimon_multi """
        CREATE DATABASE IF NOT EXISTS paimon.${dbName};

        DROP TABLE IF EXISTS paimon.${dbName}.t_edge_str;
        CREATE TABLE paimon.${dbName}.t_edge_str (
            id INT, c_string STRING, c_varchar VARCHAR(10)
        ) USING paimon;

        DROP TABLE IF EXISTS paimon.${dbName}.t_edge_numeric;
        CREATE TABLE paimon.${dbName}.t_edge_numeric (
            id INT,
            c_tiny   TINYINT,
            c_small  SMALLINT,
            c_int    INT,
            c_bigint BIGINT,
            c_float  FLOAT,
            c_double DOUBLE
        ) USING paimon;

        DROP TABLE IF EXISTS paimon.${dbName}.t_edge_bool;
        CREATE TABLE paimon.${dbName}.t_edge_bool (
            id INT, c_bool BOOLEAN
        ) USING paimon;

        DROP TABLE IF EXISTS paimon.${dbName}.t_edge_pk_null;
        CREATE TABLE paimon.${dbName}.t_edge_pk_null (
            id INT, name STRING
        ) USING paimon
        TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1', 'bucket-key' = 'id');

        DROP TABLE IF EXISTS paimon.${dbName}.t_mixed_write;
        CREATE TABLE paimon.${dbName}.t_mixed_write (
            id INT, name STRING, score DOUBLE
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
        def assertTableEquals = { String tableName, String columns, String orderBy ->
            def sparkRows = spark_paimon """SELECT ${columns} FROM paimon.${dbName}.${tableName} ${orderBy}"""
            def dorisRows = sql """SELECT ${columns} FROM ${tableName} ${orderBy}"""
            assertSparkDorisResultEquals(sparkRows, dorisRows)
        }

        // FT-042: Empty string and boundary VARCHAR
        sql """INSERT INTO t_edge_str VALUES
            (1, '', ''),
            (2, 'hello world', 'short_str'),
            (3, 'x', 'abcdefghij'),
            (4, 'very long string over 100 chars: 1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890', 'max10chars')
        """
        order_qt_edge_str """SELECT id, c_varchar FROM t_edge_str ORDER BY id"""
        assertTableEquals("t_edge_str", "*", "ORDER BY id")

        // FT-021: Numeric boundary values (INT_MIN, INT_MAX, etc.)
        sql """INSERT INTO t_edge_numeric VALUES
            (1, CAST(127 AS TINYINT),  CAST(32767 AS SMALLINT), 2147483647,
             CAST(9223372036854775807 AS BIGINT),
             CAST(3.4028235E38 AS FLOAT), CAST(1.7976931348623157E308 AS DOUBLE)),
            (2, CAST(-128 AS TINYINT), CAST(-32768 AS SMALLINT), -2147483648,
             CAST(-9223372036854775808 AS BIGINT),
             CAST(-3.4028235E38 AS FLOAT), CAST(-1.7976931348623157E308 AS DOUBLE)),
            (3, CAST(0 AS TINYINT),  CAST(0 AS SMALLINT), 0,
             CAST(0 AS BIGINT),
             CAST(0.0 AS FLOAT), CAST(0.0 AS DOUBLE))
        """
        order_qt_edge_numeric """SELECT id, c_tiny, c_small, c_int, c_bigint FROM t_edge_numeric ORDER BY id"""
        assertTableEquals("t_edge_numeric", """
                id, c_tiny, c_small, c_int, c_bigint,
                c_float / 1.0E38,
                c_double / 1.0E308
                """, "ORDER BY id")

        // BOOLEAN with NULL and both true/false
        sql """INSERT INTO t_edge_bool VALUES (1, true), (2, false), (3, NULL)"""
        order_qt_edge_bool """SELECT id, c_bool FROM t_edge_bool ORDER BY id"""
        assertTableEquals("t_edge_bool", "*", "ORDER BY id")

        // FT-041: PK table — insert NULL values, then update with non-NULL
        sql """INSERT INTO t_edge_pk_null VALUES (1, 'first'), (2, NULL)"""
        assertTableEquals("t_edge_pk_null", "*", "ORDER BY id")

        sql """INSERT INTO t_edge_pk_null VALUES (2, 'updated'), (3, 'third')"""
        order_qt_edge_pk_null """SELECT id, name FROM t_edge_pk_null ORDER BY id"""
        assertTableEquals("t_edge_pk_null", "*", "ORDER BY id")

        // Mixed INSERT patterns: VALUES, then SELECT from self, then single-row VALUES
        sql """INSERT INTO t_mixed_write VALUES (1, 'a', 10.0), (2, 'b', 20.0)"""
        sql """INSERT INTO t_mixed_write VALUES (3, 'c', 30.0)"""
        sql """INSERT INTO t_mixed_write SELECT id + 3, concat(name, '_copy'), score + 30.0 FROM t_mixed_write"""
        order_qt_edge_mixed """SELECT id, name, score FROM t_mixed_write ORDER BY id"""
        assertTableEquals("t_mixed_write", "*", "ORDER BY id")
    } finally {
        sql """drop catalog if exists ${catalogName}"""
    }
}
