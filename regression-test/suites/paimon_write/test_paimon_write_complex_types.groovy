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

suite("test_paimon_write_complex_types", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")

    String catalogName = "test_pw_cx_catalog"
    String dbName = "test_pw_cx_db"

    spark_paimon_multi """
        CREATE DATABASE IF NOT EXISTS paimon.${dbName};

        DROP TABLE IF EXISTS paimon.${dbName}.t_array;
        CREATE TABLE paimon.${dbName}.t_array (
            id INT,
            c_array_int    ARRAY<INT>,
            c_array_string ARRAY<STRING>,
            c_array_double ARRAY<DOUBLE>
        ) USING paimon;

        DROP TABLE IF EXISTS paimon.${dbName}.t_map;
        CREATE TABLE paimon.${dbName}.t_map (
            id INT,
            c_map_str_int    MAP<STRING, INT>,
            c_map_int_str    MAP<INT, STRING>
        ) USING paimon;

        DROP TABLE IF EXISTS paimon.${dbName}.t_struct;
        CREATE TABLE paimon.${dbName}.t_struct (
            id INT,
            c_struct  STRUCT<name:STRING, age:INT>
        ) USING paimon;

        DROP TABLE IF EXISTS paimon.${dbName}.t_nested;
        CREATE TABLE paimon.${dbName}.t_nested (
            id INT,
            c_map_arr MAP<STRING, ARRAY<INT>>
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

        // FT-028: ARRAY types — normal array, empty array, NULL array
        sql """INSERT INTO t_array VALUES
            (1, [1, 2, 3], ['a', 'b', 'c'], [1.1, 2.2]),
            (2, [], [], []),
            (3, [10, NULL, 30], ['x', NULL, 'z'], [NULL, 2.0]),
            (4, NULL, NULL, NULL)
        """
        order_qt_cx_array """SELECT id, c_array_int, c_array_string, c_array_double FROM t_array ORDER BY id"""
        assertTableEquals("t_array", "ORDER BY id")

        // FT-029: MAP types — normal map, empty map, NULL value
        sql """INSERT INTO t_map VALUES
            (1, map('math', 90, 'eng', 95), map(1, 'one', 2, 'two')),
            (2, map(), map()),
            (3, map('science', NULL), map(3, NULL)),
            (4, NULL, NULL)
        """
        order_qt_cx_map """SELECT id, c_map_str_int, c_map_int_str FROM t_map ORDER BY id"""
        assertTableEquals("t_map", "ORDER BY id")

        // FT-030: STRUCT types
        sql """INSERT INTO t_struct VALUES
            (1, named_struct('name', 'alice', 'age', 30)),
            (2, named_struct('name', NULL, 'age', NULL)),
            (3, NULL)
        """
        order_qt_cx_struct """SELECT id, c_struct FROM t_struct ORDER BY id"""
        assertTableEquals("t_struct", "ORDER BY id")

        // Nested: MAP<STRING, ARRAY<INT>>
        sql """INSERT INTO t_nested VALUES
            (1, map('group1', [1, 2], 'group2', [3, 4, 5])),
            (2, map('empty', [])),
            (3, NULL)
        """
        order_qt_cx_nested """SELECT id, c_map_arr FROM t_nested ORDER BY id"""
        assertTableEquals("t_nested", "ORDER BY id")
    } finally {
        sql """drop catalog if exists ${catalogName}"""
    }
}
