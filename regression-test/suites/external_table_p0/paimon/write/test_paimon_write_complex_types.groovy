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
        SET spark.sql.binaryOutputStyle=HEX;
        SET spark.sql.timestampType=TIMESTAMP_NTZ;
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

        DROP TABLE IF EXISTS paimon.${dbName}.t_recursive;
        CREATE TABLE paimon.${dbName}.t_recursive (
            id INT,
            c_array_decimal ARRAY<DECIMAL(18, 6)>,
            c_array_date ARRAY<DATE>,
            c_array_timestamp ARRAY<TIMESTAMP_NTZ>,
            c_map_decimal MAP<DECIMAL(8, 2), DECIMAL(8, 2)>,
            c_struct_mixed STRUCT<flag:BOOLEAN, amount:DECIMAL(18, 6),
                                  event_date:DATE, event_time:TIMESTAMP_NTZ>,
            c_deep MAP<STRING, ARRAY<STRUCT<score:INT, label:STRING>>>
        ) USING paimon;

        DROP TABLE IF EXISTS paimon.${dbName}.t_binary;
        CREATE TABLE paimon.${dbName}.t_binary (
            id INT,
            c_binary BINARY,
            c_array_binary ARRAY<BINARY>,
            c_map_binary MAP<STRING, BINARY>,
            c_struct_binary STRUCT<label:STRING, payload:BINARY>
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
            's3.path.style.access' = 'true',
            'enable.mapping.varbinary' = 'true'
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
        // MAP entry order is not part of the SQL result contract. Project known keys so the
        // golden result validates the nested arrays without depending on map rendering order.
        order_qt_cx_nested """SELECT id,
            element_at(c_map_arr, 'group1'),
            element_at(c_map_arr, 'group2'),
            element_at(c_map_arr, 'empty')
            FROM t_nested ORDER BY id"""
        assertTableEquals("t_nested", "ORDER BY id")

        // Recursive conversion covers the non-trivial Arrow child vectors which
        // cannot use the primitive column fast path in PaimonArrowConverter.
        sql """INSERT INTO t_recursive VALUES
            (
                1,
                array(CAST(1.250000 AS DECIMAL(18, 6)), CAST(-2.500000 AS DECIMAL(18, 6))),
                array(DATE '2024-01-01', DATE '2024-12-31'),
                array(TIMESTAMP '2024-01-01 01:02:03.123456',
                      TIMESTAMP '2024-12-31 23:59:59.654321'),
                map(CAST(1.25 AS DECIMAL(8, 2)), CAST(2.50 AS DECIMAL(8, 2)),
                    CAST(-3.75 AS DECIMAL(8, 2)), CAST(4.00 AS DECIMAL(8, 2))),
                named_struct(
                    'flag', true,
                    'amount', CAST(123.456789 AS DECIMAL(18, 6)),
                    'event_date', DATE '2024-02-29',
                    'event_time', TIMESTAMP '2024-02-29 12:34:56.000001'),
                map('term', array(
                    named_struct('score', 90, 'label', 'good'),
                    named_struct('score', 95, 'label', 'better')
                ))
            ),
            (
                2,
                array(CAST(NULL AS DECIMAL(18, 6)), CAST(0.000001 AS DECIMAL(18, 6))),
                array(CAST(NULL AS DATE), DATE '1970-01-01'),
                array(CAST(NULL AS DATETIME(6)), TIMESTAMP '1970-01-01 00:00:00.000001'),
                map(CAST(5.25 AS DECIMAL(8, 2)), CAST(NULL AS DECIMAL(8, 2))),
                named_struct(
                    'flag', CAST(NULL AS BOOLEAN),
                    'amount', CAST(NULL AS DECIMAL(18, 6)),
                    'event_date', CAST(NULL AS DATE),
                    'event_time', CAST(NULL AS DATETIME(6))),
                map('nullable', array(
                    named_struct('score', CAST(NULL AS INT), 'label', CAST(NULL AS STRING))
                ))
            ),
            (3, [], [], [], map(), named_struct(
                'flag', false,
                'amount', CAST(0 AS DECIMAL(18, 6)),
                'event_date', DATE '1970-01-01',
                'event_time', TIMESTAMP '1970-01-01 00:00:00'), map())
        """

        // Every projected field is deliberately in reverse table order. This
        // verifies that target type conversion follows Doris input order while
        // PaimonWriteSchema restores canonical table-schema order.
        sql """INSERT INTO t_recursive (
                c_deep, c_struct_mixed, c_map_decimal, c_array_timestamp,
                c_array_date, c_array_decimal, id
            ) VALUES (
                map('reverse', array(named_struct('score', 88, 'label', 'reordered'))),
                named_struct(
                    'flag', true,
                    'amount', CAST(8.800000 AS DECIMAL(18, 6)),
                    'event_date', DATE '2025-01-01',
                    'event_time', TIMESTAMP '2025-01-01 08:08:08.000008'),
                map(CAST(8.80 AS DECIMAL(8, 2)), CAST(9.90 AS DECIMAL(8, 2))),
                array(TIMESTAMP '2025-01-01 00:00:00.000008'),
                array(DATE '2025-01-01'),
                array(CAST(8.800008 AS DECIMAL(18, 6))),
                4
            )
        """

        // A reordered subset expands to a full table row with NULL in every
        // omitted nullable field.
        sql """INSERT INTO t_recursive (c_deep, c_array_date, id) VALUES (
            map('partial', array(named_struct('score', 77, 'label', 'subset'))),
            array(DATE '2026-01-01'),
            5
        )"""
        order_qt_cx_recursive """SELECT * FROM t_recursive ORDER BY id"""
        assertTableEquals("t_recursive", "ORDER BY id")

        // Top-level and recursively nested BINARY values exercise both the Arrow
        // VarBinaryVector fast path and nested convertVectorValue branches.
        sql """INSERT INTO t_binary VALUES
            (
                1,
                X'0001FEFF',
                [X'41', X'00FF'],
                map('payload', X'102030'),
                named_struct('label', 'binary_1', 'payload', X'DEADBEEF')
            ),
            (
                2,
                NULL,
                [],
                map(),
                named_struct('label', 'binary_2', 'payload', CAST(NULL AS VARBINARY))
            ),
            (3, X'E4B8ADE69687', NULL, NULL, NULL)
        """
        // Reordering binary and nested columns also verifies that their target
        // Paimon types are resolved by projected column name rather than position.
        sql """INSERT INTO t_binary (
                c_struct_binary, c_map_binary, c_array_binary, c_binary, id
            ) VALUES (
                named_struct('label', 'reordered', 'payload', X'ABCD'),
                map('payload', X'0102'),
                [X'03', X'0405'],
                X'060708',
                4
            )
        """
        order_qt_cx_binary """
            SELECT id,
                   HEX(c_binary),
                   SIZE(c_array_binary),
                   HEX(ELEMENT_AT(c_array_binary, 1)),
                   SIZE(c_map_binary),
                   HEX(ELEMENT_AT(c_map_binary, 'payload')),
                   c_struct_binary.label,
                   HEX(c_struct_binary.payload)
            FROM t_binary
            ORDER BY id
        """
        def sparkBinaryRows = spark_paimon """
            SELECT id,
                   HEX(c_binary),
                   CASE WHEN SIZE(c_array_binary) >= 1
                        THEN HEX(ELEMENT_AT(c_array_binary, 1)) END,
                   CASE WHEN SIZE(c_array_binary) >= 2
                        THEN HEX(ELEMENT_AT(c_array_binary, 2)) END,
                   HEX(ELEMENT_AT(c_map_binary, 'payload')),
                   c_struct_binary.label,
                   HEX(c_struct_binary.payload)
            FROM paimon.${dbName}.t_binary
            ORDER BY id
        """
        def dorisBinaryRows = sql """
            SELECT id,
                   HEX(c_binary),
                   CASE WHEN SIZE(c_array_binary) >= 1
                        THEN HEX(ELEMENT_AT(c_array_binary, 1)) END,
                   CASE WHEN SIZE(c_array_binary) >= 2
                        THEN HEX(ELEMENT_AT(c_array_binary, 2)) END,
                   HEX(ELEMENT_AT(c_map_binary, 'payload')),
                   c_struct_binary.label,
                   HEX(c_struct_binary.payload)
            FROM t_binary
            ORDER BY id
        """
        assertSparkDorisResultEquals(sparkBinaryRows, dorisBinaryRows)
    } finally {
        sql """drop catalog if exists ${catalogName}"""
    }
}
