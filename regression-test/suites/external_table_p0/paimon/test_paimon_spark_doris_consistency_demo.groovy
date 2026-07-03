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

suite("test_paimon_spark_doris_consistency_demo", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String catalogName = "test_paimon_spark_doris_consistency_demo"
    String dbName = "paimon_spark_doris_consistency_demo_db"
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    def expectedBasicRows = [
        [1, "alice", 10],
        [2, "bob", 20],
        [3, "cindy", null],
        [4, "doris", 40],
        [5, "edge", 0]
    ]
    def expectedAggRows = [[5L, 70L]]

    // Example: execute multiple Spark Paimon statements in one JDBC connection.
    spark_paimon_multi """
        SET spark.sql.binaryOutputStyle=HEX;
        SET spark.sql.preserveCharVarcharTypeInfo=true;
        SET spark.sql.timestampType=TIMESTAMP_NTZ;
        CREATE DATABASE IF NOT EXISTS paimon.${dbName};
        DROP TABLE IF EXISTS paimon.${dbName}.spark_written_paimon_demo;
        CREATE TABLE paimon.${dbName}.spark_written_paimon_demo (
            id INT,
            name STRING,
            score INT,
            string_col STRING,
            varchar_col VARCHAR(20),
            char_col CHAR(10),
            bool_col BOOLEAN,
            tinyint_col TINYINT,
            smallint_col SMALLINT,
            int_col INT,
            bigint_col BIGINT,
            float_col FLOAT,
            double_col DOUBLE,
            decimal_small_col DECIMAL(9, 2),
            decimal_col DECIMAL(18, 6),
            decimal_wide_col DECIMAL(38, 12),
            date_col DATE,
            timestamp_col TIMESTAMP,
            binary_col BINARY,
            array_col ARRAY<INT>,
            array_tinyint_col ARRAY<TINYINT>,
            array_smallint_col ARRAY<SMALLINT>,
            array_string_col ARRAY<STRING>,
            array_bool_col ARRAY<BOOLEAN>,
            array_binary_col ARRAY<BINARY>,
            array_decimal_col ARRAY<DECIMAL(18, 6)>,
            array_date_col ARRAY<DATE>,
            array_timestamp_col ARRAY<TIMESTAMP>,
            map_col MAP<STRING, INT>,
            map_int_string_col MAP<INT, STRING>,
            map_bool_col MAP<BOOLEAN, BOOLEAN>,
            map_binary_col MAP<STRING, BINARY>,
            map_decimal_col MAP<DECIMAL(8, 2), DECIMAL(8, 2)>,
            struct_col STRUCT<city:STRING, zip:INT>,
            struct_all_col STRUCT<string_field:STRING, bool_field:BOOLEAN, int_field:INT,
                                  bigint_field:BIGINT, float_field:FLOAT, double_field:DOUBLE,
                                  binary_field:BINARY, decimal_field:DECIMAL(18, 6), date_field:DATE,
                                  timestamp_field:TIMESTAMP>,
            nested_col MAP<STRING, ARRAY<STRUCT<score:INT, label:STRING>>>
        ) USING paimon;
        INSERT INTO paimon.${dbName}.spark_written_paimon_demo VALUES
            (
                1, 'alice', 10, 'alice-string', 'alice-varchar', 'alice_char', true,
                CAST(7 AS TINYINT), CAST(70 AS SMALLINT), 700, 7000000000,
                CAST(1.25 AS FLOAT), CAST(10.125 AS DOUBLE),
                CAST(12.34 AS DECIMAL(9, 2)),
                CAST(12345.678900 AS DECIMAL(18, 6)),
                CAST(123456789.012345678901 AS DECIMAL(38, 12)),
                DATE '2024-03-20', TIMESTAMP '2024-03-20 12:00:00.123456',
                CAST('alice-bin' AS BINARY),
                ARRAY(1, 2, 3), ARRAY(CAST(1 AS TINYINT), CAST(2 AS TINYINT)),
                ARRAY(CAST(10 AS SMALLINT), CAST(20 AS SMALLINT)),
                ARRAY(CAST(NULL AS STRING), 'a', 'b'),
                ARRAY(true, false),
                ARRAY(CAST('alice-array-bin' AS BINARY), CAST(NULL AS BINARY)),
                ARRAY(CAST(1.250000 AS DECIMAL(18, 6)), CAST(2.500000 AS DECIMAL(18, 6))),
                ARRAY(DATE '2024-03-20', DATE '2024-03-21'),
                ARRAY(TIMESTAMP '2024-03-20 12:00:00.123456'),
                MAP('math', 90, 'eng', 95),
                MAP(1, 'one', 2, 'two'),
                MAP(true, false, false, true),
                MAP('payload', CAST('alice-map-bin' AS BINARY)),
                MAP(CAST(1.25 AS DECIMAL(8, 2)), CAST(2.50 AS DECIMAL(8, 2)),
                    CAST(3.75 AS DECIMAL(8, 2)), CAST(4.00 AS DECIMAL(8, 2))),
                NAMED_STRUCT('city', 'Beijing', 'zip', 100000),
                NAMED_STRUCT('string_field', 'alice', 'bool_field', true, 'int_field', 700,
                    'bigint_field', 7000000000, 'float_field', CAST(1.25 AS FLOAT),
                    'double_field', CAST(10.125 AS DOUBLE),
                    'binary_field', CAST('alice-struct-bin' AS BINARY),
                    'decimal_field', CAST(12345.678900 AS DECIMAL(18, 6)),
                    'date_field', DATE '2024-03-20',
                    'timestamp_field', TIMESTAMP '2024-03-20 12:00:00.123456'),
                MAP('term', ARRAY(NAMED_STRUCT('score', 90, 'label', 'good')))
            ),
            (
                2, 'bob', 20, 'bob-string', 'bob-varchar', 'bob_char__', false,
                CAST(-8 AS TINYINT), CAST(-80 AS SMALLINT), -800, -8000000000,
                CAST(-2.5 AS FLOAT), CAST(-20.25 AS DOUBLE),
                CAST(-98.76 AS DECIMAL(9, 2)),
                CAST(-9876.543210 AS DECIMAL(18, 6)),
                CAST(-987654321.012345678901 AS DECIMAL(38, 12)),
                DATE '2024-03-21', TIMESTAMP '2024-03-21 13:01:02.654321',
                CAST('bob-bin' AS BINARY),
                ARRAY(4, 5), ARRAY(CAST(-1 AS TINYINT), CAST(-2 AS TINYINT)),
                ARRAY(CAST(-10 AS SMALLINT), CAST(-20 AS SMALLINT)),
                ARRAY('x', CAST(NULL AS STRING), 'z'),
                ARRAY(false, true),
                ARRAY(CAST('bob-array-bin' AS BINARY), CAST('bob-array-bin-2' AS BINARY)),
                ARRAY(CAST(-3.750000 AS DECIMAL(18, 6)), CAST(4.125000 AS DECIMAL(18, 6))),
                ARRAY(DATE '2024-03-21'),
                ARRAY(TIMESTAMP '2024-03-21 13:01:02.654321',
                    TIMESTAMP '2024-03-21 13:01:03.000000'),
                MAP('math', 80, 'eng', 85),
                MAP(3, 'three', 4, 'four'),
                MAP(true, true, false, false),
                MAP('payload', CAST('bob-map-bin' AS BINARY)),
                MAP(CAST(-1.25 AS DECIMAL(8, 2)), CAST(-2.50 AS DECIMAL(8, 2))),
                NAMED_STRUCT('city', 'Shanghai', 'zip', 200000),
                NAMED_STRUCT('string_field', 'bob', 'bool_field', false, 'int_field', -800,
                    'bigint_field', -8000000000, 'float_field', CAST(-2.5 AS FLOAT),
                    'double_field', CAST(-20.25 AS DOUBLE),
                    'binary_field', CAST('bob-struct-bin' AS BINARY),
                    'decimal_field', CAST(-9876.543210 AS DECIMAL(18, 6)),
                    'date_field', DATE '2024-03-21',
                    'timestamp_field', TIMESTAMP '2024-03-21 13:01:02.654321'),
                MAP('term', ARRAY(
                    NAMED_STRUCT('score', 80, 'label', 'pass'),
                    NAMED_STRUCT('score', 85, 'label', 'better')
                ))
            ),
            (
                3, 'cindy', NULL, NULL, NULL, NULL, NULL,
                NULL, NULL, NULL,
                NULL, NULL, NULL, NULL, NULL,
                NULL, NULL, NULL, NULL,
                ARRAY(CAST(NULL AS INT), 6), ARRAY(CAST(NULL AS TINYINT)),
                ARRAY(CAST(NULL AS SMALLINT)),
                ARRAY(CAST(NULL AS STRING)),
                ARRAY(CAST(NULL AS BOOLEAN), true),
                ARRAY(CAST(NULL AS BINARY)),
                ARRAY(CAST(NULL AS DECIMAL(18, 6))),
                ARRAY(CAST(NULL AS DATE)),
                ARRAY(CAST(NULL AS TIMESTAMP)),
                MAP('science', CAST(NULL AS INT)),
                MAP(5, CAST(NULL AS STRING)),
                MAP(false, CAST(NULL AS BOOLEAN)),
                MAP('payload', CAST(NULL AS BINARY)),
                MAP(CAST(5.25 AS DECIMAL(8, 2)), CAST(NULL AS DECIMAL(8, 2))),
                NAMED_STRUCT('city', CAST(NULL AS STRING), 'zip', CAST(NULL AS INT)),
                NAMED_STRUCT('string_field', CAST(NULL AS STRING), 'bool_field', CAST(NULL AS BOOLEAN),
                    'int_field', CAST(NULL AS INT),
                    'bigint_field', CAST(NULL AS BIGINT), 'float_field', CAST(NULL AS FLOAT),
                    'double_field', CAST(NULL AS DOUBLE),
                    'binary_field', CAST(NULL AS BINARY),
                    'decimal_field', CAST(NULL AS DECIMAL(18, 6)),
                    'date_field', CAST(NULL AS DATE),
                    'timestamp_field', CAST(NULL AS TIMESTAMP)),
                NULL
            );
    """

    // Example: write one more Paimon row through Spark SQL.
    spark_paimon """
        INSERT INTO paimon.${dbName}.spark_written_paimon_demo VALUES
            (
                4, 'doris', 40, 'doris-string', 'doris-varchar', 'doris_char', true,
                CAST(4 AS TINYINT), CAST(400 AS SMALLINT), 4000, 4000000000,
                CAST(4.5 AS FLOAT), CAST(40.75 AS DOUBLE),
                CAST(44.44 AS DECIMAL(9, 2)),
                CAST(4444.000001 AS DECIMAL(18, 6)),
                CAST(444444444.000000000001 AS DECIMAL(38, 12)),
                DATE '2024-03-22', TIMESTAMP '2024-03-22 14:02:03.000001',
                CAST('doris-bin' AS BINARY),
                ARRAY(7, 8, 9), ARRAY(CAST(3 AS TINYINT), CAST(4 AS TINYINT)),
                ARRAY(CAST(30 AS SMALLINT), CAST(40 AS SMALLINT)),
                ARRAY('d', 'o', 'ris'),
                ARRAY(true, true),
                ARRAY(CAST('doris-array-bin' AS BINARY)),
                ARRAY(CAST(4.000001 AS DECIMAL(18, 6))),
                ARRAY(DATE '2024-03-22', DATE '2024-03-23'),
                ARRAY(TIMESTAMP '2024-03-22 14:02:03.000001'),
                MAP('math', 100, 'eng', 99),
                MAP(6, 'six', 7, 'seven'),
                MAP(true, false),
                MAP('payload', CAST('doris-map-bin' AS BINARY)),
                MAP(CAST(6.25 AS DECIMAL(8, 2)), CAST(7.50 AS DECIMAL(8, 2))),
                NAMED_STRUCT('city', 'Chengdu', 'zip', 610000),
                NAMED_STRUCT('string_field', 'doris', 'bool_field', true, 'int_field', 4000,
                    'bigint_field', 4000000000, 'float_field', CAST(4.5 AS FLOAT),
                    'double_field', CAST(40.75 AS DOUBLE),
                    'binary_field', CAST('doris-struct-bin' AS BINARY),
                    'decimal_field', CAST(4444.000001 AS DECIMAL(18, 6)),
                    'date_field', DATE '2024-03-22',
                    'timestamp_field', TIMESTAMP '2024-03-22 14:02:03.000001'),
                MAP('term', ARRAY(NAMED_STRUCT('score', 100, 'label', 'excellent')))
            ),
            (
                5, 'edge', 0, '', '', 'edge_char_', false,
                CAST(-128 AS TINYINT), CAST(-32768 AS SMALLINT), 0,
                CAST('-9223372036854775808' AS BIGINT),
                CAST(0.1 AS FLOAT), CAST(-0.1 AS DOUBLE),
                CAST(0.00 AS DECIMAL(9, 2)),
                CAST(-0.000001 AS DECIMAL(18, 6)),
                CAST(99999999999999999999999999.999999999999 AS DECIMAL(38, 12)),
                DATE '1970-01-01', TIMESTAMP '1970-01-01 00:00:00.000000',
                CAST('' AS BINARY),
                CAST(ARRAY() AS ARRAY<INT>),
                CAST(ARRAY() AS ARRAY<TINYINT>),
                CAST(ARRAY() AS ARRAY<SMALLINT>),
                ARRAY('', 'space value', 'edge-value'),
                CAST(ARRAY() AS ARRAY<BOOLEAN>),
                CAST(ARRAY() AS ARRAY<BINARY>),
                CAST(ARRAY() AS ARRAY<DECIMAL(18, 6)>),
                CAST(ARRAY() AS ARRAY<DATE>),
                CAST(ARRAY() AS ARRAY<TIMESTAMP>),
                map_from_arrays(CAST(ARRAY() AS ARRAY<STRING>), CAST(ARRAY() AS ARRAY<INT>)),
                map_from_arrays(CAST(ARRAY() AS ARRAY<INT>), CAST(ARRAY() AS ARRAY<STRING>)),
                map_from_arrays(CAST(ARRAY() AS ARRAY<BOOLEAN>), CAST(ARRAY() AS ARRAY<BOOLEAN>)),
                map_from_arrays(CAST(ARRAY() AS ARRAY<STRING>), CAST(ARRAY() AS ARRAY<BINARY>)),
                map_from_arrays(CAST(ARRAY() AS ARRAY<DECIMAL(8, 2)>),
                    CAST(ARRAY() AS ARRAY<DECIMAL(8, 2)>)),
                NAMED_STRUCT('city', '', 'zip', 0),
                NAMED_STRUCT('string_field', '', 'bool_field', false, 'int_field', 0,
                    'bigint_field', CAST('-9223372036854775808' AS BIGINT),
                    'float_field', CAST(-0.0 AS FLOAT), 'double_field', CAST(0.0 AS DOUBLE),
                    'binary_field', CAST('' AS BINARY),
                    'decimal_field', CAST(-0.000001 AS DECIMAL(18, 6)),
                    'date_field', DATE '1970-01-01',
                    'timestamp_field', TIMESTAMP '1970-01-01 00:00:00.000000'),
                MAP('empty', CAST(ARRAY() AS ARRAY<STRUCT<score:INT, label:STRING>>),
                    'blank', ARRAY(NAMED_STRUCT('score', 0, 'label', '')))
            );
    """

    sql """drop catalog if exists ${catalogName}"""
    sql """
        CREATE CATALOG ${catalogName} PROPERTIES (
            'type' = 'paimon',
            'warehouse' = 's3://warehouse/wh',
            's3.endpoint' = 'http://${externalEnvIp}:${minioPort}',
            's3.access_key' = 'admin',
            's3.secret_key' = 'password',
            's3.path.style.access' = 'true',
            'enable.mapping.varbinary' = 'true'
        );
    """

    sql """switch ${catalogName}"""

    def sparkBasicRows = spark_paimon """
        SELECT id, name, score
        FROM paimon.${dbName}.spark_written_paimon_demo
        ORDER BY id
    """
    // Example 1: compare Spark Paimon query result with explicit expected values.
    assertEquals(expectedBasicRows, sparkBasicRows)

    def dorisBasicRows = sql """
        SELECT id, name, score
        FROM ${dbName}.spark_written_paimon_demo
        ORDER BY id
    """
    // Example 1: compare Doris Paimon query result with explicit expected values.
    assertEquals(expectedBasicRows, dorisBasicRows)

    // Example 2: compare Doris and Spark query results.
    def sparkRows = spark_paimon """
        SELECT *
        FROM paimon.${dbName}.spark_written_paimon_demo
        ORDER BY id
    """
    def dorisRows = sql """
        SELECT *
        FROM ${dbName}.spark_written_paimon_demo
        ORDER BY id
    """
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
