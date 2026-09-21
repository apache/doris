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

suite("test_paimon_write_types", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")

    String catalogName = "test_pw_types_catalog"
    String dbName = "test_pw_types_db"

    // Create Paimon tables via Spark
    spark_paimon_multi """
        CREATE DATABASE IF NOT EXISTS paimon.${dbName};

        DROP TABLE IF EXISTS paimon.${dbName}.t_types;
        CREATE TABLE paimon.${dbName}.t_types (
            c_boolean   BOOLEAN,
            c_int       INT,
            c_bigint    BIGINT,
            c_float     FLOAT,
            c_double    DOUBLE,
            c_decimal   DECIMAL(10,2),
            c_string    STRING,
            c_varchar   VARCHAR(100),
            c_date      DATE,
            c_datetime  TIMESTAMP
        ) USING paimon;

        DROP TABLE IF EXISTS paimon.${dbName}.t_types_null;
        CREATE TABLE paimon.${dbName}.t_types_null (
            id INT,
            c_int INT,
            c_string STRING,
            c_double DOUBLE,
            c_boolean BOOLEAN
        ) USING paimon;

        DROP TABLE IF EXISTS paimon.${dbName}.t_types_decimal;
        CREATE TABLE paimon.${dbName}.t_types_decimal (
            id INT,
            d2  DECIMAL(2,1),
            d10 DECIMAL(10,2),
            d18 DECIMAL(18,6),
            d38 DECIMAL(38,10)
        ) USING paimon;

        DROP TABLE IF EXISTS paimon.${dbName}.t_types_dt;
        CREATE TABLE paimon.${dbName}.t_types_dt (
            d DATE, dt TIMESTAMP
        ) USING paimon;

        DROP TABLE IF EXISTS paimon.${dbName}.t_types_timezone;
        CREATE TABLE paimon.${dbName}.t_types_timezone (
            id INT, event_time TIMESTAMP
        ) USING paimon;

        DROP TABLE IF EXISTS paimon.${dbName}.t_types_ltz_schema;
        CREATE TABLE paimon.${dbName}.t_types_ltz_schema (
            event_time TIMESTAMP
        ) USING paimon;

        DROP TABLE IF EXISTS paimon.${dbName}.t_types_ntz;
        CREATE TABLE paimon.${dbName}.t_types_ntz (
            id INT, event_time TIMESTAMP_NTZ
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
    def originalTimeZone = sql """SELECT @@time_zone"""

    try {
        def assertTableEquals = { String tableName, String columns, String orderBy ->
            def sparkRows = spark_paimon """SELECT ${columns} FROM paimon.${dbName}.${tableName} ${orderBy}"""
            def dorisRows = sql """SELECT ${columns} FROM ${tableName} ${orderBy}"""
            assertSparkDorisResultEquals(sparkRows, dorisRows)
        }

        // FT-020~027: Basic types with boundary values
        sql """
            INSERT INTO t_types VALUES
            (true,  1, 100, CAST(1.5 AS FLOAT), CAST(2.71828 AS DOUBLE),
             CAST(99.99 AS DECIMAL(10,2)), 'hello', 'short',
             DATE '2024-01-15', TIMESTAMP '2024-01-15 10:30:00'),
            (false, 2147483647, 9223372036854775807,
             CAST(3.4E38 AS FLOAT), CAST(1.7E308 AS DOUBLE),
             CAST(0.00 AS DECIMAL(10,2)), '', '',
             DATE '1970-01-01', TIMESTAMP '1970-01-01 00:00:00'),
            (false, -2147483648, -9223372036854775808,
             CAST(-3.4E38 AS FLOAT), CAST(-1.7E308 AS DOUBLE),
             CAST(-1.50 AS DECIMAL(10,2)), 'long_string_1234567890', 'max_varchar',
             DATE '2099-12-31', TIMESTAMP '2099-12-31 23:59:59'),
            (true,  0, 0, CAST(0.0 AS FLOAT), CAST(0.0 AS DOUBLE),
             CAST(12345678.90 AS DECIMAL(10,2)), 'hello', 'fixed_len',
             DATE '2024-06-15', TIMESTAMP '2024-06-15 12:00:00.123456')
        """
        order_qt_types_basic """SELECT * FROM t_types ORDER BY c_int"""
        assertTableEquals("t_types", """
                c_boolean, c_int, c_bigint,
                c_float / 1.0E38,
                c_double / 1.0E308,
                c_decimal, c_string, c_varchar, c_date, c_datetime
                """, "ORDER BY c_int")

        // FT-040: NULL handling
        sql """INSERT INTO t_types_null VALUES (1, 100, 'data', 1.5, true)"""
        sql """INSERT INTO t_types_null VALUES (2, NULL, NULL, NULL, NULL)"""
        sql """INSERT INTO t_types_null VALUES (3, NULL, 'partial', 2.0, false)"""
        order_qt_types_null """SELECT id, c_int, c_string, c_double, c_boolean FROM t_types_null ORDER BY id"""
        assertTableEquals("t_types_null", "*", "ORDER BY id")

        // FT-043: Decimal precision
        sql """
            INSERT INTO t_types_decimal VALUES
            (1, CAST(1.5 AS DECIMAL(2,1)), CAST(12345678.90 AS DECIMAL(10,2)),
             CAST(123456789012.123456 AS DECIMAL(18,6)),
             CAST(1234567890123456789012345678.1234567890 AS DECIMAL(38,10))),
            (2, CAST(-1.5 AS DECIMAL(2,1)), CAST(-0.01 AS DECIMAL(10,2)),
             CAST(-1.000001 AS DECIMAL(18,6)),
             CAST(0.0000000001 AS DECIMAL(38,10))),
            (3, CAST(0.0 AS DECIMAL(2,1)), CAST(0.00 AS DECIMAL(10,2)),
             CAST(0.000000 AS DECIMAL(18,6)),
             CAST(0.0000000000 AS DECIMAL(38,10)))
        """
        order_qt_types_decimal """SELECT id, d2, d10, d18, d38 FROM t_types_decimal ORDER BY id"""
        assertTableEquals("t_types_decimal", "*", "ORDER BY id")

        // DATE / DATETIME boundary
        sql """
            INSERT INTO t_types_dt VALUES
            (DATE '1970-01-01', TIMESTAMP '1970-01-01 00:00:00'),
            (DATE '2024-06-15', TIMESTAMP '2024-06-15 12:00:00'),
            (DATE '2099-12-31', TIMESTAMP '2099-12-31 23:59:59.999999')
        """
        order_qt_types_dt """SELECT d, dt FROM t_types_dt ORDER BY d"""
        assertTableEquals("t_types_dt", "*", "ORDER BY d")

        // FT-027: Spark TIMESTAMP maps to Paimon's local-zoned timestamp. Values
        // written in different Doris session timezones must represent the same instant
        // in UTC while preserving their independent microsecond fractions.
        qt_desc_types_timezone """DESC t_types_ltz_schema"""
        sql """SET time_zone = 'Asia/Shanghai'"""
        sql """INSERT INTO t_types_timezone VALUES
            (1, TIMESTAMP '2024-01-15 10:30:00.123456')"""
        sql """SET time_zone = 'UTC'"""
        sql """INSERT INTO t_types_timezone VALUES
            (2, TIMESTAMP '2024-01-15 02:30:00.654321')"""
        order_qt_types_timezone_utc """SELECT id, event_time FROM t_types_timezone ORDER BY id"""

        // Reading the same snapshot in Asia/Shanghai must apply the session offset
        // to both rows without losing their microsecond fractions.
        sql """SET time_zone = 'Asia/Shanghai'"""
        order_qt_types_timezone_shanghai """SELECT id, event_time FROM t_types_timezone ORDER BY id"""

        // Paimon TIMESTAMP_NTZ stores civil fields. A value in a DST gap and a
        // Doris-supported short timezone alias must therefore survive without
        // an instant conversion or Java ZoneId parsing.
        sql """SET time_zone = 'America/Los_Angeles'"""
        sql """INSERT INTO t_types_ntz VALUES
            (1, TIMESTAMP '2024-03-10 02:30:00.123456')"""
        sql """SET time_zone = 'CST'"""
        sql """INSERT INTO t_types_ntz VALUES
            (2, TIMESTAMP '2024-01-15 10:30:00.654321')"""
        sql """SET time_zone = 'UTC'"""
        order_qt_types_ntz """SELECT id, event_time FROM t_types_ntz ORDER BY id"""
        assertTableEquals("t_types_ntz", "*", "ORDER BY id")
    } finally {
        sql """SET time_zone = '${originalTimeZone[0][0]}'"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
