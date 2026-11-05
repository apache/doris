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

import java.nio.file.Files
import java.nio.file.Paths

suite("test_iceberg_add_column_defaults", "p0,external") {
    if (!context.config.otherConfigs.get("enableIcebergTest")?.equalsIgnoreCase("true")) {
        return
    }
    String warehouse = context.config.otherConfigs.get("icebergAddColumnDefaultsWarehouse")
    def prepareLocalDataDirectory = { String table ->
        if (warehouse) {
            Files.createDirectories(Paths.get(new URI("${warehouse}/add_column_defaults_db/${table}/data")))
        }
    }
    sql "DROP CATALOG IF EXISTS iceberg_add_column_defaults"
    if (warehouse) {
        sql """CREATE CATALOG iceberg_add_column_defaults PROPERTIES (
            'type' = 'iceberg', 'iceberg.catalog.type' = 'hadoop', 'warehouse' = '${warehouse}',
            'enable.mapping.timestamp_tz' = 'true')"""
    } else {
        String host = context.config.otherConfigs.get("externalEnvIp")
        // The newer REST fixture preserves nanosecond defaults without rescaling them.
        String restPort = context.config.otherConfigs.get("iceberg_scan_planning_rest_uri_port")
        String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
        sql """CREATE CATALOG iceberg_add_column_defaults PROPERTIES (
            'type' = 'iceberg', 'iceberg.catalog.type' = 'rest',
            'uri' = 'http://${host}:${restPort}',
            's3.access_key' = 'admin', 's3.secret_key' = 'password',
            's3.endpoint' = 'http://${host}:${minioPort}', 's3.region' = 'us-east-1',
            'enable.mapping.timestamp_tz' = 'true')"""
    }
    sql "SWITCH iceberg_add_column_defaults"
    sql "CREATE DATABASE IF NOT EXISTS add_column_defaults_db"
    sql "USE add_column_defaults_db"

    for (String format : ["parquet", "orc"]) {
        sql "DROP TABLE IF EXISTS required_defaults_${format}"
        sql """CREATE TABLE required_defaults_${format} (
            id INT, s STRUCT<a:INT>, arr ARRAY<STRUCT<a:INT>>, m MAP<STRING,STRUCT<a:INT>>
        ) PROPERTIES ('format-version' = '3', 'write.format.default' = '${format}')"""
        prepareLocalDataDirectory("required_defaults_${format}")
        sql """INSERT INTO required_defaults_${format} VALUES
            (1, STRUCT(10), ARRAY(STRUCT(100), NULL), MAP('k', STRUCT(1000), 'n', NULL)),
            (2, NULL, NULL, NULL), (3, STRUCT(30), ARRAY(), MAP())"""
        sql """ALTER TABLE required_defaults_${format}
            ADD COLUMN added INT NOT NULL DEFAULT 7 AFTER id"""
        sql """ALTER TABLE required_defaults_${format}
            ADD COLUMN (batch_required INT NOT NULL DEFAULT 8, batch_optional INT)"""
        sql """ALTER TABLE required_defaults_${format}
            ADD COLUMN s.b INT NOT NULL DEFAULT 9 AFTER a"""
        sql """ALTER TABLE required_defaults_${format}
            ADD COLUMN s.c INT DEFAULT 10"""
        sql """ALTER TABLE required_defaults_${format}
            ADD COLUMN arr.element.b INT NOT NULL DEFAULT 11"""
        sql """ALTER TABLE required_defaults_${format}
            ADD COLUMN m.value.b INT NOT NULL DEFAULT 13"""

        // Old files have no new field IDs. NULL parents and NULL collection values remain NULL.
        "order_qt_${format}_old_files" "SELECT * FROM required_defaults_${format} ORDER BY id"
        "order_qt_${format}_nested_filter" """SELECT id FROM required_defaults_${format}
            WHERE s.b = 9 ORDER BY id"""
        "order_qt_${format}_collection_filter" """SELECT id FROM required_defaults_${format}
            WHERE arr[1].b = 11 AND m['k'].b = 13 ORDER BY id"""

        sql "INSERT INTO required_defaults_${format} (id) VALUES (4)"
        sql """INSERT INTO required_defaults_${format}
            (id, added, s, arr, m, batch_required) VALUES
            (5, DEFAULT, STRUCT(20, 21, 22), ARRAY(STRUCT(200, 201), NULL),
                MAP('k', STRUCT(2000, 2001), 'n', NULL), DEFAULT),
            (6, 60, NULL, NULL, NULL, 61)"""
        "order_qt_${format}_new_files" "SELECT * FROM required_defaults_${format} ORDER BY id"
        "order_qt_${format}_source_nullability" """SELECT COLUMN_NAME, IS_NULLABLE, COLUMN_DEFAULT
            FROM iceberg_add_column_defaults.information_schema.columns
            WHERE TABLE_SCHEMA = 'add_column_defaults_db' AND TABLE_NAME = 'required_defaults_${format}'
                AND COLUMN_NAME IN ('added', 'batch_required') ORDER BY ORDINAL_POSITION"""
        String ddl = sql("SHOW CREATE TABLE required_defaults_${format}")[0][1].toString()
        "qt_${format}_show_required" "SELECT ${ddl.contains('`added` int NOT NULL') || ddl.contains('`added` int(11) NOT NULL')}"

        test {
            sql "INSERT INTO required_defaults_${format} (id, added) VALUES (7, NULL)"
            exception "NULL"
        }
        test {
            sql """INSERT INTO required_defaults_${format} (id, s)
                VALUES (7, STRUCT(1, NULL, 2))"""
            exception "NULL"
        }
        test {
            sql """INSERT INTO required_defaults_${format} (id, arr)
                VALUES (7, ARRAY(STRUCT(1, NULL)))"""
            exception "NULL"
        }
        test {
            sql """INSERT INTO required_defaults_${format} (id, m)
                VALUES (7, MAP('k', STRUCT(1, NULL)))"""
            exception "NULL"
        }
        test {
            sql "ALTER TABLE required_defaults_${format} ADD COLUMN no_default INT NOT NULL"
            exception "doesn't have a default value"
        }
        test {
            sql "ALTER TABLE required_defaults_${format} ADD COLUMN s.no_default INT NOT NULL"
            exception "doesn't have a default value"
        }
        test {
            sql "ALTER TABLE required_defaults_${format} ADD COLUMN null_default INT NOT NULL DEFAULT NULL"
            exception "Can not set null default value to non nullable column"
        }
        test {
            sql "ALTER TABLE required_defaults_${format} ADD COLUMN s.null_default INT NOT NULL DEFAULT NULL"
            exception "Can not set null default value to non nullable column"
        }
        test {
            sql "ALTER TABLE required_defaults_${format} ADD COLUMN m.`key`.x INT DEFAULT 1"
            exception "MAP key"
        }
        test {
            sql """ALTER TABLE required_defaults_${format}
                ADD COLUMN (not_committed INT NOT NULL DEFAULT 17, ADDED INT NOT NULL DEFAULT 18)"""
            exception "conflicts with existing Iceberg field"
        }
        "order_qt_${format}_failed_writes_leave_rows" "SELECT id FROM required_defaults_${format} ORDER BY id"
        "qt_${format}_failed_batch_leaves_schema" """SELECT COUNT(*)
            FROM iceberg_add_column_defaults.information_schema.columns
            WHERE TABLE_SCHEMA = 'add_column_defaults_db' AND TABLE_NAME = 'required_defaults_${format}'
                AND COLUMN_NAME = 'not_committed'"""

        sql "DROP TABLE IF EXISTS typed_defaults_${format}"
        sql """CREATE TABLE typed_defaults_${format} (id INT, s STRUCT<a:INT>)
            PROPERTIES ('format-version' = '3', 'write.format.default' = '${format}')"""
        prepareLocalDataDirectory("typed_defaults_${format}")
        sql "INSERT INTO typed_defaults_${format} VALUES (1, STRUCT(10)), (2, NULL)"
        sql "SET time_zone = 'Asia/Shanghai'"
        sql """ALTER TABLE typed_defaults_${format} ADD COLUMN (
            truth BOOLEAN NOT NULL DEFAULT ' 1 ', day DATE NOT NULL DEFAULT '2024-01-02',
            ts DATETIME(6) NOT NULL DEFAULT '2024-01-02 03:04:05.123456',
            compact_day DATE NOT NULL DEFAULT '20240102',
            compact_ts DATETIME(6) NOT NULL DEFAULT '20240102030405',
            zoned_ts TIMESTAMPTZ(6) NOT NULL DEFAULT '2024-01-02 03:04:05.123456',
            compact_zoned_ts TIMESTAMPTZ(6) NOT NULL DEFAULT '20240102030405',
            compact_fraction_zoned_ts TIMESTAMPTZ(6) NOT NULL DEFAULT '20240102030405.123456',
            rounded_zoned_ts TIMESTAMPTZ(3) NOT NULL DEFAULT '2024-01-02 03:04:05.123999',
            rounded_ts DATETIME(3) NOT NULL DEFAULT '2024-01-02 03:04:05.123999',
            amount DECIMAL(10,2) NOT NULL DEFAULT '1.5', label STRING NOT NULL DEFAULT 'owner''s')"""
        sql "ALTER TABLE typed_defaults_${format} ADD COLUMN s.truth BOOLEAN NOT NULL DEFAULT ' 0 '"
        sql "ALTER TABLE typed_defaults_${format} ADD COLUMN s.day DATE NOT NULL DEFAULT '2024-01-02'"
        sql """ALTER TABLE typed_defaults_${format}
            ADD COLUMN s.ts DATETIME(6) NOT NULL DEFAULT '2024-01-02 03:04:05.123456'"""
        sql "ALTER TABLE typed_defaults_${format} ADD COLUMN s.compact_day DATE NOT NULL DEFAULT '20240102'"
        sql """ALTER TABLE typed_defaults_${format}
            ADD COLUMN s.compact_ts DATETIME(6) NOT NULL DEFAULT '20240102030405'"""
        sql """ALTER TABLE typed_defaults_${format}
            ADD COLUMN s.zoned_ts TIMESTAMPTZ(6) NOT NULL DEFAULT '2024-01-02 03:04:05.123456'"""
        sql """ALTER TABLE typed_defaults_${format}
            ADD COLUMN s.compact_fraction_zoned_ts TIMESTAMPTZ(6) NOT NULL DEFAULT '20240102030405.123456'"""
        sql "ALTER TABLE typed_defaults_${format} ADD COLUMN s.amount DECIMAL(10,2) NOT NULL DEFAULT '1'"
        sql """ALTER TABLE typed_defaults_${format}
            ADD COLUMN uuid_value UUID NOT NULL DEFAULT '00112233445566778899aabbccddeeff'"""
        sql """ALTER TABLE typed_defaults_${format}
            ADD COLUMN s.uuid_value UUID NOT NULL DEFAULT '00112233445566778899aabbccddeeff'"""
        sql "SET time_zone = 'UTC'"
        "order_qt_${format}_typed_old_files" "SELECT * FROM typed_defaults_${format} ORDER BY id"
        sql "INSERT INTO typed_defaults_${format} (id) VALUES (3)"
        "order_qt_${format}_typed_write_defaults" "SELECT * FROM typed_defaults_${format} ORDER BY id"
    }

    sql "DROP TABLE IF EXISTS conversion_defaults"
    sql """CREATE TABLE conversion_defaults (id INT, s STRUCT<a:INT>)
        PROPERTIES ('format-version' = '3', 'write.format.default' = 'parquet')"""
    prepareLocalDataDirectory("conversion_defaults")
    sql "INSERT INTO conversion_defaults VALUES (1, STRUCT(10)), (2, NULL)"
    sql "SET time_zone = '+00:00'"
    sql """ALTER TABLE conversion_defaults ADD COLUMN ns TIMESTAMP_NS
        DEFAULT '2027-03-28 02:30:00.123456789CET'"""
    sql """ALTER TABLE conversion_defaults ADD COLUMN s.ns TIMESTAMP_NS NOT NULL
        DEFAULT '2027-03-28 02:30:00.123456789CET'"""
    sql "SET time_zone = 'CET'"
    sql """ALTER TABLE conversion_defaults ADD COLUMN (
        overlap_implicit TIMESTAMPTZ(0) NOT NULL DEFAULT '2021-10-31 02:59:59.6',
        overlap_explicit TIMESTAMPTZ(0) NOT NULL DEFAULT '2021-10-31 02:59:59.6CET',
        milli_implicit TIMESTAMPTZ(3) NOT NULL DEFAULT '2021-10-31 02:59:59.9996',
        milli_explicit TIMESTAMPTZ(3) NOT NULL DEFAULT '2021-10-31 02:59:59.9996CET')"""
    sql """ALTER TABLE conversion_defaults ADD COLUMN s.overlap_implicit TIMESTAMPTZ(0) NOT NULL
        DEFAULT '2021-10-31 02:59:59.6'"""
    sql """ALTER TABLE conversion_defaults ADD COLUMN s.overlap_explicit TIMESTAMPTZ(0) NOT NULL
        DEFAULT '2021-10-31 02:59:59.6CET'"""
    sql "SET time_zone = 'UTC'"
    sql """ALTER TABLE conversion_defaults ADD COLUMN (
        min_utc TIMESTAMPTZ(6) NOT NULL DEFAULT '0001-01-01 00:00:00',
        max_utc TIMESTAMPTZ(6) NOT NULL DEFAULT '9999-12-31 23:59:59.999999',
        min_civil DATETIME(6) NOT NULL DEFAULT '0001-01-01 00:00:00',
        max_civil DATETIME(6) NOT NULL DEFAULT '9999-12-31 23:59:59.999999')"""
    sql """ALTER TABLE conversion_defaults ADD COLUMN s.min_utc TIMESTAMPTZ(6) NOT NULL
        DEFAULT '0001-01-01 00:00:00'"""
    sql """ALTER TABLE conversion_defaults ADD COLUMN s.max_utc TIMESTAMPTZ(6) NOT NULL
        DEFAULT '9999-12-31 23:59:59.999999'"""
    sql """ALTER TABLE conversion_defaults ADD COLUMN s.min_civil DATETIME(6) NOT NULL
        DEFAULT '0001-01-01 00:00:00'"""
    sql """ALTER TABLE conversion_defaults ADD COLUMN s.max_civil DATETIME(6) NOT NULL
        DEFAULT '9999-12-31 23:59:59.999999'"""
    String previousSqlMode = sql("SELECT @@sql_mode")[0][0].toString()
    try {
        sql "SET sql_mode = ''"
        sql """ALTER TABLE conversion_defaults ADD COLUMN label STRING NOT NULL
            DEFAULT 'owner''s "field" C:\\\\tmp\\\\'"""
        sql "SET sql_mode = 'NO_BACKSLASH_ESCAPES'"
        sql """ALTER TABLE conversion_defaults ADD COLUMN s.label STRING NOT NULL
            DEFAULT 'owner''s "field" C:\\tmp\\'"""
    } finally {
        sql "SET sql_mode = '${previousSqlMode}'"
    }
    "order_qt_conversion_old_files" """SELECT *,
        ns = CAST('2027-03-28 02:30:00.123456789CET' AS TIMESTAMP_NS)
        FROM conversion_defaults ORDER BY id"""
    sql "INSERT INTO conversion_defaults (id) VALUES (3)"
    try {
        sql "SET sql_mode = 'NO_BACKSLASH_ESCAPES'"
        sql "INSERT INTO conversion_defaults (id, label) VALUES (4, DEFAULT)"
    } finally {
        sql "SET sql_mode = '${previousSqlMode}'"
    }
    "order_qt_conversion_write_defaults" "SELECT * FROM conversion_defaults ORDER BY id"
    order_qt_conversion_string_bytes """SELECT id, HEX(label), HEX(s.label)
        FROM conversion_defaults ORDER BY id"""
    order_qt_conversion_zoned_equality """SELECT id, overlap_implicit = overlap_explicit,
        milli_implicit = milli_explicit,
        overlap_implicit = CAST('2021-10-31 01:00:00+00:00' AS TIMESTAMPTZ(0)),
        s.overlap_implicit = s.overlap_explicit FROM conversion_defaults ORDER BY id"""
    for (String type : ["FLOAT", "DOUBLE"]) {
        for (String value : ["NaN", "Infinity", "-Infinity"]) {
            // FLOAT NaN fails the existing precision check before connector conversion.
            String expectedError = type == "FLOAT" && value == "NaN"
                    ? "Default value will loose precision" : "Non-finite Iceberg"
            test {
                sql "ALTER TABLE conversion_defaults ADD COLUMN bad ${type} NOT NULL DEFAULT '${value}'"
                exception expectedError
            }
            test {
                sql "ALTER TABLE conversion_defaults ADD COLUMN s.bad ${type} NOT NULL DEFAULT '${value}'"
                exception expectedError
            }
            test {
                sql """ALTER TABLE conversion_defaults ADD COLUMN
                    (not_committed INT, bad ${type} NOT NULL DEFAULT '${value}')"""
                exception expectedError
            }
        }
    }
    for (def boundary : [["-08:00", "9999-12-31 23:59:59", 6], ["+08:00", "0000-01-01 00:00:00", 6],
            ["+08:00", "0000-03-01 00:00:00", 6], ["UTC", "0000-02-28 23:59:59.6", 0],
            ["UTC", "0000-12-31 23:59:59.999999", 6]]) {
        sql "SET time_zone = '${boundary[0]}'"
        test {
            sql """ALTER TABLE conversion_defaults ADD COLUMN bad_range TIMESTAMPTZ(${boundary[2]}) NOT NULL
                DEFAULT '${boundary[1]}'"""
            exception "supported UTC range"
        }
        test {
            sql """ALTER TABLE conversion_defaults ADD COLUMN s.bad_range TIMESTAMPTZ(${boundary[2]}) NOT NULL
                DEFAULT '${boundary[1]}'"""
            exception "supported UTC range"
        }
        test {
            sql """ALTER TABLE conversion_defaults ADD COLUMN
                (not_committed_range INT, bad_range TIMESTAMPTZ(${boundary[2]}) NOT NULL DEFAULT '${boundary[1]}')"""
            exception "supported UTC range"
        }
    }
    sql "SET time_zone = 'UTC'"
    for (String value : ["0000-03-01 00:00:00", "0000-12-31 23:59:59.999999"]) {
        test {
            sql "ALTER TABLE conversion_defaults ADD COLUMN bad_range DATETIME(6) NOT NULL DEFAULT '${value}'"
            exception "supported range 0001-9999"
        }
        test {
            sql "ALTER TABLE conversion_defaults ADD COLUMN s.bad_range DATETIME(6) NOT NULL DEFAULT '${value}'"
            exception "supported range 0001-9999"
        }
        test {
            sql """ALTER TABLE conversion_defaults ADD COLUMN
                (not_committed_range INT, bad_range DATETIME(6) NOT NULL DEFAULT '${value}')"""
            exception "supported range 0001-9999"
        }
    }
    "order_qt_conversion_after_failed_add" "SELECT * FROM conversion_defaults ORDER BY id"
    qt_conversion_failed_add_leaves_schema """SELECT COUNT(*)
        FROM iceberg_add_column_defaults.information_schema.columns
        WHERE TABLE_SCHEMA = 'add_column_defaults_db' AND TABLE_NAME = 'conversion_defaults'
            AND COLUMN_NAME IN ('bad', 'not_committed', 'bad_range', 'not_committed_range')"""

    for (int version : [1, 2]) {
        sql "DROP TABLE IF EXISTS old_format_${version}"
        sql """CREATE TABLE old_format_${version} (id INT, s STRUCT<a:INT>)
            PROPERTIES ('format-version' = '${version}')"""
        for (String required : ["NULL", "NOT NULL"]) {
            test {
                sql "ALTER TABLE old_format_${version} ADD COLUMN added INT ${required} DEFAULT 7"
                exception "format-version 3"
            }
            test {
                sql "ALTER TABLE old_format_${version} ADD COLUMN s.added INT ${required} DEFAULT 7"
                exception "format-version 3"
            }
            test {
                sql """ALTER TABLE old_format_${version}
                    ADD COLUMN (not_committed INT, added INT ${required} DEFAULT 7)"""
                exception "format-version 3"
            }
        }
        "qt_v${version}_failed_batch_leaves_schema" """SELECT COUNT(*)
            FROM iceberg_add_column_defaults.information_schema.columns
            WHERE TABLE_SCHEMA = 'add_column_defaults_db' AND TABLE_NAME = 'old_format_${version}'
                AND COLUMN_NAME IN ('not_committed', 'added')"""
        sql "ALTER TABLE old_format_${version} ADD COLUMN s.plain INT"
        "qt_v${version}_optional_add" "DESC old_format_${version}"
    }
}
