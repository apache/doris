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

suite("test_iceberg_partition_evolution_query_write", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "test_iceberg_partition_evolution_query_write"

    sql """drop catalog if exists ${catalog_name}"""
    sql """
    CREATE CATALOG ${catalog_name} PROPERTIES (
        'type'='iceberg',
        'iceberg.catalog.type'='rest',
        'uri' = 'http://${externalEnvIp}:${rest_port}',
        "s3.access_key" = "admin",
        "s3.secret_key" = "password",
        "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
        "s3.region" = "us-east-1"
    );"""

    logger.info("catalog " + catalog_name + " created")
    sql """switch ${catalog_name};"""
    logger.info("switched to catalog " + catalog_name)

    sql """drop database if exists test_partition_evolution_query_write_db force"""
    sql """create database test_partition_evolution_query_write_db"""
    sql """use test_partition_evolution_query_write_db;"""

    sql """set enable_fallback_to_original_planner=false;"""

    // Scenario 1: Start as non-partition table -> ADD partition key -> query/write -> DROP back to non partition
    String addTable = "test_query_after_add_partition"
    sql """drop table if exists ${addTable}"""
    sql """
    CREATE TABLE ${addTable} (
        id INT,
        name STRING,
        ts DATETIME
    );
    """
    sql """INSERT INTO ${addTable} VALUES
        (1, 'Alice', '2024-01-01 08:00:00'),
        (2, 'Bob', '2024-02-02 09:00:00')"""
    qt_add_partition_before """SELECT * FROM ${addTable} ORDER BY id"""

    sql """ALTER TABLE ${addTable} ADD PARTITION KEY year(ts) AS ts_year"""
    sql """INSERT INTO ${addTable} VALUES
        (3, 'Charlie', '2025-03-03 10:00:00'),
        (4, 'David', '2026-04-04 11:00:00')"""

    qt_add_partition_after """SELECT * FROM ${addTable} ORDER BY id"""
    order_qt_add_partition_after_partitions """
        SELECT `partition`, spec_id, record_count
        FROM ${addTable}\$partitions ORDER BY `partition`
    """
    // drop partition key to turn table back to non-partitioned
    sql """ALTER TABLE ${addTable} DROP PARTITION KEY ts_year"""
    sql """INSERT INTO ${addTable} VALUES
        (5, 'Eve', '2027-05-05 12:00:00'),
        (6, 'Frank', '2028-06-06 13:00:00')"""
    qt_add_partition_after_drop """SELECT * FROM ${addTable} ORDER BY id"""

    // Scenario 2: Table starts partitioned -> DROP to become non-partitioned -> query/write
    String dropTable = "test_query_after_drop_partition"
    sql """drop table if exists ${dropTable}"""
    sql """
    CREATE TABLE ${dropTable} (
        id INT,
        name STRING,
        ts DATETIME
    ) PARTITION BY LIST (DAY(ts)) ();
    """
    sql """INSERT INTO ${dropTable} VALUES
        (1, 'Alice', '2024-01-01 08:00:00'),
        (2, 'Bob', '2024-01-02 09:00:00')"""
    qt_drop_partition_before """SELECT * FROM ${dropTable} ORDER BY id"""

    sql """ALTER TABLE ${dropTable} DROP PARTITION KEY day(ts)"""
    sql """INSERT INTO ${dropTable} VALUES
        (3, 'Charlie', '2024-01-03 10:00:00'),
        (4, 'David', '2024-01-04 11:00:00')"""
    qt_drop_partition_after """SELECT * FROM ${dropTable} ORDER BY id"""

    // Scenario 3: Multiple ADDs -> REPLACE -> DROP sequence
    String multiTable = "test_multi_add_replace_drop"
    sql """drop table if exists ${multiTable}"""
    sql """
    CREATE TABLE ${multiTable} (
        id INT,
        name STRING,
        ts DATETIME
    );
    """
    sql """INSERT INTO ${multiTable} VALUES
        (1, 'Alice', '2024-01-01 00:00:00'),
        (2, 'Bob', '2024-02-01 00:00:00')"""
    qt_multi_partition_before """SELECT * FROM ${multiTable} ORDER BY id"""

    sql """ALTER TABLE ${multiTable} ADD PARTITION KEY day(ts) AS ts_day"""
    sql """INSERT INTO ${multiTable} VALUES (3, 'Charlie', '2024-01-05 00:00:00')"""
    sql """ALTER TABLE ${multiTable} ADD PARTITION KEY bucket(8, id) AS shard"""
    sql """INSERT INTO ${multiTable} VALUES (4, 'David', '2024-01-06 00:00:00')"""
    qt_multi_partition_after_adds """SELECT * FROM ${multiTable} ORDER BY id"""
    order_qt_multi_partition_after_adds_partitions """
        SELECT `partition`, spec_id, record_count
        FROM ${multiTable}\$partitions ORDER BY `partition`
    """

    sql """ALTER TABLE ${multiTable} REPLACE PARTITION KEY ts_day WITH month(ts) AS ts_month"""
    sql """INSERT INTO ${multiTable} VALUES (5, 'Eve', '2024-03-01 00:00:00')"""
    qt_multi_partition_after_replace """SELECT * FROM ${multiTable} ORDER BY id"""
    order_qt_multi_partition_after_replace_partitions """
        SELECT `partition`, spec_id, record_count
        FROM ${multiTable}\$partitions ORDER BY `partition`
    """

    sql """ALTER TABLE ${multiTable} DROP PARTITION KEY shard"""
    sql """ALTER TABLE ${multiTable} DROP PARTITION KEY ts_month"""
    sql """INSERT INTO ${multiTable} VALUES (6, 'Frank', '2024-04-01 00:00:00')"""
    qt_multi_partition_after_drop_all """SELECT * FROM ${multiTable} ORDER BY id"""

    // Scenario 4: String column identity partition
    String stringTable = "test_string_partition"
    sql """drop table if exists ${stringTable}"""
    sql """
    CREATE TABLE ${stringTable} (
        sku STRING,
        descr STRING,
        price DOUBLE
    );
    """
    sql """INSERT INTO ${stringTable} VALUES
        ('A001', 'apple', 1.1),
        ('B002', 'banana', 2.2)"""
    qt_string_partition_before """SELECT * FROM ${stringTable} ORDER BY sku"""

    sql """ALTER TABLE ${stringTable} ADD PARTITION KEY sku"""
    sql """INSERT INTO ${stringTable} VALUES
        ('C003', 'candy', 3.3),
        ('D004', 'durian', 4.4)"""
    qt_string_partition_after_add """SELECT * FROM ${stringTable} ORDER BY sku"""
    order_qt_string_partition_after_add """
        SELECT `partition`, spec_id, record_count
        FROM ${stringTable}\$partitions ORDER BY `partition`
    """

    sql """ALTER TABLE ${stringTable} DROP PARTITION KEY sku"""
    sql """INSERT INTO ${stringTable} VALUES ('E005', 'espresso', 5.5)"""
    qt_string_partition_after_drop """SELECT * FROM ${stringTable} ORDER BY sku"""

    // Scenario 5: DATE column using year/month transforms
    String dateTable = "test_date_partition_chain"
    sql """drop table if exists ${dateTable}"""
    sql """
    CREATE TABLE ${dateTable} (
        order_id INT,
        order_date DATE,
        amount DECIMAL(10,2)
    );
    """
    sql """INSERT INTO ${dateTable} VALUES
        (1, '2023-12-31', 10.00),
        (2, '2024-01-01', 20.00)"""
    qt_date_partition_before """SELECT * FROM ${dateTable} ORDER BY order_id"""

    sql """ALTER TABLE ${dateTable} ADD PARTITION KEY year(order_date) AS order_year"""
    sql """ALTER TABLE ${dateTable} ADD PARTITION KEY month(order_date)"""
    sql """INSERT INTO ${dateTable} VALUES
        (3, '2024-02-15', 30.00),
        (4, '2025-03-20', 40.00)"""
    qt_date_partition_after_add """SELECT * FROM ${dateTable} ORDER BY order_id"""
    order_qt_date_partition_after_add """
        SELECT `partition`, spec_id, record_count
        FROM ${dateTable}\$partitions ORDER BY `partition`
    """

    sql """ALTER TABLE ${dateTable} REPLACE PARTITION KEY month(order_date) WITH day(order_date) AS order_day"""
    sql """INSERT INTO ${dateTable} VALUES (5, '2025-03-21', 50.00)"""
    qt_date_partition_after_replace """SELECT * FROM ${dateTable} ORDER BY order_id"""
    order_qt_date_partition_after_replace """
        SELECT `partition`, spec_id, record_count
        FROM ${dateTable}\$partitions ORDER BY `partition`
    """

    // Scenario 6: Numeric column bucket/truncate transforms
    String numericTable = "test_numeric_partition"
    sql """drop table if exists ${numericTable}"""
    sql """
    CREATE TABLE ${numericTable} (
        item_id INT,
        category_id BIGINT,
        stock INT
    );
    """
    sql """INSERT INTO ${numericTable} VALUES
        (1, 10001, 10),
        (2, 10002, 20)"""
    qt_numeric_partition_before """SELECT * FROM ${numericTable} ORDER BY item_id"""

    sql """ALTER TABLE ${numericTable} ADD PARTITION KEY bucket(4, item_id) AS item_bucket"""
    sql """ALTER TABLE ${numericTable} ADD PARTITION KEY truncate(3, category_id)"""
    sql """INSERT INTO ${numericTable} VALUES
        (3, 10003, 30),
        (4, 20004, 40)"""
    qt_numeric_partition_after_add """SELECT * FROM ${numericTable} ORDER BY item_id"""
    order_qt_numeric_partition_after_add """
        SELECT `partition`, spec_id, record_count
        FROM ${numericTable}\$partitions ORDER BY `partition`
    """

    sql """ALTER TABLE ${numericTable} DROP PARTITION KEY item_bucket"""
    sql """INSERT INTO ${numericTable} VALUES (5, 30005, 50)"""
    qt_numeric_partition_after_drop """SELECT * FROM ${numericTable} ORDER BY item_id"""

    sql """drop table if exists ${addTable}"""
    sql """drop table if exists ${dropTable}"""
    sql """drop table if exists ${multiTable}"""
    sql """drop table if exists ${stringTable}"""
    sql """drop table if exists ${dateTable}"""
    sql """drop table if exists ${numericTable}"""

    sql """ drop catalog if exists ${catalog_name} """
}

