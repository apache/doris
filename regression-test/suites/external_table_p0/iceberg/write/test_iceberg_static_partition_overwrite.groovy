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

suite("test_iceberg_static_partition_overwrite", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "test_iceberg_static_partition_overwrite"

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

    sql """ switch ${catalog_name} """

    String db1 = catalog_name + "_db"
    String tb1 = db1 + "_tb1"

    sql """ drop database if exists ${db1} force"""
    sql """ create database ${db1} """
    sql """ use ${db1} """

    // Test Case 1: Full static partition overwrite (all partition columns specified)
    // Test overwriting a specific partition with all partition columns specified
    sql """ DROP TABLE IF EXISTS ${tb1} """
    sql """
        CREATE TABLE ${tb1} (
            id BIGINT,
            name STRING,
            dt DATE,
            region STRING
        ) ENGINE=iceberg
        PARTITION BY LIST (dt, region) ()
    """

    // Insert initial data
    sql """
        INSERT INTO ${tb1} VALUES
        (1, 'Alice', '2025-01-25', 'bj'),
        (2, 'Bob', '2025-01-25', 'sh'),
        (3, 'Charlie', '2025-01-26', 'bj'),
        (4, 'David', '2025-01-26', 'sh')
    """

    // Verify initial data
    order_qt_q01 """ SELECT * FROM ${tb1} ORDER BY id """

    // Overwrite specific partition (dt='2025-01-25', region='bj')
    sql """
        INSERT OVERWRITE TABLE ${tb1} 
        PARTITION (dt='2025-01-25', region='bj')
        SELECT 10, 'Eve'
    """

    // Verify: Only (dt='2025-01-25', region='bj') partition is overwritten
    // Other partitions remain unchanged
    order_qt_q02 """ SELECT * FROM ${tb1} ORDER BY id """

    // Test Case 2: Hybrid partition mode - partial static + partial dynamic
    // Static partition column (dt) comes from PARTITION clause
    // Dynamic partition column (region) comes from SELECT query result
    sql """ DROP TABLE IF EXISTS ${tb1} """
    sql """
        CREATE TABLE ${tb1} (
            id BIGINT,
            name STRING,
            dt DATE,
            region STRING
        ) ENGINE=iceberg
        PARTITION BY LIST (dt, region) ()
    """

    // Create source table for hybrid partition test
    String tb_src = db1 + "_src"
    sql """ DROP TABLE IF EXISTS ${tb_src} """
    sql """
        CREATE TABLE ${tb_src} (
            id BIGINT,
            name STRING,
            region STRING
        ) ENGINE=iceberg
    """

    // Insert source data with different regions
    sql """
        INSERT INTO ${tb_src} VALUES
        (10, 'Eve', 'bj'),
        (11, 'Frank', 'sh'),
        (12, 'Grace', 'gz')
    """

    // Insert initial data to target table
    sql """
        INSERT INTO ${tb1} VALUES
        (1, 'Alice', '2025-01-25', 'bj'),
        (2, 'Bob', '2025-01-25', 'sh'),
        (3, 'Charlie', '2025-01-26', 'bj'),
        (4, 'David', '2025-01-26', 'sh')
    """

    // Verify initial data
    order_qt_q03_before """ SELECT * FROM ${tb1} ORDER BY id """

    // Hybrid mode: dt='2025-01-25' is static, region comes from source table dynamically
    // This should:
    // 1. Delete all data where dt='2025-01-25'
    // 2. Insert new data with dt='2025-01-25' and region values from source table
    // Note: SELECT does NOT include 'dt' column - it comes from PARTITION clause
    sql """
        INSERT OVERWRITE TABLE ${tb1} 
        PARTITION (dt='2025-01-25')
        SELECT id, name, region FROM ${tb_src}
    """

    // Verify: 
    // - Partitions with dt='2025-01-25' are replaced with new data (bj, sh, gz regions)
    // - Partitions with dt='2025-01-26' remain unchanged
    order_qt_q03_after """ SELECT * FROM ${tb1} ORDER BY id """

    // Verify partition data distribution
    order_qt_q03_partition_25 """ SELECT * FROM ${tb1} WHERE dt='2025-01-25' ORDER BY id """
    order_qt_q03_partition_26 """ SELECT * FROM ${tb1} WHERE dt='2025-01-26' ORDER BY id """

    sql """ DROP TABLE IF EXISTS ${tb_src} """

    // Test Case 3: Empty result overwrite (delete specified partition)
    // Test deleting a partition by overwriting with empty result
    sql """ DROP TABLE IF EXISTS ${tb1} """
    sql """
        CREATE TABLE ${tb1} (
            id BIGINT,
            name STRING,
            dt DATE,
            region STRING
        ) ENGINE=iceberg
        PARTITION BY LIST (dt, region) ()
    """

    sql """
        INSERT INTO ${tb1} VALUES
        (1, 'Alice', '2025-01-25', 'bj'),
        (2, 'Bob', '2025-01-25', 'sh'),
        (3, 'Charlie', '2025-01-26', 'bj')
    """

    // Overwrite with empty result to delete the specified partition
    sql """
        INSERT OVERWRITE TABLE ${tb1} 
        PARTITION (dt='2025-01-25', region='bj')
        SELECT id, name FROM ${tb1} WHERE 1=0
    """

    // Verify: Specified partition is deleted, other partitions remain unchanged
    order_qt_q04 """ SELECT * FROM ${tb1} ORDER BY id """

    // Test Case 4: Error scenario - non-existent partition column
    sql """ DROP TABLE IF EXISTS ${tb1} """
    sql """
        CREATE TABLE ${tb1} (
            id BIGINT,
            name STRING,
            dt DATE,
            region STRING
        ) ENGINE=iceberg
        PARTITION BY LIST (dt, region) ()
    """

    test {
        sql """
            INSERT OVERWRITE TABLE ${tb1} 
            PARTITION (invalid_col='value')
            SELECT * FROM ${tb1}
        """
        exception "Unknown partition column"
    }

    // Test Case 5: Multiple static partitions with different data types (full static)
    sql """ DROP TABLE IF EXISTS ${tb1} """
    sql """
        CREATE TABLE ${tb1} (
            id BIGINT,
            name STRING,
            dt DATE,
            region STRING,
            amount INT
        ) ENGINE=iceberg
        PARTITION BY LIST (dt, region, amount) ()
    """

    sql """
        INSERT INTO ${tb1} VALUES
        (1, 'Alice', '2025-01-25', 'bj', 100),
        (2, 'Bob', '2025-01-25', 'bj', 200),
        (3, 'Charlie', '2025-01-25', 'sh', 100),
        (4, 'David', '2025-01-26', 'bj', 100)
    """

    // Overwrite partition with multiple partition columns including integer type
    sql """
        INSERT OVERWRITE TABLE ${tb1} 
        PARTITION (dt='2025-01-25', region='bj', amount=100)
        SELECT 10, 'Eve'
    """

    // Verify: Only the exact matching partition is overwritten
    order_qt_q05 """ SELECT * FROM ${tb1} ORDER BY id """

    // Test Case 6: Hybrid mode with three partition columns (2 static + 1 dynamic)
    // Table has partition columns: (dt, region, category)
    // Static: dt='2025-01-25', region='bj'
    // Dynamic: category comes from SELECT
    String tb2 = db1 + "_tb2"
    sql """ DROP TABLE IF EXISTS ${tb2} """
    sql """
        CREATE TABLE ${tb2} (
            id BIGINT,
            name STRING,
            dt DATE,
            region STRING,
            category STRING
        ) ENGINE=iceberg
        PARTITION BY LIST (dt, region, category) ()
    """

    // Insert initial data with different categories
    sql """
        INSERT INTO ${tb2} VALUES
        (1, 'Alice', '2025-01-25', 'bj', 'food'),
        (2, 'Bob', '2025-01-25', 'bj', 'drink'),
        (3, 'Charlie', '2025-01-25', 'sh', 'food'),
        (4, 'David', '2025-01-26', 'bj', 'food')
    """

    order_qt_q06_before """ SELECT * FROM ${tb2} ORDER BY id """

    // Create source table for dynamic category values
    String tb2_src = db1 + "_tb2_src"
    sql """ DROP TABLE IF EXISTS ${tb2_src} """
    sql """
        CREATE TABLE ${tb2_src} (
            id BIGINT,
            name STRING,
            category STRING
        ) ENGINE=iceberg
    """

    sql """
        INSERT INTO ${tb2_src} VALUES
        (10, 'Eve', 'electronics'),
        (11, 'Frank', 'clothing')
    """

    // Hybrid mode: dt and region are static, category is dynamic from source
    // SELECT should only include: id, name, category (not dt, region)
    sql """
        INSERT OVERWRITE TABLE ${tb2} 
        PARTITION (dt='2025-01-25', region='bj')
        SELECT id, name, category FROM ${tb2_src}
    """

    // Verify:
    // - All partitions with dt='2025-01-25' AND region='bj' are replaced
    // - Other partitions remain unchanged
    order_qt_q06_after """ SELECT * FROM ${tb2} ORDER BY id """

    // Verify specific partition filters
    order_qt_q06_static_partition """ SELECT * FROM ${tb2} WHERE dt='2025-01-25' AND region='bj' ORDER BY id """
    order_qt_q06_other_partitions """ SELECT * FROM ${tb2} WHERE NOT (dt='2025-01-25' AND region='bj') ORDER BY id """

    sql """ DROP TABLE IF EXISTS ${tb2_src} """
    sql """ DROP TABLE IF EXISTS ${tb2} """

    // Test Case 7: Static partition with LONG type
    sql """ DROP TABLE IF EXISTS ${tb1} """
    sql """
        CREATE TABLE ${tb1} (
            id BIGINT,
            name STRING,
            timestamp_val BIGINT
        ) ENGINE=iceberg
        PARTITION BY LIST (timestamp_val) ()
    """
    sql """
        INSERT INTO ${tb1} VALUES
        (1, 'Alice', 1706140800000),
        (2, 'Bob', 1706227200000),
        (3, 'Charlie', 1706313600000)
    """
    sql """
        INSERT OVERWRITE TABLE ${tb1} 
        PARTITION (timestamp_val=1706140800000)
        SELECT 10, 'Eve'
    """
    order_qt_q07 """ SELECT * FROM ${tb1} ORDER BY id """

    // Test Case 8: Hybrid mode with LONG type (static) + STRING (dynamic)
    sql """ DROP TABLE IF EXISTS ${tb1} """
    sql """
        CREATE TABLE ${tb1} (
            id BIGINT,
            name STRING,
            timestamp_val BIGINT,
            region STRING
        ) ENGINE=iceberg
        PARTITION BY LIST (timestamp_val, region) ()
    """
    String tb_long_src = db1 + "_long_src"
    sql """ DROP TABLE IF EXISTS ${tb_long_src} """
    sql """
        CREATE TABLE ${tb_long_src} (
            id BIGINT,
            name STRING,
            region STRING
        ) ENGINE=iceberg
    """
    sql """
        INSERT INTO ${tb_long_src} VALUES
        (10, 'Eve', 'bj'),
        (11, 'Frank', 'sh')
    """
    sql """
        INSERT INTO ${tb1} VALUES
        (1, 'Alice', 1706140800000, 'bj'),
        (2, 'Bob', 1706140800000, 'sh'),
        (3, 'Charlie', 1706227200000, 'bj')
    """
    sql """
        INSERT OVERWRITE TABLE ${tb1} 
        PARTITION (timestamp_val=1706140800000)
        SELECT id, name, region FROM ${tb_long_src}
    """
    order_qt_q08 """ SELECT * FROM ${tb1} ORDER BY id """
    sql """ DROP TABLE IF EXISTS ${tb_long_src} """

    // Test Case 9: Static partition with FLOAT type
    sql """ DROP TABLE IF EXISTS ${tb1} """
    sql """
        CREATE TABLE ${tb1} (
            id BIGINT,
            name STRING,
            score FLOAT
        ) ENGINE=iceberg
        PARTITION BY LIST (score) ()
    """
    sql """
        INSERT INTO ${tb1} VALUES
        (1, 'Alice', 85.5),
        (2, 'Bob', 90.0),
        (3, 'Charlie', 75.5)
    """
    sql """
        INSERT OVERWRITE TABLE ${tb1} 
        PARTITION (score=85.5)
        SELECT 10, 'Eve'
    """
    order_qt_q09 """ SELECT * FROM ${tb1} ORDER BY id """

    // Test Case 10: Hybrid mode with FLOAT type (static) + INTEGER (dynamic)
    sql """ DROP TABLE IF EXISTS ${tb1} """
    sql """
        CREATE TABLE ${tb1} (
            id BIGINT,
            name STRING,
            score FLOAT,
            level INT
        ) ENGINE=iceberg
        PARTITION BY LIST (score, level) ()
    """
    String tb_float_src = db1 + "_float_src"
    sql """ DROP TABLE IF EXISTS ${tb_float_src} """
    sql """
        CREATE TABLE ${tb_float_src} (
            id BIGINT,
            name STRING,
            level INT
        ) ENGINE=iceberg
    """
    sql """
        INSERT INTO ${tb_float_src} VALUES
        (10, 'Eve', 1),
        (11, 'Frank', 2)
    """
    sql """
        INSERT INTO ${tb1} VALUES
        (1, 'Alice', 85.5, 1),
        (2, 'Bob', 85.5, 2),
        (3, 'Charlie', 90.0, 1)
    """
    sql """
        INSERT OVERWRITE TABLE ${tb1} 
        PARTITION (score=85.5)
        SELECT id, name, level FROM ${tb_float_src}
    """
    order_qt_q10 """ SELECT * FROM ${tb1} ORDER BY id """
    sql """ DROP TABLE IF EXISTS ${tb_float_src} """

    // Test Case 11: Static partition with DOUBLE type
    sql """ DROP TABLE IF EXISTS ${tb1} """
    sql """
        CREATE TABLE ${tb1} (
            id BIGINT,
            name STRING,
            price DOUBLE
        ) ENGINE=iceberg
        PARTITION BY LIST (price) ()
    """
    sql """
        INSERT INTO ${tb1} VALUES
        (1, 'Alice', 99.99),
        (2, 'Bob', 199.99),
        (3, 'Charlie', 299.99)
    """
    sql """
        INSERT OVERWRITE TABLE ${tb1} 
        PARTITION (price=99.99)
        SELECT 10, 'Eve'
    """
    order_qt_q11 """ SELECT * FROM ${tb1} ORDER BY id """

    // Test Case 12: Hybrid mode with DOUBLE type (static) + STRING (dynamic)
    sql """ DROP TABLE IF EXISTS ${tb1} """
    sql """
        CREATE TABLE ${tb1} (
            id BIGINT,
            name STRING,
            price DOUBLE,
            category STRING
        ) ENGINE=iceberg
        PARTITION BY LIST (price, category) ()
    """
    String tb_double_src = db1 + "_double_src"
    sql """ DROP TABLE IF EXISTS ${tb_double_src} """
    sql """
        CREATE TABLE ${tb_double_src} (
            id BIGINT,
            name STRING,
            category STRING
        ) ENGINE=iceberg
    """
    sql """
        INSERT INTO ${tb_double_src} VALUES
        (10, 'Eve', 'A'),
        (11, 'Frank', 'B')
    """
    sql """
        INSERT INTO ${tb1} VALUES
        (1, 'Alice', 99.99, 'A'),
        (2, 'Bob', 99.99, 'B'),
        (3, 'Charlie', 199.99, 'A')
    """
    sql """
        INSERT OVERWRITE TABLE ${tb1} 
        PARTITION (price=99.99)
        SELECT id, name, category FROM ${tb_double_src}
    """
    order_qt_q12 """ SELECT * FROM ${tb1} ORDER BY id """
    sql """ DROP TABLE IF EXISTS ${tb_double_src} """

    // Test Case 13: Static partition with BOOLEAN type
    sql """ DROP TABLE IF EXISTS ${tb1} """
    sql """
        CREATE TABLE ${tb1} (
            id BIGINT,
            name STRING,
            is_active BOOLEAN
        ) ENGINE=iceberg
        PARTITION BY LIST (is_active) ()
    """
    sql """
        INSERT INTO ${tb1} VALUES
        (1, 'Alice', true),
        (2, 'Bob', false),
        (3, 'Charlie', true)
    """
    sql """
        INSERT OVERWRITE TABLE ${tb1} 
        PARTITION (is_active=true)
        SELECT 10, 'Eve'
    """
    order_qt_q13 """ SELECT * FROM ${tb1} ORDER BY id """

    // Test Case 14: Hybrid mode with BOOLEAN type (static) + INTEGER (dynamic)
    sql """ DROP TABLE IF EXISTS ${tb1} """
    sql """
        CREATE TABLE ${tb1} (
            id BIGINT,
            name STRING,
            is_active BOOLEAN,
            status INT
        ) ENGINE=iceberg
        PARTITION BY LIST (is_active, status) ()
    """
    String tb_bool_src = db1 + "_bool_src"
    sql """ DROP TABLE IF EXISTS ${tb_bool_src} """
    sql """
        CREATE TABLE ${tb_bool_src} (
            id BIGINT,
            name STRING,
            status INT
        ) ENGINE=iceberg
    """
    sql """
        INSERT INTO ${tb_bool_src} VALUES
        (10, 'Eve', 1),
        (11, 'Frank', 2)
    """
    sql """
        INSERT INTO ${tb1} VALUES
        (1, 'Alice', true, 1),
        (2, 'Bob', true, 2),
        (3, 'Charlie', false, 1)
    """
    sql """
        INSERT OVERWRITE TABLE ${tb1} 
        PARTITION (is_active=true)
        SELECT id, name, status FROM ${tb_bool_src}
    """
    order_qt_q14 """ SELECT * FROM ${tb1} ORDER BY id """
    sql """ DROP TABLE IF EXISTS ${tb_bool_src} """

    // Test Case 15: Static partition with DATETIME type
    sql """ DROP TABLE IF EXISTS ${tb1} """
    sql """
        CREATE TABLE ${tb1} (
            id BIGINT,
            name STRING,
            ts DATETIME
        ) ENGINE=iceberg
        PARTITION BY LIST (ts) ()
    """
    sql """
        INSERT INTO ${tb1} VALUES
        (1, 'Alice', '2025-01-25 10:00:00'),
        (2, 'Bob', '2025-01-25 11:00:00'),
        (3, 'Charlie', '2025-01-26 10:00:00')
    """
    sql """
        INSERT OVERWRITE TABLE ${tb1} 
        PARTITION (ts='2025-01-25 10:00:00')
        SELECT 10, 'Eve'
    """
    order_qt_q15 """ SELECT * FROM ${tb1} ORDER BY id """

    // Test Case 16: Hybrid mode with DATETIME type (static) + STRING (dynamic)
    sql """ DROP TABLE IF EXISTS ${tb1} """
    sql """
        CREATE TABLE ${tb1} (
            id BIGINT,
            name STRING,
            ts DATETIME,
            region STRING
        ) ENGINE=iceberg
        PARTITION BY LIST (ts, region) ()
    """
    String tb_ts_src = db1 + "_ts_src"
    sql """ DROP TABLE IF EXISTS ${tb_ts_src} """
    sql """
        CREATE TABLE ${tb_ts_src} (
            id BIGINT,
            name STRING,
            region STRING
        ) ENGINE=iceberg
    """
    sql """
        INSERT INTO ${tb_ts_src} VALUES
        (10, 'Eve', 'bj'),
        (11, 'Frank', 'sh')
    """
    sql """
        INSERT INTO ${tb1} VALUES
        (1, 'Alice', '2025-01-25 10:00:00', 'bj'),
        (2, 'Bob', '2025-01-25 10:00:00', 'sh'),
        (3, 'Charlie', '2025-01-26 10:00:00', 'bj')
    """
    sql """
        INSERT OVERWRITE TABLE ${tb1}
        PARTITION (ts='2025-01-25 10:00:00')
        SELECT id, name, region FROM ${tb_ts_src}
    """
    order_qt_q16 """ SELECT * FROM ${tb1} ORDER BY id """
    sql """ DROP TABLE IF EXISTS ${tb_ts_src} """

    // ============================================================================
    // Test Cases for static partition overwrite with VALUES clause
    // ============================================================================

    // Test Case 17: Basic static partition overwrite with single VALUE
    sql """ DROP TABLE IF EXISTS ${tb1} """
    sql """
        CREATE TABLE ${tb1} (
            id BIGINT,
            name STRING,
            dt DATE,
            region STRING
        ) ENGINE=iceberg
        PARTITION BY LIST (dt, region) ()
    """
    sql """
        INSERT INTO ${tb1} VALUES
        (1, 'Alice', '2025-01-25', 'bj'),
        (2, 'Bob', '2025-01-25', 'sh'),
        (3, 'Charlie', '2025-01-26', 'bj')
    """
    // Overwrite with single VALUE
    sql """
        INSERT OVERWRITE TABLE ${tb1}
        PARTITION (dt='2025-01-25', region='bj')
        (id, name) VALUES (10, 'Eve')
    """
    order_qt_q17 """ SELECT * FROM ${tb1} ORDER BY id """

    // Test Case 18: Static partition overwrite with multiple VALUES
    sql """ DROP TABLE IF EXISTS ${tb1} """
    sql """
        CREATE TABLE ${tb1} (
            id BIGINT,
            name STRING,
            dt DATE,
            region STRING
        ) ENGINE=iceberg
        PARTITION BY LIST (dt, region) ()
    """
    sql """
        INSERT INTO ${tb1} VALUES
        (1, 'Alice', '2025-01-25', 'bj'),
        (2, 'Bob', '2025-01-25', 'sh'),
        (3, 'Charlie', '2025-01-26', 'bj')
    """
    // Overwrite with multiple VALUES
    sql """
        INSERT OVERWRITE TABLE ${tb1}
        PARTITION (dt='2025-01-25', region='bj')
        (id, name) VALUES
            (10, 'Eve'),
            (11, 'Frank'),
            (12, 'Grace')
    """
    order_qt_q18 """ SELECT * FROM ${tb1} ORDER BY id """

    // Test Case 19: Static partition overwrite with VALUES and different data types
    sql """ DROP TABLE IF EXISTS ${tb1} """
    sql """
        CREATE TABLE ${tb1} (
            id BIGINT,
            name STRING,
            amount DECIMAL(10,2),
            is_active BOOLEAN,
            score FLOAT,
            level INT
        ) ENGINE=iceberg
        PARTITION BY LIST (amount, is_active) ()
    """
    sql """
        INSERT INTO ${tb1} VALUES
        (1, 'Alice', 100.50, true, 85.5, 1),
        (2, 'Bob', 100.50, false, 90.0, 2),
        (3, 'Charlie', 200.75, true, 75.5, 1)
    """
    // Overwrite with VALUES containing multiple data types
    sql """
        INSERT OVERWRITE TABLE ${tb1}
        PARTITION (amount=100.50, is_active=true)
        VALUES (10, 'Eve', 95.5, 3)
    """
    order_qt_q19 """ SELECT * FROM ${tb1} ORDER BY id """

    // Test Case 20: Static partition overwrite with empty VALUES (delete partition)
    sql """ DROP TABLE IF EXISTS ${tb1} """
    sql """
        CREATE TABLE ${tb1} (
            id BIGINT,
            name STRING,
            dt DATE,
            region STRING
        ) ENGINE=iceberg
        PARTITION BY LIST (dt, region) ()
    """
    sql """
        INSERT INTO ${tb1} VALUES
        (1, 'Alice', '2025-01-25', 'bj'),
        (2, 'Bob', '2025-01-25', 'sh'),
        (3, 'Charlie', '2025-01-26', 'bj')
    """
    // Delete partition by overwriting with empty VALUES (should handle gracefully)
    // Note: This behavior depends on implementation - empty VALUES might not be allowed
    // Alternative: Use VALUES with a condition that yields no results
    sql """
        INSERT OVERWRITE TABLE ${tb1}
        PARTITION (dt='2025-01-25', region='bj')
        SELECT id, name FROM ${tb1} WHERE 1=0
    """
    test {
        sql """
            INSERT OVERWRITE TABLE ${tb1}
            PARTITION (dt='2025-01-25', region='bj')
            select * from ${tb1} where 1=0
        """
        exception "Expected 2 columns but got 4"
    }

    order_qt_q20 """ SELECT * FROM ${tb1} ORDER BY id """

    // Test Case 21: Multiple partitions with different VALUES
    sql """ DROP TABLE IF EXISTS ${tb1} """
    sql """
        CREATE TABLE ${tb1} (
            id BIGINT,
            name STRING,
            dt DATE,
            region STRING
        ) ENGINE=iceberg
        PARTITION BY LIST (dt, region) ()
    """
    sql """
        INSERT INTO ${tb1} VALUES
        (1, 'Alice', '2025-01-25', 'bj'),
        (2, 'Bob', '2025-01-25', 'sh'),
        (3, 'Charlie', '2025-01-26', 'bj')
    """
    // Overwrite first partition
    sql """
        INSERT OVERWRITE TABLE ${tb1}
        PARTITION (dt='2025-01-25', region='bj')
        VALUES (10, 'Eve')
    """
    // Overwrite second partition
    sql """
        INSERT OVERWRITE TABLE ${tb1}
        PARTITION (dt='2025-01-25', region='sh')
        VALUES (20, 'Frank')
    """
    order_qt_q21 """ SELECT * FROM ${tb1} ORDER BY id """

    // Test Case 22: Static partition overwrite with VALUES containing LONG timestamp
    sql """ DROP TABLE IF EXISTS ${tb1} """
    sql """
        CREATE TABLE ${tb1} (
            id BIGINT,
            name STRING,
            timestamp_val BIGINT
        ) ENGINE=iceberg
        PARTITION BY LIST (timestamp_val) ()
    """
    sql """
        INSERT INTO ${tb1} VALUES
        (1, 'Alice', 1706140800000),
        (2, 'Bob', 1706227200000),
        (3, 'Charlie', 1706313600000)
    """
    // Overwrite with VALUES containing LONG type
    sql """
        INSERT OVERWRITE TABLE ${tb1}
        PARTITION (timestamp_val=1706140800000)
        VALUES (10, 'Eve')
    """
    order_qt_q22 """ SELECT * FROM ${tb1} ORDER BY id """

    // Test Case 23: Static partition overwrite with VALUES containing DATETIME
    sql """ DROP TABLE IF EXISTS ${tb1} """
    sql """
        CREATE TABLE ${tb1} (
            id BIGINT,
            name STRING,
            ts DATETIME,
            category STRING
        ) ENGINE=iceberg
        PARTITION BY LIST (ts, category) ()
    """
    sql """
        INSERT INTO ${tb1} VALUES
        (1, 'Alice', '2025-01-25 10:00:00', 'food'),
        (2, 'Bob', '2025-01-25 10:00:00', 'drink'),
        (3, 'Charlie', '2025-01-26 11:00:00', 'food')
    """
    // Overwrite with VALUES containing DATETIME type
    sql """
        INSERT OVERWRITE TABLE ${tb1}
        PARTITION (ts='2025-01-25 10:00:00', category='food')
        VALUES (10, 'Eve')
    """
    order_qt_q23 """ SELECT * FROM ${tb1} ORDER BY id """

    // Test Case 24: Static partition overwrite with VALUES - multiple rows with different types
    sql """ DROP TABLE IF EXISTS ${tb1} """
    sql """
        CREATE TABLE ${tb1} (
            id BIGINT,
            name STRING,
            dt DATE,
            region STRING,
            amount DECIMAL(10,2)
        ) ENGINE=iceberg
        PARTITION BY LIST (dt, region) ()
    """
    sql """
        INSERT INTO ${tb1} VALUES
        (1, 'Alice', '2025-01-25', 'bj', 100.50),
        (2, 'Bob', '2025-01-25', 'sh', 200.75),
        (3, 'Charlie', '2025-01-26', 'bj', 150.25)
    """
    // Overwrite with multiple VALUES containing DECIMAL type
    sql """
        INSERT OVERWRITE TABLE ${tb1}
        PARTITION (dt='2025-01-25', region='bj')
        VALUES
            (10, 'Eve', 99.99),
            (11, 'Frank', 88.88),
            (12, 'Grace', 77.77)
    """
    order_qt_q24 """ SELECT * FROM ${tb1} ORDER BY id """

    // Test Case 25: Static partition with DECIMAL type
    sql """ DROP TABLE IF EXISTS ${tb1} """
    sql """
        CREATE TABLE ${tb1} (
            id BIGINT,
            name STRING,
            amount DECIMAL(10,2)
        ) ENGINE=iceberg
        PARTITION BY LIST (amount) ()
    """
    sql """
        INSERT INTO ${tb1} VALUES
        (1, 'Alice', 100.50),
        (2, 'Bob', 200.75),
        (3, 'Charlie', 300.25)
    """
    sql """
        INSERT OVERWRITE TABLE ${tb1}
        PARTITION (amount=100.50)
        SELECT 10, 'Eve'
    """
    order_qt_q25 """ SELECT * FROM ${tb1} ORDER BY id """

    // Test Case 26: Hybrid mode with DECIMAL type (static) + INTEGER (dynamic)
    sql """ DROP TABLE IF EXISTS ${tb1} """
    sql """
        CREATE TABLE ${tb1} (
            id BIGINT,
            name STRING,
            amount DECIMAL(10,2),
            quantity INT
        ) ENGINE=iceberg
        PARTITION BY LIST (amount, quantity) ()
    """
    String tb_decimal_src = db1 + "_decimal_src"
    sql """ DROP TABLE IF EXISTS ${tb_decimal_src} """
    sql """
        CREATE TABLE ${tb_decimal_src} (
            id BIGINT,
            name STRING,
            quantity INT
        ) ENGINE=iceberg
    """
    sql """
        INSERT INTO ${tb_decimal_src} VALUES
        (10, 'Eve', 10),
        (11, 'Frank', 20)
    """
    sql """
        INSERT INTO ${tb1} VALUES
        (1, 'Alice', 100.50, 10),
        (2, 'Bob', 100.50, 20),
        (3, 'Charlie', 200.75, 10)
    """
    sql """
        INSERT OVERWRITE TABLE ${tb1}
        PARTITION (amount=100.50)
        SELECT id, name, quantity FROM ${tb_decimal_src}
    """
    order_qt_q26 """ SELECT * FROM ${tb1} ORDER BY id """
    sql """ DROP TABLE IF EXISTS ${tb_decimal_src} """

    // Test Case 27: Multiple types in static partition (INTEGER, FLOAT, BOOLEAN, STRING)
    sql """ DROP TABLE IF EXISTS ${tb1} """
    sql """
        CREATE TABLE ${tb1} (
            id BIGINT,
            name STRING,
            level INT,
            score FLOAT,
            is_active BOOLEAN,
            category STRING
        ) ENGINE=iceberg
        PARTITION BY LIST (level, score, is_active, category) ()
    """
    sql """
        INSERT INTO ${tb1} VALUES
        (1, 'Alice', 1, 85.5, true, 'A'),
        (2, 'Bob', 1, 85.5, true, 'B'),
        (3, 'Charlie', 2, 90.0, false, 'A'),
        (4, 'David', 1, 85.5, false, 'A')
    """
    sql """
        INSERT OVERWRITE TABLE ${tb1}
        PARTITION (level=1, score=85.5, is_active=true, category='A')
        SELECT 10, 'Eve'
    """
    order_qt_q27 """ SELECT * FROM ${tb1} ORDER BY id """

    // Test Case 28: Hybrid mode with multiple types (2 static + 2 dynamic)
    sql """ DROP TABLE IF EXISTS ${tb1} """
    sql """
        CREATE TABLE ${tb1} (
            id BIGINT,
            name STRING,
            level INT,
            score FLOAT,
            is_active BOOLEAN,
            category STRING
        ) ENGINE=iceberg
        PARTITION BY LIST (level, score, is_active, category) ()
    """
    String tb_multi_src = db1 + "_multi_src"
    sql """ DROP TABLE IF EXISTS ${tb_multi_src} """
    sql """
        CREATE TABLE ${tb_multi_src} (
            id BIGINT,
            name STRING,
            is_active BOOLEAN,
            category STRING
        ) ENGINE=iceberg
    """
    sql """
        INSERT INTO ${tb_multi_src} VALUES
        (10, 'Eve', true, 'A'),
        (11, 'Frank', false, 'B')
    """
    sql """
        INSERT INTO ${tb1} VALUES
        (1, 'Alice', 1, 85.5, true, 'A'),
        (2, 'Bob', 1, 85.5, true, 'B'),
        (3, 'Charlie', 1, 85.5, false, 'A'),
        (4, 'David', 2, 90.0, true, 'A')
    """
    sql """
        INSERT OVERWRITE TABLE ${tb1}
        PARTITION (level=1, score=85.5)
        SELECT id, name, is_active, category FROM ${tb_multi_src}
    """
    order_qt_q28 """ SELECT * FROM ${tb1} ORDER BY id """
    sql """ DROP TABLE IF EXISTS ${tb_multi_src} """

    // ============================================================================
    // Test Cases for non-identity partition transforms - static partition overwrite should fail
    // ============================================================================

    // Test Case 29: Error scenario - bucket partition (non-identity transform)
    sql """ DROP TABLE IF EXISTS ${tb1} """
    sql """
        CREATE TABLE ${tb1} (
            id BIGINT,
            name STRING,
            category STRING
        ) ENGINE=iceberg
        PARTITION BY LIST (bucket(4, category)) ()
    """
    sql """
        INSERT INTO ${tb1} VALUES
        (1, 'Alice', 'food'),
        (2, 'Bob', 'drink')
    """
    test {
        sql """
            INSERT OVERWRITE TABLE ${tb1} 
            PARTITION (category='food')
            SELECT 10, 'Eve'
        """
        exception "Unknown partition column"
    }
    // Using correct partition field name should trigger non-identity error
    test {
        sql """
            INSERT OVERWRITE TABLE ${tb1} 
            PARTITION (category_bucket=0)
            SELECT 10, 'Eve'
        """
        exception "Cannot use static partition syntax for non-identity partition field"
    }

    // Test Case 30: Error scenario - truncate partition (non-identity transform)
    sql """ DROP TABLE IF EXISTS ${tb1} """
    sql """
        CREATE TABLE ${tb1} (
            id BIGINT,
            name STRING,
            description STRING
        ) ENGINE=iceberg
        PARTITION BY LIST (truncate(3, description)) ()
    """
    sql """
        INSERT INTO ${tb1} VALUES
        (1, 'Alice', 'hello world'),
        (2, 'Bob', 'goodbye')
    """
    test {
        sql """
            INSERT OVERWRITE TABLE ${tb1} 
            PARTITION (description='hello')
            SELECT 10, 'Eve'
        """
        exception "Unknown partition column"
    }
    // Using correct partition field name should trigger non-identity error
    test {
        sql """
            INSERT OVERWRITE TABLE ${tb1} 
            PARTITION (description_trunc='hel')
            SELECT 10, 'Eve'
        """
        exception "Cannot use static partition syntax for non-identity partition field"
    }

    // Test Case 31: Error scenario - day partition (non-identity time transform)
    sql """ DROP TABLE IF EXISTS ${tb1} """
    sql """
        CREATE TABLE ${tb1} (
            id BIGINT,
            name STRING,
            ts DATETIME
        ) ENGINE=iceberg
        PARTITION BY LIST (day(ts)) ()
    """
    sql """
        INSERT INTO ${tb1} VALUES
        (1, 'Alice', '2025-01-25 10:00:00'),
        (2, 'Bob', '2025-01-26 11:00:00')
    """
    test {
        sql """
            INSERT OVERWRITE TABLE ${tb1} 
            PARTITION (ts='2025-01-25 10:00:00')
            SELECT 10, 'Eve'
        """
        exception "Unknown partition column"
    }
    // Using correct partition field name should trigger non-identity error
    test {
        sql """
            INSERT OVERWRITE TABLE ${tb1} 
            PARTITION (ts_day='2025-01-25')
            SELECT 10, 'Eve'
        """
        exception "Cannot use static partition syntax for non-identity partition field"
    }

    // Test Case 32: Error scenario - year partition (non-identity time transform)
    sql """ DROP TABLE IF EXISTS ${tb1} """
    sql """
        CREATE TABLE ${tb1} (
            id BIGINT,
            name STRING,
            event_date DATE
        ) ENGINE=iceberg
        PARTITION BY LIST (year(event_date)) ()
    """
    sql """
        INSERT INTO ${tb1} VALUES
        (1, 'Alice', '2024-06-15'),
        (2, 'Bob', '2025-01-25')
    """
    test {
        sql """
            INSERT OVERWRITE TABLE ${tb1} 
            PARTITION (event_date='2024-06-15')
            SELECT 10, 'Eve'
        """
        exception "Unknown partition column"
    }
    // Using correct partition field name should trigger non-identity error
    test {
        sql """
            INSERT OVERWRITE TABLE ${tb1} 
            PARTITION (event_date_year=2024)
            SELECT 10, 'Eve'
        """
        exception "Cannot use static partition syntax for non-identity partition field"
    }

    // Test Case 33: Error scenario - month partition (non-identity time transform)
    sql """ DROP TABLE IF EXISTS ${tb1} """
    sql """
        CREATE TABLE ${tb1} (
            id BIGINT,
            name STRING,
            event_date DATE
        ) ENGINE=iceberg
        PARTITION BY LIST (month(event_date)) ()
    """
    sql """
        INSERT INTO ${tb1} VALUES
        (1, 'Alice', '2025-01-15'),
        (2, 'Bob', '2025-02-20')
    """
    test {
        sql """
            INSERT OVERWRITE TABLE ${tb1} 
            PARTITION (event_date='2025-01-15')
            SELECT 10, 'Eve'
        """
        exception "Unknown partition column"
    }
    // Using correct partition field name should trigger non-identity error
    test {
        sql """
            INSERT OVERWRITE TABLE ${tb1} 
            PARTITION (event_date_month='2025-01')
            SELECT 10, 'Eve'
        """
        exception "Cannot use static partition syntax for non-identity partition field"
    }

    // Test Case 34: Error scenario - hour partition (non-identity time transform)
    sql """ DROP TABLE IF EXISTS ${tb1} """
    sql """
        CREATE TABLE ${tb1} (
            id BIGINT,
            name STRING,
            ts DATETIME
        ) ENGINE=iceberg
        PARTITION BY LIST (hour(ts)) ()
    """
    sql """
        INSERT INTO ${tb1} VALUES
        (1, 'Alice', '2025-01-25 10:30:00'),
        (2, 'Bob', '2025-01-25 11:45:00')
    """
    test {
        sql """
            INSERT OVERWRITE TABLE ${tb1} 
            PARTITION (ts='2025-01-25 10:00:00')
            SELECT 10, 'Eve'
        """
        exception "Unknown partition column"
    }
    // Using correct partition field name should trigger non-identity error
    test {
        sql """
            INSERT OVERWRITE TABLE ${tb1} 
            PARTITION (ts_hour='2025-01-25-10')
            SELECT 10, 'Eve'
        """
        exception "Cannot use static partition syntax for non-identity partition field"
    }

    // Test Case 35: Error scenario - mixed identity and non-identity partitions (bucket)
    // Table has identity partition (region) + non-identity partition (bucket on id)
    sql """ DROP TABLE IF EXISTS ${tb1} """
    sql """
        CREATE TABLE ${tb1} (
            id BIGINT,
            name STRING,
            region STRING
        ) ENGINE=iceberg
        PARTITION BY LIST (region, bucket(4, id)) ()
    """
    sql """
        INSERT INTO ${tb1} VALUES
        (1, 'Alice', 'bj'),
        (2, 'Bob', 'sh')
    """
    // Static partition on non-identity column should fail
    test {
        sql """
            INSERT OVERWRITE TABLE ${tb1} 
            PARTITION (id=1)
            SELECT 'Eve', 'bj'
        """
        exception "Unknown partition column"
    }
    // Using correct partition field name should trigger non-identity error
    test {
        sql """
            INSERT OVERWRITE TABLE ${tb1} 
            PARTITION (id_bucket=0)
            SELECT 'Eve', 'bj'
        """
        exception "Cannot use static partition syntax for non-identity partition field"
    }

    // Test Case 36: Error scenario - truncate on integer column
    sql """ DROP TABLE IF EXISTS ${tb1} """
    sql """
        CREATE TABLE ${tb1} (
            id BIGINT,
            name STRING,
            amount INT
        ) ENGINE=iceberg
        PARTITION BY LIST (truncate(100, amount)) ()
    """
    sql """
        INSERT INTO ${tb1} VALUES
        (1, 'Alice', 150),
        (2, 'Bob', 250)
    """
    test {
        sql """
            INSERT OVERWRITE TABLE ${tb1} 
            PARTITION (amount=100)
            SELECT 10, 'Eve'
        """
        exception "Unknown partition column"
    }
    // Using correct partition field name should trigger non-identity error
    test {
        sql """
            INSERT OVERWRITE TABLE ${tb1} 
            PARTITION (amount_trunc=100)
            SELECT 10, 'Eve'
        """
        exception "Cannot use static partition syntax for non-identity partition field"
    }

    // Test Case 37: Error scenario - bucket on BIGINT column
    sql """ DROP TABLE IF EXISTS ${tb1} """
    sql """
        CREATE TABLE ${tb1} (
            id BIGINT,
            name STRING,
            user_id BIGINT
        ) ENGINE=iceberg
        PARTITION BY LIST (bucket(8, user_id)) ()
    """
    sql """
        INSERT INTO ${tb1} VALUES
        (1, 'Alice', 1001),
        (2, 'Bob', 2002)
    """
    test {
        sql """
            INSERT OVERWRITE TABLE ${tb1} 
            PARTITION (user_id=1001)
            SELECT 10, 'Eve'
        """
        exception "Unknown partition column"
    }
    // Using correct partition field name should trigger non-identity error
    test {
        sql """
            INSERT OVERWRITE TABLE ${tb1} 
            PARTITION (user_id_bucket=0)
            SELECT 10, 'Eve'
        """
        exception "Cannot use static partition syntax for non-identity partition field"
    }

    // Test Case 38: Mixed partitions - identity column is OK, but non-identity should fail
    // Test that specifying only identity partition columns works,
    // but including non-identity columns fails
    sql """ DROP TABLE IF EXISTS ${tb1} """
    sql """
        CREATE TABLE ${tb1} (
            id BIGINT,
            name STRING,
            region STRING,
            ts DATETIME
        ) ENGINE=iceberg
        PARTITION BY LIST (region, day(ts)) ()
    """
    sql """
        INSERT INTO ${tb1} VALUES
        (1, 'Alice', 'bj', '2025-01-25 10:00:00'),
        (2, 'Bob', 'sh', '2025-01-26 11:00:00')
    """
    // Specifying only identity partition column (region) - this should work normally
    // But we need to also select ts column dynamically since day(ts) is a partition
    // Note: This is a tricky case - with partial static partition, the non-specified
    // partition columns should come from SELECT. But ts column must be in the query result.
    // For simplicity, test only the error case where non-identity column is specified
    test {
        sql """
            INSERT OVERWRITE TABLE ${tb1} 
            PARTITION (ts='2025-01-25 10:00:00')
            SELECT 10, 'Eve', 'bj'
        """
        exception "Unknown partition column"
    }
    // Using correct partition field name should trigger non-identity error
    test {
        sql """
            INSERT OVERWRITE TABLE ${tb1} 
            PARTITION (ts_day='2025-01-25')
            SELECT 10, 'Eve', 'bj'
        """
        exception "Cannot use static partition syntax for non-identity partition field"
    }
}
