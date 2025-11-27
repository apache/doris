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
        SELECT 10, 'Eve', '2025-01-25', 'bj'
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
        SELECT * FROM ${tb1} WHERE 1=0
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

    // Cleanup
    sql """ DROP TABLE IF EXISTS ${tb1} """
    sql """ drop database if exists ${db1} force"""
    sql """drop catalog if exists ${catalog_name}"""
}
