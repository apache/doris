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
        ) PARTITIONED BY (dt, region)
        USING ICEBERG
        TBLPROPERTIES (
            'format-version' = '2'
        )
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

    // Test Case 2: Partial static partition overwrite (only some partition columns specified)
    // Test overwriting all partitions matching a partial partition specification
    sql """ DROP TABLE IF EXISTS ${tb1} """
    sql """
        CREATE TABLE ${tb1} (
            id BIGINT,
            name STRING,
            dt DATE,
            region STRING
        ) PARTITIONED BY (dt, region)
        USING ICEBERG
        TBLPROPERTIES (
            'format-version' = '2'
        )
    """

    sql """
        INSERT INTO ${tb1} VALUES
        (1, 'Alice', '2025-01-25', 'bj'),
        (2, 'Bob', '2025-01-25', 'sh'),
        (3, 'Charlie', '2025-01-26', 'bj')
    """

    // Overwrite all partitions where dt='2025-01-25' (including all region values)
    sql """
        INSERT OVERWRITE TABLE ${tb1} 
        PARTITION (dt='2025-01-25')
        SELECT 20, 'Frank', '2025-01-25', 'sz'
    """

    // Verify: All partitions with dt='2025-01-25' are overwritten
    // Partitions with dt='2025-01-26' remain unchanged
    order_qt_q03 """ SELECT * FROM ${tb1} ORDER BY id """

    // Test Case 3: Empty result overwrite (delete specified partition)
    // Test deleting a partition by overwriting with empty result
    sql """ DROP TABLE IF EXISTS ${tb1} """
    sql """
        CREATE TABLE ${tb1} (
            id BIGINT,
            name STRING,
            dt DATE,
            region STRING
        ) PARTITIONED BY (dt, region)
        USING ICEBERG
        TBLPROPERTIES (
            'format-version' = '2'
        )
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
        ) PARTITIONED BY (dt, region)
        USING ICEBERG
        TBLPROPERTIES (
            'format-version' = '2'
        )
    """

    test {
        sql """
            INSERT OVERWRITE TABLE ${tb1} 
            PARTITION (invalid_col='value')
            SELECT * FROM ${tb1}
        """
        exception "Unknown partition column"
    }

    // Test Case 5: Multiple static partitions with different data types
    sql """ DROP TABLE IF EXISTS ${tb1} """
    sql """
        CREATE TABLE ${tb1} (
            id BIGINT,
            name STRING,
            dt DATE,
            region STRING,
            amount INT
        ) PARTITIONED BY (dt, region, amount)
        USING ICEBERG
        TBLPROPERTIES (
            'format-version' = '2'
        )
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
        SELECT 10, 'Eve', '2025-01-25', 'bj', 100
    """

    // Verify: Only the exact matching partition is overwritten
    order_qt_q05 """ SELECT * FROM ${tb1} ORDER BY id """

    // Cleanup
    sql """ DROP TABLE IF EXISTS ${tb1} """
    sql """ drop database if exists ${db1} force"""
    sql """drop catalog if exists ${catalog_name}"""
}
