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

suite("test_iceberg_rewrite_manifests", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String catalog_name = "test_iceberg_rewrite_manifests"
    String db_name = "test_db"
    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    
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

    sql """switch ${catalog_name}"""
    sql """CREATE DATABASE IF NOT EXISTS ${db_name} """
    sql """use ${db_name}"""

    // =====================================================================================
    // Test Case 1: Basic rewrite_manifests operation
    // Tests the ability to rewrite multiple manifest files into fewer, optimized files
    // =====================================================================================
    logger.info("Starting basic rewrite_manifests test case")
    
    def table_name = "test_rewrite_manifests_basic"
    
    // Clean up if table exists
    sql """DROP TABLE IF EXISTS ${db_name}.${table_name}"""
    
    // Create a test table
    sql """
        CREATE TABLE ${db_name}.${table_name} (
            id BIGINT,
            name STRING,
            category STRING,
            value INT,
            created_date DATE
        ) ENGINE=iceberg
    """
    logger.info("Created test table: ${table_name}")
    
    // Insert data in multiple single-row operations to create multiple manifest files
    // Each INSERT operation typically creates a new manifest file
    sql """INSERT INTO ${db_name}.${table_name} VALUES (1, 'item1', 'electronics', 100, '2024-01-01')"""
    sql """INSERT INTO ${db_name}.${table_name} VALUES (2, 'item2', 'electronics', 200, '2024-01-02')"""
    sql """INSERT INTO ${db_name}.${table_name} VALUES (3, 'item3', 'books', 300, '2024-01-03')"""
    sql """INSERT INTO ${db_name}.${table_name} VALUES (4, 'item4', 'books', 400, '2024-01-04')"""
    sql """INSERT INTO ${db_name}.${table_name} VALUES (5, 'item5', 'clothing', 500, '2024-01-05')"""
    sql """INSERT INTO ${db_name}.${table_name} VALUES (6, 'item6', 'clothing', 600, '2024-01-06')"""
    sql """INSERT INTO ${db_name}.${table_name} VALUES (7, 'item7', 'electronics', 700, '2024-01-07')"""
    sql """INSERT INTO ${db_name}.${table_name} VALUES (8, 'item8', 'electronics', 800, '2024-01-08')"""
    
    // Verify data before rewrite
    qt_before_basic_rewrite """SELECT * FROM ${table_name} ORDER BY id"""
    
    // Check manifest count before rewrite
    List<List<Object>> manifestsBefore = sql """
        SELECT COUNT(*) as manifest_count FROM ${table_name}\$manifests
    """
    logger.info("Manifest count before rewrite: ${manifestsBefore}")
    
    // Execute basic rewrite_manifests operation
    List<List<Object>> rewriteResult = sql """
        ALTER TABLE ${catalog_name}.${db_name}.${table_name}
        EXECUTE rewrite_manifests()
    """
    logger.info("Basic rewrite_manifests result: ${rewriteResult}")
    
    // Verify the result structure
    assertTrue(rewriteResult.size() == 1, "Expected exactly 1 result row")
    assertTrue(rewriteResult[0].size() == 2, "Expected 2 columns in result")
    
    // Extract rewritten and total manifest counts
    int rewrittenCount = rewriteResult[0][0] as int
    int totalCount = rewriteResult[0][1] as int
    
    logger.info("Rewritten manifests: ${rewrittenCount}, Total manifests: ${totalCount}")
    assertTrue(rewrittenCount > 0, "Should have rewritten at least 1 manifest")
    assertTrue(totalCount >= rewrittenCount, "Total count should be >= rewritten count")
    
    // Verify data integrity after rewrite
    qt_after_basic_rewrite """SELECT * FROM ${table_name} ORDER BY id"""
    
    // Check manifest count after rewrite (should be fewer)
    List<List<Object>> manifestsAfter = sql """
        SELECT COUNT(*) as manifest_count FROM ${table_name}\$manifests
    """
    logger.info("Manifest count after rewrite: ${manifestsAfter}")

    // =====================================================================================
    // Test Case 2: rewrite_manifests with cluster-by-partition option
    // Tests manifest rewriting with partition clustering optimization
    // =====================================================================================
    logger.info("Starting rewrite_manifests with cluster-by-partition test case")
    
    def partitioned_table = "test_rewrite_manifests_partitioned"
    
    // Clean up if table exists
    sql """DROP TABLE IF EXISTS ${db_name}.${partitioned_table}"""
    
    // Create a partitioned table
    sql """
        CREATE TABLE ${db_name}.${partitioned_table} (
            id BIGINT,
            name STRING,
            value INT,
            year INT,
            month INT
        ) ENGINE=iceberg
        PARTITION BY (year, month)()
    """
    logger.info("Created partitioned test table: ${partitioned_table}")
    
    // Insert data across multiple partitions in single-row operations to create multiple manifest files
    sql """INSERT INTO ${db_name}.${partitioned_table} VALUES (1, 'jan_item1', 100, 2024, 1)"""
    sql """INSERT INTO ${db_name}.${partitioned_table} VALUES (2, 'jan_item2', 200, 2024, 1)"""
    sql """INSERT INTO ${db_name}.${partitioned_table} VALUES (3, 'feb_item1', 300, 2024, 2)"""
    sql """INSERT INTO ${db_name}.${partitioned_table} VALUES (4, 'feb_item2', 400, 2024, 2)"""
    sql """INSERT INTO ${db_name}.${partitioned_table} VALUES (5, 'mar_item1', 500, 2024, 3)"""
    sql """INSERT INTO ${db_name}.${partitioned_table} VALUES (6, 'mar_item2', 600, 2024, 3)"""
    
    // Verify data before partition-clustered rewrite
    qt_before_partition_rewrite """SELECT * FROM ${partitioned_table} ORDER BY id"""
    
    // Execute rewrite_manifests with cluster-by-partition
    List<List<Object>> partitionRewriteResult = sql """
        ALTER TABLE ${catalog_name}.${db_name}.${partitioned_table}
        EXECUTE rewrite_manifests('cluster-by-partition' = 'true')
    """
    logger.info("Partition-clustered rewrite_manifests result: ${partitionRewriteResult}")
    
    // Verify data integrity after partition-clustered rewrite
    qt_after_partition_rewrite """SELECT * FROM ${partitioned_table} ORDER BY id"""

    // =====================================================================================
    // Test Case 3: rewrite_manifests with rewrite-all option
    // Tests forcing rewrite of all manifests regardless of size thresholds
    // =====================================================================================
    logger.info("Starting rewrite_manifests with rewrite-all test case")
    
    def rewrite_all_table = "test_rewrite_manifests_all"
    
    // Clean up if table exists
    sql """DROP TABLE IF EXISTS ${db_name}.${rewrite_all_table}"""
    
    // Create a test table
    sql """
        CREATE TABLE ${db_name}.${rewrite_all_table} (
            id BIGINT,
            data STRING,
            status INT
        ) ENGINE=iceberg
    """
    logger.info("Created rewrite-all test table: ${rewrite_all_table}")
    
    // Insert small amounts of data in single-row operations to create small manifest files
    sql """INSERT INTO ${db_name}.${rewrite_all_table} VALUES (1, 'data1', 1)"""
    sql """INSERT INTO ${db_name}.${rewrite_all_table} VALUES (2, 'data2', 2)"""
    
    // Verify data before rewrite-all
    qt_before_rewrite_all """SELECT * FROM ${rewrite_all_table} ORDER BY id"""
    
    // Execute rewrite_manifests with rewrite-all option
    List<List<Object>> rewriteAllResult = sql """
        ALTER TABLE ${catalog_name}.${db_name}.${rewrite_all_table}
        EXECUTE rewrite_manifests('rewrite-all' = 'true')
    """
    logger.info("Rewrite-all manifests result: ${rewriteAllResult}")
    
    // Verify data integrity after rewrite-all
    qt_after_rewrite_all """SELECT * FROM ${rewrite_all_table} ORDER BY id"""

    // =====================================================================================
    // Test Case 4: rewrite_manifests with size thresholds
    // Tests manifest rewriting with minimum and maximum size constraints
    // =====================================================================================
    logger.info("Starting rewrite_manifests with size thresholds test case")
    
    def size_threshold_table = "test_rewrite_manifests_size"
    
    // Clean up if table exists
    sql """DROP TABLE IF EXISTS ${db_name}.${size_threshold_table}"""
    
    // Create a test table
    sql """
        CREATE TABLE ${db_name}.${size_threshold_table} (
            id BIGINT,
            content STRING,
            category STRING
        ) ENGINE=iceberg
    """
    logger.info("Created size threshold test table: ${size_threshold_table}")
    
    // Insert data in single-row operations to create manifest files of various sizes
    sql """INSERT INTO ${db_name}.${size_threshold_table} VALUES (1, 'small_content_1', 'A')"""
    sql """INSERT INTO ${db_name}.${size_threshold_table} VALUES (2, 'small_content_2', 'A')"""
    sql """INSERT INTO ${db_name}.${size_threshold_table} VALUES (3, 'medium_content_1', 'B')"""
    sql """INSERT INTO ${db_name}.${size_threshold_table} VALUES (4, 'medium_content_2', 'B')"""
    sql """INSERT INTO ${db_name}.${size_threshold_table} VALUES (5, 'medium_content_3', 'B')"""
    
    // Verify data before size-based rewrite
    qt_before_size_rewrite """SELECT * FROM ${size_threshold_table} ORDER BY id"""
    
    // Execute rewrite_manifests with minimum size threshold
    List<List<Object>> sizeRewriteResult = sql """
        ALTER TABLE ${catalog_name}.${db_name}.${size_threshold_table}
        EXECUTE rewrite_manifests('min-manifest-size-bytes' = '1000')
    """
    logger.info("Size threshold rewrite_manifests result: ${sizeRewriteResult}")
    
    // Verify data integrity after size-based rewrite
    qt_after_size_rewrite """SELECT * FROM ${size_threshold_table} ORDER BY id"""

    // =====================================================================================
    // Test Case 5: rewrite_manifests with scan thread pool configuration
    // Tests parallel manifest scanning with custom thread pool size
    // =====================================================================================
    logger.info("Starting rewrite_manifests with scan thread pool test case")
    
    def thread_pool_table = "test_rewrite_manifests_threads"
    
    // Clean up if table exists
    sql """DROP TABLE IF EXISTS ${db_name}.${thread_pool_table}"""
    
    // Create a test table
    sql """
        CREATE TABLE ${db_name}.${thread_pool_table} (
            id BIGINT,
            name STRING,
            value DOUBLE
        ) ENGINE=iceberg
    """
    logger.info("Created thread pool test table: ${thread_pool_table}")
    
    // Insert data in single-row operations to create multiple manifest files
    sql """INSERT INTO ${db_name}.${thread_pool_table} VALUES (1, 'item_1', 10.5)"""
    sql """INSERT INTO ${db_name}.${thread_pool_table} VALUES (2, 'item_2', 21.0)"""
    sql """INSERT INTO ${db_name}.${thread_pool_table} VALUES (3, 'item_3', 31.5)"""
    sql """INSERT INTO ${db_name}.${thread_pool_table} VALUES (4, 'item_4', 42.0)"""
    sql """INSERT INTO ${db_name}.${thread_pool_table} VALUES (5, 'item_5', 52.5)"""
    sql """INSERT INTO ${db_name}.${thread_pool_table} VALUES (6, 'item_6', 63.0)"""
    
    // Verify data before thread pool rewrite
    qt_before_thread_pool_rewrite """SELECT * FROM ${thread_pool_table} ORDER BY id"""
    
    // Execute rewrite_manifests with custom scan thread pool size
    List<List<Object>> threadPoolRewriteResult = sql """
        ALTER TABLE ${catalog_name}.${db_name}.${thread_pool_table}
        EXECUTE rewrite_manifests('scan-thread-pool-size' = '4')
    """
    logger.info("Thread pool rewrite_manifests result: ${threadPoolRewriteResult}")
    
    // Verify data integrity after thread pool rewrite
    qt_after_thread_pool_rewrite """SELECT * FROM ${thread_pool_table} ORDER BY id"""

    // =====================================================================================
    // Test Case 6: rewrite_manifests with combined parameters
    // Tests manifest rewriting with multiple parameters combined
    // =====================================================================================
    logger.info("Starting rewrite_manifests with combined parameters test case")
    
    def combined_table = "test_rewrite_manifests_combined"
    
    // Clean up if table exists
    sql """DROP TABLE IF EXISTS ${db_name}.${combined_table}"""
    
    // Create a partitioned test table
    sql """
        CREATE TABLE ${db_name}.${combined_table} (
            id BIGINT,
            name STRING,
            category STRING,
            value INT,
            partition_key STRING
        ) ENGINE=iceberg
        PARTITION BY (partition_key)()
    """
    logger.info("Created combined parameters test table: ${combined_table}")
    
    // Insert data across multiple partitions in single-row operations
    sql """INSERT INTO ${db_name}.${combined_table} VALUES (1, 'item1', 'electronics', 100, 'part_a')"""
    sql """INSERT INTO ${db_name}.${combined_table} VALUES (2, 'item2', 'electronics', 200, 'part_a')"""
    sql """INSERT INTO ${db_name}.${combined_table} VALUES (3, 'item3', 'books', 300, 'part_b')"""
    sql """INSERT INTO ${db_name}.${combined_table} VALUES (4, 'item4', 'books', 400, 'part_b')"""
    
    // Verify data before combined rewrite
    qt_before_combined_rewrite """SELECT * FROM ${combined_table} ORDER BY id"""
    
    // Execute rewrite_manifests with multiple parameters
    List<List<Object>> combinedRewriteResult = sql """
        ALTER TABLE ${catalog_name}.${db_name}.${combined_table}
        EXECUTE rewrite_manifests(
            'cluster-by-partition' = 'true',
            'min-manifest-size-bytes' = '500',
            'scan-thread-pool-size' = '2'
        )
    """
    logger.info("Combined parameters rewrite_manifests result: ${combinedRewriteResult}")
    
    // Verify data integrity after combined rewrite
    qt_after_combined_rewrite """SELECT * FROM ${combined_table} ORDER BY id"""

    // =====================================================================================
    // Negative Test Cases: Parameter validation and error handling
    // =====================================================================================
    logger.info("Starting negative test cases for rewrite_manifests")

    // Test with invalid cluster-by-partition value
    test {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${table_name}
            EXECUTE rewrite_manifests('cluster-by-partition' = 'invalid')
        """
        exception "cluster-by-partition must be 'true' or 'false', got: invalid"
    }

    // Test with invalid rewrite-all value
    test {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${table_name}
            EXECUTE rewrite_manifests('rewrite-all' = 'not-boolean')
        """
        exception "rewrite-all must be 'true' or 'false', got: not-boolean"
    }

    // Test with negative min-manifest-size-bytes
    test {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${table_name}
            EXECUTE rewrite_manifests('min-manifest-size-bytes' = '-1000')
        """
        exception "min-manifest-size-bytes must be positive, got: -1000"
    }

    // Test with invalid min-manifest-size-bytes format
    test {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${table_name}
            EXECUTE rewrite_manifests('min-manifest-size-bytes' = 'not-a-number')
        """
        exception "Invalid min-manifest-size-bytes format: not-a-number"
    }

    // Test with negative max-manifest-size-bytes
    test {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${table_name}
            EXECUTE rewrite_manifests('max-manifest-size-bytes' = '-5000')
        """
        exception "max-manifest-size-bytes must be positive, got: -5000"
    }

    // Test with invalid max-manifest-size-bytes format
    test {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${table_name}
            EXECUTE rewrite_manifests('max-manifest-size-bytes' = 'invalid-size')
        """
        exception "Invalid max-manifest-size-bytes format: invalid-size"
    }

    // Test with min-manifest-size-bytes greater than max-manifest-size-bytes
    test {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${table_name}
            EXECUTE rewrite_manifests(
                'min-manifest-size-bytes' = '10000',
                'max-manifest-size-bytes' = '5000'
            )
        """
        exception "min-manifest-size-bytes (10000) cannot be greater than max-manifest-size-bytes (5000)"
    }

    // Test with negative scan-thread-pool-size
    test {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${table_name}
            EXECUTE rewrite_manifests('scan-thread-pool-size' = '-1')
        """
        exception "scan-thread-pool-size must be between 0 and 16, got: -1"
    }

    // Test with scan-thread-pool-size exceeding maximum
    test {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${table_name}
            EXECUTE rewrite_manifests('scan-thread-pool-size' = '20')
        """
        exception "scan-thread-pool-size must be between 0 and 16, got: 20"
    }

    // Test with invalid scan-thread-pool-size format
    test {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${table_name}
            EXECUTE rewrite_manifests('scan-thread-pool-size' = 'not-a-number')
        """
        exception "Invalid scan-thread-pool-size format: not-a-number"
    }

    // Test with unknown parameter
    test {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${table_name}
            EXECUTE rewrite_manifests('unknown-parameter' = 'value')
        """
        exception "Unknown argument: unknown-parameter"
    }

    // Test rewrite_manifests with partition specification (should fail)
    test {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${table_name}
            EXECUTE rewrite_manifests() PARTITIONS (part1)
        """
        exception "Action 'rewrite_manifests' does not support partition specification"
    }

    // Test rewrite_manifests with WHERE condition (should fail)
    test {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${table_name}
            EXECUTE rewrite_manifests() WHERE id > 0
        """
        exception "Action 'rewrite_manifests' does not support WHERE condition"
    }

    // Test on non-existent table
    test {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.non_existent_table
            EXECUTE rewrite_manifests()
        """
        exception "Table non_existent_table does not exist"
    }

    // Test on empty table (should succeed but do nothing)
    def empty_table = "test_empty_table"
    sql """DROP TABLE IF EXISTS ${db_name}.${empty_table}"""
    sql """
        CREATE TABLE ${db_name}.${empty_table} (
            id BIGINT,
            name STRING
        ) ENGINE=iceberg
    """
    
    List<List<Object>> emptyTableResult = sql """
        ALTER TABLE ${catalog_name}.${db_name}.${empty_table}
        EXECUTE rewrite_manifests()
    """
    logger.info("Empty table rewrite_manifests result: ${emptyTableResult}")
    
    // Should return 0 rewritten manifests for empty table
    assertTrue(emptyTableResult[0][0] as int == 0, "Empty table should have 0 rewritten manifests")

    logger.info("All rewrite_manifests test cases completed successfully")
}