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
    
    // Execute basic rewrite_manifests operation (no parameters - rewrite all manifests)
    List<List<Object>> rewriteResult = sql """
        ALTER TABLE ${catalog_name}.${db_name}.${table_name}
        EXECUTE rewrite_manifests()
    """
    logger.info("Basic rewrite_manifests result: ${rewriteResult}")
    
    // Verify the result structure
    assertTrue(rewriteResult.size() == 1, "Expected exactly 1 result row")
    assertTrue(rewriteResult[0].size() == 2, "Expected 2 columns in result")
    
    // Extract rewritten and added manifest counts
    int rewrittenCount = rewriteResult[0][0] as int
    int addedCount = rewriteResult[0][1] as int
    
    logger.info("Rewritten manifests: ${rewrittenCount}, Added manifests: ${addedCount}")
    assertTrue(rewrittenCount > 0, "Should have rewritten at least 1 manifest")
    assertTrue(addedCount >= 0, "Added count should be non-negative")
    // Note: addedCount can be 0 if Iceberg determines manifests are already optimal
    // or if it reuses existing manifest files
    if (addedCount > 0) {
        assertTrue(addedCount <= rewrittenCount, "Added count should be <= rewritten count (consolidation)")
    }
    
    // Verify data integrity after rewrite
    qt_after_basic_rewrite """SELECT * FROM ${table_name} ORDER BY id"""
    
    // Check manifest count after rewrite (should be fewer or equal)
    List<List<Object>> manifestsAfter = sql """
        SELECT COUNT(*) as manifest_count FROM ${table_name}\$manifests
    """
    logger.info("Manifest count after rewrite: ${manifestsAfter}")
    assertTrue(manifestsAfter[0][0] as int <= manifestsBefore[0][0] as int, 
        "Manifest count after rewrite should be <= count before")

    // =====================================================================================
    // Test Case 2: rewrite_manifests on partitioned table
    // Tests manifest rewriting on a table with partition specifications
    // =====================================================================================
    logger.info("Starting rewrite_manifests on partitioned table test case")
    
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
    
    // Insert data into different partitions to create multiple manifest files
    sql """INSERT INTO ${db_name}.${partitioned_table} VALUES (1, 'item1', 100, 2024, 1)"""
    sql """INSERT INTO ${db_name}.${partitioned_table} VALUES (2, 'item2', 200, 2024, 1)"""
    sql """INSERT INTO ${db_name}.${partitioned_table} VALUES (3, 'item3', 300, 2024, 2)"""
    sql """INSERT INTO ${db_name}.${partitioned_table} VALUES (4, 'item4', 400, 2024, 2)"""
    sql """INSERT INTO ${db_name}.${partitioned_table} VALUES (5, 'item5', 500, 2024, 3)"""
    sql """INSERT INTO ${db_name}.${partitioned_table} VALUES (6, 'item6', 600, 2024, 3)"""
    
    // Verify data before rewrite
    qt_before_partitioned_rewrite """SELECT * FROM ${partitioned_table} ORDER BY id"""
    
    // Check manifest count before rewrite
    List<List<Object>> partitionedManifestsBefore = sql """
        SELECT COUNT(*) as manifest_count FROM ${partitioned_table}\$manifests
    """
    logger.info("Partitioned table manifest count before rewrite: ${partitionedManifestsBefore}")
    
    // Execute rewrite_manifests on partitioned table
    List<List<Object>> partitionedRewriteResult = sql """
        ALTER TABLE ${catalog_name}.${db_name}.${partitioned_table}
        EXECUTE rewrite_manifests()
    """
    logger.info("Partitioned table rewrite_manifests result: ${partitionedRewriteResult}")
    
    // Verify result structure
    assertTrue(partitionedRewriteResult.size() == 1, "Expected exactly 1 result row for partitioned table")
    assertTrue(partitionedRewriteResult[0].size() == 2, "Expected 2 columns in result for partitioned table")
    
    int partitionedRewrittenCount = partitionedRewriteResult[0][0] as int
    int partitionedAddedCount = partitionedRewriteResult[0][1] as int
    
    logger.info("Partitioned table - Rewritten manifests: ${partitionedRewrittenCount}, Added manifests: ${partitionedAddedCount}")
    assertTrue(partitionedRewrittenCount > 0, "Partitioned table should have rewritten at least 1 manifest")
    assertTrue(partitionedAddedCount >= 0, "Partitioned table added count should be non-negative")
    
    // Verify data integrity after rewrite
    qt_after_partitioned_rewrite """SELECT * FROM ${partitioned_table} ORDER BY id"""
    
    // Check manifest count after rewrite
    List<List<Object>> partitionedManifestsAfter = sql """
        SELECT COUNT(*) as manifest_count FROM ${partitioned_table}\$manifests
    """
    logger.info("Partitioned table manifest count after rewrite: ${partitionedManifestsAfter}")
    assertTrue(partitionedManifestsAfter[0][0] as int <= partitionedManifestsBefore[0][0] as int,
        "Partitioned table manifest count after rewrite should be <= count before")

    // =====================================================================================
    // Test Case 3: rewrite_manifests with spec_id parameter
    // Tests manifest rewriting for a specific partition spec
    // =====================================================================================
    logger.info("Starting rewrite_manifests with spec_id test case")
    
    def spec_id_table = "test_rewrite_manifests_spec_id"
    
    // Clean up if table exists
    sql """DROP TABLE IF EXISTS ${db_name}.${spec_id_table}"""
    
    // Create a partitioned table (this will have spec_id = 0)
    sql """
        CREATE TABLE ${db_name}.${spec_id_table} (
            id BIGINT,
            name STRING,
            value INT,
            year INT,
            month INT,
            day INT
        ) ENGINE=iceberg
        PARTITION BY (year, month)()
    """
    logger.info("Created spec_id test table: ${spec_id_table}")
    
    // Insert data to create manifests with spec_id 0
    sql """INSERT INTO ${db_name}.${spec_id_table} VALUES (1, 'item1', 100, 2024, 1, 15)"""
    sql """INSERT INTO ${db_name}.${spec_id_table} VALUES (2, 'item2', 200, 2024, 1, 16)"""
    sql """INSERT INTO ${db_name}.${spec_id_table} VALUES (3, 'item3', 300, 2024, 2, 17)"""
    
    // Check initial spec_id and manifest count
    List<List<Object>> initialSpecs = sql """
        SELECT partition_spec_id, COUNT(*) as manifest_count 
        FROM ${spec_id_table}\$manifests 
        GROUP BY partition_spec_id 
        ORDER BY partition_spec_id
    """
    logger.info("Initial spec IDs and manifest counts: ${initialSpecs}")
    
    // Add day as a new partition field to create spec_id = 1
    sql """ALTER TABLE ${catalog_name}.${db_name}.${spec_id_table} ADD PARTITION KEY day"""
    
    // Insert more data to create manifests with spec_id 1
    sql """INSERT INTO ${db_name}.${spec_id_table} VALUES (4, 'item4', 400, 2024, 3, 18)"""
    sql """INSERT INTO ${db_name}.${spec_id_table} VALUES (5, 'item5', 500, 2024, 3, 19)"""
    
    // Check spec_ids after adding new partition field
    List<List<Object>> allSpecs = sql """
        SELECT partition_spec_id, COUNT(*) as manifest_count 
        FROM ${spec_id_table}\$manifests 
        GROUP BY partition_spec_id 
        ORDER BY partition_spec_id
    """
    logger.info("All spec IDs and manifest counts: ${allSpecs}")
    
    if (allSpecs.size() > 0) {
        int targetSpec = allSpecs[0][0] as int
        int targetCount = allSpecs[0][1] as int
        List<List<Object>> specResult = sql """
            ALTER TABLE ${catalog_name}.${db_name}.${spec_id_table}
            EXECUTE rewrite_manifests('spec_id' = '${targetSpec}')
        """
        int specRewritten = specResult[0][0] as int
        assertTrue(specRewritten == targetCount,
            "Should rewrite exactly ${targetCount} manifests for spec_id=${targetSpec}, got ${specRewritten}")
        qt_after_spec_id_rewrite """SELECT * FROM ${spec_id_table} ORDER BY id"""
        logger.info("spec_id filtering test completed successfully")
    } else {
        logger.warn("Could not create spec_id, skipping spec_id filtering test")
    }

    // =====================================================================================
    // Test Case 4: rewrite_manifests on empty table (no current snapshot)
    // Tests that rewrite_manifests handles tables with no current snapshot gracefully
    // =====================================================================================
    logger.info("Starting rewrite_manifests on empty table test case")
    
    def empty_table = "test_empty_table"
    sql """DROP TABLE IF EXISTS ${db_name}.${empty_table}"""
    sql """
        CREATE TABLE ${db_name}.${empty_table} (
            id BIGINT,
            name STRING
        ) ENGINE=iceberg
    """
    logger.info("Created empty test table: ${empty_table}")
    
    // Execute rewrite_manifests on empty table (no current snapshot)
    List<List<Object>> emptyTableResult = sql """
        ALTER TABLE ${catalog_name}.${db_name}.${empty_table}
        EXECUTE rewrite_manifests()
    """
    logger.info("Empty table rewrite_manifests result: ${emptyTableResult}")
    
    // Verify result structure
    assertTrue(emptyTableResult.size() == 1, "Expected exactly 1 result row for empty table")
    assertTrue(emptyTableResult[0].size() == 2, "Expected 2 columns in result for empty table")
    
    // Should return 0 rewritten manifests and 0 added manifests for empty table
    int emptyRewrittenCount = emptyTableResult[0][0] as int
    int emptyAddedCount = emptyTableResult[0][1] as int
    
    assertTrue(emptyRewrittenCount == 0, "Empty table should have 0 rewritten manifests, got: ${emptyRewrittenCount}")
    assertTrue(emptyAddedCount == 0, "Empty table should have 0 added manifests, got: ${emptyAddedCount}")
    
    logger.info("Empty table test completed: rewritten=${emptyRewrittenCount}, added=${emptyAddedCount}")

    // =====================================================================================
    // Negative Test Cases: Parameter validation and error handling
    // =====================================================================================
    logger.info("Starting negative test cases for rewrite_manifests")

    // Test with invalid spec_id format

    test {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${table_name}
            EXECUTE rewrite_manifests('spec_id' = 'not-a-number')
        """
        exception "Invalid"
    }

    // Test with non-existent spec_id (on spec_id table)

    // Test with non-existent spec_id (very large number unlikely to exist) on spec_id table
    List<List<Object>> nonExistentSpecOnSpecTable = sql """
        ALTER TABLE ${catalog_name}.${db_name}.${spec_id_table}
        EXECUTE rewrite_manifests('spec_id' = '99999')
    """
    assertTrue(nonExistentSpecOnSpecTable[0][0] as int == 0, "Non-existent spec_id on spec_id_table should return 0 rewritten")
    assertTrue(nonExistentSpecOnSpecTable[0][1] as int == 0, "Non-existent spec_id on spec_id_table should return 0 added")

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

    logger.info("All rewrite_manifests test cases completed successfully")
}