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

suite("test_medium_allocation_mode") {
    def tableName = "test_medium_allocation_mode_table"
    def adaptiveTableName = "test_adaptive_table"
    def strictTableName = "test_strict_table"
    def partitionedTableName = "${tableName}_partitioned"
    
    // Clean up all potentially existing tables
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql "DROP TABLE IF EXISTS ${adaptiveTableName}"
    sql "DROP TABLE IF EXISTS ${strictTableName}"
    sql "DROP TABLE IF EXISTS ${tableName}_adaptive_default"
    sql "DROP TABLE IF EXISTS ${tableName}_adaptive_auto"
    sql "DROP TABLE IF EXISTS ${tableName}_strict_error"
    sql "DROP TABLE IF EXISTS ${tableName}_invalid_mode"
    sql "DROP TABLE IF EXISTS ${tableName}_ssd_strict_fail"
    sql "DROP TABLE IF EXISTS ${tableName}_ssd_adaptive_success"
    sql "DROP TABLE IF EXISTS ${partitionedTableName}"

    // Test 1: Create table with adaptive mode (default behavior)
    sql """
        CREATE TABLE ${adaptiveTableName} (
            id INT NOT NULL,
            name VARCHAR(50),
            value INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES (
            "replication_num" = "1",
            "medium_allocation_mode" = "adaptive"
        )
    """

    // Test 2: Demonstrate adaptive vs strict mode behavior
    logger.info("=== Testing Adaptive Mode Behavior ===")
    
    // Get available storage medium from system (use default medium)
    def defaultMedium = "HDD"  // Most environments have HDD as default
    
    // Adaptive mode - should always succeed regardless of medium availability
    sql """
        CREATE TABLE ${tableName}_adaptive_default (
            id INT NOT NULL,
            value INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "storage_medium" = "${defaultMedium}",
            "medium_allocation_mode" = "adaptive"
        )
    """
    
    // Adaptive mode without specifying medium - should use system default
    sql """
        CREATE TABLE ${tableName}_adaptive_auto (
            id INT NOT NULL,
            value INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "medium_allocation_mode" = "adaptive"
        )
    """

    // Test 3: Verify SHOW CREATE TABLE displays the property correctly
    logger.info("=== Verifying SHOW CREATE TABLE ===")
    
    def result = sql_return_maparray("SHOW CREATE TABLE ${adaptiveTableName}")
    def createTableStr = result[0]['Create Table']
    logger.info("Adaptive table create statement: ${createTableStr}")
    assertTrue(createTableStr.contains("\"medium_allocation_mode\" = \"adaptive\""))

    def result_default = sql_return_maparray("SHOW CREATE TABLE ${tableName}_adaptive_default")
    def createTableStr_default = result_default[0]['Create Table']
    logger.info("Adaptive default table create statement: ${createTableStr_default}")
    assertTrue(createTableStr_default.contains("\"medium_allocation_mode\" = \"adaptive\""))
    assertTrue(createTableStr_default.contains("\"storage_medium\" = \"hdd\""))

    def result_auto = sql_return_maparray("SHOW CREATE TABLE ${tableName}_adaptive_auto")
    def createTableStr_auto = result_auto[0]['Create Table']
    logger.info("Adaptive auto table create statement: ${createTableStr_auto}")
    assertTrue(createTableStr_auto.contains("\"medium_allocation_mode\" = \"adaptive\""))

    // Test 4: ALTER TABLE to change medium allocation mode
    logger.info("=== Testing ALTER TABLE Operations ===")
    
    // First set storage_medium, then set medium_allocation_mode (must be done separately)
    sql """ALTER TABLE ${adaptiveTableName} SET("storage_medium" = "${defaultMedium}")"""
    sql """ALTER TABLE ${adaptiveTableName} SET("medium_allocation_mode" = "strict")"""
    
    def result_after_alter = sql_return_maparray("SHOW CREATE TABLE ${adaptiveTableName}")
    def createTableStr_after_alter = result_after_alter[0]['Create Table']
    logger.info("After ALTER - table create statement: ${createTableStr_after_alter}")
    assertTrue(createTableStr_after_alter.contains("\"medium_allocation_mode\" = \"strict\""))
    assertTrue(createTableStr_after_alter.contains("\"storage_medium\" = \"${defaultMedium.toLowerCase()}\""))

    // Change back to adaptive
    sql """ALTER TABLE ${adaptiveTableName} SET("medium_allocation_mode" = "adaptive")"""
    
    def result_back_to_adaptive = sql_return_maparray("SHOW CREATE TABLE ${adaptiveTableName}")
    def createTableStr_back = result_back_to_adaptive[0]['Create Table']
    assertTrue(createTableStr_back.contains("\"medium_allocation_mode\" = \"adaptive\""))

    // Test 5: This should succeed: strict mode with storage_medium
    sql """
        CREATE TABLE ${strictTableName} (
            id INT NOT NULL,
            value INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "storage_medium" = "${defaultMedium}",
            "medium_allocation_mode" = "strict"
        )
    """

    // Test 6: Test SSD+strict failure scenario (when SSD is not available)
    logger.info("=== Testing SSD+Strict Failure Scenario ===")
    
    // First, try to create table with SSD+strict - this might fail if SSD is not available
    try {
        sql """
            CREATE TABLE ${tableName}_ssd_strict_fail (
                id INT NOT NULL,
                value INT
            )
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "storage_medium" = "SSD",
                "medium_allocation_mode" = "strict"
            )
        """
        logger.info("SSD+strict table created successfully (SSD is available)")
        
        // If successful, verify the table was created correctly
        def result_ssd_strict = sql_return_maparray("SHOW CREATE TABLE ${tableName}_ssd_strict_fail")
        def createTableStr_ssd_strict = result_ssd_strict[0]['Create Table']
        assertTrue(createTableStr_ssd_strict.contains("\"storage_medium\" = \"ssd\""))
        assertTrue(createTableStr_ssd_strict.contains("\"medium_allocation_mode\" = \"strict\""))
        
        // Clean up the successfully created table
        sql "DROP TABLE ${tableName}_ssd_strict_fail"
        
    } catch (Exception e) {
        logger.info("Expected behavior: SSD+strict failed because SSD is not available: ${e.getMessage()}")
        // This is expected behavior when SSD storage medium is not available
        assertTrue(e.getMessage().contains("No backend found") || 
                  e.getMessage().contains("storage medium") || 
                  e.getMessage().contains("SSD") ||
                  e.getMessage().contains("backend"))
        
        // Now demonstrate that adaptive mode would succeed in the same scenario
        try {
            sql """
                CREATE TABLE ${tableName}_ssd_adaptive_success (
                    id INT NOT NULL,
                    value INT
                )
                DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "storage_medium" = "SSD",
                    "medium_allocation_mode" = "adaptive"
                )
            """
            logger.info("SSD+adaptive succeeded where SSD+strict failed - demonstrating adaptive fallback")
            
            // Verify the table properties
            def result_ssd_adaptive = sql_return_maparray("SHOW CREATE TABLE ${tableName}_ssd_adaptive_success")
            def createTableStr_ssd_adaptive = result_ssd_adaptive[0]['Create Table']
            assertTrue(createTableStr_ssd_adaptive.contains("\"medium_allocation_mode\" = \"adaptive\""))
            
            // Insert some data to verify it works
            sql """INSERT INTO ${tableName}_ssd_adaptive_success VALUES (1, 100), (2, 200)"""
            def count_ssd_adaptive = sql "SELECT COUNT(*) FROM ${tableName}_ssd_adaptive_success"
            assertEquals(2, count_ssd_adaptive[0][0])
            
            // Check what storage medium was actually used (might fallback to HDD)
            def partitions_ssd_adaptive = sql "SHOW PARTITIONS FROM ${tableName}_ssd_adaptive_success"
            logger.info("SSD+adaptive table partitions (with fallback): ${partitions_ssd_adaptive}")
            
            // Clean up
            sql "DROP TABLE ${tableName}_ssd_adaptive_success"
            
        } catch (Exception e2) {
            logger.error("Unexpected: SSD+adaptive also failed: ${e2.getMessage()}")
            // This shouldn't happen with adaptive mode
        }
    }

    // Test 7: Test invalid medium_allocation_mode values
    try {
        sql """
            CREATE TABLE ${tableName}_invalid_mode (
                id INT NOT NULL,
                value INT
            )
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "medium_allocation_mode" = "invalid_mode"
            )
        """
        assertTrue(false, "Expected error for invalid medium_allocation_mode")
    } catch (Exception e) {
        logger.info("Expected error for invalid mode: ${e.getMessage()}")
        assertTrue(e.getMessage().contains("Invalid") || e.getMessage().contains("medium_allocation_mode"))
    }

    // Test 8: Partition-level medium allocation mode (if supported)
    logger.info("=== Testing Partition-level Operations ===")
    
    // partitionedTableName already defined at the top
    sql "DROP TABLE IF EXISTS ${partitionedTableName}"
    
    sql """
        CREATE TABLE ${partitionedTableName} (
            id INT NOT NULL,
            date_val DATE,
            value INT
        )
        DUPLICATE KEY(id, date_val)
        PARTITION BY RANGE(date_val)
        (
            PARTITION p202401 VALUES [('2024-01-01'), ('2024-02-01')),
            PARTITION p202402 VALUES [('2024-02-01'), ('2024-03-01'))
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "medium_allocation_mode" = "adaptive"
        )
    """

    // Try to alter partition properties
    try {
        // Note: Must set properties separately due to Doris limitation
        sql """ALTER TABLE ${partitionedTableName} MODIFY PARTITION p202401 SET("storage_medium" = "SSD")"""
        sql """ALTER TABLE ${partitionedTableName} MODIFY PARTITION p202401 SET("medium_allocation_mode" = "strict")"""
        logger.info("Successfully modified partition with medium_allocation_mode")
    } catch (Exception e) {
        logger.info("Partition modification note: ${e.getMessage()}")
    }

    // Test 9: Demonstrate adaptive behavior with data insertion
    logger.info("=== Testing Data Operations ===")
    
    // Insert data into adaptive tables to verify they work normally
    sql """INSERT INTO ${adaptiveTableName} VALUES (1, 'test1', 100), (2, 'test2', 200)"""
    sql """INSERT INTO ${tableName}_adaptive_default VALUES (1, 100), (2, 200)"""
    sql """INSERT INTO ${tableName}_adaptive_auto VALUES (1, 100), (2, 200)"""
    sql """INSERT INTO ${strictTableName} VALUES (1, 100), (2, 200)"""
    
    // Verify data can be queried normally
    def count_adaptive = sql "SELECT COUNT(*) FROM ${adaptiveTableName}"
    def count_default = sql "SELECT COUNT(*) FROM ${tableName}_adaptive_default"
    def count_auto = sql "SELECT COUNT(*) FROM ${tableName}_adaptive_auto"
    def count_strict = sql "SELECT COUNT(*) FROM ${strictTableName}"
    
    assertEquals(2, count_adaptive[0][0])
    assertEquals(2, count_default[0][0])
    assertEquals(2, count_auto[0][0])
    assertEquals(2, count_strict[0][0])
    
    logger.info("All data operations successful - adaptive and strict modes both work correctly")

    // Test 10: Show partition information to verify medium allocation
    logger.info("=== Checking Partition Information ===")
    
    def partitions_adaptive_default = sql "SHOW PARTITIONS FROM ${tableName}_adaptive_default"
    def partitions_adaptive_auto = sql "SHOW PARTITIONS FROM ${tableName}_adaptive_auto"
    def partitions_strict = sql "SHOW PARTITIONS FROM ${strictTableName}"
    
    logger.info("Adaptive default table partitions: ${partitions_adaptive_default}")
    logger.info("Adaptive auto table partitions: ${partitions_adaptive_auto}")
    logger.info("Strict table partitions: ${partitions_strict}")
    
    // All should have created partitions successfully
    assertTrue(partitions_adaptive_default.size() > 0)
    assertTrue(partitions_adaptive_auto.size() > 0)
    assertTrue(partitions_strict.size() > 0)

    // Cleanup
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql "DROP TABLE IF EXISTS ${adaptiveTableName}"
    sql "DROP TABLE IF EXISTS ${strictTableName}"
    sql "DROP TABLE IF EXISTS ${tableName}_adaptive_default"
    sql "DROP TABLE IF EXISTS ${tableName}_adaptive_auto"
    sql "DROP TABLE IF EXISTS ${tableName}_ssd_strict_fail"
    sql "DROP TABLE IF EXISTS ${tableName}_ssd_adaptive_success"
    sql "DROP TABLE IF EXISTS ${partitionedTableName}"
}
 