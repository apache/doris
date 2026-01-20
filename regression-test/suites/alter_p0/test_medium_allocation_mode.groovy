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
    def tableName = "test_alter_partition_medium_mode"
    def tableTableName = "test_alter_table_medium_mode"
    
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql "DROP TABLE IF EXISTS ${tableTableName}"
    
    // Create partitioned table with adaptive mode (default)
    sql """
        CREATE TABLE ${tableName} (
            id INT NOT NULL,
            date_val DATE,
            value INT
        )
        DUPLICATE KEY(id, date_val)
        PARTITION BY RANGE(date_val)
        (
            PARTITION p1 VALUES [('2024-01-01'), ('2024-02-01')),
            PARTITION p2 VALUES [('2024-02-01'), ('2024-03-01'))
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "medium_allocation_mode" = "adaptive"
        )
    """
    
    // Insert test data
    sql """INSERT INTO ${tableName} VALUES 
           (1, '2024-01-15', 100), 
           (2, '2024-02-15', 200)"""
    
    // Verify initial state
    def partitions_before = sql "SHOW PARTITIONS FROM ${tableName}"
    def p1_before = partitions_before.find { it[1] == "p1" }
    def p1_mode_before = p1_before[22]  // MediumAllocationMode column
    logger.info("Initial p1 medium_allocation_mode: ${p1_mode_before}")
    assertTrue(p1_mode_before != null && p1_mode_before.toString().toUpperCase() == "ADAPTIVE",
               "Partition p1 should initially have medium_allocation_mode=ADAPTIVE")
    
    // Test 1: ALTER PARTITION to change medium_allocation_mode from adaptive to strict
    sql """ALTER TABLE ${tableName} MODIFY PARTITION p1 SET("medium_allocation_mode" = "strict")"""
    
    // Verify p1 changed to strict
    def partitions_after = sql "SHOW PARTITIONS FROM ${tableName}"
    def p1_after = partitions_after.find { it[1] == "p1" }
    def p1_mode_after = p1_after[22]
    logger.info("After ALTER, p1 medium_allocation_mode: ${p1_mode_after}")
    assertTrue(p1_mode_after != null && p1_mode_after.toString().toUpperCase() == "STRICT",
               "Partition p1 should have medium_allocation_mode=STRICT after ALTER, but got: ${p1_mode_after}")
    
    // Verify p2 remains adaptive (unchanged)
    def p2_after = partitions_after.find { it[1] == "p2" }
    def p2_mode_after = p2_after[22]
    logger.info("After ALTER, p2 medium_allocation_mode: ${p2_mode_after}")
    assertTrue(p2_mode_after != null && p2_mode_after.toString().toUpperCase() == "ADAPTIVE",
               "Partition p2 should still have medium_allocation_mode=ADAPTIVE, but got: ${p2_mode_after}")
    
    // Test 2: ALTER PARTITION to change medium_allocation_mode from strict back to adaptive
    sql """ALTER TABLE ${tableName} MODIFY PARTITION p1 SET("medium_allocation_mode" = "adaptive")"""
    
    // Verify p1 changed back to adaptive
    def partitions_final = sql "SHOW PARTITIONS FROM ${tableName}"
    def p1_final = partitions_final.find { it[1] == "p1" }
    def p1_mode_final = p1_final[22]
    logger.info("After second ALTER, p1 medium_allocation_mode: ${p1_mode_final}")
    assertTrue(p1_mode_final != null && p1_mode_final.toString().toUpperCase() == "ADAPTIVE",
               "Partition p1 should have medium_allocation_mode=ADAPTIVE after second ALTER, but got: ${p1_mode_final}")
    
    // Test 3: Verify invalid medium_allocation_mode value is rejected
    test {
        sql """ALTER TABLE ${tableName} MODIFY PARTITION p1 SET("medium_allocation_mode" = "invalid_mode")"""
        exception "Invalid"
    }
    
    // Verify data integrity after ALTER operations
    def count = sql "SELECT COUNT(*) FROM ${tableName}"
    assertEquals(2, count[0][0])
    
    def data = sql "SELECT * FROM ${tableName} ORDER BY id"
    assertEquals(2, data.size())
    assertEquals(1, data[0][0])
    assertEquals(2, data[1][0])
    
    // ============================================================
    // Test 4: ALTER TABLE (table-level medium_allocation_mode)
    // This covers ModifyTablePropertiesOp.java:163-173
    // ============================================================
    logger.info("=== Test 4: Table-level medium_allocation_mode ALTER ===")
    
    // Create table with adaptive mode
    sql """
        CREATE TABLE ${tableTableName} (
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
    
    // Verify initial table-level mode
    def showCreate1 = sql "SHOW CREATE TABLE ${tableTableName}"
    def createSql1 = showCreate1[0][1]
    logger.info("Initial CREATE TABLE: ${createSql1}")
    assertTrue(createSql1.contains("\"medium_allocation_mode\" = \"adaptive\""),
               "Table should initially have medium_allocation_mode=adaptive")
    
    // Test 4.1: ALTER TABLE to change medium_allocation_mode from adaptive to strict
    sql """ALTER TABLE ${tableTableName} SET("medium_allocation_mode" = "strict")"""
    
    // Verify table-level mode changed to strict
    def showCreate2 = sql "SHOW CREATE TABLE ${tableTableName}"
    def createSql2 = showCreate2[0][1]
    logger.info("After ALTER TABLE: ${createSql2}")
    assertTrue(createSql2.contains("\"medium_allocation_mode\" = \"strict\""),
               "Table should have medium_allocation_mode=strict after ALTER TABLE")
    
    // Test 4.2: ALTER TABLE to change medium_allocation_mode from strict back to adaptive
    sql """ALTER TABLE ${tableTableName} SET("medium_allocation_mode" = "adaptive")"""
    
    // Verify table-level mode changed back to adaptive
    def showCreate3 = sql "SHOW CREATE TABLE ${tableTableName}"
    def createSql3 = showCreate3[0][1]
    logger.info("After second ALTER TABLE: ${createSql3}")
    assertTrue(createSql3.contains("\"medium_allocation_mode\" = \"adaptive\""),
               "Table should have medium_allocation_mode=adaptive after second ALTER TABLE")
    
    // Test 4.3: Verify invalid medium_allocation_mode value is rejected at table level
    test {
        sql """ALTER TABLE ${tableTableName} SET("medium_allocation_mode" = "invalid_mode")"""
        exception "Invalid"
    }
    
    logger.info("✅ Table-level medium_allocation_mode ALTER test passed")
    
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql "DROP TABLE IF EXISTS ${tableTableName}"
}

