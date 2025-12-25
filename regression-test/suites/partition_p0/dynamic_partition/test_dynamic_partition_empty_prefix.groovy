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

suite('test_dynamic_partition_empty_prefix', 'nonConcurrent') {
    def tableName = "test_dynamic_partition_empty_prefix"
    
    sql "DROP TABLE IF EXISTS ${tableName} FORCE"
    
    // Test creating table with empty prefix
    sql """
        CREATE TABLE ${tableName} (
            `k1` datetime NULL
        )
        PARTITION BY RANGE (k1)()
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES (
            "dynamic_partition.enable" = "true",
            "dynamic_partition.end" = "3",
            "dynamic_partition.buckets" = "1",
            "dynamic_partition.start" = "-3",
            "dynamic_partition.prefix" = "",
            "dynamic_partition.time_unit" = "DAY",
            "dynamic_partition.create_history_partition" = "true",
            "dynamic_partition.replication_allocation" = "tag.location.default: 1"
        )
    """
    
    // Wait for dynamic partition scheduler to create partitions
    sleep(5000)
    
    // Check that partitions are created with no prefix (just date format)
    def partitions = sql_return_maparray "SHOW PARTITIONS FROM ${tableName}"
    assertTrue(partitions.size() > 0)
    
    // Verify partition names don't have prefix (should start with date like "20231201")
    def partitionNames = partitions.collect { it.PartitionName }
    logger.info("Partition names: " + partitionNames)
    
    // Verify partition names have no prefix when prefix is empty (just date format)
    partitionNames.each { name ->
        assertTrue(name.matches("\\d{8}"), "Partition name ${name} should be in date format (YYYYMMDD) without prefix")
    }
    
    // Check dynamic partition info shows empty prefix
    def dynamicInfo = sql_return_maparray("SHOW DYNAMIC PARTITION TABLES").find { it.TableName == tableName }
    assertNotNull(dynamicInfo)
    assertEquals("", dynamicInfo.Prefix)
    
    // Test altering table to set empty prefix
    def tableName2 = "test_dynamic_partition_alter_empty_prefix"
    sql "DROP TABLE IF EXISTS ${tableName2} FORCE"
    
    sql """
        CREATE TABLE ${tableName2} (
            `k1` datetime NULL
        )
        PARTITION BY RANGE (k1)()
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES (
            "dynamic_partition.enable" = "true",
            "dynamic_partition.end" = "3",
            "dynamic_partition.buckets" = "1", 
            "dynamic_partition.start" = "-3",
            "dynamic_partition.prefix" = "p",
            "dynamic_partition.time_unit" = "DAY",
            "dynamic_partition.create_history_partition" = "true",
            "dynamic_partition.replication_allocation" = "tag.location.default: 1"
        )
    """
    
    // Alter to empty prefix
    sql """
        ALTER TABLE ${tableName2} SET (
            "dynamic_partition.prefix" = ""
        )
    """
    
    // Check that prefix is now empty
    def dynamicInfo2 = sql_return_maparray("SHOW DYNAMIC PARTITION TABLES").find { it.TableName == tableName2 }
    assertNotNull(dynamicInfo2)
    assertEquals("", dynamicInfo2.Prefix)
    
    // Test dropping partitions with empty prefix
    def tableName3 = "test_dynamic_partition_drop_empty_prefix"
    sql "DROP TABLE IF EXISTS ${tableName3} FORCE"
    
    sql """
        CREATE TABLE ${tableName3} (
            `k1` datetime NULL
        )
        PARTITION BY RANGE (k1)(
            PARTITION `20251028` VALUES LESS THAN ("2025-10-29"),
            PARTITION `20251029` VALUES LESS THAN ("2025-10-30"),
            PARTITION `20251030` VALUES LESS THAN ("2025-10-31"),
            PARTITION `20251031` VALUES LESS THAN ("2025-11-01"),
            PARTITION `20251101` VALUES LESS THAN ("2025-11-02")
        )
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """
    
    // Get partition names before dropping
    def partitionsBefore = sql_return_maparray "SHOW PARTITIONS FROM ${tableName3}"
    assertTrue(partitionsBefore.size() > 0)
    def partitionNamesBefore = partitionsBefore.collect { it.PartitionName }
    logger.info("Partitions before drop: " + partitionNamesBefore)
    
    // Drop a specific partition with empty prefix (date format only)
    def partitionToDrop = partitionNamesBefore[0]
    sql "ALTER TABLE ${tableName3} DROP PARTITION `${partitionToDrop}`"
    
    // Verify partition was dropped
    def partitionsAfter = sql_return_maparray "SHOW PARTITIONS FROM ${tableName3}"
    def partitionNamesAfter = partitionsAfter.collect { it.PartitionName }
    logger.info("Partitions after drop: " + partitionNamesAfter)
    
    assertFalse(partitionNamesAfter.contains(partitionToDrop), "Partition ${partitionToDrop} should be dropped")
    assertEquals(partitionsBefore.size() - 1, partitionsAfter.size(), "Partition count should decrease by 1")
    
    // Test dropping multiple partitions with empty prefix
    if (partitionNamesAfter.size() >= 2) {
        def partitionsToDrop = partitionNamesAfter.take(2)
        partitionsToDrop.each { partition ->
            sql "ALTER TABLE ${tableName3} DROP PARTITION `${partition}`"
        }
        
        def partitionsAfterMultiDrop = sql_return_maparray "SHOW PARTITIONS FROM ${tableName3}"
        def partitionNamesAfterMultiDrop = partitionsAfterMultiDrop.collect { it.PartitionName }
        
        partitionsToDrop.each { partition ->
            assertFalse(partitionNamesAfterMultiDrop.contains(partition), "Partition ${partition} should be dropped")
        }
    }
    
    sql "DROP TABLE IF EXISTS ${tableName} FORCE"
    sql "DROP TABLE IF EXISTS ${tableName2} FORCE"
    sql "DROP TABLE IF EXISTS ${tableName3} FORCE"
}
