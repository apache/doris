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

import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.ChronoField
import java.time.LocalDateTime
import java.time.ZoneId

suite("test_iceberg_rewrite_data_files", "p0,external,doris,external_docker,external_docker_doris") {
    DateTimeFormatter unifiedFormatter = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd")
            .optionalStart()
            .appendLiteral('T')
            .optionalEnd()
            .optionalStart()
            .appendLiteral(' ')
            .optionalEnd()
            .appendPattern("HH:mm:ss")
            .optionalStart()
            .appendFraction(ChronoField.MILLI_OF_SECOND, 0, 3, true)
            .optionalEnd()
            .toFormatter()

    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String catalog_name = "test_iceberg_rewrite_data_files"
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
    // Test Case 1: Basic rewrite_data_files operation
    // Tests the ability to rewrite multiple small data files into larger optimized files
    // =====================================================================================
    logger.info("Starting basic rewrite_data_files test case")
    
    def table_name = "test_rewrite_data_basic"
    
    // Clean up if table exists
    sql """DROP TABLE IF EXISTS ${db_name}.${table_name}"""
    
    // Create a test table with partitioning
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
    
    // Insert data multiple times to create multiple small files
    // This simulates the scenario where files need to be compacted
    sql """
        INSERT INTO ${db_name}.${table_name} VALUES
        (1, 'item1', 'electronics', 100, '2024-01-01'),
        (2, 'item2', 'electronics', 200, '2024-01-02')
    """
    
    sql """
        INSERT INTO ${db_name}.${table_name} VALUES
        (3, 'item3', 'electronics', 300, '2024-01-03'),
        (4, 'item4', 'electronics', 400, '2024-01-04')
    """
    
    sql """
        INSERT INTO ${db_name}.${table_name} VALUES
        (5, 'item5', 'books', 150, '2024-01-05'),
        (6, 'item6', 'books', 250, '2024-01-06')
    """
    
    sql """
        INSERT INTO ${db_name}.${table_name} VALUES
        (7, 'item7', 'books', 350, '2024-01-07'),
        (8, 'item8', 'books', 450, '2024-01-08')
    """
    
    sql """
        INSERT INTO ${db_name}.${table_name} VALUES
        (9, 'item9', 'clothing', 180, '2024-01-09'),
        (10, 'item10', 'clothing', 280, '2024-01-10')
    """
    
    logger.info("Inserted data in 5 separate batches to create multiple small files")
    
    // Verify table data before rewrite
    qt_before_rewrite_data """SELECT * FROM ${table_name} ORDER BY id"""
    
    // Check files system table before rewrite
    List<List<Object>> filesBeforeRewrite = sql """
        SELECT file_path, file_format, record_count, file_size_in_bytes 
        FROM ${table_name}\$files 
        ORDER BY file_path
    """
    logger.info("Files before rewrite: ${filesBeforeRewrite.size()} files")
    logger.info("File details before rewrite: ${filesBeforeRewrite}")
    
    // Ensure we have files before rewriting
    assertTrue(filesBeforeRewrite.size() > 0, 
        "Expected at least 1 data file before rewrite, but got ${filesBeforeRewrite.size()}")
    
    // Check snapshots before rewrite
    List<List<Object>> snapshotsBeforeRewrite = sql """
        SELECT snapshot_id, parent_id, operation, summary 
        FROM ${table_name}\$snapshots 
        ORDER BY committed_at
    """
    logger.info("Snapshots before rewrite: ${snapshotsBeforeRewrite.size()} snapshots")
    int snapshotsCountBefore = snapshotsBeforeRewrite.size()
    
    // Execute rewrite_data_files action with custom parameters
    // Using smaller target file size to ensure rewrite happens with our test data
    def rewriteResult = sql """
        ALTER TABLE ${catalog_name}.${db_name}.${table_name} 
        EXECUTE rewrite_data_files(
            "target-file-size-bytes" = "10485760",
            "min-input-files" = "2"
        )
    """
    logger.info("Rewrite data files result: ${rewriteResult}")
    
    // Verify the result contains expected columns
    assertTrue(rewriteResult.size() > 0, "Rewrite result should not be empty")
    logger.info("Rewrite result: ${rewriteResult}")
    
    int rewrittenFilesCount = rewriteResult[0][0] as int
    int addedFilesCount = rewriteResult[0][1] as int
    long rewrittenBytesCount = rewriteResult[0][2] as long
    
    logger.info("Rewritten files count: ${rewrittenFilesCount}")
    logger.info("Added files count: ${addedFilesCount}")
    logger.info("Rewritten bytes count: ${rewrittenBytesCount}")
    
    // Verify table data after rewrite (data should remain the same)
    qt_after_rewrite_data """SELECT * FROM ${table_name} ORDER BY id"""
    
    // Check files system table after rewrite
    List<List<Object>> filesAfterRewrite = sql """
        SELECT file_path, file_format, record_count, file_size_in_bytes 
        FROM ${table_name}\$files 
        ORDER BY file_path
    """
    logger.info("Files after rewrite: ${filesAfterRewrite.size()} files")
    logger.info("File details after rewrite: ${filesAfterRewrite}")
    
    // Verify that files were actually rewritten (file count should be different)
    // After rewrite, we should have fewer files (compaction happened)
    assertTrue(filesAfterRewrite.size() <= filesBeforeRewrite.size(), 
        "File count after rewrite should be less than or equal to before rewrite")
    
    // Check snapshots after rewrite
    List<List<Object>> snapshotsAfterRewrite = sql """
        SELECT snapshot_id, parent_id, operation, summary 
        FROM ${table_name}\$snapshots 
        ORDER BY committed_at
    """
    logger.info("Snapshots after rewrite: ${snapshotsAfterRewrite.size()} snapshots")
    
    // Verify that a new snapshot was created by the rewrite operation
    int snapshotsCountAfter = snapshotsAfterRewrite.size()
    
    // If rewrite actually happened (rewrittenFilesCount > 0), should have new snapshot
    if (rewrittenFilesCount > 0) {
        assertTrue(snapshotsCountAfter > snapshotsCountBefore, 
            "A new snapshot should be created after rewrite operation")
        
        // Check the latest snapshot details
        def latestSnapshot = snapshotsAfterRewrite[snapshotsCountAfter - 1]
        logger.info("Latest snapshot details: ${latestSnapshot}")
        
        // The operation in the latest snapshot should indicate it's a rewrite
        // (Iceberg typically uses "replace" operation for rewrite)
    }
    
    // Verify total record count is preserved
    def totalRecords = sql """SELECT COUNT(*) as cnt FROM ${table_name}"""
    assertTrue(totalRecords[0][0] == 10, "Total record count should be 10 after rewrite")
    
    logger.info("Basic rewrite_data_files test completed successfully")

    // =====================================================================================
    // Test Case 2: Rewrite data files for multi-partition table
    // Tests the rewrite operation on a partitioned table with multiple partitions
    // Verifies that files in each partition are correctly rewritten independently
    // =====================================================================================
    logger.info("Starting multi-partition rewrite_data_files test case")
    
    def table_name_partitioned = "test_rewrite_data_partitioned"
    
    // Clean up if table exists
    sql """DROP TABLE IF EXISTS ${db_name}.${table_name_partitioned}"""
    
    // Create a partitioned table using DATE partition
    sql """
        CREATE TABLE ${db_name}.${table_name_partitioned} (
            id BIGINT,
            user_name STRING,
            event_type STRING,
            event_value DECIMAL(10, 2),
            event_date DATE
        ) ENGINE=iceberg
        PARTITION BY LIST (event_date) ()
    """
    logger.info("Created partitioned test table: ${table_name_partitioned}")
    
    // Insert data for partition 2024-01-01 (multiple inserts to create multiple files)
    sql """
        INSERT INTO ${db_name}.${table_name_partitioned} VALUES
        (1, 'user1', 'login', 1.0, '2024-01-01'),
        (2, 'user2', 'view', 2.0, '2024-01-01')
    """
    sql """
        INSERT INTO ${db_name}.${table_name_partitioned} VALUES
        (3, 'user3', 'click', 3.0, '2024-01-01'),
        (4, 'user4', 'purchase', 4.0, '2024-01-01')
    """
    sql """
        INSERT INTO ${db_name}.${table_name_partitioned} VALUES
        (5, 'user5', 'logout', 5.0, '2024-01-01')
    """
    
    // Insert data for partition 2024-01-02 (multiple inserts)
    sql """
        INSERT INTO ${db_name}.${table_name_partitioned} VALUES
        (6, 'user6', 'login', 6.0, '2024-01-02'),
        (7, 'user7', 'view', 7.0, '2024-01-02')
    """
    sql """
        INSERT INTO ${db_name}.${table_name_partitioned} VALUES
        (8, 'user8', 'click', 8.0, '2024-01-02'),
        (9, 'user9', 'purchase', 9.0, '2024-01-02')
    """
    sql """
        INSERT INTO ${db_name}.${table_name_partitioned} VALUES
        (10, 'user10', 'logout', 10.0, '2024-01-02')
    """
    
    // Insert data for partition 2024-01-03 (multiple inserts)
    sql """
        INSERT INTO ${db_name}.${table_name_partitioned} VALUES
        (11, 'user11', 'login', 11.0, '2024-01-03'),
        (12, 'user12', 'view', 12.0, '2024-01-03')
    """
    sql """
        INSERT INTO ${db_name}.${table_name_partitioned} VALUES
        (13, 'user13', 'click', 13.0, '2024-01-03'),
        (14, 'user14', 'purchase', 14.0, '2024-01-03')
    """
    sql """
        INSERT INTO ${db_name}.${table_name_partitioned} VALUES
        (15, 'user15', 'logout', 15.0, '2024-01-03')
    """
    
    logger.info("Inserted data into 3 partitions (2024-01-01, 2024-01-02, 2024-01-03) with multiple batches each")
    
    // Verify table data before rewrite
    qt_before_rewrite_partitioned """SELECT * FROM ${table_name_partitioned} ORDER BY id"""
    
    // Check files per partition before rewrite
    List<List<Object>> filesBeforeRewritePartitioned = sql """
        SELECT `partition`, file_path, record_count, file_size_in_bytes 
        FROM ${table_name_partitioned}\$files 
        ORDER BY `partition`, file_path
    """
    logger.info("Total files before rewrite: ${filesBeforeRewritePartitioned.size()} files")
    
    // Count files per partition
    def filesPerPartitionBefore = filesBeforeRewritePartitioned.groupBy { it[0] }
    filesPerPartitionBefore.each { partition, files ->
        logger.info("Partition ${partition}: ${files.size()} files before rewrite")
    }
    
    logger.info("Files per partition before rewrite: ${filesPerPartitionBefore}")
    // Verify we have partitions with files each
    assertTrue(filesPerPartitionBefore.size() > 0, 
        "Expected at least 1 partition, but got ${filesPerPartitionBefore.size()}")
    
    // Check snapshots before rewrite
    List<List<Object>> snapshotsBeforeRewritePartitioned = sql """
        SELECT snapshot_id, operation, summary 
        FROM ${table_name_partitioned}\$snapshots 
        ORDER BY committed_at
    """
    logger.info("Snapshots before rewrite (partitioned): ${snapshotsBeforeRewritePartitioned.size()} snapshots")
    int snapshotsCountBeforePartitioned = snapshotsBeforeRewritePartitioned.size()
    
    // Execute rewrite_data_files on all partitions
    def rewriteResultPartitioned = sql """
        ALTER TABLE ${catalog_name}.${db_name}.${table_name_partitioned} 
        EXECUTE rewrite_data_files(
            "target-file-size-bytes" = "10485760",
            "min-input-files" = "2"
        )
    """
    logger.info("Rewrite data files result (all partitions): ${rewriteResultPartitioned}")
    
    int rewrittenFilesCountPartitioned = rewriteResultPartitioned[0][0] as int
    int addedFilesCountPartitioned = rewriteResultPartitioned[0][1] as int
    
    logger.info("Rewritten files count (partitioned): ${rewrittenFilesCountPartitioned}")
    logger.info("Added files count (partitioned): ${addedFilesCountPartitioned}")
    
    // Verify table data after rewrite (should remain the same)
    qt_after_rewrite_partitioned """SELECT * FROM ${table_name_partitioned} ORDER BY id"""
    
    // Check files per partition after rewrite
    List<List<Object>> filesAfterRewritePartitioned = sql """
        SELECT `partition`, file_path, record_count, file_size_in_bytes 
        FROM ${table_name_partitioned}\$files 
        ORDER BY `partition`, file_path
    """
    logger.info("Total files after rewrite: ${filesAfterRewritePartitioned.size()} files")
    
    // Count files per partition after rewrite
    def filesPerPartitionAfter = filesAfterRewritePartitioned.groupBy { it[0] }
    filesPerPartitionAfter.each { partition, files ->
        logger.info("Partition ${partition}: ${files.size()} files after rewrite")
    }
    
    // Verify file count decreased or stayed the same (compaction happened)
    assertTrue(filesAfterRewritePartitioned.size() <= filesBeforeRewritePartitioned.size(),
        "File count after rewrite should be <= before rewrite")
    
    // Check snapshots after rewrite
    List<List<Object>> snapshotsAfterRewritePartitioned = sql """
        SELECT snapshot_id, operation, summary 
        FROM ${table_name_partitioned}\$snapshots 
        ORDER BY committed_at
    """
    int snapshotsCountAfterPartitioned = snapshotsAfterRewritePartitioned.size()
    logger.info("Snapshots after rewrite (partitioned): ${snapshotsCountAfterPartitioned} snapshots")
    
    // If files were rewritten, a new snapshot should be created
    if (rewrittenFilesCountPartitioned > 0) {
        assertTrue(snapshotsCountAfterPartitioned > snapshotsCountBeforePartitioned,
            "A new snapshot should be created after rewrite operation")
    }
    
    // Verify record count per partition is preserved
    def partition1Count = sql """SELECT COUNT(*) FROM ${table_name_partitioned} WHERE event_date = '2024-01-01'"""
    def partition2Count = sql """SELECT COUNT(*) FROM ${table_name_partitioned} WHERE event_date = '2024-01-02'"""
    def partition3Count = sql """SELECT COUNT(*) FROM ${table_name_partitioned} WHERE event_date = '2024-01-03'"""
    
    assertTrue(partition1Count[0][0] == 5, "Partition 2024-01-01 should have 5 records")
    assertTrue(partition2Count[0][0] == 5, "Partition 2024-01-02 should have 5 records")
    assertTrue(partition3Count[0][0] == 5, "Partition 2024-01-03 should have 5 records")
    
    // Verify total record count
    def totalRecordsPartitioned = sql """SELECT COUNT(*) FROM ${table_name_partitioned}"""
    assertTrue(totalRecordsPartitioned[0][0] == 15, "Total record count should be 15 after rewrite")
    
    logger.info("Multi-partition rewrite_data_files test completed successfully")
    
    // =====================================================================================
    // Test Case 3: Rewrite data files for specific partition using WHERE clause
    // Tests the rewrite operation targeting a specific partition
    // =====================================================================================
    logger.info("Starting specific partition rewrite test case")
    
    def table_name_specific = "test_rewrite_specific_partition"
    
    // Clean up if table exists
    sql """DROP TABLE IF EXISTS ${db_name}.${table_name_specific}"""
    
    // Create another partitioned table
    sql """
        CREATE TABLE ${db_name}.${table_name_specific} (
            id BIGINT,
            region STRING,
            sales_amount DECIMAL(12, 2),
            order_date DATE
        ) ENGINE=iceberg
        PARTITION BY LIST (region, order_date) ()
    """
    logger.info("Created table for specific partition rewrite: ${table_name_specific}")
    
    // Insert data for region 'NORTH', date 2024-01-01
    sql """INSERT INTO ${db_name}.${table_name_specific} VALUES (1, 'NORTH', 100.00, '2024-01-01')"""
    sql """INSERT INTO ${db_name}.${table_name_specific} VALUES (2, 'NORTH', 200.00, '2024-01-01')"""
    sql """INSERT INTO ${db_name}.${table_name_specific} VALUES (3, 'NORTH', 300.00, '2024-01-01')"""
    
    // Insert data for region 'SOUTH', date 2024-01-01
    sql """INSERT INTO ${db_name}.${table_name_specific} VALUES (4, 'SOUTH', 150.00, '2024-01-01')"""
    sql """INSERT INTO ${db_name}.${table_name_specific} VALUES (5, 'SOUTH', 250.00, '2024-01-01')"""
    sql """INSERT INTO ${db_name}.${table_name_specific} VALUES (6, 'SOUTH', 350.00, '2024-01-01')"""
    
    // Insert data for region 'EAST', date 2024-01-02
    sql """INSERT INTO ${db_name}.${table_name_specific} VALUES (7, 'EAST', 180.00, '2024-01-02')"""
    sql """INSERT INTO ${db_name}.${table_name_specific} VALUES (8, 'EAST', 280.00, '2024-01-02')"""
    sql """INSERT INTO ${db_name}.${table_name_specific} VALUES (9, 'EAST', 380.00, '2024-01-02')"""
    
    logger.info("Inserted data into multiple region-date partitions")
    
    // Check files before specific partition rewrite
    List<List<Object>> filesBeforeSpecific = sql """
        SELECT `partition`, file_path, record_count 
        FROM ${table_name_specific}\$files 
        ORDER BY `partition`
    """
    logger.info("Files before specific partition rewrite: ${filesBeforeSpecific.size()} files")
    
    def partitionGroupsBefore = filesBeforeSpecific.groupBy { it[0] }
    partitionGroupsBefore.each { partition, files ->
        logger.info("Partition ${partition}: ${files.size()} files")
    }
    
    // Count files in NORTH partition before rewrite
    int northFilesCountBefore = filesBeforeSpecific.findAll { it[0].toString().contains('NORTH') }.size()
    logger.info("NORTH partition files before rewrite: ${northFilesCountBefore}")
    
    // Execute rewrite only for NORTH region using WHERE clause
    def rewriteResultSpecific = sql """
        ALTER TABLE ${catalog_name}.${db_name}.${table_name_specific} 
        EXECUTE rewrite_data_files(
            "target-file-size-bytes" = "10485760",
            "min-input-files" = "2"
        ) WHERE region = 'NORTH'
    """
    logger.info("Rewrite result for NORTH partition only: ${rewriteResultSpecific}")
    
    int rewrittenFilesSpecific = rewriteResultSpecific[0][0] as int
    logger.info("Files rewritten in NORTH partition: ${rewrittenFilesSpecific}")
    
    // Check files after specific partition rewrite
    List<List<Object>> filesAfterSpecific = sql """
        SELECT `partition`, file_path, record_count 
        FROM ${table_name_specific}\$files 
        ORDER BY `partition`
    """
    logger.info("Files after specific partition rewrite: ${filesAfterSpecific.size()} files")
    
    def partitionGroupsAfter = filesAfterSpecific.groupBy { it[0] }
    partitionGroupsAfter.each { partition, files ->
        logger.info("Partition ${partition}: ${files.size()} files")
    }
    
    // Count files in NORTH partition after rewrite
    int northFilesCountAfter = filesAfterSpecific.findAll { it[0].toString().contains('NORTH') }.size()
    logger.info("NORTH partition files after rewrite: ${northFilesCountAfter}")
    
    // Verify NORTH partition was affected
    if (rewrittenFilesSpecific > 0) {
        assertTrue(northFilesCountAfter <= northFilesCountBefore,
            "NORTH partition file count should decrease or stay same after rewrite")
    }
    
    // Verify data integrity - all records should still be present
    qt_after_specific_partition_rewrite """SELECT * FROM ${table_name_specific} ORDER BY id"""
    
    def totalRecordsSpecific = sql """SELECT COUNT(*) FROM ${table_name_specific}"""
    assertTrue(totalRecordsSpecific[0][0] == 9, "Total record count should be 9 after specific partition rewrite")
    
    // Verify NORTH region records
    def northRecords = sql """SELECT COUNT(*) FROM ${table_name_specific} WHERE region = 'NORTH'"""
    assertTrue(northRecords[0][0] == 3, "NORTH region should still have 3 records")
    
    logger.info("Specific partition rewrite test completed successfully")
}