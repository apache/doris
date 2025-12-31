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

suite("test_iceberg_rewrite_data_files_where_conditions", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String catalog_name = "test_iceberg_rewrite_where_conditions"
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
    // Test: Verify WHERE conditions affect rewrite data files operation
    // 
    // Test strategy:
    // Tables are pre-created in Docker initialization (run21.sql) using Spark SQL
    // to ensure min/max metadata is properly generated. This is required for WHERE
    // condition filtering to work correctly, as Doris-written Iceberg tables may
    // not have min/max information.
    // 
    // 1. Test without WHERE condition - should rewrite files (baseline)
    // 2. Test with WHERE condition matching subset of data - should rewrite fewer files
    // 3. Test with WHERE condition matching no data - should rewrite no files
    // 
    // Result format: [files_rewritten, files_added, bytes_written, bytes_deleted]
    // =====================================================================================
    logger.info("Starting WHERE conditions test for rewrite_data_files")
    logger.info("Using pre-created tables from Docker initialization (run21.sql)")
    
    // Tables are pre-created in demo.test_db database (Spark SQL), 
    // but accessed as test_db in Doris Iceberg catalog
    // Table names for each test case (pre-created in run21.sql)
    def table_baseline = "test_rewrite_where_conditions_baseline"
    def table_with_where = "test_rewrite_where_conditions_with_where"
    def table_no_match = "test_rewrite_where_conditions_no_match"
    
    // Test 1: Rewrite without WHERE condition (baseline - should rewrite files)
    logger.info("Test 1: Rewrite without WHERE condition (baseline)")
    
    def totalRecordsBaselineBefore = sql """SELECT COUNT(*) FROM ${table_baseline}"""
    logger.info("Baseline table record count before rewrite: ${totalRecordsBaselineBefore[0][0]}")
    assertTrue(totalRecordsBaselineBefore[0][0] == 30, "Total record count should be 30 before baseline test")
    
    def rewriteResultNoWhere = sql """
        ALTER TABLE ${catalog_name}.${db_name}.${table_baseline} 
        EXECUTE rewrite_data_files(
            "target-file-size-bytes" = "10485760",
            "min-input-files" = "2"
        )
    """
    logger.info("Rewrite result without WHERE: ${rewriteResultNoWhere}")
    // Assert: Should rewrite some files when no WHERE condition
    assertTrue(rewriteResultNoWhere[0][0] > 0, "Files should be rewritten without WHERE condition")
    assertTrue(rewriteResultNoWhere[0][1] > 0, "Files should be added without WHERE condition")
    assertTrue(rewriteResultNoWhere[0][2] > 0, "Bytes should be written without WHERE condition")
    assertTrue(rewriteResultNoWhere[0][3] == 0, "No bytes should be deleted")
    
    int filesRewrittenBaseline = rewriteResultNoWhere[0][0] as int
    logger.info("Baseline: ${filesRewrittenBaseline} files rewritten")
    
    // Verify data integrity
    def totalRecords1 = sql """SELECT COUNT(*) FROM ${table_baseline}"""
    logger.info("Total record count after baseline test: ${totalRecords1[0][0]}")
    assertTrue(totalRecords1[0][0] == 30, "Total record count should be 30 after baseline test")
    
    // Test 2: Rewrite with WHERE condition matching subset of data
    // WHERE condition: id >= 11 AND id <= 20 (only matches 10 records)
    logger.info("Test 2: Rewrite with WHERE condition matching subset (id >= 11 AND id <= 20)")
    
    def rewriteResultWithWhere = sql """
        ALTER TABLE ${catalog_name}.${db_name}.${table_with_where} 
        EXECUTE rewrite_data_files(
            "target-file-size-bytes" = "10485760",
            "min-input-files" = "2"
        ) WHERE id >= 11 AND id <= 20
    """
    logger.info("Rewrite result with WHERE (id >= 11 AND id <= 20): ${rewriteResultWithWhere}")
    // Assert: Should rewrite fewer or equal files than baseline when WHERE condition filters data
    int filesRewrittenWithWhere = rewriteResultWithWhere[0][0] as int
    assertTrue(filesRewrittenWithWhere > 0, "Files rewritten count should be positive")
    assertTrue(filesRewrittenWithWhere < filesRewrittenBaseline, 
        "Files rewritten with WHERE condition (${filesRewrittenWithWhere}) should be <= baseline (${filesRewrittenBaseline})")
    assertTrue(rewriteResultWithWhere[0][1] > 0, "Files added count should be positive")
    assertTrue(rewriteResultWithWhere[0][2] > 0, "Bytes written count should be positive")
    assertTrue(rewriteResultWithWhere[0][3] == 0, "No bytes should be deleted")
    logger.info("With WHERE: ${filesRewrittenWithWhere} files rewritten (expected <= ${filesRewrittenBaseline})")
    
    // Verify data integrity
    def totalRecords2 = sql """SELECT COUNT(*) FROM ${table_with_where}"""
    assertTrue(totalRecords2[0][0] == 30, "Total record count should be 30 after WHERE condition test")
    
    // Test 3: Rewrite with WHERE condition matching no data
    // WHERE condition: id = 99999 (matches no records)
    logger.info("Test 3: Rewrite with WHERE condition matching no data (id = 99999)")
    
    def rewriteResultNoMatch = sql """
        ALTER TABLE ${catalog_name}.${db_name}.${table_no_match} 
        EXECUTE rewrite_data_files(
            "target-file-size-bytes" = "10485760",
            "min-input-files" = "2"
        ) WHERE id = 99999
    """
    logger.info("Rewrite result with WHERE (id = 99999, no matches): ${rewriteResultNoMatch}")
    // Assert: Should rewrite no files when WHERE condition matches no data
    assertTrue(rewriteResultNoMatch[0][0] == 0, 
        "No files should be rewritten when WHERE condition matches no data (expected 0, got ${rewriteResultNoMatch[0][0]})")
    assertTrue(rewriteResultNoMatch[0][1] == 0, 
        "No files should be added when WHERE condition matches no data (expected 0, got ${rewriteResultNoMatch[0][1]})")
    assertTrue(rewriteResultNoMatch[0][2] == 0, 
        "No bytes should be written when WHERE condition matches no data (expected 0, got ${rewriteResultNoMatch[0][2]})")
    assertTrue(rewriteResultNoMatch[0][3] == 0, "No bytes should be deleted")
    logger.info("With WHERE (no match): 0 files rewritten as expected")
    
    // Verify data integrity
    def totalRecords3 = sql """SELECT COUNT(*) FROM ${table_no_match}"""
    assertTrue(totalRecords3[0][0] == 30, "Total record count should be 30 after no-match WHERE condition test")
    
    logger.info("WHERE conditions test completed successfully")
    
    // Note: Tables are not cleaned up as they are pre-created in Docker initialization
    // and may be reused or manually cleaned up if needed
    logger.info("Test completed - pre-created tables remain for potential reuse")
}
