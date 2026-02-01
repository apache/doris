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

// Test for struct field schema evolution in Iceberg tables.
// This test case verifies the fix for the bug where querying a struct field 
// that was added after schema evolution fails when all queried columns are 
// missing in the original file, and the reference column used for RL/DL 
// was dropped from the table schema.
//
// Bug: "File column name 'removed' not found in struct children"
// Fix: Use ConstNode for reference column when reading RL/DL information
//
// Prerequisites: 
// - Tables created by run24.sql in docker iceberg scripts

suite("test_iceberg_struct_schema_evolution", "p0,external,doris,external_docker,external_docker_doris") {

    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "test_iceberg_struct_schema_evolution"

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

    logger.info("catalog " + catalog_name + " created")
    sql """switch ${catalog_name};"""
    logger.info("switched to catalog " + catalog_name)
    sql """use test_db;"""

    sql """set enable_fallback_to_original_planner=false;"""

    def table_name = "test_struct_evolution"

    // Verify table schema after evolution
    qt_desc """DESC ${table_name}"""

    // Test 1: Query all columns - should work
    qt_select_all """SELECT * FROM ${table_name} ORDER BY id"""

    // Test 2: Query struct field that exists in both old and new files
    qt_struct_keep """SELECT struct_element(a_struct, 'keep') FROM ${table_name} ORDER BY id"""
    qt_struct_renamed """SELECT struct_element(a_struct, 'renamed') FROM ${table_name} ORDER BY id"""

    // Test 3: Query struct field that was dropped and re-added (BUG FIX TEST)
    // This query would crash before the fix with:
    // "Not support read struct 'a_struct' which columns are all missing"
    // or "File column name 'removed' not found in struct children"
    qt_struct_drop_and_add """SELECT struct_element(a_struct, 'drop_and_add') FROM ${table_name} ORDER BY id"""

    // Test 4: Query struct field that was newly added (BUG FIX TEST)
    qt_struct_added """SELECT struct_element(a_struct, 'added') FROM ${table_name} ORDER BY id"""

    // Test 5: Query entire struct column
    qt_struct_full """SELECT a_struct FROM ${table_name} ORDER BY id"""

    // Test 6: Query with predicate on struct field
    qt_struct_predicate_1 """SELECT id FROM ${table_name} WHERE struct_element(a_struct, 'renamed') = 11 ORDER BY id"""
    qt_struct_predicate_2 """SELECT id FROM ${table_name} WHERE struct_element(a_struct, 'drop_and_add') IS NULL ORDER BY id"""
    qt_struct_predicate_3 """SELECT id FROM ${table_name} WHERE struct_element(a_struct, 'added') IS NULL ORDER BY id"""
    qt_struct_predicate_4 """SELECT id FROM ${table_name} WHERE struct_element(a_struct, 'added') IS NOT NULL ORDER BY id"""

    // Test 7: Multiple struct fields in one query
    qt_struct_multi """SELECT struct_element(a_struct, 'renamed'), struct_element(a_struct, 'keep'), struct_element(a_struct, 'drop_and_add'), struct_element(a_struct, 'added') FROM ${table_name} ORDER BY id"""

    // Test 8: DISTINCT query on struct fields
    qt_struct_distinct """SELECT DISTINCT struct_element(a_struct, 'renamed'), struct_element(a_struct, 'added'), struct_element(a_struct, 'keep') FROM ${table_name} ORDER BY 1, 2, 3"""

    // ============================================================
    // Test with ORC format (for completeness)
    // ============================================================
    def orc_table_name = "test_struct_evolution_orc"

    // Verify ORC table schema after evolution
    qt_orc_desc """DESC ${orc_table_name}"""

    // Test 1: Query all columns - should work
    qt_orc_select_all """SELECT * FROM ${orc_table_name} ORDER BY id"""

    // Test 2: Query struct field that exists in both old and new files
    qt_orc_struct_keep """SELECT struct_element(a_struct, 'keep') FROM ${orc_table_name} ORDER BY id"""
    qt_orc_struct_renamed """SELECT struct_element(a_struct, 'renamed') FROM ${orc_table_name} ORDER BY id"""

    // Test 3: Query struct field that was dropped and re-added
    qt_orc_struct_drop_and_add """SELECT struct_element(a_struct, 'drop_and_add') FROM ${orc_table_name} ORDER BY id"""

    // Test 4: Query struct field that was newly added
    qt_orc_struct_added """SELECT struct_element(a_struct, 'added') FROM ${orc_table_name} ORDER BY id"""

    // Test 5: Query entire struct column
    qt_orc_struct_full """SELECT a_struct FROM ${orc_table_name} ORDER BY id"""

    // Test 6: Multiple struct fields in one query
    qt_orc_struct_multi """SELECT struct_element(a_struct, 'renamed'), struct_element(a_struct, 'keep'), struct_element(a_struct, 'drop_and_add'), struct_element(a_struct, 'added') FROM ${orc_table_name} ORDER BY id"""

    // ============================================================
    // Test with mixed case field names (case sensitivity test)
    // ============================================================
    def case_table_name = "test_struct_evolution_case"

    // Verify case-sensitive table schema after evolution
    qt_case_desc """DESC ${case_table_name}"""

    // Test 1: Query all columns - should work
    qt_case_select_all """SELECT * FROM ${case_table_name} ORDER BY id"""

    // Test 2: Query struct field that exists in both old and new files
    qt_case_struct_keep """SELECT struct_element(a_struct, 'keep') FROM ${case_table_name} ORDER BY id"""
    qt_case_struct_renamed """SELECT struct_element(a_struct, 'renamed') FROM ${case_table_name} ORDER BY id"""

    // Test 3: Query struct field that was dropped and re-added with case change
    // Note: Even though we use DROP_AND_ADD (uppercase) in SQL, the system normalizes
    // field names to lowercase, so we query with 'drop_and_add' (lowercase)
    qt_case_struct_drop_and_add """SELECT struct_element(a_struct, 'drop_and_add') FROM ${case_table_name} ORDER BY id"""

    // Test 4: Query struct field that was newly added
    qt_case_struct_added """SELECT struct_element(a_struct, 'added') FROM ${case_table_name} ORDER BY id"""

    // Test 5: Query entire struct column
    qt_case_struct_full """SELECT a_struct FROM ${case_table_name} ORDER BY id"""

    // Test 6: Query with predicate on struct field
    qt_case_struct_predicate_1 """SELECT id FROM ${case_table_name} WHERE struct_element(a_struct, 'renamed') = 11 ORDER BY id"""
    qt_case_struct_predicate_2 """SELECT id FROM ${case_table_name} WHERE struct_element(a_struct, 'drop_and_add') IS NULL ORDER BY id"""
    qt_case_struct_predicate_3 """SELECT id FROM ${case_table_name} WHERE struct_element(a_struct, 'added') IS NULL ORDER BY id"""
    qt_case_struct_predicate_4 """SELECT id FROM ${case_table_name} WHERE struct_element(a_struct, 'added') IS NOT NULL ORDER BY id"""

    // Test 7: Multiple struct fields in one query
    qt_case_struct_multi """SELECT struct_element(a_struct, 'renamed'), struct_element(a_struct, 'keep'), struct_element(a_struct, 'drop_and_add'), struct_element(a_struct, 'added') FROM ${case_table_name} ORDER BY id"""

    // Test 8: DISTINCT query on struct fields
    qt_case_struct_distinct """SELECT DISTINCT struct_element(a_struct, 'renamed'), struct_element(a_struct, 'added'), struct_element(a_struct, 'keep') FROM ${case_table_name} ORDER BY 1, 2, 3"""

    // ============================================================
    // Test with ORC format and mixed case field names
    // ============================================================
    def case_orc_table_name = "test_struct_evolution_case_orc"

    // Verify ORC case-sensitive table schema after evolution
    qt_case_orc_desc """DESC ${case_orc_table_name}"""

    // Test 1: Query all columns - should work
    qt_case_orc_select_all """SELECT * FROM ${case_orc_table_name} ORDER BY id"""

    // Test 2: Query struct field that exists in both old and new files
    qt_case_orc_struct_keep """SELECT struct_element(a_struct, 'keep') FROM ${case_orc_table_name} ORDER BY id"""
    qt_case_orc_struct_renamed """SELECT struct_element(a_struct, 'renamed') FROM ${case_orc_table_name} ORDER BY id"""

    // Test 3: Query struct field that was dropped and re-added with case change
    // Note: Even though we use DROP_AND_ADD (uppercase) in SQL, the system normalizes
    // field names to lowercase, so we query with 'drop_and_add' (lowercase)
    qt_case_orc_struct_drop_and_add """SELECT struct_element(a_struct, 'drop_and_add') FROM ${case_orc_table_name} ORDER BY id"""

    // Test 4: Query struct field that was newly added
    qt_case_orc_struct_added """SELECT struct_element(a_struct, 'added') FROM ${case_orc_table_name} ORDER BY id"""

    // Test 5: Query entire struct column
    qt_case_orc_struct_full """SELECT a_struct FROM ${case_orc_table_name} ORDER BY id"""

    // Test 6: Multiple struct fields in one query
    qt_case_orc_struct_multi """SELECT struct_element(a_struct, 'renamed'), struct_element(a_struct, 'keep'), struct_element(a_struct, 'drop_and_add'), struct_element(a_struct, 'added') FROM ${case_orc_table_name} ORDER BY id"""

    // Clean up
    sql """drop catalog if exists ${catalog_name}"""
}
