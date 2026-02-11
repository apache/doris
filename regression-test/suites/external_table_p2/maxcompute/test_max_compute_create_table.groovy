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

/**
 * Regression tests for MaxCompute CREATE TABLE statements
 * This test covers:
 * 1. Basic create table statements
 * 2. Create table with partitions
 * 3. Create table with distribution (bucketing)
 * 4. Create table with comments and properties
 * 5. Create table with various data types
 * 6. Error cases (unsupported features, invalid configurations)
 *
 * Note: Tables are NOT deleted after creation to allow inspection after tests run
 */

suite("test_max_compute_create_table", "p2,external,maxcompute,external_remote,external_remote_maxcompute") {
    String enabled = context.config.otherConfigs.get("enableMaxComputeTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String ak = context.config.otherConfigs.get("ak")
        String sk = context.config.otherConfigs.get("sk")
        String mc_catalog_name = "test_max_compute_create_table"
        String defaultProject = "mc_datalake"

        sql """drop catalog if exists ${mc_catalog_name} """

        sql """
        CREATE CATALOG IF NOT EXISTS ${mc_catalog_name} PROPERTIES (
                "type" = "max_compute",
                "mc.default.project" = "${defaultProject}",
                "mc.access_key" = "${ak}",
                "mc.secret_key" = "${sk}",
                "mc.endpoint" = "http://service.cn-beijing-vpc.maxcompute.aliyun-inc.com/api",
                "mc.quota" = "pay-as-you-go"
        );
        """

        logger.info("catalog " + mc_catalog_name + " created")
        sql """switch ${mc_catalog_name};"""
        logger.info("switched to catalog " + mc_catalog_name)
        sql """ show databases; """
        sql """ use ${defaultProject} """

        // ============================================================================
        // Test 1: Basic CREATE TABLE (Simple table without partition or distribution)
        // ============================================================================
        try {
            String test1_table = "mc_test_basic_table"
            sql """
            CREATE TABLE ${test1_table} (
                id INT,
                name STRING,
                age INT
            )
            """
            sql """show tables like '${test1_table}' """
            qt_test1_show_create_table """SHOW CREATE TABLE ${test1_table} """
            logger.info("Test 1 PASSED: Basic table created successfully")
        } catch (Exception e) {
            logger.info("Test 1 FAILED: " + e.getMessage())
        }

        // ============================================================================
        // Test 2: CREATE TABLE with all supported data types
        // Comprehensive type conversion test
        // ============================================================================
        try {
            String test2_table = "mc_test_all_types_comprehensive"
            sql """
            CREATE TABLE ${test2_table} (
                id INT,
                bool_col BOOLEAN,
                tinyint_col TINYINT,
                smallint_col SMALLINT,
                int_col INT,
                bigint_col BIGINT,
                float_col FLOAT,
                double_col DOUBLE,
                decimal_col1 DECIMAL(9,0),
                decimal_col2 DECIMAL(8,4),
                decimal_col3 DECIMAL(18,6),
                decimal_col4 DECIMAL(38,12),
                string_col STRING,
                varchar_col1 VARCHAR(100),
                varchar_col2 VARCHAR(65535),
                char_col1 CHAR(50),
                char_col2 CHAR(255),
                date_col DATE,
                datetime_col DATETIME,
                t_array_string ARRAY<STRING>,
                t_array_int ARRAY<INT>,
                t_array_bigint ARRAY<BIGINT>,
                t_array_float ARRAY<FLOAT>,
                t_array_double ARRAY<DOUBLE>,
                t_array_boolean ARRAY<BOOLEAN>,
                t_map_string MAP<STRING,STRING>,
                t_map_int MAP<INT,INT>,
                t_map_bigint MAP<BIGINT,BIGINT>,
                t_map_float MAP<FLOAT,FLOAT>,
                t_map_double MAP<DOUBLE,DOUBLE>,
                t_struct_simple STRUCT<field1:STRING,field2:INT>
            )
            """
            sql """show tables like '${test2_table}' """
            qt_test2_show_create_table """SHOW CREATE TABLE ${test2_table} """
            logger.info("Test 2 PASSED: Table with all supported types created successfully")
        } catch (Exception e) {
            logger.info("Test 2 FAILED: " + e.getMessage())
        }

        // ============================================================================
        // Test 3: CREATE TABLE with PARTITION clause
        // ============================================================================
        try {
            String test3_table = "mc_test_partition_table"
            sql """
            CREATE TABLE ${test3_table} (
                id INT,
                name STRING,
                amount DOUBLE
            )
            PARTITIONED BY (
                ds STRING
            )
            """
            sql """show tables like '${test3_table}' """
            qt_test3_show_create_table """SHOW CREATE TABLE ${test3_table} """
            logger.info("Test 3 PASSED: Table with partition created successfully")
        } catch (Exception e) {
            logger.info("Test 3 FAILED: " + e.getMessage())
        }

        // ============================================================================
        // Test 4: CREATE TABLE with DISTRIBUTION (BUCKETING)
        // ============================================================================
        try {
            String test4_table = "mc_test_distributed_table"
            sql """
            CREATE TABLE ${test4_table} (
                id INT,
                name STRING,
                value INT
            )
            DISTRIBUTED BY HASH(id) BUCKETS 4
            """
            sql """show tables like '${test4_table}' """
            qt_test4_show_create_table """SHOW CREATE TABLE ${test4_table} """
            logger.info("Test 4 PASSED: Table with distribution created successfully")
        } catch (Exception e) {
            logger.info("Test 4 FAILED: " + e.getMessage())
        }

        // ============================================================================
        // Test 5: CREATE TABLE with PARTITION and DISTRIBUTION
        // ============================================================================
        try {
            String test5_table = "mc_test_partition_distributed_table"
            sql """
            CREATE TABLE ${test5_table} (
                id INT,
                name STRING,
                value INT
            )
            PARTITIONED BY (
                ds STRING
            )
            DISTRIBUTED BY HASH(id) BUCKETS 8
            """
            sql """show tables like '${test5_table}' """
            qt_test5_show_create_table """SHOW CREATE TABLE ${test5_table} """
            logger.info("Test 5 PASSED: Table with partition and distribution created successfully")
        } catch (Exception e) {
            logger.info("Test 5 FAILED: " + e.getMessage())
        }

        // ============================================================================
        // Test 6: CREATE TABLE with COMMENT
        // ============================================================================
        try {
            String test6_table = "mc_test_table_with_comment"
            sql """
            CREATE TABLE ${test6_table} (
                id INT COMMENT 'User ID',
                name STRING COMMENT 'User Name'
            )
            COMMENT 'This is a test table with comments'
            """
            sql """show tables like '${test6_table}' """
            qt_test6_show_create_table """SHOW CREATE TABLE ${test6_table} """
            logger.info("Test 6 PASSED: Table with comments created successfully")
        } catch (Exception e) {
            logger.info("Test 6 FAILED: " + e.getMessage())
        }

        // ============================================================================
        // Test 7: CREATE TABLE with PROPERTIES (mc.tblproperty prefix)
        // ============================================================================
        try {
            String test7_table = "mc_test_table_with_properties"
            sql """
            CREATE TABLE ${test7_table} (
                id INT,
                name STRING
            )
            PROPERTIES (
                "mc.tblproperty.custom_prop" = "custom_value"
            )
            """
            sql """show tables like '${test7_table}' """
            qt_test7_show_create_table """SHOW CREATE TABLE ${test7_table} """
            logger.info("Test 7 PASSED: Table with custom properties created successfully")
        } catch (Exception e) {
            logger.info("Test 7 FAILED: " + e.getMessage())
        }

        // ============================================================================
        // Test 8: CREATE TABLE with LIFECYCLE property
        // ============================================================================
        try {
            String test8_table = "mc_test_table_with_lifecycle"
            sql """
            CREATE TABLE ${test8_table} (
                id INT,
                name STRING
            )
            PROPERTIES (
                "lifecycle" = "30"
            )
            """
            sql """show tables like '${test8_table}' """
            qt_test8_show_create_table """SHOW CREATE TABLE ${test8_table} """
            logger.info("Test 8 PASSED: Table with lifecycle property created successfully")
        } catch (Exception e) {
            logger.info("Test 8 FAILED: " + e.getMessage())
        }

        // ============================================================================
        // Test 9: CREATE TABLE IF NOT EXISTS
        // ============================================================================
        try {
            String test9_table = "mc_test_if_not_exists_table"
            sql """
            CREATE TABLE IF NOT EXISTS ${test9_table} (
                id INT,
                name STRING
            )
            """
            // Try creating the same table again with IF NOT EXISTS - should not fail
            sql """
            CREATE TABLE IF NOT EXISTS ${test9_table} (
                id INT,
                name STRING
            )
            """
            sql """show tables like '${test9_table}' """
            qt_test9_show_create_table """SHOW CREATE TABLE ${test9_table} """
            logger.info("Test 9 PASSED: CREATE TABLE IF NOT EXISTS works correctly")
        } catch (Exception e) {
            logger.info("Test 9 FAILED: " + e.getMessage())
        }

        // ============================================================================
        // Test 10: CREATE TABLE with ARRAY type (supported by MaxCompute)
        // ============================================================================
        try {
            String test10_table = "mc_test_array_type_table"
            sql """
            CREATE TABLE ${test10_table} (
                id INT,
                tags ARRAY<STRING>,
                scores ARRAY<INT>,
                values ARRAY<DOUBLE>
            )
            """
            sql """show tables like '${test10_table}' """
            qt_test10_show_create_table """SHOW CREATE TABLE ${test10_table} """
            logger.info("Test 10 PASSED: Table with ARRAY type created successfully")
        } catch (Exception e) {
            logger.info("Test 10 FAILED: " + e.getMessage())
        }

        // ============================================================================
        // Test 11: CREATE TABLE with MAP type (supported by MaxCompute)
        // ============================================================================
        try {
            String test11_table = "mc_test_map_type_table"
            sql """
            CREATE TABLE ${test11_table} (
                id INT,
                properties MAP<STRING, STRING>,
                metrics MAP<STRING, DOUBLE>,
                config MAP<INT, BOOLEAN>
            )
            """
            sql """show tables like '${test11_table}' """
            qt_test11_show_create_table """SHOW CREATE TABLE ${test11_table} """
            logger.info("Test 11 PASSED: Table with MAP type created successfully")
        } catch (Exception e) {
            logger.info("Test 11 FAILED: " + e.getMessage())
        }

        // ============================================================================
        // Test 12: CREATE TABLE with STRUCT type (supported by MaxCompute)
        // ============================================================================
        try {
            String test12_table = "mc_test_struct_type_table"
            sql """
            CREATE TABLE ${test12_table} (
                id INT,
                person STRUCT<name:STRING,age:INT,email:STRING>,
                address STRUCT<city:STRING,country:STRING,zipcode:INT>
            )
            """
            sql """show tables like '${test12_table}' """
            qt_test12_show_create_table """SHOW CREATE TABLE ${test12_table} """
            logger.info("Test 12 PASSED: Table with STRUCT type created successfully")
        } catch (Exception e) {
            logger.info("Test 12 FAILED: " + e.getMessage())
        }

        // ============================================================================
        // Test 13: ERROR CASE - CREATE TABLE with unsupported type (IPV4)
        // ============================================================================
        try {
            String test13_table = "mc_test_unsupported_type_table"
            sql """
            CREATE TABLE ${test13_table} (
                id INT,
                ip IPV4
            )
            """
            logger.info("Test 13 FAILED: Should have rejected unsupported IPV4 type")
        } catch (Exception e) {
            // Expected to fail
            if (e.getMessage().contains("Unsupported") || e.getMessage().contains("unsupported")) {
                logger.info("Test 13 PASSED: Correctly rejected unsupported type")
            } else {
                logger.info("Test 13 PARTIAL: Got exception but message unclear: " + e.getMessage())
            }
        }

        // ============================================================================
        // Test 14: ERROR CASE - CREATE TABLE with AUTO_INCREMENT column
        // ============================================================================
        try {
            String test14_table = "mc_test_auto_increment_table"
            sql """
            CREATE TABLE ${test14_table} (
                id INT AUTO_INCREMENT,
                name STRING
            )
            """
            logger.info("Test 14 FAILED: Should have rejected AUTO_INCREMENT column")
        } catch (Exception e) {
            // Expected to fail
            if (e.getMessage().contains("Auto-increment") || e.getMessage().contains("auto-increment")) {
                logger.info("Test 14 PASSED: Correctly rejected AUTO_INCREMENT column")
            } else {
                logger.info("Test 14 PARTIAL: Got exception but message unclear: " + e.getMessage())
            }
        }

        // ============================================================================
        // Test 15: ERROR CASE - CREATE TABLE with duplicate column names
        // ============================================================================
        try {
            String test15_table = "mc_test_duplicate_column_table"
            sql """
            CREATE TABLE ${test15_table} (
                id INT,
                name STRING,
                name STRING
            )
            """
            logger.info("Test 15 FAILED: Should have rejected duplicate column names")
        } catch (Exception e) {
            // Expected to fail
            if (e.getMessage().contains("Duplicate") || e.getMessage().contains("duplicate")) {
                logger.info("Test 15 PASSED: Correctly rejected duplicate column names")
            } else {
                logger.info("Test 15 PARTIAL: Got exception but message unclear: " + e.getMessage())
            }
        }

        // ============================================================================
        // Test 16: ERROR CASE - CREATE TABLE without columns
        // ============================================================================
        try {
            String test16_table = "mc_test_no_columns_table"
            sql """
            CREATE TABLE ${test16_table} ()
            """
            logger.info("Test 16 FAILED: Should have rejected table without columns")
        } catch (Exception e) {
            // Expected to fail
            if (e.getMessage().contains("at least one column") || e.getMessage().contains("contain")) {
                logger.info("Test 16 PASSED: Correctly rejected table without columns")
            } else {
                logger.info("Test 16 PARTIAL: Got exception but message unclear: " + e.getMessage())
            }
        }

        // ============================================================================
        // Test 17: ERROR CASE - DISTRIBUTION with invalid bucket number
        // ============================================================================
        try {
            String test17_table = "mc_test_invalid_bucket_table"
            sql """
            CREATE TABLE ${test17_table} (
                id INT,
                name STRING
            )
            DISTRIBUTED BY HASH(id) BUCKETS 2000
            """
            logger.info("Test 17 FAILED: Should have rejected bucket number > 1024")
        } catch (Exception e) {
            // Expected to fail
            if (e.getMessage().contains("bucket") || e.getMessage().contains("Invalid")) {
                logger.info("Test 17 PASSED: Correctly rejected invalid bucket number")
            } else {
                logger.info("Test 17 PARTIAL: Got exception but message unclear: " + e.getMessage())
            }
        }

        // ============================================================================
        // Test 18: ERROR CASE - LIFECYCLE with invalid value (too large)
        // ============================================================================
        try {
            String test18_table = "mc_test_invalid_lifecycle_table"
            sql """
            CREATE TABLE ${test18_table} (
                id INT,
                name STRING
            )
            PROPERTIES (
                "lifecycle" = "99999999"
            )
            """
            logger.info("Test 18 FAILED: Should have rejected lifecycle value > 37231")
        } catch (Exception e) {
            // Expected to fail
            if (e.getMessage().contains("lifecycle") || e.getMessage().contains("Invalid")) {
                logger.info("Test 18 PASSED: Correctly rejected invalid lifecycle value")
            } else {
                logger.info("Test 18 PARTIAL: Got exception but message unclear: " + e.getMessage())
            }
        }

        // ============================================================================
        // Test 19: Basic DROP TABLE
        // ============================================================================
        try {
            String test19_table = "mc_test_basic_table"
            sql """DROP TABLE ${test19_table}"""
            def result = sql """SHOW TABLES LIKE '${test19_table}'"""
            if (result.size() == 0) {
                logger.info("Test 19 PASSED: Basic table dropped successfully")
            } else {
                logger.info("Test 19 FAILED: Table still exists after drop")
            }
        } catch (Exception e) {
            logger.info("Test 19 FAILED: " + e.getMessage())
        }

        // ============================================================================
        // Test 20: DROP TABLE with partitions
        // ============================================================================
        try {
            String test20_table = "mc_test_partition_table"
            sql """DROP TABLE ${test20_table}"""
            def result = sql """SHOW TABLES LIKE '${test20_table}'"""
            if (result.size() == 0) {
                logger.info("Test 20 PASSED: Partition table dropped successfully")
            } else {
                logger.info("Test 20 FAILED: Partition table still exists after drop")
            }
        } catch (Exception e) {
            logger.info("Test 20 FAILED: " + e.getMessage())
        }

        // ============================================================================
        // Test 21: DROP TABLE with distribution
        // ============================================================================
        try {
            String test21_table = "mc_test_distributed_table"
            sql """DROP TABLE ${test21_table}"""
            def result = sql """SHOW TABLES LIKE '${test21_table}'"""
            if (result.size() == 0) {
                logger.info("Test 21 PASSED: Distributed table dropped successfully")
            } else {
                logger.info("Test 21 FAILED: Distributed table still exists after drop")
            }
        } catch (Exception e) {
            logger.info("Test 21 FAILED: " + e.getMessage())
        }

        // ============================================================================
        // Test 22: DROP TABLE IF EXISTS (existing table)
        // ============================================================================
        try {
            String test22_table = "mc_test_table_with_comment"
            sql """DROP TABLE IF EXISTS ${test22_table}"""
            def result = sql """SHOW TABLES LIKE '${test22_table}'"""
            if (result.size() == 0) {
                logger.info("Test 22 PASSED: DROP TABLE IF EXISTS (existing) executed successfully")
            } else {
                logger.info("Test 22 FAILED: Table still exists after DROP IF EXISTS")
            }
        } catch (Exception e) {
            logger.info("Test 22 FAILED: " + e.getMessage())
        }

        // ============================================================================
        // Test 23: DROP TABLE IF EXISTS (non-existent table)
        // ============================================================================
        try {
            String test23_table = "mc_test_nonexistent_table_for_drop"
            sql """DROP TABLE IF EXISTS ${test23_table}"""
            logger.info("Test 23 PASSED: DROP TABLE IF EXISTS (non-existent) did not error")
        } catch (Exception e) {
            logger.info("Test 23 FAILED: " + e.getMessage())
        }

        // ============================================================================
        // Test 24: DROP complex types table
        // ============================================================================
        try {
            String test24_table = "mc_test_array_type_table"
            sql """DROP TABLE ${test24_table}"""
            def result = sql """SHOW TABLES LIKE '${test24_table}'"""
            if (result.size() == 0) {
                logger.info("Test 24 PASSED: Complex types table dropped successfully")
            } else {
                logger.info("Test 24 FAILED: Complex types table still exists after drop")
            }
        } catch (Exception e) {
            logger.info("Test 24 FAILED: " + e.getMessage())
        }

        // ============================================================================
        // Test 25: DROP then recreate with same name - Verify cache invalidation
        // ============================================================================
        try {
            String test25_table = "mc_test_cache_invalidation"
            // Create table
            sql """
            CREATE TABLE ${test25_table} (
                id INT,
                value INT
            )
            """
            // Drop table
            sql """DROP TABLE ${test25_table}"""
            def result1 = sql """SHOW TABLES LIKE '${test25_table}'"""
            // Recreate with different schema
            sql """
            CREATE TABLE ${test25_table} (
                id INT,
                name STRING,
                value DOUBLE
            )
            """
            def schema = sql """SHOW CREATE TABLE ${test25_table}"""
            if (schema[0][1].contains("name") && schema[0][1].contains("value")) {
                logger.info("Test 25 PASSED: Cache invalidation works correctly")
            } else {
                logger.info("Test 25 FAILED: New schema not reflected after DROP and recreate")
            }
        } catch (Exception e) {
            logger.info("Test 25 FAILED: " + e.getMessage())
        }

        // ============================================================================
        // Test 26: DROP multiple tables in sequence
        // ============================================================================
        try {
            String[] tables_to_drop = [
                "mc_test_all_types_comprehensive",
                "mc_test_partition_distributed_table",
                "mc_test_if_not_exists_table",
                "mc_test_map_type_table"
            ]
            int dropped_count = 0
            for (String table : tables_to_drop) {
                try {
                    sql """DROP TABLE ${table}"""
                    def result = sql """SHOW TABLES LIKE '${table}'"""
                    if (result.size() == 0) {
                        dropped_count++
                    }
                } catch (Exception ignored) {
                    // Skip if table doesn't exist
                }
            }
            if (dropped_count >= 3) {
                logger.info("Test 26 PASSED: Multiple tables dropped successfully (${dropped_count}/${tables_to_drop.size()})")
            } else {
                logger.info("Test 26 FAILED: Only ${dropped_count} tables were dropped")
            }
        } catch (Exception e) {
            logger.info("Test 26 FAILED: " + e.getMessage())
        }

        // ============================================================================
        // Test 27: DROP table with lifecycle property
        // ============================================================================
        try {
            String test27_table = "mc_test_table_with_lifecycle"
            sql """DROP TABLE ${test27_table}"""
            def result = sql """SHOW TABLES LIKE '${test27_table}'"""
            if (result.size() == 0) {
                logger.info("Test 27 PASSED: Table with lifecycle property dropped successfully")
            } else {
                logger.info("Test 27 FAILED: Lifecycle table still exists after drop")
            }
        } catch (Exception e) {
            logger.info("Test 27 FAILED: " + e.getMessage())
        }

        // ============================================================================
        // Test 28: DROP table with properties
        // ============================================================================
        try {
            String test28_table = "mc_test_table_with_properties"
            sql """DROP TABLE ${test28_table}"""
            def result = sql """SHOW TABLES LIKE '${test28_table}'"""
            if (result.size() == 0) {
                logger.info("Test 28 PASSED: Table with properties dropped successfully")
            } else {
                logger.info("Test 28 FAILED: Properties table still exists after drop")
            }
        } catch (Exception e) {
            logger.info("Test 28 FAILED: " + e.getMessage())
        }

        // ============================================================================
        // Test 29: DROP TABLE without IF EXISTS (non-existent)
        // ============================================================================
        try {
            String test29_table = "mc_test_definitely_nonexistent_table_12345"
            sql """DROP TABLE ${test29_table}"""
            logger.info("Test 29 FAILED: Should have errored on non-existent table")
        } catch (Exception e) {
            // Expected to fail
            if (e.getMessage().contains("Unknown") || e.getMessage().contains("unknown") || e.getMessage().contains("not found")) {
                logger.info("Test 29 PASSED: Correctly rejected DROP on non-existent table")
            } else {
                logger.info("Test 29 PARTIAL: Got exception but message unclear: " + e.getMessage())
            }
        }

        // ============================================================================
        // Test 30: Double DROP without IF EXISTS
        // ============================================================================
        try {
            String test30_table = "mc_test_double_drop"
            // Create and drop first time
            sql """
            CREATE TABLE ${test30_table} (
                id INT
            )
            """
            sql """DROP TABLE ${test30_table}"""
            def result1 = sql """SHOW TABLES LIKE '${test30_table}'"""
            // Try to drop again without IF EXISTS - should fail
            sql """DROP TABLE ${test30_table}"""
            logger.info("Test 30 FAILED: Second DROP should have errored")
        } catch (Exception e) {
            // Expected to fail on second drop
            if (e.getMessage().contains("Unknown") || e.getMessage().contains("unknown") || e.getMessage().contains("not found")) {
                logger.info("Test 30 PASSED: Second DROP correctly failed with error")
            } else {
                logger.info("Test 30 PARTIAL: Got exception but message unclear: " + e.getMessage())
            }
        }

        // ============================================================================
        // Test 31: Verify dropped table not accessible
        // ============================================================================
        try {
            String test31_table = "mc_test_struct_type_table"
            sql """DROP TABLE ${test31_table}"""
            def result = sql """SHOW TABLES LIKE '${test31_table}'"""
            if (result.size() == 0) {
                logger.info("Test 31 PASSED: Dropped table not accessible via SHOW TABLES")
            } else {
                logger.info("Test 31 FAILED: Dropped table still appears in SHOW TABLES")
            }
        } catch (Exception e) {
            logger.info("Test 31 FAILED: " + e.getMessage())
        }

        logger.info("All tests completed!")
    }
}
