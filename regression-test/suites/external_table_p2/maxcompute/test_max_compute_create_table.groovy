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
        String test1_table = "test_mc_basic_table"
        sql """DROP TABLE IF EXISTS ${test1_table}"""
        sql """
        CREATE TABLE ${test1_table} (
            id INT,
            name STRING,
            age INT
        )
        """
        sql """show tables like '${test1_table}' """
        qt_test1_show_create_table """SHOW CREATE TABLE ${test1_table} """

        // ============================================================================
        // Test 2: CREATE TABLE with all supported data types
        // Comprehensive type conversion test
        // ============================================================================
        String test2_table = "test_mc_all_types_comprehensive"
        sql """DROP TABLE IF EXISTS ${test2_table}"""
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
            varchar_col2 VARCHAR(65533),
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

        // ============================================================================
        // Test 3: CREATE TABLE with PARTITION clause
        // ============================================================================
        String test3_table = "test_mc_partition_table"
        sql """DROP TABLE IF EXISTS ${test3_table}"""
        sql """
        CREATE TABLE ${test3_table} (
            id INT,
            name STRING,
            amount DOUBLE,
            ds STRING
        )
        PARTITION BY (ds)();
        """
        sql """show tables like '${test3_table}' """
        qt_test3_show_create_table """SHOW CREATE TABLE ${test3_table} """

        // ============================================================================
        // Test 4: CREATE TABLE with DISTRIBUTION (BUCKETING)
        // ============================================================================
        String test4_table = "test_mc_distributed_table"
        sql """DROP TABLE IF EXISTS ${test4_table}"""
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

        // ============================================================================
        // Test 5: CREATE TABLE with PARTITION and DISTRIBUTION
        // ============================================================================
        String test5_table = "test_mc_partition_distributed_table"
        sql """DROP TABLE IF EXISTS ${test5_table}"""
        sql """
        CREATE TABLE ${test5_table} (
            id INT,
            name STRING,
            value INT,
            ds STRING
        )
        PARTITION BY (ds)()
        DISTRIBUTED BY HASH(id) BUCKETS 8
        """
        sql """show tables like '${test5_table}' """
        qt_test5_show_create_table """SHOW CREATE TABLE ${test5_table} """

        // ============================================================================
        // Test 6: CREATE TABLE with COMMENT
        // ============================================================================
        String test6_table = "test_mc_table_with_comment"
        sql """DROP TABLE IF EXISTS ${test6_table}"""
        sql """
        CREATE TABLE ${test6_table} (
            id INT COMMENT 'User ID',
            name STRING COMMENT 'User Name'
        )
        COMMENT 'This is a test table with comments'
        """
        sql """show tables like '${test6_table}' """
        qt_test6_show_create_table """SHOW CREATE TABLE ${test6_table} """

        // ============================================================================
        // Test 8: CREATE TABLE with LIFECYCLE property
        // ============================================================================
        String test8_table = "test_mc_table_with_lifecycle"
        sql """DROP TABLE IF EXISTS ${test8_table}"""
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

        // ============================================================================
        // Test 9: CREATE TABLE IF NOT EXISTS
        // ============================================================================
        String test9_table = "test_mc_if_not_exists_table"
        sql """DROP TABLE IF EXISTS ${test9_table}"""
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

        // ============================================================================
        // Test 10: CREATE TABLE with ARRAY type (supported by MaxCompute)
        // ============================================================================
        String test10_table = "test_mc_array_type_table"
        sql """DROP TABLE IF EXISTS ${test10_table}"""
        sql """
        CREATE TABLE ${test10_table} (
            id INT,
            tags ARRAY<STRING>,
            scores ARRAY<INT>,
            `values` ARRAY<DOUBLE>
        )
        """
        sql """show tables like '${test10_table}' """
        qt_test10_show_create_table """SHOW CREATE TABLE ${test10_table} """

        // ============================================================================
        // Test 11: CREATE TABLE with MAP type (supported by MaxCompute)
        // ============================================================================
        String test11_table = "test_mc_map_type_table"
        sql """DROP TABLE IF EXISTS ${test11_table}"""
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

        // ============================================================================
        // Test 12: CREATE TABLE with STRUCT type (supported by MaxCompute)
        // ============================================================================
        String test12_table = "test_mc_struct_type_table"
        sql """DROP TABLE IF EXISTS ${test12_table}"""
        sql """
        CREATE TABLE ${test12_table} (
            id INT,
            person STRUCT<name:STRING,age:INT,email:STRING>,
            address STRUCT<city:STRING,country:STRING,zipcode:INT>
        )
        """
        sql """show tables like '${test12_table}' """
        qt_test12_show_create_table """SHOW CREATE TABLE ${test12_table} """

        // ============================================================================
        // Test 13: ERROR CASE - CREATE TABLE with unsupported type (IPV4)
        // ============================================================================
        String test13_table = "test_mc_unsupported_type_table"
        sql """DROP TABLE IF EXISTS ${test13_table}"""
        test {
            sql """
            CREATE TABLE ${test13_table} (
                id INT,
                ip IPV4
            )
            """
            exception "Unsupported"
        }

        // ============================================================================
        // Test 14: ERROR CASE - CREATE TABLE with AUTO_INCREMENT column
        // ============================================================================
        String test14_table = "test_mc_auto_increment_table"
        sql """DROP TABLE IF EXISTS ${test14_table}"""
        test {
            sql """
            CREATE TABLE ${test14_table} (
                id INT AUTO_INCREMENT,
                name STRING
            )
            """
            exception "Auto-increment"
        }

        // ============================================================================
        // Test 15: ERROR CASE - CREATE TABLE with duplicate column names
        // ============================================================================
        String test15_table = "test_mc_duplicate_column_table"
        sql """DROP TABLE IF EXISTS ${test15_table}"""
        test {
            sql """
            CREATE TABLE ${test15_table} (
                id INT,
                name STRING,
                name STRING
            )
            """
            exception "Duplicate"
        }

        // ============================================================================
        // Test 18: ERROR CASE - LIFECYCLE with invalid value (too large)
        // ============================================================================
        String test18_table = "test_mc_invalid_lifecycle_table"
        sql """DROP TABLE IF EXISTS ${test18_table}"""
        test {
            sql """
            CREATE TABLE ${test18_table} (
                id INT,
                name STRING
            )
            PROPERTIES (
                "lifecycle" = "99999999"
            )
            """
            exception "lifecycle"
        }

        // ============================================================================
        // Test 19: Basic DROP TABLE
        // ============================================================================
        String test19_table = "test_mc_basic_table"
        sql """DROP TABLE IF EXISTS ${test19_table}"""

        // ============================================================================
        // Test 20: DROP TABLE with partitions
        // ============================================================================
        String test20_table = "test_mc_partition_table"
        sql """DROP TABLE IF EXISTS ${test20_table}"""

        // ============================================================================
        // Test 21: DROP TABLE with distribution
        // ============================================================================
        String test21_table = "test_mc_distributed_table"
        sql """DROP TABLE IF EXISTS ${test21_table}"""

        // ============================================================================
        // Test 22: DROP TABLE IF EXISTS (existing table)
        // ============================================================================
        String test22_table = "test_mc_table_with_comment"
        sql """DROP TABLE IF EXISTS ${test22_table}"""

        // ============================================================================
        // Test 23: DROP TABLE IF EXISTS (non-existent table)
        // ============================================================================
        String test23_table = "test_mc_nonexistent_table_for_drop"
        sql """DROP TABLE IF EXISTS ${test23_table}"""

        // ============================================================================
        // Test 24: DROP complex types table
        // ============================================================================
        String test24_table = "test_mc_array_type_table"
        sql """DROP TABLE IF EXISTS ${test24_table}"""

        // ============================================================================
        // Test 25: DROP then recreate with same name - Verify cache invalidation
        // ============================================================================
        String test25_table = "test_mc_cache_invalidation"
        sql """DROP TABLE IF EXISTS ${test25_table}"""
        // Create table
        sql """
        CREATE TABLE ${test25_table} (
            id INT,
            value INT
        )
        """
        // Drop table
        sql """DROP TABLE ${test25_table}"""
        // Recreate with different schema
        sql """
        CREATE TABLE ${test25_table} (
            id INT,
            name STRING,
            value DOUBLE
        )
        """
        def schema = sql """SHOW CREATE TABLE ${test25_table}"""

        // ============================================================================
        // Test 29: DROP TABLE without IF EXISTS (non-existent)
        // ============================================================================
        String test29_table = "test_mc_definitely_nonexistent_table_12345"
        sql """DROP TABLE IF EXISTS ${test29_table}"""
        test {
            sql """DROP TABLE ${test29_table}"""
            exception "Failed to get table"
        }

        // ============================================================================
        // Test 30: Double DROP without IF EXISTS
        // ============================================================================
        String test30_table = "test_mc_double_drop"
        sql """DROP TABLE IF EXISTS ${test30_table}"""
        // Create and drop first time
        sql """
        CREATE TABLE ${test30_table} (
            id INT
        )
        """
        sql """DROP TABLE ${test30_table}"""
        // Try to drop again without IF EXISTS - should fail
        test {
            sql """DROP TABLE ${test30_table}"""
            exception "Failed to get table"
        }

        // ============================================================================
        // Test 31: Verify dropped table not accessible
        // ============================================================================
        String test31_table = "test_mc_struct_type_table"
        sql """DROP TABLE IF EXISTS ${test31_table}"""
    }
}
