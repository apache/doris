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

suite("test_iceberg_rewrite_data_files_expression_conversion", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String catalog_name = "test_iceberg_rewrite_expression_conversion"
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
    // Test: Expression conversion coverage tests
    // 
    // Test strategy:
    // These tests verify that various expression types can be successfully converted
    // to Iceberg expressions without throwing exceptions. The tables are created using
    // Doris (not pre-created in Docker) and we don't care about filter effectiveness,
    // only that the expression conversion succeeds.
    // 
    // Tests cover:
    // 1. Various column types (BOOLEAN, INT, BIGINT, FLOAT, DOUBLE, STRING, DATE, DATETIME, DECIMAL)
    // 2. Various expression types (=, >, >=, <, <=, IN, BETWEEN, AND, OR, NOT, IS NULL)
    // =====================================================================================
    logger.info("Starting expression conversion coverage tests")
    
    def expr_test_table = "test_rewrite_where_expression_conversion"
    
    // Create a test table with various column types
    sql """DROP TABLE IF EXISTS ${db_name}.${expr_test_table}"""
    sql """
        CREATE TABLE ${db_name}.${expr_test_table} (
            id BIGINT,
            name STRING,
            age INT,
            salary DOUBLE,
            score FLOAT,
            is_active BOOLEAN,
            birth_date DATE,
            created_at DATETIME,
            balance DECIMAL(10, 2)
        ) ENGINE=iceberg
    """
    logger.info("Created expression conversion test table: ${expr_test_table}")
    
    // Insert some test data
    sql """
        INSERT INTO ${db_name}.${expr_test_table} VALUES
        (1, 'Alice', 25, 50000.0, 85.5, true, '1998-01-01', '2024-01-01 10:00:00', 1000.50),
        (2, 'Bob', 30, 60000.0, 90.0, false, '1993-02-15', '2024-01-02 11:00:00', 2000.75),
        (3, 'Charlie', 35, 70000.0, 92.5, true, '1988-03-20', '2024-01-03 12:00:00', 3000.00)
    """
    
    logger.info("Inserted test data for expression conversion tests")
    
    // Test expression conversion - should not throw exceptions
    // We only check that the command executes without exception,
    // not that the filtering works correctly
    
    // Test 1: Basic comparison operators on different column types
    logger.info("Test: Basic comparison operators (=, >, >=, <, <=)")
    
    // BIGINT: =
    try {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${expr_test_table} 
            EXECUTE rewrite_data_files("target-file-size-bytes" = "10485760") WHERE id = 1
        """
        logger.info("✓ BIGINT = conversion succeeded")
    } catch (Exception e) {
        logger.error("✗ BIGINT = conversion failed: ${e.getMessage()}")
        throw e
    }
    
    // INT: >
    try {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${expr_test_table} 
            EXECUTE rewrite_data_files("target-file-size-bytes" = "10485760") WHERE age > 25
        """
        logger.info("✓ INT > conversion succeeded")
    } catch (Exception e) {
        logger.error("✗ INT > conversion failed: ${e.getMessage()}")
        throw e
    }
    
    // DOUBLE: >=
    try {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${expr_test_table} 
            EXECUTE rewrite_data_files("target-file-size-bytes" = "10485760") WHERE salary >= 60000.0
        """
        logger.info("✓ DOUBLE >= conversion succeeded")
    } catch (Exception e) {
        logger.error("✗ DOUBLE >= conversion failed: ${e.getMessage()}")
        throw e
    }
    
    // FLOAT: <
    try {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${expr_test_table} 
            EXECUTE rewrite_data_files("target-file-size-bytes" = "10485760") WHERE score < 90.0
        """
        logger.info("✓ FLOAT < conversion succeeded")
    } catch (Exception e) {
        logger.error("✗ FLOAT < conversion failed: ${e.getMessage()}")
        throw e
    }
    
    // STRING: <=
    try {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${expr_test_table} 
            EXECUTE rewrite_data_files("target-file-size-bytes" = "10485760") WHERE name <= 'Charlie'
        """
        logger.info("✓ STRING <= conversion succeeded")
    } catch (Exception e) {
        logger.error("✗ STRING <= conversion failed: ${e.getMessage()}")
        throw e
    }
    
    // BOOLEAN: =
    try {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${expr_test_table} 
            EXECUTE rewrite_data_files("target-file-size-bytes" = "10485760") WHERE is_active = true
        """
        logger.info("✓ BOOLEAN = conversion succeeded")
    } catch (Exception e) {
        logger.error("✗ BOOLEAN = conversion failed: ${e.getMessage()}")
        throw e
    }
    
    // DATE: =
    try {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${expr_test_table} 
            EXECUTE rewrite_data_files("target-file-size-bytes" = "10485760") WHERE birth_date = '1998-01-01'
        """
        logger.info("✓ DATE = conversion succeeded")
    } catch (Exception e) {
        logger.error("✗ DATE = conversion failed: ${e.getMessage()}")
        throw e
    }
    
    // DATETIME: >
    try {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${expr_test_table} 
            EXECUTE rewrite_data_files("target-file-size-bytes" = "10485760") WHERE created_at > '2024-01-01 10:00:00'
        """
        logger.info("✓ DATETIME > conversion succeeded")
    } catch (Exception e) {
        logger.error("✗ DATETIME > conversion failed: ${e.getMessage()}")
        throw e
    }
    
    // DECIMAL: >=
    try {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${expr_test_table} 
            EXECUTE rewrite_data_files("target-file-size-bytes" = "10485760") WHERE balance >= 2000.0
        """
        logger.info("✓ DECIMAL >= conversion succeeded")
    } catch (Exception e) {
        logger.error("✗ DECIMAL >= conversion failed: ${e.getMessage()}")
        throw e
    }
    
    // Test 2: IN predicate on different column types
    logger.info("Test: IN predicate")
    
    // INT: IN
    try {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${expr_test_table} 
            EXECUTE rewrite_data_files("target-file-size-bytes" = "10485760") WHERE age IN (25, 30, 35)
        """
        logger.info("✓ INT IN conversion succeeded")
    } catch (Exception e) {
        logger.error("✗ INT IN conversion failed: ${e.getMessage()}")
        throw e
    }
    
    // STRING: IN
    try {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${expr_test_table} 
            EXECUTE rewrite_data_files("target-file-size-bytes" = "10485760") WHERE name IN ('Alice', 'Bob', 'Charlie')
        """
        logger.info("✓ STRING IN conversion succeeded")
    } catch (Exception e) {
        logger.error("✗ STRING IN conversion failed: ${e.getMessage()}")
        throw e
    }
    
    // DOUBLE: IN
    try {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${expr_test_table} 
            EXECUTE rewrite_data_files("target-file-size-bytes" = "10485760") WHERE salary IN (50000.0, 60000.0, 70000.0)
        """
        logger.info("✓ DOUBLE IN conversion succeeded")
    } catch (Exception e) {
        logger.error("✗ DOUBLE IN conversion failed: ${e.getMessage()}")
        throw e
    }
    
    // Test 3: BETWEEN predicate on different column types
    logger.info("Test: BETWEEN predicate")
    
    // BIGINT: BETWEEN
    try {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${expr_test_table} 
            EXECUTE rewrite_data_files("target-file-size-bytes" = "10485760") WHERE id BETWEEN 1 AND 3
        """
        logger.info("✓ BIGINT BETWEEN conversion succeeded")
    } catch (Exception e) {
        logger.error("✗ BIGINT BETWEEN conversion failed: ${e.getMessage()}")
        throw e
    }
    
    // INT: BETWEEN
    try {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${expr_test_table} 
            EXECUTE rewrite_data_files("target-file-size-bytes" = "10485760") WHERE age BETWEEN 25 AND 35
        """
        logger.info("✓ INT BETWEEN conversion succeeded")
    } catch (Exception e) {
        logger.error("✗ INT BETWEEN conversion failed: ${e.getMessage()}")
        throw e
    }
    
    // DOUBLE: BETWEEN
    try {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${expr_test_table} 
            EXECUTE rewrite_data_files("target-file-size-bytes" = "10485760") WHERE salary BETWEEN 50000.0 AND 70000.0
        """
        logger.info("✓ DOUBLE BETWEEN conversion succeeded")
    } catch (Exception e) {
        logger.error("✗ DOUBLE BETWEEN conversion failed: ${e.getMessage()}")
        throw e
    }
    
    // STRING: BETWEEN
    try {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${expr_test_table} 
            EXECUTE rewrite_data_files("target-file-size-bytes" = "10485760") WHERE name BETWEEN 'Alice' AND 'Charlie'
        """
        logger.info("✓ STRING BETWEEN conversion succeeded")
    } catch (Exception e) {
        logger.error("✗ STRING BETWEEN conversion failed: ${e.getMessage()}")
        throw e
    }
    
    // DATE: BETWEEN
    try {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${expr_test_table} 
            EXECUTE rewrite_data_files("target-file-size-bytes" = "10485760") WHERE birth_date BETWEEN '1993-01-01' AND '1998-12-31'
        """
        logger.info("✓ DATE BETWEEN conversion succeeded")
    } catch (Exception e) {
        logger.error("✗ DATE BETWEEN conversion failed: ${e.getMessage()}")
        throw e
    }
    
    // DATETIME: BETWEEN
    try {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${expr_test_table} 
            EXECUTE rewrite_data_files("target-file-size-bytes" = "10485760") WHERE created_at BETWEEN '2024-01-01 10:00:00' AND '2024-01-03 12:00:00'
        """
        logger.info("✓ DATETIME BETWEEN conversion succeeded")
    } catch (Exception e) {
        logger.error("✗ DATETIME BETWEEN conversion failed: ${e.getMessage()}")
        throw e
    }
    
    // DECIMAL: BETWEEN
    try {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${expr_test_table} 
            EXECUTE rewrite_data_files("target-file-size-bytes" = "10485760") WHERE balance BETWEEN 1000.0 AND 3000.0
        """
        logger.info("✓ DECIMAL BETWEEN conversion succeeded")
    } catch (Exception e) {
        logger.error("✗ DECIMAL BETWEEN conversion failed: ${e.getMessage()}")
        throw e
    }
    
    // Test 4: Logical operators (AND, OR, NOT)
    logger.info("Test: Logical operators (AND, OR, NOT)")
    
    // AND
    try {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${expr_test_table} 
            EXECUTE rewrite_data_files("target-file-size-bytes" = "10485760") WHERE id > 1 AND age < 35
        """
        logger.info("✓ AND conversion succeeded")
    } catch (Exception e) {
        logger.error("✗ AND conversion failed: ${e.getMessage()}")
        throw e
    }
    
    // OR
    try {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${expr_test_table} 
            EXECUTE rewrite_data_files("target-file-size-bytes" = "10485760") WHERE id = 1 OR id = 2
        """
        logger.info("✓ OR conversion succeeded")
    } catch (Exception e) {
        logger.error("✗ OR conversion failed: ${e.getMessage()}")
        throw e
    }
    
    // NOT
    try {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${expr_test_table} 
            EXECUTE rewrite_data_files("target-file-size-bytes" = "10485760") WHERE NOT id = 1
        """
        logger.info("✓ NOT conversion succeeded")
    } catch (Exception e) {
        logger.error("✗ NOT conversion failed: ${e.getMessage()}")
        throw e
    }
    
    // Complex AND with OR
    try {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${expr_test_table} 
            EXECUTE rewrite_data_files("target-file-size-bytes" = "10485760") WHERE (id > 1 AND age < 35) OR salary > 65000.0
        """
        logger.info("✓ Complex AND/OR conversion succeeded")
    } catch (Exception e) {
        logger.error("✗ Complex AND/OR conversion failed: ${e.getMessage()}")
        throw e
    }
    
    // Test 5: IS NULL
    logger.info("Test: IS NULL")
    
    // Note: We don't have NULL values in our test data, but the conversion should still work
    try {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${expr_test_table} 
            EXECUTE rewrite_data_files("target-file-size-bytes" = "10485760") WHERE name IS NULL
        """
        logger.info("✓ IS NULL conversion succeeded")
    } catch (Exception e) {
        logger.error("✗ IS NULL conversion failed: ${e.getMessage()}")
        throw e
    }
    
    // Test 6: Combined expressions with different operators
    logger.info("Test: Combined expressions")
    
    // BETWEEN AND IN
    try {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${expr_test_table} 
            EXECUTE rewrite_data_files("target-file-size-bytes" = "10485760") WHERE id BETWEEN 1 AND 3 AND name IN ('Alice', 'Bob')
        """
        logger.info("✓ BETWEEN AND IN combination succeeded")
    } catch (Exception e) {
        logger.error("✗ BETWEEN AND IN combination failed: ${e.getMessage()}")
        throw e
    }
    
    // Multiple AND conditions
    try {
        sql """
            ALTER TABLE ${catalog_name}.${db_name}.${expr_test_table} 
            EXECUTE rewrite_data_files("target-file-size-bytes" = "10485760") WHERE id >= 1 AND age <= 35 AND salary > 50000.0 AND is_active = true
        """
        logger.info("✓ Multiple AND conditions succeeded")
    } catch (Exception e) {
        logger.error("✗ Multiple AND conditions failed: ${e.getMessage()}")
        throw e
    }
    
    logger.info("Expression conversion coverage tests completed successfully")
    
    // Clean up
    sql """DROP TABLE IF EXISTS ${db_name}.${expr_test_table}"""
    logger.info("Expression conversion test cleanup completed")
}

