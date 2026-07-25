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

suite("test_mysql_jdbc_create_drop_database", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"

    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return;
    }

    String mysql_port = context.config.otherConfigs.get("mysql_57_port");
    String catalog_name = "mysql_db_test";

    sql """drop catalog if exists ${catalog_name}"""
    sql """create catalog if not exists ${catalog_name} properties(
        "type"="jdbc",
        "user"="root",
        "password"="123456",
        "jdbc_url"="jdbc:mysql://${externalEnvIp}:${mysql_port}",
        "driver_url"="${driver_url}",
        "driver_class"="com.mysql.cj.jdbc.Driver"
    )"""

    String test_db = "test_create_db_test";

    // Clean up any leftover from previous failed runs
    try {
        sql """drop database if exists ${catalog_name}.${test_db}"""
    } catch (Exception e) {
        // ignore
    }

    // ===== Test 1: CREATE DATABASE =====
    logger.info("=== Test 1: CREATE DATABASE ===")
    sql """create database ${catalog_name}.${test_db}"""

    // Verify database exists (cache invalidation is automatic, no need to refresh)
    def dbs = sql("show databases from ${catalog_name}").collect { it[0] as String }
    assertTrue(dbs.contains(test_db), "Database '${test_db}' should exist after CREATE DATABASE")

    // ===== Test 2: CREATE DATABASE again without IF NOT EXISTS should error =====
    logger.info("=== Test 2: CREATE DATABASE on existing DB should error ===")
    test {
        sql """create database ${catalog_name}.${test_db}"""
        exception "database exists"
    }

    // ===== Test 3: DROP DATABASE =====
    logger.info("=== Test 3: DROP DATABASE ===")
    sql """drop database ${catalog_name}.${test_db}"""

    // Verify database no longer exists (cache invalidation is automatic)
    dbs = sql("show databases from ${catalog_name}").collect { it[0] as String }
    assertFalse(dbs.contains(test_db), "Database '${test_db}' should not exist after DROP DATABASE")

    // ===== Test 4: DROP DATABASE on non-existent database should error =====
    logger.info("=== Test 4: DROP DATABASE on non-existent DB should error ===")
    test {
        sql """drop database ${catalog_name}.${test_db}"""
        exception "does not exist"
    }

    // ===== Test 5: DROP DATABASE IF EXISTS =====
    logger.info("=== Test 5: DROP DATABASE IF EXISTS (idempotent) ===")
    // Should not throw even though database doesn't exist
    sql """drop database if exists ${catalog_name}.${test_db}"""

    // ===== Test 6: CREATE DATABASE IF NOT EXISTS =====
    logger.info("=== Test 6: CREATE DATABASE IF NOT EXISTS (idempotent) ===")
    sql """create database if not exists ${catalog_name}.${test_db}"""
    sql """create database if not exists ${catalog_name}.${test_db}"""  // should be idempotent

    // ===== Test 7: CREATE and DROP with mixed-case database name =====
    logger.info("=== Test 7: CREATE/DROP with mixed-case name ===")
    String mixed_case_db = "TestMixedCase_DB"
    sql """create database ${catalog_name}.${mixed_case_db}"""
    dbs = sql("show databases from ${catalog_name}").collect { it[0] as String }
    assertTrue(dbs.contains(mixed_case_db), "Database '${mixed_case_db}' should exist")
    sql """drop database ${catalog_name}.${mixed_case_db}"""
    dbs = sql("show databases from ${catalog_name}").collect { it[0] as String }
    assertFalse(dbs.contains(mixed_case_db), "Database '${mixed_case_db}' should not exist after DROP")
    
    // ===== Test 8: Test with lower_case_meta_names catalog =====
    logger.info("=== Test 8: lower_case_meta_names identifier mapping ===")
    String catalog_lower = "mysql_db_test_lower"

    sql """drop catalog if exists ${catalog_lower}"""
    sql """create catalog ${catalog_lower} properties(
        "type"="jdbc",
        "user"="root",
        "password"="123456",
        "jdbc_url"="jdbc:mysql://${externalEnvIp}:${mysql_port}",
        "driver_url"="${driver_url}",
        "driver_class"="com.mysql.cj.jdbc.Driver",
        "lower_case_meta_names"="true",
        "meta_names_mapping" = '{"databases": [{"remoteDatabase": "DORIS","mapping": "doris_1"},{"remoteDatabase": "Doris","mapping": "doris_2"},{"remoteDatabase": "doris","mapping": "doris_3"}],"tables": [{"remoteDatabase": "Doris","remoteTable": "DORIS","mapping": "doris_1"},{"remoteDatabase": "Doris","remoteTable": "Doris","mapping": "doris_2"},{"remoteDatabase": "Doris","remoteTable": "doris","mapping": "doris_3"}]}'
    )"""

    String mixed_case_db_lower = "MixedCaseTestDB"

    // Create database with mixed case
    sql """create database ${catalog_lower}.${mixed_case_db_lower}"""

    // Verify database exists (cache invalidation is automatic)
    dbs = sql("show databases from ${catalog_lower}").collect { it[0] as String }

    // With lower_case_meta_names=true, the local name should be lowercase
    assertTrue(dbs.contains(mixed_case_db_lower.toLowerCase()),
        "Database '${mixed_case_db_lower.toLowerCase()}' should exist in lowercase")

    // Drop using the normal name 
    sql """drop database if exists ${catalog_lower}.${mixed_case_db_lower}"""

    // Verify database is gone (cache invalidation is automatic)
    dbs = sql("show databases from ${catalog_lower}").collect { it[0] as String }
    assertFalse(dbs.contains(mixed_case_db_lower.toLowerCase()),
        "Database should not exist after DROP")

    // Create database with mixed case
    sql """create database ${catalog_lower}.${mixed_case_db_lower}"""
    // Drop using the lowercase local name - this should resolve to the correct remote name
    sql """drop database ${catalog_lower}.${mixed_case_db_lower.toLowerCase()}"""

    // Verify database is gone (cache invalidation is automatic)
    dbs = sql("show databases from ${catalog_lower}").collect { it[0] as String }
    assertFalse(dbs.contains(mixed_case_db_lower.toLowerCase()),
        "Database should not exist after DROP")

    // Cleanup
    sql """drop catalog if exists ${catalog_lower}"""
    sql """drop database if exists ${catalog_name}.${test_db}"""
    sql """drop catalog if exists ${catalog_name}"""

}

