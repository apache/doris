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

suite("test_fluss_catalog", "p0,external,fluss,external_docker") {

    String enabled = context.config.otherConfigs.get("enableFlussTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Fluss test is not enabled, skipping")
        return
    }

    String catalog_name = "fluss_test_catalog"
    String bootstrap_servers = context.config.otherConfigs.get("flussBootstrapServers")
    
    if (bootstrap_servers == null || bootstrap_servers.isEmpty()) {
        bootstrap_servers = "localhost:9123"
    }

    // ============================================
    // Test: Create Fluss Catalog
    // ============================================
    sql """DROP CATALOG IF EXISTS ${catalog_name}"""
    
    sql """
        CREATE CATALOG ${catalog_name} PROPERTIES (
            "type" = "fluss",
            "bootstrap.servers" = "${bootstrap_servers}"
        );
    """
    
    // Verify catalog was created
    def catalogs = sql """SHOW CATALOGS"""
    assertTrue(catalogs.toString().contains(catalog_name), "Catalog should be created")

    // ============================================
    // Test: List Databases
    // ============================================
    def databases = sql """SHOW DATABASES FROM ${catalog_name}"""
    logger.info("Databases in Fluss catalog: ${databases}")
    assertTrue(databases.size() > 0, "Should have at least one database")
    
    // ============================================
    // Test: Switch to Fluss Catalog
    // ============================================
    sql """USE ${catalog_name}.test_db"""
    
    // ============================================
    // Test: List Tables
    // ============================================
    def tables = sql """SHOW TABLES"""
    logger.info("Tables in test_db: ${tables}")
    assertTrue(tables.size() > 0, "Should have at least one table")

    // ============================================
    // Test: Describe Table
    // ============================================
    def schema = sql """DESC all_types"""
    logger.info("Schema of all_types: ${schema}")
    
    // Verify expected columns exist
    def columnNames = schema.collect { it[0] }
    assertTrue(columnNames.contains("id"), "Should have 'id' column")
    assertTrue(columnNames.contains("string_col"), "Should have 'string_col' column")

    // ============================================
    // Test: Create Catalog with Invalid Properties
    // ============================================
    test {
        sql """
            CREATE CATALOG invalid_fluss_catalog PROPERTIES (
                "type" = "fluss"
            );
        """
        exception "Missing required property"
    }

    // ============================================
    // Test: Catalog Properties with Security
    // ============================================
    sql """DROP CATALOG IF EXISTS secure_fluss_catalog"""
    sql """
        CREATE CATALOG secure_fluss_catalog PROPERTIES (
            "type" = "fluss",
            "bootstrap.servers" = "${bootstrap_servers}",
            "fluss.security.protocol" = "PLAINTEXT"
        );
    """
    sql """DROP CATALOG secure_fluss_catalog"""

    // ============================================
    // Cleanup
    // ============================================
    sql """DROP CATALOG IF EXISTS ${catalog_name}"""
}
