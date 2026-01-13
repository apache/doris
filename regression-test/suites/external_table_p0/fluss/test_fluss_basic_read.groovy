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

suite("test_fluss_basic_read", "p0,external,fluss,external_docker") {

    String enabled = context.config.otherConfigs.get("enableFlussTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Fluss test is not enabled, skipping")
        return
    }

    String catalog_name = "fluss_read_catalog"
    String bootstrap_servers = context.config.otherConfigs.get("flussBootstrapServers")
    
    if (bootstrap_servers == null || bootstrap_servers.isEmpty()) {
        bootstrap_servers = "localhost:9123"
    }

    // Setup catalog
    sql """DROP CATALOG IF EXISTS ${catalog_name}"""
    sql """
        CREATE CATALOG ${catalog_name} PROPERTIES (
            "type" = "fluss",
            "bootstrap.servers" = "${bootstrap_servers}"
        );
    """
    sql """USE ${catalog_name}.test_db"""

    // ============================================
    // Test: Basic SELECT *
    // ============================================
    def result1 = sql """SELECT * FROM all_types LIMIT 10"""
    logger.info("SELECT * result: ${result1}")
    
    // ============================================
    // Test: SELECT with Column Projection
    // ============================================
    def result2 = sql """SELECT id, string_col FROM all_types LIMIT 10"""
    logger.info("Projected SELECT result: ${result2}")

    // ============================================
    // Test: SELECT with WHERE clause (equality)
    // ============================================
    def result3 = sql """SELECT * FROM all_types WHERE id = 1"""
    logger.info("WHERE id=1 result: ${result3}")

    // ============================================
    // Test: SELECT with WHERE clause (range)
    // ============================================
    def result4 = sql """SELECT * FROM all_types WHERE id > 0 AND id < 100"""
    logger.info("WHERE range result: ${result4}")

    // ============================================
    // Test: SELECT with ORDER BY
    // ============================================
    def result5 = sql """SELECT id, string_col FROM all_types ORDER BY id LIMIT 10"""
    logger.info("ORDER BY result: ${result5}")
    
    // Verify ordering
    if (result5.size() > 1) {
        for (int i = 1; i < result5.size(); i++) {
            assertTrue(result5[i][0] >= result5[i-1][0], "Results should be ordered by id")
        }
    }

    // ============================================
    // Test: SELECT COUNT(*)
    // ============================================
    def result6 = sql """SELECT COUNT(*) FROM all_types"""
    logger.info("COUNT(*) result: ${result6}")
    assertTrue(result6[0][0] >= 0, "Count should be non-negative")

    // ============================================
    // Test: SELECT with GROUP BY
    // ============================================
    def result7 = sql """SELECT bool_col, COUNT(*) as cnt FROM all_types GROUP BY bool_col"""
    logger.info("GROUP BY result: ${result7}")

    // ============================================
    // Test: SELECT from Partitioned Table
    // ============================================
    def result8 = sql """SELECT * FROM partitioned_table LIMIT 10"""
    logger.info("Partitioned table result: ${result8}")

    // ============================================
    // Test: SELECT from Log Table
    // ============================================
    def result9 = sql """SELECT * FROM log_table LIMIT 10"""
    logger.info("Log table result: ${result9}")

    // ============================================
    // Cleanup
    // ============================================
    sql """DROP CATALOG IF EXISTS ${catalog_name}"""
}
