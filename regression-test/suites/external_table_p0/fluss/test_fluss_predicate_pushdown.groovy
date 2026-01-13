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

suite("test_fluss_predicate_pushdown", "p0,external,fluss,external_docker") {

    String enabled = context.config.otherConfigs.get("enableFlussTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Fluss test is not enabled, skipping")
        return
    }

    String catalog_name = "fluss_pushdown_catalog"
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
    // Test: Equality Predicate Pushdown
    // ============================================
    explain {
        sql """SELECT * FROM all_types WHERE id = 1"""
        contains "FLUSS_SCAN_NODE"
    }

    // ============================================
    // Test: Range Predicate Pushdown
    // ============================================
    explain {
        sql """SELECT * FROM all_types WHERE id > 10 AND id < 100"""
        contains "FLUSS_SCAN_NODE"
    }

    // ============================================
    // Test: String Predicate Pushdown
    // ============================================
    explain {
        sql """SELECT * FROM all_types WHERE string_col = 'test'"""
        contains "FLUSS_SCAN_NODE"
    }

    // ============================================
    // Test: IN Predicate Pushdown
    // ============================================
    explain {
        sql """SELECT * FROM all_types WHERE id IN (1, 2, 3, 4, 5)"""
        contains "FLUSS_SCAN_NODE"
    }

    // ============================================
    // Test: IS NULL Predicate
    // ============================================
    explain {
        sql """SELECT * FROM all_types WHERE string_col IS NULL"""
        contains "FLUSS_SCAN_NODE"
    }

    // ============================================
    // Test: IS NOT NULL Predicate
    // ============================================
    explain {
        sql """SELECT * FROM all_types WHERE string_col IS NOT NULL"""
        contains "FLUSS_SCAN_NODE"
    }

    // ============================================
    // Test: Compound Predicates (AND)
    // ============================================
    explain {
        sql """SELECT * FROM all_types WHERE id > 0 AND string_col IS NOT NULL"""
        contains "FLUSS_SCAN_NODE"
    }

    // ============================================
    // Test: Compound Predicates (OR)
    // ============================================
    explain {
        sql """SELECT * FROM all_types WHERE id = 1 OR id = 2"""
        contains "FLUSS_SCAN_NODE"
    }

    // ============================================
    // Test: Date/Time Predicates
    // ============================================
    explain {
        sql """SELECT * FROM all_types WHERE date_col > '2024-01-01'"""
        contains "FLUSS_SCAN_NODE"
    }

    // ============================================
    // Test: Partition Pruning (Partitioned Table)
    // ============================================
    explain {
        sql """SELECT * FROM partitioned_table WHERE dt = '2024-01-01'"""
        contains "FLUSS_SCAN_NODE"
    }

    // ============================================
    // Test: Column Projection
    // ============================================
    explain {
        sql """SELECT id, string_col FROM all_types"""
        contains "FLUSS_SCAN_NODE"
    }

    // ============================================
    // Cleanup
    // ============================================
    sql """DROP CATALOG IF EXISTS ${catalog_name}"""
}
