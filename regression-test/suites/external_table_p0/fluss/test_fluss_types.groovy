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

suite("test_fluss_types", "p0,external,fluss,external_docker") {

    String enabled = context.config.otherConfigs.get("enableFlussTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Fluss test is not enabled, skipping")
        return
    }

    String catalog_name = "fluss_types_catalog"
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
    // Test: Boolean Type
    // ============================================
    def boolResult = sql """SELECT id, bool_col FROM all_types WHERE bool_col = true LIMIT 5"""
    logger.info("Boolean type result: ${boolResult}")

    // ============================================
    // Test: Integer Types (TINYINT, SMALLINT, INT, BIGINT)
    // ============================================
    def intResult = sql """
        SELECT id, tinyint_col, smallint_col, int_col, bigint_col 
        FROM all_types 
        WHERE tinyint_col IS NOT NULL 
        LIMIT 5
    """
    logger.info("Integer types result: ${intResult}")

    // ============================================
    // Test: Floating Point Types (FLOAT, DOUBLE)
    // ============================================
    def floatResult = sql """
        SELECT id, float_col, double_col 
        FROM all_types 
        WHERE float_col IS NOT NULL 
        LIMIT 5
    """
    logger.info("Float types result: ${floatResult}")

    // ============================================
    // Test: Decimal Type
    // ============================================
    def decimalResult = sql """
        SELECT id, decimal_col 
        FROM all_types 
        WHERE decimal_col IS NOT NULL 
        LIMIT 5
    """
    logger.info("Decimal type result: ${decimalResult}")

    // ============================================
    // Test: String Type
    // ============================================
    def stringResult = sql """
        SELECT id, string_col 
        FROM all_types 
        WHERE string_col IS NOT NULL 
        LIMIT 5
    """
    logger.info("String type result: ${stringResult}")

    // ============================================
    // Test: Date Type
    // ============================================
    def dateResult = sql """
        SELECT id, date_col 
        FROM all_types 
        WHERE date_col IS NOT NULL 
        LIMIT 5
    """
    logger.info("Date type result: ${dateResult}")

    // ============================================
    // Test: Timestamp Type
    // ============================================
    def timestampResult = sql """
        SELECT id, timestamp_col 
        FROM all_types 
        WHERE timestamp_col IS NOT NULL 
        LIMIT 5
    """
    logger.info("Timestamp type result: ${timestampResult}")

    // ============================================
    // Test: Type Casting
    // ============================================
    def castResult = sql """
        SELECT 
            CAST(int_col AS BIGINT) as int_to_bigint,
            CAST(float_col AS DOUBLE) as float_to_double,
            CAST(date_col AS STRING) as date_to_string
        FROM all_types
        WHERE int_col IS NOT NULL
        LIMIT 5
    """
    logger.info("Type casting result: ${castResult}")

    // ============================================
    // Test: Aggregation on Numeric Types
    // ============================================
    def aggResult = sql """
        SELECT 
            SUM(int_col) as sum_int,
            AVG(double_col) as avg_double,
            MIN(bigint_col) as min_bigint,
            MAX(float_col) as max_float
        FROM all_types
    """
    logger.info("Aggregation result: ${aggResult}")

    // ============================================
    // Test: Date/Time Functions
    // ============================================
    def dateFunc = sql """
        SELECT 
            id,
            date_col,
            YEAR(date_col) as year_val,
            MONTH(date_col) as month_val,
            DAY(date_col) as day_val
        FROM all_types
        WHERE date_col IS NOT NULL
        LIMIT 5
    """
    logger.info("Date functions result: ${dateFunc}")

    // ============================================
    // Test: Schema Type Verification
    // ============================================
    def schema = sql """DESC all_types"""
    
    def expectedTypes = [
        "id": "INT",
        "bool_col": "BOOLEAN",
        "tinyint_col": "TINYINT",
        "smallint_col": "SMALLINT",
        "int_col": "INT",
        "bigint_col": "BIGINT",
        "float_col": "FLOAT",
        "double_col": "DOUBLE",
        "string_col": "TEXT"
    ]
    
    for (row in schema) {
        String colName = row[0]
        String colType = row[1]
        if (expectedTypes.containsKey(colName)) {
            logger.info("Column ${colName}: expected contains ${expectedTypes[colName]}, got ${colType}")
        }
    }

    // ============================================
    // Cleanup
    // ============================================
    sql """DROP CATALOG IF EXISTS ${catalog_name}"""
}
