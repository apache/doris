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

suite("test_paimon_sql_block_rule", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String catalog_name = "test_paimon_sql_block_rule_ctl"
    String db_name = "test_paimon_partition"
    String table_name = "sales_by_date"
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    sql """drop catalog if exists ${catalog_name}"""
    sql """
        CREATE CATALOG ${catalog_name} PROPERTIES (
            "type" = "paimon",
            "warehouse" = "s3://warehouse/wh",
            "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.path.style.access" = "true"
        );
    """

    sql """switch ${catalog_name}"""
    sql """use ${db_name}"""
    // Use an existing Paimon table; DDL is not supported for this catalog.

    // Clean up existing rules
    sql """drop sql_block_rule if exists paimon_partition_rule"""
    sql """drop sql_block_rule if exists paimon_split_rule"""
    sql """drop sql_block_rule if exists paimon_cardinality_rule"""
    sql """drop sql_block_rule if exists paimon_regex_rule"""

    // Test 1: Partition number limit rule
    sql """create sql_block_rule paimon_partition_rule properties("partition_num" = "1", "global" = "true", "enable" = "true");"""
    
    test {
        sql """select * from ${table_name}"""
        exception """sql hits sql block rule: paimon_partition_rule, reach partition_num : 1"""
    }

    // Test EXPLAIN should not be blocked
    sql """explain select * from ${table_name}"""

    sql """drop sql_block_rule paimon_partition_rule"""

    // Test 2: Split number limit rule (equivalent to tablet_num for external tables)
    sql """create sql_block_rule paimon_split_rule properties("tablet_num" = "1", "global" = "true", "enable" = "true");"""
    
    test {
        sql """select * from ${table_name}"""
        exception """sql hits sql block rule: paimon_split_rule, reach tablet_num : 1"""
    }

    // Test EXPLAIN should not be blocked
    sql """explain select * from ${table_name}"""

    sql """drop sql_block_rule paimon_split_rule"""

    // Test 3: Cardinality limit rule
    sql """create sql_block_rule paimon_cardinality_rule properties("cardinality" = "1", "global" = "true", "enable" = "true");"""
    
    test {
        sql """select * from ${table_name}"""
        exception """sql hits sql block rule: paimon_cardinality_rule, reach cardinality : 1"""
    }

    // Test EXPLAIN should not be blocked
    sql """explain select * from ${table_name}"""

    sql """drop sql_block_rule paimon_cardinality_rule"""

    // Test 4: Regex match rule
    sql """create sql_block_rule paimon_regex_rule properties("sql" = "SELECT \\\\* FROM ${table_name}", "global" = "true", "enable" = "true");"""
    
    test {
        sql """SELECT * FROM ${table_name}"""
        exception """sql match regex sql block rule: paimon_regex_rule"""
    }

    // Test EXPLAIN should not be blocked by regex rule
    sql """EXPLAIN SELECT * FROM ${table_name}"""

    sql """drop sql_block_rule paimon_regex_rule"""

    // Test 5: User-specific rules
    sql """drop sql_block_rule if exists paimon_user_rule"""
    sql """create sql_block_rule paimon_user_rule properties("cardinality" = "1", "global" = "false");"""
    
    sql """drop user if exists paimon_block_user"""
    sql """create user paimon_block_user;"""
    sql """SET PROPERTY FOR 'paimon_block_user' 'sql_block_rules' = 'paimon_user_rule';"""
    sql """grant all on *.*.* to paimon_block_user;"""
    
    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO paimon_block_user;""";
    }

    // Login as paimon_block_user and test
    def result = connect('paimon_block_user', '', context.config.jdbcUrl) {
        test {
            sql """select * from ${catalog_name}.${db_name}.${table_name}"""
            exception """sql hits sql block rule: paimon_user_rule, reach cardinality : 1"""
        }
        // EXPLAIN should not be blocked
        sql """explain select * from ${catalog_name}.${db_name}.${table_name}"""
    }

    // Cleanup
    sql """drop user if exists paimon_block_user"""
    sql """drop sql_block_rule if exists paimon_user_rule"""
    sql """drop catalog if exists ${catalog_name}"""
}
