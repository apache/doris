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

suite("test_external_sql_block_rule", "external_docker,hive,external_docker_hive,p0,external") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String hms_port = context.config.otherConfigs.get("hive2HmsPort")
    String catalog_name = "test_hive2_external_sql_block_rule"

    sql """drop catalog if exists ${catalog_name}"""

    sql """CREATE CATALOG ${catalog_name} PROPERTIES (
            'type'='hms',
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
            'hadoop.username' = 'hive'
        );"""

    sql "use ${catalog_name}.`default`";
    qt_sql01 """select * from parquet_partition_table order by l_linenumber,l_orderkey limit 10;"""

    // Clean up existing rules and users
    sql """drop sql_block_rule if exists hive_partition_rule"""
    sql """drop sql_block_rule if exists hive_split_rule"""
    sql """drop sql_block_rule if exists hive_cardinality_rule"""
    sql """drop sql_block_rule if exists hive_regex_rule"""
    sql """drop user if exists hive_block_user1"""
    sql """drop user if exists hive_block_user2"""
    sql """drop user if exists hive_block_user3"""
    sql """drop user if exists hive_block_user4"""

    // Create non-global rules (won't affect other parallel tests)
    sql """create sql_block_rule hive_partition_rule properties("partition_num" = "3", "global" = "false");"""
    sql """create sql_block_rule hive_split_rule properties("tablet_num" = "3", "global" = "false");"""
    sql """create sql_block_rule hive_cardinality_rule properties("cardinality" = "3", "global" = "false");"""
    sql """create sql_block_rule hive_regex_rule properties("sql" = "SELECT \\\\*", "global" = "false");"""

    // Create test users and bind rules
    sql """create user hive_block_user1;"""
    sql """SET PROPERTY FOR 'hive_block_user1' 'sql_block_rules' = 'hive_partition_rule';"""
    sql """grant all on *.*.* to hive_block_user1;"""

    sql """create user hive_block_user2;"""
    sql """SET PROPERTY FOR 'hive_block_user2' 'sql_block_rules' = 'hive_split_rule';"""
    sql """grant all on *.*.* to hive_block_user2;"""

    sql """create user hive_block_user3;"""
    sql """SET PROPERTY FOR 'hive_block_user3' 'sql_block_rules' = 'hive_cardinality_rule';"""
    sql """grant all on *.*.* to hive_block_user3;"""

    sql """create user hive_block_user4;"""
    sql """SET PROPERTY FOR 'hive_block_user4' 'sql_block_rules' = 'hive_regex_rule';"""
    sql """grant all on *.*.* to hive_block_user4;"""

    // cloud-mode: grant cluster privileges
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO hive_block_user1;"""
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO hive_block_user2;"""
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO hive_block_user3;"""
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO hive_block_user4;"""
    }

    // Test 1: partition_num rule
    connect('hive_block_user1', '', context.config.jdbcUrl) {
        test {
            sql """select * from ${catalog_name}.`default`.parquet_partition_table order by l_linenumber limit 10;"""
            exception """sql hits sql block rule: hive_partition_rule, reach partition_num : 3"""
        }
        // Test EXPLAIN should not be blocked
        sql """explain select * from ${catalog_name}.`default`.parquet_partition_table order by l_linenumber limit 10;"""
    }

    // Test 2: tablet_num (split) rule
    connect('hive_block_user2', '', context.config.jdbcUrl) {
        test {
            sql """select * from ${catalog_name}.`default`.parquet_partition_table order by l_linenumber limit 10;"""
            exception """sql hits sql block rule: hive_split_rule, reach tablet_num : 3"""
        }
        // Test EXPLAIN should not be blocked
        sql """explain select * from ${catalog_name}.`default`.parquet_partition_table order by l_linenumber limit 10;"""
    }

    // Test 3: cardinality rule
    connect('hive_block_user3', '', context.config.jdbcUrl) {
        test {
            sql """select * from ${catalog_name}.`default`.parquet_partition_table order by l_linenumber limit 10;"""
            exception """sql hits sql block rule: hive_cardinality_rule, reach cardinality : 3"""
        }
        // Test EXPLAIN should not be blocked
        sql """explain select * from ${catalog_name}.`default`.parquet_partition_table order by l_linenumber limit 10;"""
    }

    // Test 4: regex rule
    connect('hive_block_user4', '', context.config.jdbcUrl) {
        test {
            sql """SELECT * FROM ${catalog_name}.`default`.parquet_partition_table limit 10;"""
            exception """sql match regex sql block rule: hive_regex_rule"""
        }
        // Test EXPLAIN should not be blocked by regex rule
        sql """EXPLAIN SELECT * FROM ${catalog_name}.`default`.parquet_partition_table limit 10;"""
    }

    // Cleanup
    sql """drop user if exists hive_block_user1"""
    sql """drop user if exists hive_block_user2"""
    sql """drop user if exists hive_block_user3"""
    sql """drop user if exists hive_block_user4"""
    sql """drop sql_block_rule if exists hive_partition_rule"""
    sql """drop sql_block_rule if exists hive_split_rule"""
    sql """drop sql_block_rule if exists hive_cardinality_rule"""
    sql """drop sql_block_rule if exists hive_regex_rule"""
    sql """drop catalog if exists ${catalog_name}"""
}
