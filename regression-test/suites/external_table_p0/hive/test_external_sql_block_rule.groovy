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

suite("test_external_sql_block_rule", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String hms_port = context.config.otherConfigs.get("hive2HmsPort")
    String catalog_name = "test_hive2_external_sql_block_rule"
    String db_name = "test_external_sql_block_rule_db"
    String table_name = "test_external_sql_block_rule_tbl"

    sql """drop catalog if exists ${catalog_name}"""

    sql """CREATE CATALOG ${catalog_name} PROPERTIES (
            'type'='hms',
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
            'hadoop.username' = 'hive'
        );"""

    sql """switch ${catalog_name}"""
    sql """CREATE DATABASE IF NOT EXISTS ${catalog_name}.${db_name}"""
    sql """use `${catalog_name}`.`${db_name}`"""
    sql """DROP TABLE IF EXISTS ${table_name}"""
    sql """
        CREATE TABLE ${catalog_name}.${db_name}.${table_name} (
            `id` INT,
            `val` INT,
            `pt1` STRING,
            `pt2` STRING
        ) ENGINE=hive
        PARTITION BY LIST (pt1, pt2) ()
        PROPERTIES (
            'file_format'='parquet'
        )
    """
    sql """
        INSERT INTO ${catalog_name}.${db_name}.${table_name} (id, val, pt1, pt2) VALUES
        (1, 10, 'a', 'x'),
        (2, 20, 'a', 'y'),
        (3, 30, 'b', 'x'),
        (4, 40, 'b', 'y')
    """
    def baseRows = sql """
        SELECT id, val, pt1, pt2
        FROM ${catalog_name}.${db_name}.${table_name}
        ORDER BY id
    """
    assertEquals(4, baseRows.size())
    assertEquals("1", baseRows[0][0].toString())
    assertEquals("a", baseRows[0][2].toString())
    assertEquals("4", baseRows[3][0].toString())
    assertEquals("y", baseRows[3][3].toString())

    // Clean up existing rules and users
    sql """drop sql_block_rule if exists hive_partition_rule"""
    sql """drop sql_block_rule if exists hive_split_rule"""
    sql """drop sql_block_rule if exists hive_cardinality_rule"""
    sql """drop sql_block_rule if exists hive_regex_rule"""
    sql """drop sql_block_rule if exists hive_require_partition_filter_rule"""
    sql """drop user if exists hive_block_user1"""
    sql """drop user if exists hive_block_user2"""
    sql """drop user if exists hive_block_user3"""
    sql """drop user if exists hive_block_user4"""
    sql """drop user if exists hive_block_user5"""

    // Create non-global rules (won't affect other parallel tests)
    sql """create sql_block_rule hive_partition_rule properties("partition_num" = "3", "global" = "false");"""
    sql """create sql_block_rule hive_split_rule properties("tablet_num" = "3", "global" = "false");"""
    sql """create sql_block_rule hive_cardinality_rule properties("cardinality" = "3", "global" = "false");"""
    sql """create sql_block_rule hive_regex_rule properties("sql" = "SELECT \\\\*", "global" = "false");"""
    sql """create sql_block_rule hive_require_partition_filter_rule
            properties("require_partition_filter" = "true", "global" = "false");"""

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

    sql """create user hive_block_user5;"""
    sql """SET PROPERTY FOR 'hive_block_user5' 'sql_block_rules' = 'hive_require_partition_filter_rule';"""
    sql """grant all on *.*.* to hive_block_user5;"""

    // cloud-mode: grant cluster privileges
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO hive_block_user1;"""
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO hive_block_user2;"""
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO hive_block_user3;"""
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO hive_block_user4;"""
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO hive_block_user5;"""
    }

    // Test 1: partition_num rule
    connect('hive_block_user1', '', context.config.jdbcUrl) {
        test {
            sql """select * from ${catalog_name}.${db_name}.${table_name} order by id limit 10;"""
            exception """sql hits sql block rule: hive_partition_rule, reach partition_num : 3"""
        }
        // Test EXPLAIN should not be blocked
        sql """explain select * from ${catalog_name}.${db_name}.${table_name} order by id limit 10;"""
    }

    // Test 2: tablet_num (split) rule
    connect('hive_block_user2', '', context.config.jdbcUrl) {
        test {
            sql """select * from ${catalog_name}.${db_name}.${table_name} order by id limit 10;"""
            exception """sql hits sql block rule: hive_split_rule, reach tablet_num : 3"""
        }
        // Test EXPLAIN should not be blocked
        sql """explain select * from ${catalog_name}.${db_name}.${table_name} order by id limit 10;"""
    }

    // Test 3: cardinality rule
    connect('hive_block_user3', '', context.config.jdbcUrl) {
        test {
            sql """select * from ${catalog_name}.${db_name}.${table_name} order by id limit 10;"""
            exception """sql hits sql block rule: hive_cardinality_rule, reach cardinality : 3"""
        }
        // Test EXPLAIN should not be blocked
        sql """explain select * from ${catalog_name}.${db_name}.${table_name} order by id limit 10;"""
    }

    // Test 4: regex rule
    connect('hive_block_user4', '', context.config.jdbcUrl) {
        test {
            sql """SELECT * FROM ${catalog_name}.${db_name}.${table_name} limit 10;"""
            exception """sql match regex sql block rule: hive_regex_rule"""
        }
        // Test EXPLAIN should not be blocked by regex rule
        sql """EXPLAIN SELECT * FROM ${catalog_name}.${db_name}.${table_name} limit 10;"""
    }

    // Test 5: require_partition_filter rule on Hive partition table
    connect('hive_block_user5', '', context.config.jdbcUrl) {
        test {
            sql """select * from ${catalog_name}.${db_name}.${table_name} order by id limit 10;"""
            exception """sql hits sql block rule: hive_require_partition_filter_rule, missing partition filter"""
        }

        test {
            sql """select * from ${catalog_name}.${db_name}.${table_name}
                    where val = 10 limit 10;"""
            exception """sql hits sql block rule: hive_require_partition_filter_rule, missing partition filter"""
        }

        def partitionFilteredRows = sql """
            select id, pt1, pt2 from ${catalog_name}.${db_name}.${table_name}
            where pt1 = 'a'
            order by id;
        """
        assertEquals(2, partitionFilteredRows.size())
        assertEquals("1", partitionFilteredRows[0][0].toString())
        assertEquals("2", partitionFilteredRows[1][0].toString())

        // Test EXPLAIN should not be blocked
        sql """explain select * from ${catalog_name}.${db_name}.${table_name}
                where pt1 = 'a' order by id;"""
    }

    // Cleanup
    sql """drop user if exists hive_block_user1"""
    sql """drop user if exists hive_block_user2"""
    sql """drop user if exists hive_block_user3"""
    sql """drop user if exists hive_block_user4"""
    sql """drop user if exists hive_block_user5"""
    sql """drop sql_block_rule if exists hive_partition_rule"""
    sql """drop sql_block_rule if exists hive_split_rule"""
    sql """drop sql_block_rule if exists hive_cardinality_rule"""
    sql """drop sql_block_rule if exists hive_regex_rule"""
    sql """drop sql_block_rule if exists hive_require_partition_filter_rule"""
    sql """DROP TABLE IF EXISTS ${catalog_name}.${db_name}.${table_name}"""
    sql """DROP DATABASE IF EXISTS ${catalog_name}.${db_name}"""
    sql """drop catalog if exists ${catalog_name}"""
}
