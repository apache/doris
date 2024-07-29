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

    String hivePrefix = "hive2";
    String catalog_name = "test_${hivePrefix}_external_sql_block_rule";
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")

    sql """drop catalog if exists ${catalog_name} """

    sql """CREATE CATALOG ${catalog_name} PROPERTIES (
            'type'='hms',
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
            'hadoop.username' = 'hive'
        );"""

    sql "use ${catalog_name}.`default`";
    qt_sql01 """select * from parquet_partition_table order by l_linenumber,l_orderkey limit 10;"""

    sql """drop sql_block_rule if exists external_hive_partition"""
    sql """create sql_block_rule external_hive_partition properties("partition_num" = "3", "global" = "false");"""
    sql """drop sql_block_rule if exists external_hive_partition2"""
    sql """create sql_block_rule external_hive_partition2 properties("tablet_num" = "3", "global" = "false");"""
    sql """drop sql_block_rule if exists external_hive_partition3"""
    sql """create sql_block_rule external_hive_partition3 properties("cardinality" = "3", "global" = "false");"""
    // create 3 users
    sql """drop user if exists external_block_user1"""
    sql """create user external_block_user1;"""
    sql """SET PROPERTY FOR 'external_block_user1' 'sql_block_rules' = 'external_hive_partition';"""
    sql """grant all on *.*.* to external_block_user1;"""

    sql """drop user if exists external_block_user2"""
    sql """create user external_block_user2;"""
    sql """SET PROPERTY FOR 'external_block_user2' 'sql_block_rules' = 'external_hive_partition2';"""
    sql """grant all on *.*.* to external_block_user2;"""

    sql """drop user if exists external_block_user3"""
    sql """create user external_block_user3;"""
    sql """SET PROPERTY FOR 'external_block_user3' 'sql_block_rules' = 'external_hive_partition3';"""
    sql """grant all on *.*.* to external_block_user3;"""

    // login as external_block_user1 
    def result1 = connect(user = 'external_block_user1', password = '', url = context.config.jdbcUrl) {
        test {
            sql """select * from ${catalog_name}.`default`.parquet_partition_table order by l_linenumber limit 10;"""
            exception """sql hits sql block rule: external_hive_partition, reach partition_num : 3"""
        }
    }
    // login as external_block_user2
    def result2 = connect(user = 'external_block_user2', password = '', url = context.config.jdbcUrl) {
        test {
            sql """select * from ${catalog_name}.`default`.parquet_partition_table order by l_linenumber limit 10;"""
            exception """sql hits sql block rule: external_hive_partition2, reach tablet_num : 3"""
        }
    }
    // login as external_block_user3
    def result3 = connect(user = 'external_block_user3', password = '', url = context.config.jdbcUrl) {
        test {
            sql """select * from ${catalog_name}.`default`.parquet_partition_table order by l_linenumber limit 10;"""
            exception """sql hits sql block rule: external_hive_partition3, reach cardinality : 3"""
        }
    }
}

