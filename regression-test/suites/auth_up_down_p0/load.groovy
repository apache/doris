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

suite("test_upgrade_downgrade_prepare_auth","p0,auth,restart_fe") {

    String user1 = 'test_upgrade_downgrade_compatibility_auth_user1'
    String user2 = 'test_upgrade_downgrade_compatibility_auth_user2'
    String role1 = 'test_upgrade_downgrade_compatibility_auth_role1'
    String role2 = 'test_upgrade_downgrade_compatibility_auth_role2'
    String pwd = 'C123_567p'

    String dbName = 'test_auth_up_down_db'
    String tableName1 = 'test_auth_up_down_table1'
    String tableName2 = 'test_auth_up_down_table2'

    String wg1 = 'wg_1'
    String wg2 = 'wg_2'

    String rg1 = 'test_up_down_resource_1_hdfs'
    String rg2 = 'test_up_down_resource_2_hdfs'

    try_sql("DROP USER ${user1}")
    try_sql("DROP USER ${user2}")
    try_sql("DROP role ${role1}")
    try_sql("DROP role ${role2}")
    sql """CREATE USER '${user1}' IDENTIFIED BY '${pwd}'"""
    sql """CREATE USER '${user2}' IDENTIFIED BY '${pwd}'"""
    sql """grant select_priv on regression_test to ${user1}"""
    sql """grant select_priv on regression_test to ${user2}"""

    sql """CREATE ROLE ${role1}"""
    sql """CREATE ROLE ${role2}"""

    try_sql """drop table if exists ${dbName}.${tableName1}"""
    sql """drop database if exists ${dbName}"""
    sql """create database ${dbName}"""
    sql """
        CREATE TABLE IF NOT EXISTS ${dbName}.`${tableName1}` (
            id BIGINT,
            username VARCHAR(20)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
        """

    try_sql """drop table if exists ${dbName}.${tableName2}"""
    sql """
        CREATE TABLE IF NOT EXISTS ${dbName}.`${tableName2}` (
            id BIGINT,
            username VARCHAR(20)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """

    sql """drop WORKLOAD GROUP if exists '${wg1}'"""
    sql """drop WORKLOAD GROUP if exists '${wg2}'"""
    sql """CREATE WORKLOAD GROUP "${wg1}"
        PROPERTIES (
            "cpu_share"="10"
        );"""
    sql """CREATE WORKLOAD GROUP "${wg2}"
        PROPERTIES (
            "cpu_share"="10"
        );"""

    sql """DROP RESOURCE if exists ${rg1}"""
    sql """DROP RESOURCE if exists ${rg2}"""
    sql """
            CREATE RESOURCE IF NOT EXISTS "${rg1}"
            PROPERTIES(
            "type"="hdfs",
            "fs.defaultFS"="127.0.0.1:8120",
            "hadoop.username"="hive",
            "hadoop.password"="hive",
            "dfs.nameservices" = "my_ha",
            "dfs.ha.namenodes.my_ha" = "my_namenode1, my_namenode2",
            "dfs.namenode.rpc-address.my_ha.my_namenode1" = "127.0.0.1:10000",
            "dfs.namenode.rpc-address.my_ha.my_namenode2" = "127.0.0.1:10000",
            "dfs.client.failover.proxy.provider" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
            );
        """
    sql """
            CREATE RESOURCE IF NOT EXISTS "${rg2}"
            PROPERTIES(
            "type"="hdfs",
            "fs.defaultFS"="127.0.0.1:8120",
            "hadoop.username"="hive",
            "hadoop.password"="hive",
            "dfs.nameservices" = "my_ha",
            "dfs.ha.namenodes.my_ha" = "my_namenode1, my_namenode2",
            "dfs.namenode.rpc-address.my_ha.my_namenode1" = "127.0.0.1:10000",
            "dfs.namenode.rpc-address.my_ha.my_namenode2" = "127.0.0.1:10000",
            "dfs.client.failover.proxy.provider" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
            );
        """

    sql """ADMIN SET FRONTEND CONFIG ('experimental_enable_workload_group' = 'true');"""
    sql """set experimental_enable_pipeline_engine = true;"""

    // user
    sql """grant select_priv on ${dbName}.${tableName1} to ${user1}"""
    sql """grant select_priv on ${dbName}.${tableName2} to ${user1}"""
    connect(user1, "${pwd}", context.config.jdbcUrl) {
        sql "select username from ${dbName}.${tableName1}"
    }
    connect(user1, "${pwd}", context.config.jdbcUrl) {
        sql "select username from ${dbName}.${tableName2}"
    }

    sql """revoke select_priv on ${dbName}.${tableName1} from ${user1}"""
    connect(user1, "${pwd}", context.config.jdbcUrl) {
        try {
            sql "select username from ${dbName}.${tableName1}"
        } catch (Exception e) {
            log.info(e.getMessage())
            assertTrue(e.getMessage().contains("denied"))
        }
    }
    connect(user1, "${pwd}", context.config.jdbcUrl) {
        sql "select username from ${dbName}.${tableName2}"
    }

    // role
    sql """grant select_priv on ${dbName}.${tableName1} to ROLE '${role1}'"""
    sql """grant Load_priv on ${dbName}.${tableName1} to ROLE '${role2}'"""
    sql """grant '${role1}', '${role2}' to '${user2}'"""
    connect(user2, "${pwd}", context.config.jdbcUrl) {
        sql "select username from ${dbName}.${tableName1}"
        sql """insert into ${dbName}.`${tableName1}` values (4, "444")"""
    }

    sql """revoke '${role1}' from '${user2}'"""
    connect(user2, "${pwd}", context.config.jdbcUrl) {
        try {
            sql "select username from ${dbName}.${tableName1}"
        } catch (Exception e) {
            log.info(e.getMessage())
            assertTrue(e.getMessage().contains("denied"))
        }
    }
    connect(user2, "${pwd}", context.config.jdbcUrl) {
        sql """insert into ${dbName}.`${tableName1}` values (5, "555")"""
    }

    // workload group
    connect(user1, "${pwd}", context.config.jdbcUrl) {
        sql """set workload_group = '${wg1}';"""
        try {
            sql "select username from ${dbName}.${tableName2}"
        } catch (Exception e) {
            log.info(e.getMessage())
            assertTrue(e.getMessage().contains("denied"))
        }
    }
    sql """GRANT USAGE_PRIV ON WORKLOAD GROUP '${wg1}' TO '${user1}';"""
    connect(user1, "${pwd}", context.config.jdbcUrl) {
        sql """set workload_group = '${wg1}';"""
        sql """select username from ${dbName}.${tableName2}"""
    }

    // resource group
    connect(user1, "${pwd}", context.config.jdbcUrl) {
        def res = sql """SHOW RESOURCES;"""
        assertTrue(res == [])
    }
    sql """GRANT USAGE_PRIV ON RESOURCE ${rg1} TO ${user1};"""
    connect(user1, "${pwd}", context.config.jdbcUrl) {
        def res = sql """SHOW RESOURCES;"""
        assertTrue(res.size == 10)
    }
}
