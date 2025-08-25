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

suite("test_grant_nereids)") {

    def forComputeGroupStr = "";

    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        forComputeGroupStr = " for  $validCluster "
    }

    String user1 = 'test_grant_nereids_user1'
    String user2 = 'test_grant_nereids_user2'
    String role1 = 'test_grant_nereids_role1'
    String role2 = 'test_grant_nereids_role2'
    String pwd = 'C123_567p'

    String dbName = 'test_grant_nereis_db'
    String tableName1 = 'test_grant_nereids_table1'
    String tableName2 = 'test_grant_nereids_table2'

    String wg1 = 'test_workload_group_nereids_1'
    String wg2 = 'test_workload_group_nereids_2'

    String rg1 = 'test_resource_nereids_1_hdfs'
    String rg2 = 'test_resource_nereids_2_hdfs'

    try_sql("DROP USER ${user1}")
    try_sql("DROP USER ${user2}")
    try_sql("DROP role ${role1}")
    try_sql("DROP role ${role2}")
    sql """CREATE USER '${user1}' IDENTIFIED BY '${pwd}'"""
    sql """CREATE USER '${user2}' IDENTIFIED BY '${pwd}'"""
    checkNereidsExecute("grant select_priv on regression_test to ${user1};")
    checkNereidsExecute("grant select_priv on regression_test to ${user2};")

    sql """CREATE ROLE ${role1}"""
    sql """CREATE ROLE ${role2}"""

    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        checkNereidsExecute("GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO ${user1};")
        checkNereidsExecute("GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO ${user2};")
    }

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

    sql """drop WORKLOAD GROUP if exists '${wg1}' $forComputeGroupStr """
    sql """drop WORKLOAD GROUP if exists '${wg2}' $forComputeGroupStr """
    sql """CREATE WORKLOAD GROUP "${wg1}" $forComputeGroupStr
        PROPERTIES (
            "min_cpu_percent"="10"
        );"""
    sql """CREATE WORKLOAD GROUP "${wg2}" $forComputeGroupStr
        PROPERTIES (
            "min_cpu_percent"="10"
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
    checkNereidsExecute("grant select_priv on ${dbName}.${tableName1} to ${user1};")
    checkNereidsExecute("grant select_priv on ${dbName}.${tableName2} to ${user1};")
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
    checkNereidsExecute("grant select_priv on ${dbName}.${tableName1} to ROLE '${role1}';")
    checkNereidsExecute("grant Load_priv on ${dbName}.${tableName1} to ROLE '${role2}';")
    checkNereidsExecute("grant '${role1}', '${role2}' to '${user2}';")
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
    checkNereidsExecute("GRANT USAGE_PRIV ON WORKLOAD GROUP '${wg1}' TO '${user1}';")
    connect(user1, "${pwd}", context.config.jdbcUrl) {
        sql """set workload_group = '${wg1}';"""
        sql """select username from ${dbName}.${tableName2}"""
    }

    // resource group
    connect(user1, "${pwd}", context.config.jdbcUrl) {
        def res = sql """SHOW RESOURCES;"""
        assertTrue(res == [])
    }
    checkNereidsExecute("GRANT USAGE_PRIV ON RESOURCE ${rg1} TO ${user1};")
    connect(user1, "${pwd}", context.config.jdbcUrl) {
        def res = sql """SHOW RESOURCES;"""
        assertTrue(res.size() == 10)
    }
}
