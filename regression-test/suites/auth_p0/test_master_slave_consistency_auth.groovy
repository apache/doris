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

suite ("test_follower_consistent_auth","p0,auth") {

    def get_follower_ip = {
        def result = sql """show frontends;"""
        for (int i = 0; i < result.size(); i++) {
            if (result[i][7] == "FOLLOWER" && result[i][8] == "false") {
                return result[i][1]
            }
        }
        return "null"
    }
    def switch_ip = get_follower_ip()
    if (switch_ip != "null") {
        logger.info("switch_ip: " + switch_ip)
        def new_jdbc_url = context.config.jdbcUrl.replaceAll(/\/\/[0-9.]+:/, "//${switch_ip}:")
        logger.info("new_jdbc_url: " + new_jdbc_url)

        String user = 'test_follower_consistent_user'
        String pwd = 'C123_567p'
        String dbName = 'test_select_column_auth_db'
        String tableName = 'test_select_column_auth_table'
        String role = 'test_select_column_auth_role'
        String wg = 'test_select_column_auth_wg'
        String rg = 'test_select_column_auth_rg'
        try_sql("DROP role ${role}")
        sql """CREATE ROLE ${role}"""
        sql """drop WORKLOAD GROUP if exists '${wg}'"""
        sql """CREATE WORKLOAD GROUP "${wg}"
        PROPERTIES (
            "cpu_share"="10"
        );"""
        sql """DROP RESOURCE if exists ${rg}"""
        sql """
            CREATE RESOURCE IF NOT EXISTS "${rg}"
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
        try_sql("drop user ${user}")
        try_sql """drop table if exists ${dbName}.${tableName}"""
        sql """drop database if exists ${dbName}"""
        sql """create database ${dbName}"""
        sql """
        CREATE TABLE IF NOT EXISTS ${dbName}.`${tableName}` (
            id BIGINT,
            username VARCHAR(20)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """

        sql """create view ${dbName}.v1 as select * from ${dbName}.${tableName};"""
        sql """alter table ${dbName}.${tableName} add rollup rollup1(username)"""
        sleep(5 * 1000)
        sql """create materialized view mv1 as select username from ${dbName}.${tableName}"""
        sleep(5 * 1000)
        sql """CREATE MATERIALIZED VIEW ${dbName}.mtmv1 
        BUILD IMMEDIATE REFRESH AUTO ON MANUAL 
        DISTRIBUTED BY RANDOM BUCKETS 1 
        PROPERTIES ('replication_num' = '1') 
        AS select username, sum(id) from ${dbName}.${tableName} group by username"""
        sql """
        insert into ${dbName}.`${tableName}` values 
        (1, "111"),
        (2, "222"),
        (3, "333");
        """
        sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
        sql """refresh MATERIALIZED VIEW ${dbName}.mtmv1 auto"""
        sql """grant select_priv on regression_test to ${user}"""


        connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
            try {
                sql "SHOW CATALOG RECYCLE BIN WHERE NAME = 'test'"
            } catch (Exception e) {
                log.info(e.getMessage())
                assertTrue(e.getMessage().contains("Admin_priv"))
            }
        }
        connect(user=user, password="${pwd}", url=new_jdbc_url) {
            try {
                sql "SHOW CATALOG RECYCLE BIN WHERE NAME = 'test'"
            } catch (Exception e) {
                log.info(e.getMessage())
                assertTrue(e.getMessage().contains("Admin_priv"))
            }
        }
        connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
            try {
                sql "SHOW DATA"
            } catch (Exception e) {
                log.info(e.getMessage())
                assertTrue(e.getMessage().contains("Admin_priv"))
            }
        }
        connect(user=user, password="${pwd}", url=new_jdbc_url) {
            try {
                sql "SHOW DATA"
            } catch (Exception e) {
                log.info(e.getMessage())
                assertTrue(e.getMessage().contains("Admin_priv"))
            }
        }

        connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
            try {
                sql "select username from ${dbName}.${tableName}"
            } catch (Exception e) {
                log.info(e.getMessage())
                assertTrue(e.getMessage().contains("Admin_priv,Select_priv"))
            }
        }
        connect(user=user, password="${pwd}", url=new_jdbc_url) {
            try {
                sql "select username from ${dbName}.${tableName}"
            } catch (Exception e) {
                log.info(e.getMessage())
                assertTrue(e.getMessage().contains("Admin_priv,Select_priv"))
            }
        }
        sql """grant select_priv(username) on ${dbName}.${tableName} to ${user}"""
        connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
            sql "select username from ${dbName}.${tableName}"
        }
        connect(user=user, password="${pwd}", url=new_jdbc_url) {
            sql "select username from ${dbName}.${tableName}"
        }


        connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
            try {
                sql "select username from ${dbName}.v1"
            } catch (Exception e) {
                log.info(e.getMessage())
                assertTrue(e.getMessage().contains("Admin_priv,Select_priv"))
            }
        }
        connect(user=user, password="${pwd}", url=new_jdbc_url) {
            try {
                sql "select username from ${dbName}.v1"
            } catch (Exception e) {
                log.info(e.getMessage())
                assertTrue(e.getMessage().contains("Admin_priv,Select_priv"))
            }
        }
        sql """grant select_priv(username) on ${dbName}.v1 to ${user}"""
        connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
            sql "select username from ${dbName}.v1"
        }
        connect(user=user, password="${pwd}", url=new_jdbc_url) {
            sql "select username from ${dbName}.v1"
        }


        connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
            try {
                sql "select username from ${dbName}.mtmv1"
            } catch (Exception e) {
                log.info(e.getMessage())
                assertTrue(e.getMessage().contains("Admin_priv,Select_priv"))
            }
        }
        connect(user=user, password="${pwd}", url=new_jdbc_url) {
            try {
                sql "select username from ${dbName}.mtmv1"
            } catch (Exception e) {
                log.info(e.getMessage())
                assertTrue(e.getMessage().contains("Admin_priv,Select_priv"))
            }
        }
        sql """grant select_priv(username) on ${dbName}.mtmv1 to ${user}"""
        connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
            sql "select username from ${dbName}.mtmv1"
        }
        connect(user=user, password="${pwd}", url=new_jdbc_url) {
            sql "select username from ${dbName}.mtmv1"
        }


        sql """ADMIN SET FRONTEND CONFIG ('experimental_enable_workload_group' = 'true');"""
        sql """set experimental_enable_pipeline_engine = true;"""

        // user
        sql """grant select_priv on ${dbName}.${tableName} to ${user}"""
        connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
            sql "select username from ${dbName}.${tableName}"
        }
        connect(user=user, password="${pwd}", url=new_jdbc_url) {
            sql "select username from ${dbName}.${tableName}"
        }

        sql """revoke select_priv on ${dbName}.${tableName} from ${user}"""
        connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
            try {
                sql "select username from ${dbName}.${tableName}"
            } catch (Exception e) {
                log.info(e.getMessage())
                assertTrue(e.getMessage().contains("Admin_priv,Select_priv"))
            }
        }
        connect(user=user, password="${pwd}", url=new_jdbc_url) {
            try {
                sql "select username from ${dbName}.${tableName}"
            } catch (Exception e) {
                log.info(e.getMessage())
                assertTrue(e.getMessage().contains("Admin_priv,Select_priv"))
            }
        }

        // role
        sql """grant select_priv on ${dbName}.${tableName} to ROLE '${role}'"""
        sql """grant Load_priv on ${dbName}.${tableName} to ROLE '${role}'"""
        sql """grant '${role}' to '${user}'"""
        connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
            sql "select username from ${dbName}.${tableName}"
            sql """insert into ${dbName}.`${tableName}` values (4, "444")"""
        }
        connect(user=user, password="${pwd}", url=new_jdbc_url) {
            sql "select username from ${dbName}.${tableName}"
            sql """insert into ${dbName}.`${tableName}` values (4, "444")"""
        }

        sql """revoke '${role}' from '${user}'"""
        connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
            try {
                sql "select username from ${dbName}.${tableName}"
            } catch (Exception e) {
                log.info(e.getMessage())
                assertTrue(e.getMessage().contains("Admin_priv,Select_priv"))
            }
        }
        connect(user=user, password="${pwd}", url=new_jdbc_url) {
            try {
                sql "select username from ${dbName}.${tableName}"
            } catch (Exception e) {
                log.info(e.getMessage())
                assertTrue(e.getMessage().contains("Admin_priv,Select_priv"))
            }
        }


        // workload group
        connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
            sql """set workload_group = '${wg}';"""
            try {
                sql "select username from ${dbName}.${tableName}"
            } catch (Exception e) {
                log.info(e.getMessage())
                assertTrue(e.getMessage().contains("USAGE/ADMIN privilege"))
            }
        }
        connect(user=user, password="${pwd}", url=new_jdbc_url) {
            sql """set workload_group = '${wg}';"""
            try {
                sql "select username from ${dbName}.${tableName}"
            } catch (Exception e) {
                log.info(e.getMessage())
                assertTrue(e.getMessage().contains("USAGE/ADMIN privilege"))
            }
        }
        sql """GRANT USAGE_PRIV ON WORKLOAD GROUP '${wg}' TO '${user}';"""
        connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
            sql """set workload_group = '${wg}';"""
            sql """select username from ${dbName}.${tableName}"""
        }
        connect(user=user, password="${pwd}", url=new_jdbc_url) {
            sql """set workload_group = '${wg}';"""
            sql """select username from ${dbName}.${tableName}"""
        }

        // resource group
        connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
            def res = sql """SHOW RESOURCES;"""
            assertTrue(res == [])
        }
        connect(user=user, password="${pwd}", url=new_jdbc_url) {
            def res = sql """SHOW RESOURCES;"""
            assertTrue(res == [])
        }
        sql """GRANT USAGE_PRIV ON RESOURCE ${rg} TO ${user};"""
        connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
            def res = sql """SHOW RESOURCES;"""
            assertTrue(res.size == 10)
        }
        connect(user=user, password="${pwd}", url=new_jdbc_url) {
            def res = sql """SHOW RESOURCES;"""
            assertTrue(res.size == 10)
        }

        try_sql("DROP USER ${user}")
        try_sql("drop workload group if exists ${wg};")

    }

}
