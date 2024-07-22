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
        logger.info("result:" + result)
        for (int i = 0; i < result.size(); i++) {
            if (result[i][7] == "FOLLOWER" && result[i][8] == "false" && result[i][11] == "true") {
                return result[i][1]
            }
        }
        return "null"
    }
    def switch_ip = get_follower_ip()
    if (switch_ip != "null") {
        logger.info("switch_ip: " + switch_ip)

        def tokens = context.config.jdbcUrl.split('/')
        def url_tmp1 = tokens[0] + "//" + tokens[2] + "/" + "information_schema" + "?"
        def new_jdbc_url = url_tmp1.replaceAll(/\/\/[0-9.]+:/, "//${switch_ip}:")
        logger.info("new_jdbc_url: " + new_jdbc_url)

        String user = 'test_follower_consistent_user'
        String pwd = 'C123_567p'
        String dbName = 'test_follower_consistent_db'
        String tableName = 'test_follower_consistent_table'
        String role = 'test_follower_consistent_role'
        String wg = 'test_follower_consistent_wg'
        String rg = 'test_follower_consistent_rg'
        String mv_name = 'test_follower_consistent_mv'
        String mtmv_name = 'test_follower_consistent_mtmv'
        String view_name = 'test_follower_consistent_view'
        String rollup_name = 'test_follower_consistent_rollup'
        String catalog_name = 'test_follower_consistent_catalog'
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

        sql """create view ${dbName}.${view_name} as select * from ${dbName}.${tableName};"""
        sql """alter table ${dbName}.${tableName} add rollup ${rollup_name}(username)"""
        sleep(5 * 1000)
        sql """create materialized view ${mv_name} as select username from ${dbName}.${tableName}"""
        sleep(5 * 1000)
        sql """CREATE MATERIALIZED VIEW ${dbName}.${mtmv_name} 
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
        sql """refresh MATERIALIZED VIEW ${dbName}.${mtmv_name} auto"""
        sql """grant select_priv on regression_test to ${user}"""

        //cloud-mode
        if (isCloudMode()) {
            def clusters = sql " SHOW CLUSTERS; "
            assertTrue(!clusters.isEmpty())
            def validCluster = clusters[0][0]
            sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO ${user}""";
        }

        logger.info("url_tmp1:" + url_tmp1)
        logger.info("new_jdbc_url:" + new_jdbc_url)
        connect(user=user, password="${pwd}", url=url_tmp1) {
            try {
                sql "SHOW CATALOG RECYCLE BIN WHERE NAME = '${catalog_name}'"
            } catch (Exception e) {
                log.info(e.getMessage())
                assertTrue(e.getMessage().contains("Admin_priv"))
            }
        }
        connect(user=user, password="${pwd}", url=new_jdbc_url) {
            try {
                sql "SHOW CATALOG RECYCLE BIN WHERE NAME = '${catalog_name}'"
            } catch (Exception e) {
                log.info(e.getMessage())
                assertTrue(e.getMessage().contains("Admin_priv"))
            }
        }
        connect(user=user, password="${pwd}", url=url_tmp1) {
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

        connect(user=user, password="${pwd}", url=url_tmp1) {
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
        connect(user=user, password="${pwd}", url=url_tmp1) {
            sql "select username from ${dbName}.${tableName}"
        }
        connect(user=user, password="${pwd}", url=new_jdbc_url) {
            sql "select username from ${dbName}.${tableName}"
        }


        connect(user=user, password="${pwd}", url=url_tmp1) {
            try {
                sql "select username from ${dbName}.${view_name}"
            } catch (Exception e) {
                log.info(e.getMessage())
                assertTrue(e.getMessage().contains("Admin_priv,Select_priv"))
            }
        }
        connect(user=user, password="${pwd}", url=new_jdbc_url) {
            try {
                sql "select username from ${dbName}.${view_name}"
            } catch (Exception e) {
                log.info(e.getMessage())
                assertTrue(e.getMessage().contains("Admin_priv,Select_priv"))
            }
        }
        sql """grant select_priv(username) on ${dbName}.${view_name} to ${user}"""
        connect(user=user, password="${pwd}", url=url_tmp1) {
            sql "select username from ${dbName}.${view_name}"
        }
        connect(user=user, password="${pwd}", url=new_jdbc_url) {
            sql "select username from ${dbName}.${view_name}"
        }


        connect(user=user, password="${pwd}", url=url_tmp1) {
            try {
                sql "select username from ${dbName}.${mtmv_name}"
            } catch (Exception e) {
                log.info(e.getMessage())
                assertTrue(e.getMessage().contains("Admin_priv,Select_priv"))
            }
        }
        connect(user=user, password="${pwd}", url=new_jdbc_url) {
            try {
                sql "select username from ${dbName}.${mtmv_name}"
            } catch (Exception e) {
                log.info(e.getMessage())
                assertTrue(e.getMessage().contains("Admin_priv,Select_priv"))
            }
        }
        sql """grant select_priv(username) on ${dbName}.${mtmv_name} to ${user}"""
        connect(user=user, password="${pwd}", url=url_tmp1) {
            sql "select username from ${dbName}.${mtmv_name}"
        }
        connect(user=user, password="${pwd}", url=new_jdbc_url) {
            sql "select username from ${dbName}.${mtmv_name}"
        }


        sql """ADMIN SET FRONTEND CONFIG ('experimental_enable_workload_group' = 'true');"""
        sql """set experimental_enable_pipeline_engine = true;"""

        // user
        sql """grant select_priv on ${dbName}.${tableName} to ${user}"""
        connect(user=user, password="${pwd}", url=url_tmp1) {
            sql "select username from ${dbName}.${tableName}"
        }
        connect(user=user, password="${pwd}", url=new_jdbc_url) {
            sql "select username from ${dbName}.${tableName}"
        }

        sql """revoke select_priv on ${dbName}.${tableName} from ${user}"""
        connect(user=user, password="${pwd}", url=url_tmp1) {
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
        connect(user=user, password="${pwd}", url=url_tmp1) {
            sql "select username from ${dbName}.${tableName}"
            sql """insert into ${dbName}.`${tableName}` values (4, "444")"""
        }
        connect(user=user, password="${pwd}", url=new_jdbc_url) {
            sql "select username from ${dbName}.${tableName}"
            sql """insert into ${dbName}.`${tableName}` values (4, "444")"""
        }

        sql """revoke '${role}' from '${user}'"""
        connect(user=user, password="${pwd}", url=url_tmp1) {
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
        connect(user=user, password="${pwd}", url=url_tmp1) {
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
        connect(user=user, password="${pwd}", url=url_tmp1) {
            sql """set workload_group = '${wg}';"""
            sql """select username from ${dbName}.${tableName}"""
        }
        connect(user=user, password="${pwd}", url=new_jdbc_url) {
            sql """set workload_group = '${wg}';"""
            sql """select username from ${dbName}.${tableName}"""
        }

        // resource group
        connect(user=user, password="${pwd}", url=url_tmp1) {
            def res = sql """SHOW RESOURCES;"""
            assertTrue(res == [])
        }
        connect(user=user, password="${pwd}", url=new_jdbc_url) {
            def res = sql """SHOW RESOURCES;"""
            assertTrue(res == [])
        }
        sql """GRANT USAGE_PRIV ON RESOURCE ${rg} TO ${user};"""
        connect(user=user, password="${pwd}", url=url_tmp1) {
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
