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

suite("test_upgrade_downgrade_compatibility_auth","p0,auth,restart_fe") {

    sql """ADMIN SET FRONTEND CONFIG ('experimental_enable_workload_group' = 'true');"""
    sql """set experimental_enable_pipeline_engine = true;"""

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

    // user
    connect(user=user1, password="${pwd}", url=context.config.jdbcUrl) {
        try {
            sql "select username from ${dbName}.${tableName1}"
        } catch (Exception e) {
            log.info(e.getMessage())
            assertTrue(e.getMessage().contains("Admin_priv,Select_priv"))
        }
    }
    connect(user=user1, password="${pwd}", url=context.config.jdbcUrl) {
        sql "select username from ${dbName}.${tableName2}"
    }

    // role
    connect(user=user2, password="${pwd}", url=context.config.jdbcUrl) {
        try {
            sql "select username from ${dbName}.${tableName1}"
        } catch (Exception e) {
            log.info(e.getMessage())
            assertTrue(e.getMessage().contains("Admin_priv,Select_priv"))
        }
    }
    connect(user=user2, password="${pwd}", url=context.config.jdbcUrl) {
        sql """insert into ${dbName}.`${tableName1}` values (5, "555")"""
    }

    // workload group
    connect(user=user1, password="${pwd}", url=context.config.jdbcUrl) {
        sql """set workload_group = '${wg1}';"""
        sql """select username from ${dbName}.${tableName2}"""
    }

    // resource group
    connect(user=user1, password="${pwd}", url=context.config.jdbcUrl) {
        def res = sql """SHOW RESOURCES;"""
        assertTrue(res.size == 10)
    }
}
