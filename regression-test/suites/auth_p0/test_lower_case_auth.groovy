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

import org.junit.Assert;

// In the case of permissions, the user name and role name are case-sensitive.
suite("test_lower_case_auth","p0,auth") {
    String user1 = 'test_lower_case_auth_user'
    String user2 = 'TEST_LOWER_CASE_AUTH_USER'
    String user3 = 'test_lower_case_auth_USER'

    String role1 = 'test_lower_case_auth_role'
    String role2 = 'TEST_LOWER_CASE_AUTH_ROLE'
    String role3 = 'test_LOWER_case_auth_ROLE'

    String pwd = 'C123_567p'
    String catalogName = 'test_lower_case_auth_catalog'
    String dbName = 'test_lower_case_auth_db'
    String tableName = 'test_lower_case_auth_tb'
    String mtmvName = 'test_lower_case_auth_mtmv'

    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO ${user1}""";
        sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO ${user2}""";
        sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO ${user3}""";
    }

    try_sql("DROP USER ${user1}")
    try_sql("DROP USER ${user2}")
    try_sql("DROP USER ${user3}")
    try_sql("DROP role ${role1}")
    try_sql("DROP role ${role2}")
    try_sql("DROP role ${role3}")
    try_sql """drop database if exists ${dbName}"""
    sql """CREATE USER '${user1}' IDENTIFIED BY '${pwd}'"""
    sql """CREATE USER '${user2}' IDENTIFIED BY '${pwd}'"""
    sql """CREATE USER '${user3}' IDENTIFIED BY '${pwd}'"""
    sql """grant select_priv on regression_test to ${user1}"""
    sql """grant select_priv on regression_test to ${user2}"""
    sql """grant select_priv on regression_test to ${user3}"""
    sql """create database ${dbName}"""
    sql """create table ${dbName}.${tableName} (
            id BIGINT,
            username VARCHAR(20)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );"""
    sql """CREATE MATERIALIZED VIEW ${dbName}.${mtmvName} 
            BUILD IMMEDIATE REFRESH AUTO ON MANUAL 
            DISTRIBUTED BY RANDOM BUCKETS 1 
            PROPERTIES ('replication_num' = '1') 
            AS select username, sum(id) as sum_id from ${dbName}.${tableName} group by username"""

    sql """grant Create_priv on ${dbName}.${mtmvName} to ${user1}"""
    sql """grant Create_priv on ${dbName}.${mtmvName} to ${user2}"""
    sql """grant Create_priv on ${dbName}.${mtmvName} to ${user3}"""

    def res = sql """show all grants;"""
    logger.info("res: " + res)
    def count = 0
    for (int i = 0; i < res.size(); i++) {
        if (res[i][0] == """'${user1}'@'%'""" || res[i][0] == """'${user2}'@'%'""" || res[i][0] == """'${user3}'@'%'""") {
            if (isCloudMode()) {
                assertTrue(res[i][6].contains("""internal.regression_test: Select_priv"""))
            } else {
                assertTrue(res[i][6] == """internal.information_schema: Select_priv; internal.mysql: Select_priv; internal.regression_test: Select_priv""")
            }
            assertTrue(res[i][7] == """internal.test_lower_case_auth_db.test_lower_case_auth_mtmv: Create_priv""")
            count ++
        }
    }
    assertTrue(count == 3)

    sql """revoke Create_priv on ${dbName}.${mtmvName} from ${user1}"""
    sql """revoke Create_priv on ${dbName}.${mtmvName} from ${user2}"""
    sql """revoke Create_priv on ${dbName}.${mtmvName} from ${user3}"""

    res = sql """show all grants;"""
    count = 0
    for (int i = 0; i < res.size(); i++) {
        if (res[i][0] == """'${user1}'@'%'""" || res[i][0] == """'${user2}'@'%'""" || res[i][0] == """'${user3}'@'%'""") {
            if (isCloudMode()) {
                assertTrue(res[i][6].contains("""internal.regression_test: Select_priv"""))
            } else {
                assertTrue(res[i][6] == """internal.information_schema: Select_priv; internal.mysql: Select_priv; internal.regression_test: Select_priv""")
            }
            assertTrue(res[i][7] == null)
            count ++
        }
    }
    assertTrue(count == 3)

    sql """grant Create_priv on ${dbName}.${mtmvName} to role '${role1}'"""
    sql """grant Create_priv on ${dbName}.${mtmvName} to role '${role2}'"""
    sql """grant Create_priv on ${dbName}.${mtmvName} to role '${role3}'"""

    sql """GRANT '${role1}' TO ${user1};"""
    sql """GRANT '${role2}' TO ${user2};"""
    sql """GRANT '${role3}' TO ${user3};"""


    res = sql """show all grants;"""
    count = 0
    for (int i = 0; i < res.size(); i++) {
        if (res[i][0] == """'${user1}'@'%'""") {
            assertTrue(res[i][3] == role1)
            if (isCloudMode()) {
                assertTrue(res[i][6].contains("""internal.regression_test: Select_priv"""))
            } else {
                assertTrue(res[i][6] == """internal.information_schema: Select_priv; internal.mysql: Select_priv; internal.regression_test: Select_priv""")
            }
            assertTrue(res[i][7] == """internal.test_lower_case_auth_db.test_lower_case_auth_mtmv: Create_priv""")
            count ++
        } else if (res[i][0] == """'${user2}'@'%'""") {
            assertTrue(res[i][3] == role2)
            if (isCloudMode()) {
                assertTrue(res[i][6].contains("""internal.regression_test: Select_priv"""))
            } else {
                assertTrue(res[i][6] == """internal.information_schema: Select_priv; internal.mysql: Select_priv; internal.regression_test: Select_priv""")
            }
            assertTrue(res[i][7] == """internal.test_lower_case_auth_db.test_lower_case_auth_mtmv: Create_priv""")
            count ++
        } else if (res[i][0] == """'${user3}'@'%'""") {
            assertTrue(res[i][3] == role3)
            if (isCloudMode()) {
                assertTrue(res[i][6].contains("""internal.regression_test: Select_priv"""))
            } else {
                assertTrue(res[i][6] == """internal.information_schema: Select_priv; internal.mysql: Select_priv; internal.regression_test: Select_priv""")
            }
            assertTrue(res[i][7] == """internal.test_lower_case_auth_db.test_lower_case_auth_mtmv: Create_priv""")
            count ++
        }
    }
    assertTrue(count == 3)

    try_sql("DROP USER ${user1}")
    try_sql("DROP USER ${user2}")
    try_sql("DROP USER ${user3}")
    try_sql("DROP role ${role1}")
    try_sql("DROP role ${role2}")
    try_sql("DROP role ${role3}")




}
