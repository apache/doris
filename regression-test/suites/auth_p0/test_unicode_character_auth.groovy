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

// There will be no problem when the user or role is a Unicode character.
suite("test_unicode_character_auth") {

    sql """set enable_unicode_name_support=true;"""

    String user1 = 'test_unicode_character_auth_userA'
    String user2 = 'test_unicode_character_auth_userΩ'
    String user3 = 'test_unicode_character_auth_user一飞'
    String user4 = 'test_unicode_character_auth_user.,!?©'
    String user5 = 'test_unicode_character_auth_user+-*÷>=≥$&'
    String user6 = 'test_unicode_character_auth_user😊😢🎉'
    String user7 = 'test_unicode_character_auth_user𝓐Δ'
    String user8 = 'test_unicode_character_auth_user𝔸⌘'


    String role1 = 'test_unicode_character_auth_roleA'
    String role2 = 'test_unicode_character_auth_roleΩ'
    String role3 = 'test_unicode_character_auth_role一飞'
    String role4 = 'test_unicode_character_auth_role.,!?©'
    String role5 = 'test_unicode_character_auth_role+-*÷>=≥$&'
    String role6 = 'test_unicode_character_auth_role😊😢🎉'
    String role7 = 'test_unicode_character_auth_role𝓐Δ'
    String role8 = 'test_unicode_character_auth_role𝔸⌘'


    String pwd = 'C123_567p'
    String dbName = 'test_unicode_character_auth_db'
    String tableName = 'test_unicode_character_auth_tb'

    try_sql("DROP USER ${user1}")
    try_sql("DROP USER ${user2}")
    try_sql("DROP USER ${user3}")
    try_sql("DROP USER ${user4}")
    try_sql("DROP USER ${user5}")
    try_sql("DROP USER ${user6}")
    try_sql("DROP USER ${user7}")
    try_sql("DROP USER ${user8}")

    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO '${user1}'"""
        sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO '${user2}'"""
        sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO '${user3}'"""
        test {
            sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO '${user4}'"""
            exception "invalid user name"
        }
        test {
            sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO '${user5}'"""
            exception "invalid user name"
        }
        test {
            sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO '${user6}'"""
            exception "invalid user name"
        }
//        test {
//            sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO '${user7}'"""
//            exception "invalid user name"
//        }
        try {
            sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO '${user7}'"""
        } catch (Exception e) {
            logger.info(e.getMessage())
        }
        test {
            sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO '${user8}'"""
            exception "invalid user name"
        }
    }

    try_sql """drop database if exists ${dbName}"""

    sql """CREATE USER '${user1}' IDENTIFIED BY '${pwd}'"""
    sql """CREATE USER '${user2}' IDENTIFIED BY '${pwd}'"""
    sql """CREATE USER '${user3}' IDENTIFIED BY '${pwd}'"""

    test {
        sql """CREATE USER '${user4}' IDENTIFIED BY '${pwd}'"""
        exception "invalid user name"
    }
    test {
        sql """CREATE USER '${user5}' IDENTIFIED BY '${pwd}'"""
        exception "invalid user name"
    }
    test {
        sql """CREATE USER '${user6}' IDENTIFIED BY '${pwd}'"""
        exception "invalid user name"
    }
    test {
        sql """CREATE USER '${user7}' IDENTIFIED BY '${pwd}'"""
        exception "invalid user name"
    }
    test {
        sql """CREATE USER '${user8}' IDENTIFIED BY '${pwd}'"""
        exception "invalid user name"
    }

    sql """grant select_priv on regression_test to '${user1}'"""
    sql """grant select_priv on regression_test to '${user2}'"""
    sql """grant select_priv on regression_test to '${user3}'"""

    test {
        sql """grant select_priv on regression_test to '${user4}'"""
        exception "invalid user name"
    }
    test {
        sql """grant select_priv on regression_test to '${user5}'"""
        exception "invalid user name"
    }
    test {
        sql """grant select_priv on regression_test to '${user6}'"""
        exception "invalid user name"
    }
    test {
        sql """grant select_priv on regression_test to '${user7}'"""
        exception "invalid user name"
    }
    test {
        sql """grant select_priv on regression_test to '${user8}'"""
        exception "invalid user name"
    }

    sql """create database ${dbName}"""
    sql """create table ${dbName}.${tableName} (
            id BIGINT,
            username VARCHAR(20)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );"""

    sql """grant SELECT_PRIV on ${dbName}.${tableName} to ${user1}"""
    connect(user1, "${pwd}", context.config.jdbcUrl) {
        sql """select * from ${dbName}.${tableName}"""
        def res = sql """show grants"""
        assertTrue(res[0][3] == "")
        if (isCloudMode()) {
            assertTrue(res[0][6].contains("""internal.regression_test: Select_priv"""))
        } else {
            assertTrue(res[0][6] == """internal.information_schema: Select_priv; internal.mysql: Select_priv; internal.regression_test: Select_priv""")
        }
        assertTrue(res[0][7] == "internal.test_unicode_character_auth_db.test_unicode_character_auth_tb: Select_priv")
    }
    sql """revoke SELECT_PRIV on ${dbName}.${tableName} from ${user1}"""

    sql """grant SELECT_PRIV on ${dbName}.${tableName} to ${user1}"""
    connect(user2, "${pwd}", context.config.jdbcUrl) {
        sql """select * from ${dbName}.${tableName}"""
        def res = sql """show grants"""
        assertTrue(res[0][3] == "")
        if (isCloudMode()) {
            assertTrue(res[0][6].contains("""internal.regression_test: Select_priv"""))
        } else {
            assertTrue(res[0][6] == """internal.information_schema: Select_priv; internal.mysql: Select_priv; internal.regression_test: Select_priv""")
        }
        assertTrue(res[0][7] == "internal.test_unicode_character_auth_db.test_unicode_character_auth_tb: Select_priv")
    }
    sql """revoke SELECT_PRIV on ${dbName}.${tableName} from ${user1}"""

    sql """grant SELECT_PRIV on ${dbName}.${tableName} to ${user1}"""
    connect(user3, "${pwd}", context.config.jdbcUrl) {
        sql """select * from ${dbName}.${tableName}"""
        def res = sql """show grants"""
        assertTrue(res[0][3] == "")
        if (isCloudMode()) {
            assertTrue(res[0][6].contains("""internal.regression_test: Select_priv"""))
        } else {
            assertTrue(res[0][6] == """internal.information_schema: Select_priv; internal.mysql: Select_priv; internal.regression_test: Select_priv""")
        }
        assertTrue(res[0][7] == "internal.test_unicode_character_auth_db.test_unicode_character_auth_tb: Select_priv")
    }
    sql """revoke SELECT_PRIV on ${dbName}.${tableName} from ${user1}"""

    try_sql("DROP role ${role1}")
    try_sql("DROP role ${role2}")
    try_sql("DROP role ${role3}")
    try_sql("DROP role ${role4}")
    try_sql("DROP role ${role5}")
    try_sql("DROP role ${role6}")
    try_sql("DROP role ${role7}")
    try_sql("DROP role ${role8}")

    sql """CREATE ROLE ${role1}"""
    sql """CREATE ROLE ${role2}"""
    sql """CREATE ROLE ${role3}"""

    test {
        sql """CREATE ROLE ${role4}"""
        exception "invalid role format"
    }
    try {
        sql """CREATE ROLE ${role5}"""
    } catch (Exception e) {
        logger.info(e.getMessage())
    }
    try {
        sql """CREATE ROLE ${role6}"""
    } catch (Exception e) {
        logger.info(e.getMessage())
    }
    try {
        sql """CREATE ROLE ${role7}"""
    } catch (Exception e) {
        logger.info(e.getMessage())
    }
    try {
        sql """CREATE ROLE ${role8}"""
    } catch (Exception e) {
        logger.info(e.getMessage())
    }

    sql """GRANT '${role1}' TO ${user1};"""
    sql """grant SELECT_PRIV on ${dbName}.${tableName} to role '${role1}'"""
    connect(user1, "${pwd}", context.config.jdbcUrl) {
        sql """select * from ${dbName}.${tableName}"""
        def res = sql """show grants"""
        assertTrue(res[0][3] == "test_unicode_character_auth_roleA")
        if (isCloudMode()) {
            assertTrue(res[0][6].contains("""internal.regression_test: Select_priv"""))
        } else {
            assertTrue(res[0][6] == """internal.information_schema: Select_priv; internal.mysql: Select_priv; internal.regression_test: Select_priv""")
        }
        assertTrue(res[0][7] == "internal.test_unicode_character_auth_db.test_unicode_character_auth_tb: Select_priv")
    }
    sql """revoke SELECT_PRIV on ${dbName}.${tableName} from role '${role1}'"""

    sql """GRANT '${role2}' TO ${user2};"""
    sql """grant SELECT_PRIV on ${dbName}.${tableName} to role '${role2}'"""
    connect(user2, "${pwd}", context.config.jdbcUrl) {
        sql """select * from ${dbName}.${tableName}"""
        def res = sql """show grants"""
        assertTrue(res[0][3] == "test_unicode_character_auth_roleA")
        if (isCloudMode()) {
            assertTrue(res[0][6].contains("""internal.regression_test: Select_priv"""))
        } else {
            assertTrue(res[0][6] == """internal.information_schema: Select_priv; internal.mysql: Select_priv; internal.regression_test: Select_priv""")
        }
        assertTrue(res[0][7] == "internal.test_unicode_character_auth_db.test_unicode_character_auth_tb: Select_priv")
    }
    sql """revoke SELECT_PRIV on ${dbName}.${tableName} from role '${role2}'"""


    sql """GRANT '${role3}' TO ${user3};"""
    sql """grant SELECT_PRIV on ${dbName}.${tableName} to role '${role3}'"""
    connect(user3, "${pwd}", context.config.jdbcUrl) {
        sql """select * from ${dbName}.${tableName}"""
        def res = sql """show grants"""
        assertTrue(res[0][3] == "test_unicode_character_auth_roleA")
        if (isCloudMode()) {
            assertTrue(res[0][6].contains("""internal.regression_test: Select_priv"""))
        } else {
            assertTrue(res[0][6] == """internal.information_schema: Select_priv; internal.mysql: Select_priv; internal.regression_test: Select_priv""")
        }
        assertTrue(res[0][7] == "internal.test_unicode_character_auth_db.test_unicode_character_auth_tb: Select_priv")
    }
    sql """revoke SELECT_PRIV on ${dbName}.${tableName} from role '${role3}'"""

    try_sql("DROP USER ${user1}")
    try_sql("DROP USER ${user2}")
    try_sql("DROP USER ${user3}")
    try_sql("DROP USER ${user4}")
    try_sql("DROP USER ${user5}")
    try_sql("DROP USER ${user6}")
    try_sql("DROP USER ${user7}")
    try_sql("DROP USER ${user8}")

}
