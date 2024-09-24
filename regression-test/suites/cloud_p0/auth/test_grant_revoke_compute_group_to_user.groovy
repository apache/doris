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

suite("test_grant_revoke_compute_group_to_user", "cloud_auth") {
    if (!isCloudMode()) {
        log.info("not cloud mode just return")
        return
    }
    def role = "admin"
    def user1 = "regression_test_compute_group_user1"
    def user2 = "regression_test_compute_group_user2"
    def user3 = "regression_test_compute_group_user3"
    def tbl = "test_auth_compute_group_tbl"

    def logAndExecuteSql = { sqlStatement ->
        log.info("Executing SQL: ${sqlStatement}")
        return sql(sqlStatement)
    }

    logAndExecuteSql """drop user if exists ${user1}"""
    logAndExecuteSql """drop user if exists ${user2}"""
    logAndExecuteSql """drop user if exists ${user3}"""
    logAndExecuteSql """drop table if exists ${tbl}"""

    def getCluster = { group ->
        def result = sql " SHOW COMPUTE GROUPS; "
        for (int i = 0; i < result.size(); i++) {
            if (result[i][0] == group) {
                return result[i]
            }
        }
        return null
    }

    def commonAuth = { result, UserIdentity, Password, Roles, GlobalPrivs ->
        assertEquals(UserIdentity as String, result.UserIdentity[0] as String)
        assertEquals(Password as String, result.Password[0] as String)
        assertEquals(Roles as String, result.Roles[0] as String)
        assertEquals(GlobalPrivs as String, result.GlobalPrivs[0] as String)
    }

    def getProperty = { property, user ->
        def result = null
        if (user == "") {
            result = sql_return_maparray """SHOW PROPERTY""" 
        } else {
            result = sql_return_maparray """SHOW PROPERTY FOR '${user}'""" 
        }
        result.find {
            it.Key == property as String
        }
    }

    def groups = sql " SHOW COMPUTE GROUPS; "
    logger.info("compute groups {}", groups);
    assertTrue(!groups.isEmpty())
    def validCluster = groups[0][0]

    // 1. change user
    // ${user1} admin role
    logAndExecuteSql """create user ${user1} identified by 'Cloud12345' default role 'admin'"""
    result = sql_return_maparray """show grants for '${user1}'"""
    commonAuth result, "'${user1}'@'%'" as String, "Yes", "admin", "Admin_priv"
    assertNull(result.ComputeGroupPrivs[0])


    // ${user2} not admin role
    logAndExecuteSql """create user ${user2} identified by 'Cloud12345'"""
    logAndExecuteSql """GRANT USAGE_PRIV ON COMPUTE GROUP '${validCluster}' TO '${user2}'"""
    // for use default_group:regression_test
    logAndExecuteSql """grant select_priv on *.*.* to ${user2}"""


    logAndExecuteSql """
    CREATE TABLE ${tbl} (
    `k1` int(11) NULL,
    `k2` char(5) NULL
    )
    DUPLICATE KEY(`k1`, `k2`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`k1`) BUCKETS 1
    PROPERTIES (
    "replication_num"="1"
    );
    """

    logAndExecuteSql """
        insert into ${tbl} (k1, k2) values (1, "10");
    """

    logAndExecuteSql """create user ${user3} identified by 'Cloud12345'"""
    logAndExecuteSql """GRANT SELECT_PRIV ON *.*.* TO '${user3}'@'%'"""
    result = connect(user = "${user3}", password = 'Cloud12345', url = context.config.jdbcUrl) {
            sql """SHOW COMPUTE GROUPS"""
    }
    // not grant any group to user3
    assertTrue(result.isEmpty())
    def db = context.dbName

    connect(user = "${user3}", password = 'Cloud12345', url = context.config.jdbcUrl) {
        test {
            sql """select * from ${db}.${tbl}"""
            exception "the user is not granted permission to the compute group"
        }
    }

    // 2. grant group
    def group1 = "groupA"
    def result

    logAndExecuteSql "sync"

    // admin role user can grant group to use
    result = connect(user = "${user1}", password = 'Cloud12345', url = context.config.jdbcUrl) {
            sql """GRANT USAGE_PRIV ON COMPUTE GROUP '${group1}' TO '${user1}'"""
    }

    // case run user(default root), and show grant again, should be same result
    result = sql_return_maparray """show grants for '${user1}'"""
    commonAuth result, "'${user1}'@'%'" as String, "Yes", "admin", "Admin_priv"
    assertTrue((result.ComputeGroupPrivs as String).contains("${group1}: Cluster_usage_priv"))

    logAndExecuteSql """GRANT USAGE_PRIV ON COMPUTE GROUP '${group1}' TO '${user1}'"""
    result = sql_return_maparray """show grants for '${user1}'"""
    commonAuth result, "'${user1}'@'%'" as String, "Yes", "admin", "Admin_priv"
    assertTrue((result.ComputeGroupPrivs as String).contains("${group1}: Cluster_usage_priv"))

    connect(user = "${user1}", password = 'Cloud12345', url = context.config.jdbcUrl) {
        test {
            sql """use @${group1}"""
            exception "${group1} not exist"
        }
        result = sql_return_maparray """show grants for '${user1}'"""
        commonAuth result, "'${user1}'@'%'", "Yes", "admin", "Admin_priv"
        assertTrue((result.ComputeGroupPrivs as String).contains("${group1}: Cluster_usage_priv"))
    }


    logAndExecuteSql """GRANT USAGE_PRIV ON COMPUTE GROUP '${group1}' TO '${user2}'"""
    try {
        result = connect(user = "${user2}", password = 'Cloud12345', url = context.config.jdbcUrl) {
             sql """GRANT USAGE_PRIV ON COMPUTE GROUP '${group1}' TO '${user1}'"""
        }
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Access denied; you need all  [Grant_priv, Cluster_usage_priv] privilege(s) for this operation"), e.getMessage())
    }
    logAndExecuteSql """REVOKE USAGE_PRIV ON COMPUTE GROUP '${group1}' FROM '${user2}'"""

    // default compute group
    logAndExecuteSql """SET PROPERTY FOR '${user1}' 'default_compute_group' = '${validCluster}'"""
    logAndExecuteSql """SET PROPERTY FOR '${user2}' 'default_compute_group' = '${validCluster}'"""
    def show_group_1 = getCluster(validCluster)

    assertTrue(show_group_1[2].contains(user2), "Expect contain users ${user2}")

    result = getProperty("default_compute_group", "${user1}")
    assertEquals(result.Value as String, "${validCluster}" as String)

    connect(user = "${user2}", password = 'Cloud12345', url = context.config.jdbcUrl) {
        result = sql """use @${validCluster}"""
        assertEquals(result[0][0], 0)
        result = getProperty("default_compute_group", "")
        assertEquals(result.Value as String, "${validCluster}" as String) 
    }
        // set default_compute_group to ''
    logAndExecuteSql """SET PROPERTY FOR '${user2}' 'default_compute_group' = ''"""
    connect(user = "${user2}", password = 'Cloud12345', url = context.config.jdbcUrl) {
        result = getProperty("default_compute_group", "")
        assertEquals(result.Value as String, "" as String) 
    } 

    logAndExecuteSql """SET PROPERTY FOR '${user2}' 'default_compute_group' = '${validCluster}'"""
    result = logAndExecuteSql """REVOKE USAGE_PRIV ON COMPUTE GROUP '${validCluster}' FROM '${user2}'"""
    assertEquals(result[0][0], 0)
    connect(user = "${user2}", password = 'Cloud12345', url = context.config.jdbcUrl) {
        test {
            sql """use @${group1}"""
            exception "USAGE denied to user"
        }
    }

    connect(user = "${user2}", password = 'Cloud12345', url = context.config.jdbcUrl) {
        test {
            sql """use @${validCluster}"""
            exception "USAGE denied to user"
        }
    }

    logAndExecuteSql """GRANT USAGE_PRIV ON COMPUTE GROUP '${group1}' TO '${user2}'"""
    logAndExecuteSql """GRANT USAGE_PRIV ON COMPUTE GROUP '${validCluster}' TO '${user2}'"""
    show_group_2 = connect(user = "${user2}", password = 'Cloud12345', url = context.config.jdbcUrl) {
            getCluster(validCluster)
    }

    assertTrue(show_group_2[2].equals(user2), "Expect just only have user ${user2}")

    result = connect(user = "${user2}", password = 'Cloud12345', url = context.config.jdbcUrl) {
            sql """USE @${validCluster}"""
    }
    assertEquals(result[0][0], 0)

    logAndExecuteSql """REVOKE USAGE_PRIV ON COMPUTE GROUP '${validCluster}' FROM '${user2}'"""

    connect(user = "${user2}", password = 'Cloud12345', url = context.config.jdbcUrl) {
        test {
            sql """use @${validCluster}"""
            exception "USAGE denied to user"
        }
        result = sql_return_maparray """show grants for '${user2}'"""
        commonAuth result, "'${user2}'@'%'" as String, "Yes", "", "Select_priv"
        assertTrue((result.ComputeGroupPrivs as String).contains("${group1}: Cluster_usage_priv"))

        test {
            sql """REVOKE USAGE_PRIV ON COMPUTE GROUP 'NotExistCluster' FROM '${user2}'"""
            exception "Access denied; you need all"
        }
    }
    
    logAndExecuteSql """REVOKE USAGE_PRIV ON COMPUTE GROUP '${validCluster}' FROM '${user2}'"""
    result = sql_return_maparray """show grants for '${user2}'"""
    commonAuth result, "'${user2}'@'%'" as String, "Yes", "", "Select_priv"
    assertTrue((result.ComputeGroupPrivs as String).contains("${group1}: Cluster_usage_priv"))

    logAndExecuteSql "sync"
    // 3. revoke group
    // admin role user can revoke group
    result = connect(user = "${user1}", password = 'Cloud12345', url = context.config.jdbcUrl) {
            sql """REVOKE USAGE_PRIV ON COMPUTE GROUP '${group1}' FROM '${user1}'"""
    }

    // revoke GRANT_PRIV from general user, he can not revoke group to other user.
    logAndExecuteSql """revoke GRANT_PRIV on *.*.* from ${user2}"""

    logAndExecuteSql "sync"
    
    // general user can't revoke group
    try {
        result = connect(user = "${user2}", password = 'Cloud12345', url = context.config.jdbcUrl) {
             sql """REVOKE USAGE_PRIV ON COMPUTE GROUP '${group1}' FROM '${user2}'"""
        }
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Access denied; you need all"), e.getMessage())
    }

    result = sql_return_maparray """show grants for '${user1}'"""
    commonAuth result, "'${user1}'@'%'" as String, "Yes", "admin", "Admin_priv"
    assertNull(result.ComputeGroupPrivs[0])

    result = sql_return_maparray """show grants for '${user2}'"""
    commonAuth result, "'${user2}'@'%'" as String, "Yes", "", "Select_priv"
    assertTrue((result.ComputeGroupPrivs as String).contains("${group1}: Cluster_usage_priv")) 

    // revoke user1 admin role
    logAndExecuteSql """REVOKE 'admin' FROM ${user1}"""
    result = sql_return_maparray """show grants for '${user1}'"""
    assertEquals("'${user1}'@'%'" as String, result.UserIdentity[0] as String)
    assertEquals("", result.Roles[0])
    assertNull(result.GlobalPrivs[0])
    assertNull(result.ComputeGroupPrivs[0])

    // user1 no admin auth, so failed to set other default compute group
    try {
        result = connect(user = "${user1}", password = 'Cloud12345', url = context.config.jdbcUrl) {
            sql """SET PROPERTY FOR '${user2}' 'default_compute_group' = '${validCluster}'""" 
        }
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Access denied for user"), e.getMessage())
    }

    logAndExecuteSql """drop user if exists ${user1}"""
        // grant not exists user
    result = logAndExecuteSql """GRANT USAGE_PRIV ON COMPUTE GROUP '${group1}' TO 'NotExitUser'"""
    assertEquals(result[0][0], 0)

    // drop user and grant he group priv
    result = logAndExecuteSql """GRANT USAGE_PRIV ON COMPUTE GROUP '${group1}' TO '${user1}'"""
    assertEquals(result[0][0], 0)
    result = logAndExecuteSql """REVOKE USAGE_PRIV ON COMPUTE GROUP '${group1}' FROM '${user1}'"""
    assertEquals(result[0][0], 0)
    // general user can't grant group to use
    logAndExecuteSql """drop user if exists ${user2}"""
    logAndExecuteSql """drop user if exists ${user3}"""
}


