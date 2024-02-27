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

suite("test_grant_revoke_cluster_to_role", "cloud_auth") {
    def isCloudMode = {
        def ret = sql_return_maparray  """show backends"""
        ret.Tag[0].contains("cloud_cluster_name")
    }
    def cloudMode = isCloudMode.call()
    log.info("is cloud mode $cloudMode")
    if (!cloudMode) {
        return
    }
    def roleName = "testRole"
    def user1 = "test_grant_revoke_cluster_to_user1"
    def tbl = "test_auth_role_tbl"
    def testClusterA = "clusterA"
    sql """drop table if exists ${tbl}"""

    sql """drop user if exists ${user1}"""
    sql """
         drop role if exists ${roleName}
        """

    def showRoles = { name ->
        def ret = sql_return_maparray """show roles"""
        ret.find {
            def matcher = it.Name =~ /.*${name}$/
            matcher.matches()
        }
    }

    def getCluster = { cluster ->
        def result = sql " SHOW CLUSTERS; "
        for (int i = 0; i < result.size(); i++) {
            if (result[i][0] == cluster) {
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
    
    def fieldDisorder = { result, expected1, expected2 ->
        boolean ret = false
        if ((result as String) == (expected1 as String) || (result as String) == (expected2 as String)) {
            ret = true
        }
        return ret
    }

    def clusters = sql " SHOW CLUSTERS; "
    assertTrue(!clusters.isEmpty())
    def validCluster = clusters[0][0]

    // 1. create role
    sql """
        create role ${roleName}
        """

    def result = showRoles.call(roleName)
    assertNull(result.CloudClusterPrivs)
    assertEquals(result.Users, "")

    // grant cluster usage_priv to role
    sql """
        grant usage_priv on cluster '$testClusterA' to role "${roleName}";
        """
    result = showRoles.call(roleName)
    assertEquals(result.CloudClusterPrivs as String, "$testClusterA: Cluster_Usage_priv " as String)
    assertEquals(result.Users as String, "")

    sql """
        create user "${user1}" identified by 'Cloud12345' default role "${roleName}"
        """
    
    sql """
        GRANT USAGE_PRIV ON CLUSTER '${validCluster}' TO ROLE '${roleName}'
    """

    sql """
        GRANT SELECT_PRIV ON *.*.* TO ROLE '${roleName}'
    """
    result = sql_return_maparray """show grants for '${user1}'"""
    commonAuth result, "'${user1}'@'%'", "Yes", "testRole", "Select_priv "
    assertEquals("[$testClusterA: Cluster_Usage_priv ; $validCluster: Cluster_Usage_priv ]" as String, result.CloudClusterPrivs as String)
    def db = context.dbName

    sql """
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

    sql """
    set enable_memtable_on_sink_node = false
    """

    sql """
        insert into ${tbl} (k1, k2) values (1, "10");
    """

    connect(user = "${user1}", password = 'Cloud12345', url = context.config.jdbcUrl) {
        sql """use @${validCluster}"""
        def sqlRet = sql """SELECT * FROM ${db}.${tbl}"""
        assertEquals(sqlRet[0][0] as int, 1)
        assertEquals(sqlRet[0][1] as int, 10)
    }

    // grant * to role
    sql """
        grant usage_priv on cluster * to role "${roleName}";
        """

    result = showRoles.call(roleName)
    assertTrue(fieldDisorder.call(result.CloudClusterPrivs as String,
        "$testClusterA: Cluster_Usage_priv ; $validCluster: Cluster_Usage_priv " as String, 
        "$validCluster: Cluster_Usage_priv ; $testClusterA: Cluster_Usage_priv " as String) as boolean)
    assertEquals(result.GlobalPrivs as String, "Select_priv  Cluster_Usage_priv " as String)
    def matcher = result.Users =~ /.*${user1}.*/
    assertTrue(matcher.matches())


    result = sql_return_maparray """show grants for '${user1}'"""
    commonAuth result, "'${user1}'@'%'", "Yes", "testRole", "Select_priv Cluster_Usage_priv "
    assertEquals("[$testClusterA: Cluster_Usage_priv ; $validCluster: Cluster_Usage_priv ]" as String, result.CloudClusterPrivs as String)

    def otherRole = "testOtherRole"
    def testClusterB = "clusterB"

    sql """
        grant usage_priv on cluster ${testClusterB} to role "${otherRole}";
    """

    result = showRoles.call(otherRole)
    assertEquals(result.CloudClusterPrivs as String, "$testClusterB: Cluster_Usage_priv " as String)
    assertEquals(result.Users as String, "")

    // add more roles to user1
    sql """
        GRANT '$otherRole' TO '$user1';  
    """
    result = showRoles.call(otherRole)
    assertEquals(result.CloudClusterPrivs as String, "$testClusterB: Cluster_Usage_priv " as String)
    matcher = result.Users =~ /.*${user1}.*/
    assertTrue(matcher.matches())

    result = sql_return_maparray """show grants for '${user1}'"""
    commonAuth result, "'${user1}'@'%'", "Yes", "testOtherRole,testRole", "Select_priv Cluster_Usage_priv "
    assertEquals("[$testClusterA: Cluster_Usage_priv ; $testClusterB: Cluster_Usage_priv ; $validCluster: Cluster_Usage_priv ]" as String,
        result.CloudClusterPrivs as String) 

    // revoke cluster usage_priv from role
    sql """
        revoke usage_priv on cluster '$testClusterA' from role "${roleName}";
        """

    result = showRoles.call(roleName)
    assertTrue(fieldDisorder.call(result.CloudClusterPrivs as String,
        "$testClusterA: ; $validCluster: Cluster_Usage_priv " as String, 
        "$validCluster: Cluster_Usage_priv ; $testClusterA: " as String) as boolean)
    matcher = result.Users =~ /.*${user1}.*/
    assertTrue(matcher.matches())

    result = sql_return_maparray """show grants for '${user1}'"""
    commonAuth result, "'${user1}'@'%'", "Yes", "testOtherRole,testRole", "Select_priv Cluster_Usage_priv "
    assertEquals("[$testClusterB: Cluster_Usage_priv ; $validCluster: Cluster_Usage_priv ]" as String,
        result.CloudClusterPrivs as String) 
    
    // revoke otherRole from user1
    sql """
        REVOKE '$otherRole' FROM '${user1}'
    """

    result = showRoles.call(otherRole)
    assertEquals(result.CloudClusterPrivs as String, "$testClusterB: Cluster_Usage_priv " as String)
    assertEquals(result.Users as String, "")

    result = sql_return_maparray """show grants for '${user1}'"""
    commonAuth result, "'${user1}'@'%'", "Yes", "testRole", "Select_priv Cluster_Usage_priv "
    assertEquals("[$validCluster: Cluster_Usage_priv ]" as String, result.CloudClusterPrivs as String) 

    sql """
        revoke usage_priv on cluster ${validCluster} from role "${roleName}";
        """

    result = showRoles.call(roleName)
    assertTrue(fieldDisorder.call(result.CloudClusterPrivs as String,
        "$testClusterA: ; $validCluster: " as String, 
        "$validCluster: ; $testClusterA: " as String as String) as boolean)

    // still can select, because have global * cluster
    connect(user = "${user1}", password = 'Cloud12345', url = context.config.jdbcUrl) {
        sql """use @${validCluster}"""
        def sqlRet = sql """SELECT * FROM ${db}.${tbl}"""
        assertEquals(sqlRet[0][0] as int, 1)
        assertEquals(sqlRet[0][1] as int, 10)
    } 

    result = sql_return_maparray """show grants for '${user1}'"""
    commonAuth result, "'${user1}'@'%'", "Yes", "testRole", "Select_priv Cluster_Usage_priv "
    assertNull(result.CloudClusterPrivs[0])

    // revoke global * from role
    sql """
        revoke usage_priv on cluster * from role "${roleName}";
        """

    result = showRoles.call(roleName)
    assertTrue(fieldDisorder.call(result.CloudClusterPrivs as String,
        "$testClusterA: ; $validCluster: " as String, 
        "$validCluster: ; $testClusterA: " as String as String) as boolean)
    assertEquals(result.GlobalPrivs as String, "Select_priv  " as String)

    result = sql_return_maparray """show grants for '${user1}'"""
    commonAuth result, "'${user1}'@'%'", "Yes", "testRole", "Select_priv "

    // can not use @cluster, because no cluster auth
    connect(user = "${user1}", password = 'Cloud12345', url = context.config.jdbcUrl) {
        test {
            sql """use @${validCluster}"""
            exception "USAGE denied to user"
        }
    } 

    sql """
        drop user ${user1}
        """

    sql """
        drop role ${roleName}
        """

    sql """
        drop role ${otherRole}
        """
}

