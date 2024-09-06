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

suite("test_grant_revoke_cluster_stage_to_role", "cloud_auth") {
    if (!isCloudMode()) {
        log.info("not cloud mode just return")
        return
    }
    def roleName = "testRole"
    def user1 = "test_grant_revoke_cluster_stage_to_user1"
    def tbl = "test_auth_role_tbl"
    def testClusterA = "clusterA"
    def testStageA = "stageA"
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

    def splitCloudAuth = { String st ->
        def map = [:]
        st.split(';').each { entry ->
            def parts = entry.trim().split(':')
            if (parts.size() == 2) {
                map[parts[0].trim()] = parts[1].trim()
            }
        }
        return map
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
    assertNull(result.CloudStagePrivs)
    assertEquals(result.Users, "")

    // grant cluster and stage usage_priv to role
    sql """
        grant usage_priv on cluster '$testClusterA' to role "${roleName}";
        """

    sql """
        grant usage_priv on stage '$testStageA' to role "${roleName}";
        """

    result = showRoles.call(roleName)
    assertTrue((result.CloudClusterPrivs as String).contains("$testClusterA: Cluster_usage_priv")) 
    assertTrue((result.CloudStagePrivs as String).contains("$testStageA: Stage_usage_priv")) 
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
    commonAuth result, "'${user1}'@'%'", "Yes", "testRole", "Select_priv"
    assertTrue((result.CloudClusterPrivs as String).contains("$validCluster: Cluster_usage_priv"))
    assertTrue((result.CloudClusterPrivs as String).contains("$testClusterA: Cluster_usage_priv"))
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

    sql """
        grant usage_priv on stage * to role "${roleName}";
        """

    result = showRoles.call(roleName)
    log.info(result as String)
    def m = splitCloudAuth(result.CloudClusterPrivs as String)
    assertTrue(m.keySet().containsAll(Arrays.asList("${testClusterA}" as String, "%", "${validCluster}" as String)) as boolean)
    assertEquals(m.values().toUnique().asList().size(), 1)
    assertEquals(m.values().toUnique().asList().get(0) as String, "Cluster_usage_priv")
    assertEquals(result.GlobalPrivs as String, "Select_priv" as String)
    m = splitCloudAuth(result.CloudStagePrivs as String)
    assertTrue(m.keySet().containsAll(Arrays.asList("${testStageA}" as String, "%")) as boolean)
    assertEquals(m.values().toUnique().asList().size(), 1)
    assertEquals(m.values().toUnique().asList().get(0) as String, "Stage_usage_priv")
    assertEquals(result.GlobalPrivs as String, "Select_priv" as String) 
    def matcher = result.Users =~ /.*${user1}.*/
    assertTrue(matcher.matches())


    result = sql_return_maparray """show grants for '${user1}'"""
    commonAuth result, "'${user1}'@'%'", "Yes", "testRole", "Select_priv"
    m = splitCloudAuth(result.CloudClusterPrivs[0] as String)
    log.info(m.keySet() as String)
    // clusterA: Cluster_usage_priv ; %: Cluster_usage_priv ; compute_cluster: Cluster_usage_priv
    assertTrue(m.keySet().containsAll(Arrays.asList("${testClusterA}" as String, "%", "${validCluster}" as String)) as boolean)
    assertEquals(m.values().toUnique().asList().size(), 1)
    assertEquals(m.values().toUnique().asList().get(0) as String, "Cluster_usage_priv")
    assertEquals(result.GlobalPrivs[0] as String, "Select_priv" as String)

    def otherRole = "testOtherRole"
    def testClusterB = "clusterB"
    def testStageB = "stageB"

    sql """
        grant usage_priv on cluster ${testClusterB} to role "${otherRole}";
    """

    sql """
        grant usage_priv on stage ${testStageB} to role "${otherRole}"; 
    """

    result = showRoles.call(otherRole)
    assertTrue((result.CloudClusterPrivs as String).contains("$testClusterB: Cluster_usage_priv"))
    assertTrue((result.CloudStagePrivs as String).contains("$testStageB: Stage_usage_priv"))
    assertEquals(result.Users as String, "")

    // add more roles to user1
    sql """
        GRANT '$otherRole' TO '$user1';  
    """
    result = showRoles.call(otherRole)
    assertTrue((result.CloudClusterPrivs as String).contains("$testClusterB: Cluster_usage_priv"))
    assertTrue((result.CloudStagePrivs as String).contains("$testStageB: Stage_usage_priv"))
    matcher = result.Users =~ /.*${user1}.*/
    assertTrue(matcher.matches())

    result = sql_return_maparray """show grants for '${user1}'"""
    log.info(result as String)
    commonAuth result, "'${user1}'@'%'", "Yes", "testOtherRole,testRole", "Select_priv"
    // [%: Cluster_usage_priv ; clusterA: Cluster_usage_priv ; clusterB: Cluster_usage_priv ; compute_cluster: Cluster_usage_priv ]
    m = splitCloudAuth(result.CloudClusterPrivs[0] as String)
    assertTrue(m.keySet().containsAll(Arrays.asList("${testClusterA}" as String, "%", "${validCluster}" as String, "${testClusterB}" as String)) as boolean)
    assertEquals(m.values().toUnique().asList().size(), 1)
    assertEquals(m.values().toUnique().asList().get(0) as String, "Cluster_usage_priv")

    // revoke cluster and stage usage_priv from role
    sql """
        revoke usage_priv on cluster '$testClusterA' from role "${roleName}";
        """

    sql """
        revoke usage_priv on stage '$testStageA' from role "${roleName}";
        """
    
    result = showRoles.call(roleName)
    log.info(result as String)
    // CloudClusterPrivs:clusterA: ; %: Cluster_usage_priv ; compute_cluster: Cluster_usage_priv
    m = splitCloudAuth(result.CloudClusterPrivs as String)
    // clusterA lost in splitCloudAuth
    assertTrue(m.keySet().containsAll(Arrays.asList("%", "${validCluster}" as String)) as boolean)
    assertEquals(m.values().toUnique().asList().size(), 1)
    assertEquals(m.values().toUnique().asList().get(0) as String, "Cluster_usage_priv")
    matcher = result.Users =~ /.*${user1}.*/
    assertTrue(matcher.matches())

    result = sql_return_maparray """show grants for '${user1}'"""
    commonAuth result, "'${user1}'@'%'", "Yes", "testOtherRole,testRole", "Select_priv"
    log.info(result as String)
    // CloudClusterPrivs:%: Cluster_usage_priv ; clusterB: Cluster_usage_priv ; compute_cluster: Cluster_usage_priv
    m = splitCloudAuth(result.CloudClusterPrivs[0] as String)
    assertTrue(m.keySet().containsAll(Arrays.asList("%", "${validCluster}" as String, "${testClusterB}" as String)) as boolean)
    assertEquals(m.values().toUnique().asList().size(), 1)
    assertEquals(m.values().toUnique().asList().get(0) as String, "Cluster_usage_priv")
    
    // revoke otherRole from user1
    sql """
        REVOKE '$otherRole' FROM '${user1}'
    """

    result = showRoles.call(otherRole)
    assertTrue((result.CloudClusterPrivs as String).contains("$testClusterB: Cluster_usage_priv"))
    assertEquals(result.Users as String, "")

    result = sql_return_maparray """show grants for '${user1}'"""
    commonAuth result, "'${user1}'@'%'", "Yes", "testRole", "Select_priv"
    log.info(result as String)
    // CloudClusterPrivs:%: Cluster_usage_priv ; compute_cluster: Cluster_usage_priv
    m = splitCloudAuth(result.CloudClusterPrivs[0] as String)
    assertTrue(m.keySet().containsAll(Arrays.asList("%", "${validCluster}" as String)) as boolean)
    assertEquals(m.values().toUnique().asList().size(), 1)
    assertEquals(m.values().toUnique().asList().get(0) as String, "Cluster_usage_priv")

    sql """
        revoke usage_priv on cluster ${validCluster} from role "${roleName}";
        """

    result = showRoles.call(roleName)
    log.info(result as String)
    // CloudClusterPrivs:clusterA: ; %: Cluster_usage_priv ; compute_cluster:
    m = splitCloudAuth(result.CloudClusterPrivs as String)
    assertTrue(m.keySet().containsAll(Arrays.asList("%")) as boolean)
    assertEquals(m.values().toUnique().asList().size(), 1)
    assertEquals(m.values().toUnique().asList().get(0) as String, "Cluster_usage_priv")

    // still can select, because have global * cluster
    connect(user = "${user1}", password = 'Cloud12345', url = context.config.jdbcUrl) {
        sql """use @${validCluster}"""
        def sqlRet = sql """SELECT * FROM ${db}.${tbl}"""
        assertEquals(sqlRet[0][0] as int, 1)
        assertEquals(sqlRet[0][1] as int, 10)
    } 

    result = sql_return_maparray """show grants for '${user1}'"""
    commonAuth result, "'${user1}'@'%'", "Yes", "testRole", "Select_priv"
    m = splitCloudAuth(result.CloudClusterPrivs[0] as String)
    assertTrue(m.keySet().containsAll(Arrays.asList("%")) as boolean)
    assertEquals(m.values().toUnique().asList().size(), 1)
    assertEquals(m.values().toUnique().asList().get(0) as String, "Cluster_usage_priv")

    // revoke global * from role
    sql """
        revoke usage_priv on cluster * from role "${roleName}";
        """

    sql """
        revoke usage_priv on stage * from role "${roleName}";
        """

    result = showRoles.call(roleName)
    log.info(result as String)
    // CloudClusterPrivs:clusterA: ; %: ; compute_cluster:
    m = splitCloudAuth(result.CloudClusterPrivs as String)
    assertEquals(m.size(), 0)
    m = splitCloudAuth(result.CloudStagePrivs as String)
    assertEquals(m.size(), 0)
    assertEquals(result.GlobalPrivs as String, "Select_priv" as String)

    result = sql_return_maparray """show grants for '${user1}'"""
    commonAuth result, "'${user1}'@'%'", "Yes", "testRole", "Select_priv"

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

