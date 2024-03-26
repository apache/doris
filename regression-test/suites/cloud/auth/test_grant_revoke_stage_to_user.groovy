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

suite("test_grant_revoke_stage_to_user", "cloud_auth") {
    if (!isCloudMode()) {
        log.info("not cloud mode just return")
        return
    }
    def user1 = "regression_test_user1"
    def stage1 = "test_stage_1"
    def role = "admin"
    def user2 = "regression_test_user2"
    def stage2 = "test_stage_2"

    def commonAuth = { result, UserIdentity, Password, Roles, GlobalPrivs ->
        assertEquals(UserIdentity as String, result.UserIdentity[0] as String)
        assertEquals(Password as String, result.Password[0] as String)
        assertEquals(Roles as String, result.Roles[0] as String)
        assertEquals(GlobalPrivs as String, result.GlobalPrivs[0] as String)
    }

    try_sql("DROP USER if exists ${user1}")
    try_sql("DROP USER if exists ${user2}")
    sql """CREATE USER '${user1}' IDENTIFIED BY 'Cloud123456' DEFAULT ROLE '${role}'"""
    sql """CREATE USER '${user2}' IDENTIFIED BY 'Cloud123456'"""

    // TODO(dx): wait laihui copy into merge
    return
    try {
        // admin role create stage
        connect(user=user1, password='Cloud123456', url=context.config.jdbcUrl) {
            sql """
                CREATE STAGE IF NOT EXISTS ${stage1} PROPERTIES (
                'endpoint' = 'oss-cn-hangzhou-internal.aliyuncs.com',
                'region' = 'oss-cn-hangzhou',
                'bucket' = 'selectdb-test',
                'prefix' = 'test_stage',
                'provider' = 'OSS',
                'access_type' = 'AKSK',
                'ak' = 'XX',
                'sk' = 'XX'
                )
            """
        }
    } catch (Exception e) {
        // error, not auth error
        assertTrue(e.getMessage().contains("Incorrect object storage info, connect timed out"), e.getMessage())
    }

    try {
        // non admin role create stage
        connect(user=user2, password='Cloud123456', url=context.config.jdbcUrl) {
            sql """
                DROP STAGE IF EXISTS ${stage1}
            """
        }
    } catch (Exception e) {
        // error, not auth error
        assertTrue(e.getMessage().contains("Access denied for user"), e.getMessage())
    }

    connect(user=user1, password='Cloud123456', url=context.config.jdbcUrl) {
        sql """
            DROP STAGE IF EXISTS ${stage1}
        """
    }

    try {
        // non admin role create stage
        connect(user=user2, password='Cloud123456', url=context.config.jdbcUrl) {
            sql """
                CREATE STAGE IF NOT EXISTS ${stage2} PROPERTIES (
                'endpoint' = 'oss-cn-hangzhou-internal.aliyuncs.com',
                'region' = 'oss-cn-hangzhou',
                'bucket' = 'selectdb-test',
                'prefix' = 'test_stage',
                'provider' = 'OSS',
                'access_type' = 'AKSK',
                'ak' = 'XX',
                'sk' = 'XX'
                )
            """
        }
    } catch (Exception e) {
        // error, not auth error
        assertTrue(e.getMessage().contains("Access denied for user"), e.getMessage())
    }


    def succ1 = try_sql """
        GRANT USAGE_PRIV ON STAGE ${stage1} TO ${user1};
    """
    // OK
    assertEquals(succ1.size(), 1)
    def result = sql_return_maparray """show grants for '${user1}'"""
    commonAuth result, "'${user1}'@'%'" as String, "Yes", "admin", "Admin_priv "
    assertEquals(result.CloudStagePrivs as String, "[$stage1: Stage_Usage_priv ]" as String)

    sql """GRANT USAGE_PRIV ON STAGE ${stage1} TO ${user2};"""
    result = sql_return_maparray """show grants for '${user2}'"""
    assertEquals(result.CloudStagePrivs as String, "[$stage1: Stage_Usage_priv ]" as String)
    assertEquals("", result.Roles[0])

    def clusters = sql " SHOW CLUSTERS; "
    assertTrue(!clusters.isEmpty())
    def validCluster = clusters[0][0]

    sql """GRANT Usage_priv on cluster '${validCluster}' to ${user1}"""
    sql """GRANT Usage_priv on cluster '${validCluster}' to ${user2}"""

    connect(user = "${user1}", password = 'Cloud123456', url = context.config.jdbcUrl) {
        test {
            sql """use @${validCluster}"""
            sql """COPY INTO test_table FROM @${stage1}"""
            // error, but not auth error, througth auth check
            exception "Stage does not exist"
        }
    } 
    
    try {
        result = connect(user = "${user2}", password = 'Cloud12345', url = context.config.jdbcUrl) {
            sql """use @${validCluster}"""
            sql """COPY INTO test_table FROM @${stage1}"""
        }
    } catch (Exception e) {
        // error, auth error, user2 no stage auth
        assertTrue(e.getMessage().contains("Access denied for user"), e.getMessage())
    }

    sql """
        GRANT USAGE_PRIV ON STAGE ${stage1} TO ${user2};
    """

    sql """
        GRANT LOAD_PRIV ON *.*.* TO ${user2};
    """

    connect(user = "${user2}", password = 'Cloud123456', url = context.config.jdbcUrl) {
        test {
            sql """use @${validCluster}"""
            sql """COPY INTO test_table FROM @${stage1}"""
            // error, but not auth error, througth auth check
            exception "Stage does not exist"
        }
    }

    sql """
        GRANT USAGE_PRIV ON STAGE 'notExistStage' TO ${user2}; 
    """

    result = sql_return_maparray """show grants for '${user2}'"""
    assertEquals(result.CloudStagePrivs as String, "[notExistStage: Stage_Usage_priv ; $stage1: Stage_Usage_priv ]" as String)
    assertEquals("", result.Roles[0])

    sql """
        REVOKE USAGE_PRIV ON STAGE 'notExistStage' FROM ${user2}; 
    """

    result = sql_return_maparray """show grants for '${user2}'"""
    assertEquals(result.CloudStagePrivs as String, "[$stage1: Stage_Usage_priv ]" as String)

    sql "sync"

    connect(user=user1, password='Cloud123456', url=context.config.jdbcUrl) {
        result = sql_return_maparray """show grants for '${user1}'"""
        commonAuth result, "'${user1}'@'%'" as String, "Yes", "admin", "Admin_priv "
        assertEquals(result.CloudStagePrivs as String, "[$stage1: Stage_Usage_priv ]" as String)
    }



    def succ3 = try_sql """
        REVOKE USAGE_PRIV ON STAGE ${stage1} FROM ${user1};
    """
    assertEquals(succ3.size(), 1)

    try {
        result = connect(user = "${user1}", password = 'Cloud12345', url = context.config.jdbcUrl) {
            sql """use @${validCluster}"""
            sql """COPY INTO test_table FROM @${stage1}"""
        }
    } catch (Exception e) {
        // error, auth error
        assertTrue(e.getMessage().contains("Access denied for user"), e.getMessage())
    }

    sql """
        REVOKE USAGE_PRIV ON STAGE '${stage1}' FROM ${user1};
    """

    result = sql_return_maparray """show grants for '${user1}'"""
    commonAuth result, "'${user1}'@'%'" as String, "Yes", "admin", "Admin_priv "
    assertNull(result.CloudStagePrivs[0])

    def succ4 = try_sql """
        DROP USER ${user1}
    """
    assertEquals(succ4.size(), 1)

    def succ5 = try_sql """
        DROP USER ${user2}
    """
    assertEquals(succ5.size(), 1)
}
