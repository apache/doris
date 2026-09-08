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

suite("test_show_grants_for_role", "p0,auth") {
    def currentTimeMillis = System.currentTimeMillis()
    String roleName = "test_show_grants_for_role_${currentTimeMillis}_role"
    String noneExistRoleName = "test_show_grants_for_role_${currentTimeMillis}_none_exist_role"
    String userName1WithRole = "'test_show_grants_for_role_${currentTimeMillis}_user1_with_role'@'%'"
    String userName2WithRole = "'test_show_grants_for_role_${currentTimeMillis}_user2_with_role'@'%'"
    String pwd = "123456"
    String dbName = "test_show_grants_for_role_${currentTimeMillis}_db"
    String tableName = "test_show_grants_for_role_${currentTimeMillis}_table"
    String userWithGrantPriv = "test_show_grants_for_role_${currentTimeMillis}_user_with_grant_priv"

    try_sql("DROP ROLE IF EXISTS ${roleName}")
    try_sql("DROP USER IF EXISTS ${userName1WithRole}")
    try_sql("DROP USER IF EXISTS ${userName2WithRole}")

    // Create role and user
    sql """CREATE ROLE ${roleName}"""
    sql """CREATE USER ${userName1WithRole} IDENTIFIED BY '${pwd}' default role '${roleName}'"""
    sql """CREATE USER ${userName2WithRole} IDENTIFIED BY '${pwd}' default role '${roleName}'"""
    sql """CREATE USER ${userWithGrantPriv} IDENTIFIED BY '${pwd}'"""
    sql """create database if not exists ${dbName}"""
    sql """
    CREATE TABLE if not exists ${dbName}.${tableName}(
        id bigint NOT NULL,
        name varchar
    ) ENGINE=OLAP
    unique KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
    );
    """
    sql """insert into ${dbName}.${tableName}(id,name)values(1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),(6,'f');"""

    // Grant privileges to role
    sql """GRANT SELECT_PRIV ON ${dbName}.${tableName} TO ROLE '${roleName}'"""

    // Test show grants for role exists
    def roleGrants = sql """SHOW GRANTS FOR ROLE '${roleName}'"""
    assertTrue(roleGrants.size() == 2)
    logger.info("roleGrants: ${roleGrants}")

    def userTablePrivs = "internal.${dbName}.${tableName}: Select_priv"
    for (int i = 0; i < roleGrants.size(); i++) {
        def grant = roleGrants.get(i)
        logger.info("grant: ${grant}")
        def userIdentity = grant.get(0)
        def roles = grant.get(4)
        def tablePrivs = grant.get(8)
        logger.info("Roles: ${roles}")
        logger.info("UserIdentity: ${userIdentity}")
        logger.info("TablePrivs: ${tablePrivs}")
        assertTrue(userName1WithRole == userIdentity || userName2WithRole == userIdentity)
        assertTrue(roles == roleName)
        assertTrue(tablePrivs == userTablePrivs)
    }

    // test show grants for none exist role
    test {
        sql """SHOW GRANTS FOR ROLE '${noneExistRoleName}'"""
        exception "Role: ${noneExistRoleName} does not exist"
    }

    // test show grants for root role
    def rootUserGrants = sql """show grants for 'root'"""
    def rootRoleGrants = sql """show grants for role 'operator'"""
    assertTrue(rootUserGrants.size() == rootRoleGrants.size())
    for (int i = 0; i < rootUserGrants.size(); i++) {
        def rootUserGrant = rootUserGrants.get(0)
        def rootRoleGrant = rootRoleGrants.get(0)
        for (int j = 0; j < rootUserGrant.size(); j++) {
            assertTrue(rootUserGrant.get(j) == rootRoleGrant.get(j))
        }
    }

    // test show grants for admin role
    def adminUserGrants = sql """show grants for 'admin'"""
    def adminRoleGrants = sql """show grants for role 'admin'"""
    assertTrue(adminUserGrants.size() == adminRoleGrants.size())
    for (int i = 0; i < adminUserGrants.size(); i++) {
        def adminUserGrant = adminUserGrants.get(0)
        def adminRoleGrant = adminRoleGrants.get(0)
        for (int j = 0; j < adminUserGrant.size(); j++) {
            assertTrue(adminUserGrant.get(j) == adminRoleGrant.get(j))
        }
    }

    def tokens = context.config.jdbcUrl.split('/')
    def infoSchemaJdbcUrl = tokens[0] + "//" + tokens[2] + "/" + "information_schema" + "?"
    connect(userWithGrantPriv, "${pwd}", infoSchemaJdbcUrl) {
        test {
            sql """SHOW GRANTS FOR ROLE ${roleName}"""
            exception "Access denied; you need (at least one of) the (GRANT) privilege(s) for this operation"
        }
    }
    """GRANT GRANT_PRIV ON * TO ${userWithGrantPriv}"""
    def globalPrivs = sql """show grants for ${userWithGrantPriv}"""
    logger.info("globalPrivs: ${globalPrivs}")
    connect(userWithGrantPriv, "${pwd}", infoSchemaJdbcUrl) {
        test {
            sql """SHOW GRANTS FOR ROLE ${roleName}"""
        }
    }

    // Cleanup
    try_sql("DROP ROLE if exists ${roleName}")
    try_sql("DROP ROLE if exists ${noneExistRoleName}")
    try_sql("DROP USER if exists ${userName1WithRole}")
    try_sql("DROP USER if exists ${userName2WithRole}")
    try_sql("DROP USER if exists ${userWithGrantPriv}")
}
