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

import org.junit.Assert

// End-to-end check, over real MySQL connections, of the two rules PR #70371 adds:
//   1. GRANT <role> and CREATE USER ... DEFAULT ROLE are held to the rule a direct GRANT already
//      enforces: the grantor needs grant_priv plus every privilege the role carries. admin_priv is
//      exempt. A grant that is refused must leave the target without the role, which is proved here
//      by the target being unable to USE the database until the grant actually goes through.
//   2. Changing another account's credentials (ALTER USER ... IDENTIFIED BY, SET PASSWORD FOR) needs
//      admin_priv; one's own credentials and the account-lock operations stay under grant_priv.
suite("test_grant_role_scope_auth", "p0,auth_call") {
    String grantor = 'test_grs_grantor'
    String target = 'test_grs_target'
    String adminUser = 'test_grs_admin'
    String newUser = 'test_grs_new'
    String role = 'test_grs_role'
    String dbName = 'test_grs_db'
    String pwd = 'C123_567p'

    // Drop before use (keep the environment for debugging on failure).
    try_sql("DROP USER ${grantor}")
    try_sql("DROP USER ${target}")
    try_sql("DROP USER ${adminUser}")
    try_sql("DROP USER ${newUser}")
    try_sql("DROP ROLE ${role}")
    sql "DROP DATABASE IF EXISTS ${dbName}"

    sql "CREATE DATABASE ${dbName}"
    sql "CREATE ROLE ${role}"
    sql "GRANT SELECT_PRIV ON internal.${dbName}.* TO ROLE '${role}'"

    sql "CREATE USER '${grantor}'@'%' IDENTIFIED BY '${pwd}'"
    sql "GRANT GRANT_PRIV ON *.*.* TO '${grantor}'@'%'"
    sql "CREATE USER '${target}'@'%' IDENTIFIED BY '${pwd}'"
    sql "CREATE USER '${adminUser}'@'%' IDENTIFIED BY '${pwd}'"
    sql "GRANT ADMIN_PRIV ON *.*.* TO '${adminUser}'@'%'"

    // Each account opens its connection with the framework's default database, so it needs read access
    // to that database just to connect. This is a different object from the one the role under test
    // carries (internal.${dbName}), so it does not affect any grant decision being checked below.
    sql "GRANT SELECT_PRIV ON regression_test TO '${grantor}'@'%'"
    sql "GRANT SELECT_PRIV ON regression_test TO '${target}'@'%'"

    // In cloud mode a user needs cluster usage to open a working connection; the role under test does
    // not carry a cluster privilege, so this does not affect the rule being checked. newUser is created
    // later, so its cluster usage is granted then.
    String validCluster = null
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        validCluster = clusters[0][0]
        sql "GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO '${grantor}'@'%'"
        sql "GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO '${target}'@'%'"
    }

    // The target has no privilege on the database yet: it cannot use it.
    connect(target, "${pwd}", context.config.jdbcUrl) {
        test {
            sql "USE internal.${dbName}"
            exception "denied"
        }
    }

    // grant_priv alone cannot hand out a role carrying SELECT, nor the built-in admin role.
    connect(grantor, "${pwd}", context.config.jdbcUrl) {
        test {
            sql "GRANT '${role}' TO '${grantor}'@'%'"
            exception "denied"
        }
        test {
            sql "GRANT '${role}' TO '${target}'@'%'"
            exception "denied"
        }
        test {
            sql "CREATE USER '${newUser}'@'%' IDENTIFIED BY '${pwd}' DEFAULT ROLE '${role}'"
            exception "denied"
        }
        test {
            sql "GRANT 'admin' TO '${target}'@'%'"
            exception "denied"
        }

        // Another account's credentials need admin_priv.
        test {
            sql "ALTER USER '${target}'@'%' IDENTIFIED BY '${pwd}'"
            exception "denied"
        }
        test {
            sql "SET PASSWORD FOR '${target}'@'%' = PASSWORD('${pwd}')"
            exception "denied"
        }

        // One's own credentials, and locking another account, stay under grant_priv.
        sql "SET PASSWORD FOR '${grantor}'@'%' = PASSWORD('${pwd}')"
        sql "ALTER USER '${grantor}'@'%' REQUIRE NONE"
        sql "ALTER USER '${target}'@'%' ACCOUNT_LOCK"
        sql "ALTER USER '${target}'@'%' ACCOUNT_UNLOCK"
    }

    // The refused grants left the target without the role: it still cannot use the database.
    connect(target, "${pwd}", context.config.jdbcUrl) {
        test {
            sql "USE internal.${dbName}"
            exception "denied"
        }
    }

    // Once the grantor holds what the role carries, the grant is its to make.
    sql "GRANT SELECT_PRIV ON internal.${dbName}.* TO '${grantor}'@'%'"
    connect(grantor, "${pwd}", context.config.jdbcUrl) {
        sql "GRANT '${role}' TO '${target}'@'%'"
        sql "CREATE USER '${newUser}'@'%' IDENTIFIED BY '${pwd}' DEFAULT ROLE '${role}'"
    }

    // Let the freshly created account reach the framework's default database, so it too can connect.
    sql "GRANT SELECT_PRIV ON regression_test TO '${newUser}'@'%'"
    if (isCloudMode()) {
        sql "GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO '${newUser}'@'%'"
    }

    // The grant took effect end to end: both accounts can now use the database through the role.
    connect(target, "${pwd}", context.config.jdbcUrl) {
        sql "USE internal.${dbName}"
    }
    connect(newUser, "${pwd}", context.config.jdbcUrl) {
        sql "USE internal.${dbName}"
    }

    // admin_priv may grant any role and change another account's credentials.
    connect(adminUser, "${pwd}", context.config.jdbcUrl) {
        sql "GRANT '${role}' TO '${adminUser}'@'%'"
        sql "ALTER USER '${target}'@'%' IDENTIFIED BY '${pwd}'"
        sql "SET PASSWORD FOR '${target}'@'%' = PASSWORD('${pwd}')"
    }
}
