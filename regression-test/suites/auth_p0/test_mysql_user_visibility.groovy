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

suite("test_mysql_user_visibility", "p0,auth") {
    String suiteName = "test_mysql_user_visibility"
    String user1 = "${suiteName}_user1"
    String user2 = "${suiteName}_user2"
    String pwd = 'C123_567p'

    try_sql("DROP USER ${user1}")
    try_sql("DROP USER ${user2}")
    sql """CREATE USER '${user1}' IDENTIFIED BY '${pwd}'"""
    sql """CREATE USER '${user2}' IDENTIFIED BY '${pwd}'"""

    // cloud-mode: a user needs cluster usage before it can run any query.
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO ${user1}"""
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO ${user2}"""
    }

    // The connection targets the regression_test database, so user1 needs a privilege on it
    // to establish the session. This is unrelated to mysql.user visibility (that comes from
    // the default role's SELECT on mysql.*), it only makes connect() below succeed.
    sql """GRANT SELECT_PRIV ON regression_test TO ${user1}"""

    // A role administrator (root here) sees every account, but the password-derived
    // columns are always masked, even though these users have a non-empty password.
    def adminRows = sql """
        SELECT User, authentication_string, `password_policy.history_passwords`
        FROM mysql.user
    """
    assertTrue(adminRows.any { it[0] == user1 }, "admin should see ${user1}")
    assertTrue(adminRows.any { it[0] == user2 }, "admin should see ${user2}")
    adminRows.each {
        assertEquals("***", it[1], "authentication_string must be masked for admin")
        assertEquals("***", it[2], "history_passwords must be masked for admin")
    }

    // A non-privileged user only sees their own row, with the password columns masked,
    // and must not be able to enumerate other accounts through mysql.user.
    connect(user1, "${pwd}", context.config.jdbcUrl) {
        def rows = sql """
            SELECT User, authentication_string, `password_policy.history_passwords`
            FROM mysql.user
        """
        assertTrue(!rows.isEmpty(), "${user1} should see its own row")
        rows.each {
            assertEquals(user1, it[0], "${user1} should only see its own account")
            assertEquals("***", it[1], "authentication_string must be masked")
            assertEquals("***", it[2], "history_passwords must be masked")
        }
        assertFalse(rows.any { it[0] == user2 }, "${user1} must not see ${user2}")
    }

    try_sql("DROP USER ${user1}")
    try_sql("DROP USER ${user2}")
}
