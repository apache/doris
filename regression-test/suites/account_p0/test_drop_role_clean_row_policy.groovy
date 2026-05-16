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

suite("test_drop_role_clean_row_policy") {
    def dbName = context.config.getDbNameByFile(context.file)
    def tableName = "drop_role_row_policy_tbl"
    def user1 = "drop_role_policy_user1"
    def user2 = "drop_role_policy_user2"
    def role = "drop_role_policy_role"
    def tokens = context.config.jdbcUrl.split('/')
    def url = tokens[0] + "//" + tokens[2] + "/" + dbName + "?"

    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql """CREATE TABLE ${tableName} (k INT, v INT) DISTRIBUTED BY HASH(k) PROPERTIES('replication_num'='1')"""
    sql """INSERT INTO ${tableName} VALUES (1,1), (2,2), (3,3)"""

    // ========== Test 1: drop role should clean row policies bound to that role ==========
    sql """DROP USER IF EXISTS ${user1}"""
    sql """CREATE USER ${user1} IDENTIFIED BY '123456'"""
    sql """GRANT SELECT_PRIV ON ${dbName} TO ${user1}"""

    sql """DROP ROLE IF EXISTS ${role}"""
    sql """CREATE ROLE ${role}"""
    sql """GRANT ${role} TO ${user1}"""
    sql """GRANT SELECT_PRIV ON ${dbName} TO '${role}'"""

    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO ${user1}""";
    }

    sql """DROP ROW POLICY IF EXISTS policy_drop_role_test ON ${tableName} FOR ROLE ${role}"""
    sql """CREATE ROW POLICY policy_drop_role_test ON ${tableName} AS RESTRICTIVE TO ROLE ${role} USING(k=1)"""

    order_qt_select_before_drop_role "SELECT * FROM ${tableName} ORDER BY k"

    connect(user1, '123456', url) {
        def result = sql "SELECT * FROM ${tableName} ORDER BY k"
        assertEquals(1, result.size())
        assertEquals(1, result[0][0])
    }

    sql """DROP ROLE ${role}"""

    connect(user1, '123456', url) {
        def result = sql "SELECT * FROM ${tableName} ORDER BY k"
        assertEquals(3, result.size())
    }

    sql """DROP ROW POLICY IF EXISTS policy_drop_role_test ON ${tableName} FOR ROLE ${role}"""
    sql """DROP USER IF EXISTS ${user1}"""

    // ========== Test 2: drop user should clean row policies bound to that user ==========
    sql """DROP USER IF EXISTS ${user2}"""
    sql """CREATE USER ${user2} IDENTIFIED BY '123456'"""
    sql """GRANT SELECT_PRIV ON ${dbName} TO ${user2}"""

    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO ${user2}""";
    }

    sql """DROP ROW POLICY IF EXISTS policy_drop_user_test ON ${tableName} FOR ${user2}"""
    sql """CREATE ROW POLICY policy_drop_user_test ON ${tableName} AS RESTRICTIVE TO ${user2} USING(k=2)"""

    connect(user2, '123456', url) {
        def result = sql "SELECT * FROM ${tableName} ORDER BY k"
        assertEquals(1, result.size())
        assertEquals(2, result[0][0])
    }

    sql """DROP USER ${user2}"""

    // After dropping the user, the row policy bound to that user should be cleaned up.
    // Verify by creating a new user with the same name - it should see all rows.
    sql """CREATE USER ${user2} IDENTIFIED BY '123456'"""
    sql """GRANT SELECT_PRIV ON ${dbName} TO ${user2}"""

    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO ${user2}""";
    }

    connect(user2, '123456', url) {
        def result = sql "SELECT * FROM ${tableName} ORDER BY k"
        assertEquals(3, result.size())
    }

    sql """DROP ROW POLICY IF EXISTS policy_drop_user_test ON ${tableName} FOR ${user2}"""
    sql """DROP USER IF EXISTS ${user2}"""
}
