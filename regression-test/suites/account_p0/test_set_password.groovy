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

suite("test_set_password", "account") {
    def user = "test_set_password_user"
    def password1 = "Password_123"
    def password2 = "Password_456"

    // cleanup
    try_sql "DROP USER IF EXISTS ${user}"

    // create user with initial password
    sql "CREATE USER '${user}'@'%' IDENTIFIED BY '${password1}'"

    // grant cluster usage for cloud mode
    if (isCloudMode()) {
        def clusters = sql "SHOW CLUSTERS"
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO '${user}'@'%'"""
    }

    // verify login with initial password
    def tokens = context.config.jdbcUrl.split('/')
    def url = tokens[0] + "//" + tokens[2] + "/" + "information_schema" + "?"

    connect(user, password1, url) {
        def result = sql "SELECT 1"
        assertEquals(1, result[0][0])
    }

    // test SET PASSWORD FOR 'user'@'%' - this was the bug scenario
    // Previously isDomain was incorrectly set to true for 'user'@'%' format
    sql "SET PASSWORD FOR '${user}'@'%' = PASSWORD('${password2}')"

    // verify login with new password
    connect(user, password2, url) {
        def result = sql "SELECT 1"
        assertEquals(1, result[0][0])
    }

    // verify old password no longer works
    try {
        connect(user, password1, url) {
            sql "SELECT 1"
        }
        assertTrue(false, "Old password should not work after SET PASSWORD")
    } catch (Exception e) {
        logger.info("Expected error with old password: " + e.getMessage())
        assertTrue(e.getMessage().contains("Access denied") || e.getMessage().contains("authentication failed"))
    }

    // cleanup
    sql "DROP USER IF EXISTS '${user}'@'%'"
}
