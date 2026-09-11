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

// MySQL-compatible administrative account lock: CREATE USER ... ACCOUNT_LOCK,
// ALTER USER ... ACCOUNT_LOCK | ACCOUNT_UNLOCK. Refuses the account's own logins
// with ER_ACCOUNT_HAS_BEEN_LOCKED, survives policy edits, is cleared by
// ACCOUNT_UNLOCK (which also clears a failed-login lock), shows in SHOW CREATE
// USER.
suite("test_account_lock", "account,nonConcurrent") {
    def user = "test_account_lock_user"
    def locked = "test_account_lock_born_locked"
    def tokens = context.config.jdbcUrl.split('/')
    def url = tokens[0] + "//" + tokens[2] + "/" + "information_schema" + "?"

    def loginError = { String u, String password ->
        try {
            connect(u, password, url) {
                sql "SELECT 1"
            }
            return null
        } catch (Exception e) {
            logger.info("login of ${u} refused: " + e.getMessage())
            return e.getMessage()
        }
    }
    def canLogin = { String u, String password -> loginError(u, password) == null }

    def grantClusterUsage = { String u ->
        if (isCloudMode()) {
            def clusters = sql "SHOW CLUSTERS"
            assertTrue(!clusters.isEmpty())
            sql """GRANT USAGE_PRIV ON CLUSTER `${clusters[0][0]}` TO '${u}'@'%'"""
        }
    }

    try_sql "DROP USER IF EXISTS '${user}'@'%'"
    try_sql "DROP USER IF EXISTS '${locked}'@'%'"

    // 1. lock an existing account: the right password is refused with MySQL's text
    sql "CREATE USER '${user}'@'%' IDENTIFIED BY 'p1'"
    grantClusterUsage(user)
    assertTrue(canLogin(user, "p1"))
    sql "ALTER USER '${user}'@'%' ACCOUNT_LOCK"
    def err = loginError(user, "p1")
    assertNotNull(err)
    assertTrue(err.contains("Account is locked"), err)

    // 2. SHOW CREATE USER prints the lock
    def created = sql "SHOW CREATE USER '${user}'@'%'"
    assertEquals(1, created.size())
    assertTrue(created[0].toString().contains("ACCOUNT_LOCK"), created[0].toString())

    // 3. a policy edit does not clear the lock; ACCOUNT_UNLOCK does
    sql "ALTER USER '${user}'@'%' FAILED_LOGIN_ATTEMPTS 3"
    assertFalse(canLogin(user, "p1"))
    sql "ALTER USER '${user}'@'%' ACCOUNT_UNLOCK"
    assertTrue(canLogin(user, "p1"))
    created = sql "SHOW CREATE USER '${user}'@'%'"
    assertFalse(created[0].toString().contains("ACCOUNT_LOCK"), created[0].toString())

    // 4. CREATE USER ... ACCOUNT_LOCK is honored
    sql "CREATE USER '${locked}'@'%' IDENTIFIED BY 'p2' ACCOUNT_LOCK"
    grantClusterUsage(locked)
    err = loginError(locked, "p2")
    assertNotNull(err)
    assertTrue(err.contains("Account is locked"), err)
    sql "ALTER USER '${locked}'@'%' ACCOUNT_UNLOCK"
    assertTrue(canLogin(locked, "p2"))

    // 5. ACCOUNT_UNLOCK also clears a failed-login lock
    sql "ALTER USER '${locked}'@'%' FAILED_LOGIN_ATTEMPTS 1 PASSWORD_LOCK_TIME UNBOUNDED"
    assertFalse(canLogin(locked, "wrong"))
    err = loginError(locked, "p2")
    assertNotNull(err)
    assertTrue(err.contains("Account is blocked"), err)
    sql "ALTER USER '${locked}'@'%' ACCOUNT_UNLOCK"
    assertTrue(canLogin(locked, "p2"))

    sql "DROP USER IF EXISTS '${user}'@'%'"
    sql "DROP USER IF EXISTS '${locked}'@'%'"
}
