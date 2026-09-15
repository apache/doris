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

// MySQL-compatible dual password: RETAIN CURRENT PASSWORD keeps the previous
// password valid as a secondary password until the next retaining change or
// an explicit DISCARD OLD PASSWORD. Covers rotation, eviction, discard, the
// empty-password rules, the privilege gate on the clause, and the
// interaction with the password history / expiration policies.
suite("test_dual_password", "account,nonConcurrent") {
    def user = "test_dual_password_user"
    def tokens = context.config.jdbcUrl.split('/')
    def url = tokens[0] + "//" + tokens[2] + "/" + "information_schema" + "?"

    def canLogin = { String password ->
        try {
            connect(user, password, url) {
                sql "SELECT 1"
            }
            return true
        } catch (Exception e) {
            logger.info("login of ${user} with '${password}' refused: " + e.getMessage())
            assertTrue(e.getMessage().contains("Access denied") || e.getMessage().contains("authentication failed")
                    || e.getMessage().contains("password has expired"), e.getMessage())
            return false
        }
    }

    def grantClusterUsage = {
        if (isCloudMode()) {
            def clusters = sql "SHOW CLUSTERS"
            assertTrue(!clusters.isEmpty())
            sql """GRANT USAGE_PRIV ON CLUSTER `${clusters[0][0]}` TO '${user}'@'%'"""
        }
    }

    try_sql "DROP USER IF EXISTS '${user}'@'%'"
    sql "CREATE USER '${user}'@'%' IDENTIFIED BY 'p1'"
    grantClusterUsage()
    assertTrue(canLogin("p1"))
    assertFalse(canLogin("p2"))

    // 1. rotation with RETAIN: both the new primary and the retained
    //    secondary authenticate, anything else does not
    sql "SET PASSWORD FOR '${user}'@'%' = PASSWORD('p2') RETAIN CURRENT PASSWORD"
    assertTrue(canLogin("p2"))
    assertTrue(canLogin("p1"))
    assertFalse(canLogin("p3"))

    // 2. a second RETAIN evicts the older secondary (one secondary slot)
    sql "ALTER USER '${user}'@'%' IDENTIFIED BY 'p3' RETAIN CURRENT PASSWORD"
    assertTrue(canLogin("p3"))
    assertTrue(canLogin("p2"))
    assertFalse(canLogin("p1"))

    // 3. a change WITHOUT retain replaces the primary and leaves the
    //    secondary unchanged (MySQL semantics)
    sql "ALTER USER '${user}'@'%' IDENTIFIED BY 'p4'"
    assertTrue(canLogin("p4"))
    assertFalse(canLogin("p3"))
    assertTrue(canLogin("p2"))

    // 4. DISCARD OLD PASSWORD drops the secondary; repeating it with no
    //    secondary present is a silent no-op
    sql "ALTER USER '${user}'@'%' DISCARD OLD PASSWORD"
    assertTrue(canLogin("p4"))
    assertFalse(canLogin("p2"))
    sql "ALTER USER '${user}'@'%' DISCARD OLD PASSWORD"
    assertTrue(canLogin("p4"))

    // 5. the clause is privileged even on one's own account: a plain
    //    self-service SET PASSWORD works, RETAIN CURRENT PASSWORD does not
    connect(user, "p4", url) {
        sql "SET PASSWORD = PASSWORD('p5')"
        test {
            sql "SET PASSWORD = PASSWORD('p6') RETAIN CURRENT PASSWORD"
            exception "Access denied"
        }
    }
    assertTrue(canLogin("p5"))
    assertFalse(canLogin("p4"))
    assertFalse(canLogin("p6"))

    // 6. RETAIN cannot be combined with an empty new password in a way that
    //    keeps a secondary: an empty new password empties the secondary too
    sql "SET PASSWORD FOR '${user}'@'%' = PASSWORD('p7') RETAIN CURRENT PASSWORD"
    assertTrue(canLogin("p5"))
    sql "ALTER USER '${user}'@'%' IDENTIFIED BY '' RETAIN CURRENT PASSWORD"
    assertTrue(canLogin(""))
    assertFalse(canLogin("p7"))
    assertFalse(canLogin("p5"))

    // 7. RETAIN on an account whose primary password is empty fails
    test {
        sql "ALTER USER '${user}'@'%' IDENTIFIED BY 'p8' RETAIN CURRENT PASSWORD"
        exception "cannot be retained"
    }
    assertTrue(canLogin(""))
    assertFalse(canLogin("p8"))

    // 8. RETAIN without a password change is rejected
    test {
        sql "ALTER USER '${user}'@'%' RETAIN CURRENT PASSWORD"
        exception "RETAIN CURRENT PASSWORD requires a password change"
    }

    // 9. password history: a retaining change is still a password change,
    //    so the new password must not contradict the history
    sql "ALTER USER '${user}'@'%' PASSWORD_HISTORY 2"
    sql "ALTER USER '${user}'@'%' IDENTIFIED BY 'h1'"
    sql "ALTER USER '${user}'@'%' IDENTIFIED BY 'h2' RETAIN CURRENT PASSWORD"
    assertTrue(canLogin("h2"))
    assertTrue(canLogin("h1"))
    test {
        sql "ALTER USER '${user}'@'%' IDENTIFIED BY 'h1' RETAIN CURRENT PASSWORD"
        exception "contradict the password history policy"
    }
    // DISCARD is not a password change: the history is left as it was, so
    // the discarded secondary still contradicts it and a fresh value passes
    sql "ALTER USER '${user}'@'%' DISCARD OLD PASSWORD"
    assertFalse(canLogin("h1"))
    test {
        sql "ALTER USER '${user}'@'%' IDENTIFIED BY 'h1'"
        exception "contradict the password history policy"
    }
    sql "ALTER USER '${user}'@'%' IDENTIFIED BY 'h3' RETAIN CURRENT PASSWORD"
    assertTrue(canLogin("h3"))
    assertTrue(canLogin("h2"))
    sql "ALTER USER '${user}'@'%' PASSWORD_HISTORY 0"

    // 10. password expiration: a retaining change restarts the expiry
    //     clock (it is a password change); DISCARD does not touch it
    sql "ALTER USER '${user}'@'%' PASSWORD_EXPIRE INTERVAL 8 SECOND"
    sql "ALTER USER '${user}'@'%' IDENTIFIED BY 'e1' RETAIN CURRENT PASSWORD"
    assertTrue(canLogin("e1"))
    assertTrue(canLogin("h3"))
    sleep(5000)
    sql "ALTER USER '${user}'@'%' DISCARD OLD PASSWORD"
    assertTrue(canLogin("e1"))
    assertFalse(canLogin("h3"))
    sleep(4000)
    // >8s since the last password change: expired, although the discard
    // happened only 4s ago
    assertFalse(canLogin("e1"))
    sql "ALTER USER '${user}'@'%' IDENTIFIED BY 'e2' RETAIN CURRENT PASSWORD"
    assertTrue(canLogin("e2"))
    assertTrue(canLogin("e1"))
    sql "ALTER USER '${user}'@'%' PASSWORD_EXPIRE NEVER"

    // 11. SHOW CREATE USER round-trips without the transient clauses
    def created = sql "SHOW CREATE USER '${user}'@'%'"
    assertEquals(1, created.size())
    def createStmt = created[0].toString().toUpperCase()
    assertFalse(createStmt.contains("RETAIN"))
    assertFalse(createStmt.contains("DISCARD"))

    // 12. DISCARD and OLD stay usable as identifiers
    sql "DROP TABLE IF EXISTS test_dual_password_tbl"
    sql """CREATE TABLE test_dual_password_tbl (old INT, discard INT) DISTRIBUTED BY HASH(old) BUCKETS 1
            PROPERTIES ("replication_num" = "1")"""
    sql "INSERT INTO test_dual_password_tbl VALUES (1, 2)"
    def rows = sql "SELECT old, discard FROM test_dual_password_tbl"
    assertEquals(1, rows[0][0])
    assertEquals(2, rows[0][1])
    sql "DROP TABLE IF EXISTS test_dual_password_tbl"

    sql "DROP USER IF EXISTS '${user}'@'%'"
}
