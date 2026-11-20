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

// mysql.user carries a user's password policy, read through a backend's scan: the frontend fills the
// row by column name and the backend copies it by position, so this pins that the two agree. Since
// the durable ACCOUNT_LOCK added a ninth policy row, every password_policy column read empty.
suite("test_mysql_user_policy_columns", "p0,auth") {
    String user = "test_mysql_user_policy_columns_user"
    String pwd = 'C123_567p'

    try_sql("DROP USER ${user}")
    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}' PASSWORD_HISTORY 5 PASSWORD_EXPIRE INTERVAL 30 DAY
           FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 1 DAY"""

    def policy = {
        def rows = sql """
            SELECT `password_policy.expiration_seconds`, `password_policy.history_num`,
                   `password_policy.history_passwords`, `password_policy.num_failed_login`,
                   `password_policy.password_lock_seconds`, `password_policy.failed_login_counter`,
                   `password_policy.lock_time`, `password_policy.account_locked`
            FROM mysql.user WHERE User = '${user}'
        """
        assertEquals(1, rows.size(), "one mysql.user row for ${user}: ${rows}")
        return rows[0]
    }

    def row = policy()
    assertEquals("2592000", row[0], "expiration_seconds: ${row}")
    assertEquals("5", row[1], "history_num: ${row}")
    assertEquals("***", row[2], "history_passwords stays masked: ${row}")
    assertEquals("3", row[3], "num_failed_login: ${row}")
    assertEquals("86400", row[4], "password_lock_seconds: ${row}")
    assertEquals("0", row[5], "failed_login_counter: ${row}")
    assertEquals("", row[6], "lock_time: ${row}")
    assertEquals("false", row[7], "account_locked: ${row}")

    sql """ALTER USER '${user}' ACCOUNT_LOCK"""
    assertEquals("true", policy()[7], "account_locked after ACCOUNT_LOCK")
    sql """ALTER USER '${user}' ACCOUNT_UNLOCK"""
    assertEquals("false", policy()[7], "account_locked after ACCOUNT_UNLOCK")

    try_sql("DROP USER ${user}")
}
