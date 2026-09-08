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

// SU 'user' WITH ROLES (...): a session-narrowed identity switch (MySQL's proxy-user model with a
// mandatory role list). A service account holding PROXY_PRIV runs a session as a person, restricted
// to a subset of the person's granted roles; Doris enforces the narrowed privileges, the person's
// direct grants are inert under the switch, name visibility follows the narrowed set, the switch is
// one-shot, and the same person's own login is unaffected.
suite("test_su_session_narrowing", "p0,auth") {
    String svc = 'su_narrow_svc'
    String person = 'su_narrow_person'
    String pwd = 'C123_567p'
    String tenantRole = 'su_narrow_tenant'
    String otherRole = 'su_narrow_other'
    String roleDb = 'su_narrow_role_db'
    String ownDb = 'su_narrow_own_db'

    try_sql("DROP USER ${svc}")
    try_sql("DROP USER ${person}")
    try_sql("DROP ROLE ${tenantRole}")
    try_sql("DROP ROLE ${otherRole}")
    sql """DROP DATABASE IF EXISTS ${roleDb}"""
    sql """DROP DATABASE IF EXISTS ${ownDb}"""

    sql """CREATE DATABASE ${roleDb}"""
    sql """CREATE DATABASE ${ownDb}"""
    sql """CREATE TABLE ${roleDb}.t (k INT) DISTRIBUTED BY HASH(k) BUCKETS 1
           PROPERTIES ('replication_num' = '1')"""
    sql """CREATE TABLE ${ownDb}.t (k INT) DISTRIBUTED BY HASH(k) BUCKETS 1
           PROPERTIES ('replication_num' = '1')"""
    sql """INSERT INTO ${roleDb}.t VALUES (1), (2)"""
    sql """INSERT INTO ${ownDb}.t VALUES (1)"""

    sql """CREATE ROLE ${tenantRole}"""
    sql """CREATE ROLE ${otherRole}"""
    sql """CREATE USER '${person}' IDENTIFIED BY '${pwd}'"""
    sql """CREATE USER '${svc}' IDENTIFIED BY '${pwd}'"""
    // the person: a tenant role on one database, a direct grant on another
    sql """GRANT SELECT_PRIV ON ${roleDb}.* TO ROLE '${tenantRole}'"""
    sql """GRANT '${tenantRole}' TO '${person}'@'%'"""
    sql """GRANT SELECT_PRIV ON ${ownDb}.* TO '${person}'@'%'"""
    // the service account: nothing but the right to switch
    sql """GRANT PROXY_PRIV ON *.*.* TO '${svc}'@'%'"""

    try {
        // PROXY_PRIV is a global privilege, listed like any other
        test {
            sql """GRANT PROXY_PRIV ON ${roleDb}.* TO '${svc}'@'%'"""
            exception "PROXY_PRIV"
        }
        def grants = sql """SHOW GRANTS FOR '${svc}'@'%'"""
        assertTrue(grants.toString().contains("Proxy_priv"))

        // without PROXY_PRIV (or ADMIN_PRIV) the switch is refused
        connect(person, pwd, context.config.jdbcUrl) {
            test {
                sql """SU '${person}'@'%' WITH ROLES ('${tenantRole}')"""
                exception "PROXY_PRIV"
            }
        }

        // the ceiling is the target's granted roles: a role the person does not hold is refused
        connect(svc, pwd, context.config.jdbcUrl) {
            test {
                sql """SU '${person}'@'%' WITH ROLES ('${otherRole}')"""
                exception "not granted"
            }
        }

        connect(svc, pwd, context.config.jdbcUrl) {
            // before the switch the service account sees nothing of the person's
            test {
                sql """SELECT * FROM ${roleDb}.t"""
                exception "denied"
            }

            sql """SU '${person}'@'%' WITH ROLES ('${tenantRole}')"""

            // the session is the person, narrowed to the requested role
            def who = sql """SELECT current_user()"""
            assertTrue(who[0][0].toString().contains(person))
            def narrowed = sql """SELECT CAST(session_is_narrowed() AS INT)"""
            assertEquals(1, narrowed[0][0] as int)

            // the requested role's grant works ...
            def rows = sql """SELECT COUNT(*) FROM ${roleDb}.t"""
            assertEquals(2, rows[0][0] as int)
            // ... the person's direct grant is inert under the switch
            test {
                sql """SELECT * FROM ${ownDb}.t"""
                exception "denied"
            }

            // name visibility follows the narrowed set, on the FE and through the BE scanners
            def dbs = sql """SHOW DATABASES LIKE '${ownDb}'"""
            assertEquals(0, dbs.size())
            def visibleRole = sql """SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '${roleDb}'"""
            assertEquals(1, visibleRole[0][0] as int)
            def visibleOwn = sql """SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '${ownDb}'"""
            assertEquals(0, visibleOwn[0][0] as int)

            // one-shot: a switched session cannot switch again, not even to itself
            test {
                sql """SU '${svc}'@'%' WITH ROLES ('${tenantRole}')"""
                exception "already-switched"
            }
        }

        // the person's own login is untouched: both grants live, nothing is narrowed
        connect(person, pwd, context.config.jdbcUrl) {
            def own = sql """SELECT COUNT(*) FROM ${ownDb}.t"""
            assertEquals(1, own[0][0] as int)
            def role = sql """SELECT COUNT(*) FROM ${roleDb}.t"""
            assertEquals(2, role[0][0] as int)
            def narrowed = sql """SELECT CAST(session_is_narrowed() AS INT)"""
            assertEquals(0, narrowed[0][0] as int)
        }
    } finally {
        try_sql("DROP USER ${svc}")
        try_sql("DROP USER ${person}")
        try_sql("DROP ROLE ${tenantRole}")
        try_sql("DROP ROLE ${otherRole}")
        try_sql("DROP DATABASE IF EXISTS ${roleDb}")
        try_sql("DROP DATABASE IF EXISTS ${ownDb}")
    }
}
