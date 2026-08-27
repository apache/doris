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

import org.apache.doris.regression.util.Http

suite("test_show_processlist") {
    def victimUser = "test_processlist_victim"
    def attackerUser = "test_processlist_attacker"
    def userPassword = "C123_567p"
    try_sql "DROP USER '${victimUser}'"
    try_sql "DROP USER '${attackerUser}'"
    sql "CREATE USER '${victimUser}' IDENTIFIED BY '${userPassword}'"
    sql "CREATE USER '${attackerUser}' IDENTIFIED BY '${userPassword}'"
    sql "GRANT SELECT_PRIV ON regression_test.* TO '${victimUser}'"
    sql "GRANT SELECT_PRIV ON regression_test.* TO '${attackerUser}'"
    if (isCloudMode()) {
        sql "GRANT USAGE_PRIV ON COMPUTE GROUP '%' TO '${victimUser}'"
        sql "GRANT USAGE_PRIV ON COMPUTE GROUP '%' TO '${attackerUser}'"
    }

    sql """set fetch_all_fe_for_system_table = false;"""
    def result = sql """show processlist;"""
    logger.info("result:${result}")
    assertTrue(result[0].size() == 15)
    sql """set fetch_all_fe_for_system_table = true;"""
    result = sql """show processlist;"""
    logger.info("result:${result}")
    assertTrue(result[0].size() == 15)
    sql """set fetch_all_fe_for_system_table = false;"""

    def url1 = "http://${context.config.feHttpAddress}/rest/v1/session"
    result =  Http.GET(url1, true)
    logger.info("result:${result}")
    assertTrue(result["data"]["column_names"].size() == 15);

    def url2 = "http://${context.config.feHttpAddress}/rest/v1/session/all"
    result = Http.GET(url2, true)
    logger.info("result:${result}")
    assertTrue(result["data"]["column_names"].size() == 15);

    result = sql """select * from information_schema.processlist"""
    logger.info("result:${result}")
    assertTrue(result[0].size() == 15)

    connect(victimUser, userPassword, context.config.jdbcUrl) {
        sql "select 1"
        connect(attackerUser, userPassword, context.config.jdbcUrl) {
            def attackerRows = sql """
                SELECT User, Info
                FROM information_schema.processlist
                WHERE User IN ('${victimUser}', '${attackerUser}')
                ORDER BY User
            """
            assertFalse(attackerRows.isEmpty())
            assertTrue(attackerRows.every { row -> row[0] == attackerUser })
            assertFalse(attackerRows.any { row -> row[0] == victimUser })
            assertTrue(attackerRows.any { row ->
                row[1] != null && row[1].toString().contains("information_schema.processlist")
            })

            def showRows = sql "SHOW FULL PROCESSLIST"
            assertFalse(showRows.isEmpty())
            assertTrue(showRows.every { row -> row[2] == attackerUser })

            connect('root', context.config.jdbcPassword, context.config.jdbcUrl) {
                def adminRows = sql """
                    SELECT User
                    FROM information_schema.processlist
                    WHERE User IN ('${victimUser}', '${attackerUser}')
                    ORDER BY User
                """
                assertTrue(adminRows.any { row -> row[0] == victimUser })
                assertTrue(adminRows.any { row -> row[0] == attackerUser })
            }
        }
    }

    def result1 = connect('root', context.config.jdbcPassword, context.config.jdbcUrl) {
        // execute sql with admin user
        sql 'select 99 + 1'
        sql 'set session_context="trace_id:test_show_processlist_trace_id"'
        def result2 = sql """select * from information_schema.processlist"""
        def found = false;
        for (def row in result2) {
            if (row[11].equals("test_show_processlist_trace_id")) {
                found = true;
            }
        }
        assertTrue(found)
    }
}
