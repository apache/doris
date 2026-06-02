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

suite("test_reset_connection_session_variable", "p0") {
    def currentDb = (sql "select database()")[0][0]

    sql "set sql_select_limit = 0"
    sql "set @reset_connection_user_variable = 1"

    def limitedResult = sql "select 1 union all select 2"
    assertEquals(0, limitedResult.size())
    assertEquals(1, (sql "select @reset_connection_user_variable")[0][0])

    resetConnection()

    def resetDb = (sql "select database()")[0][0]
    assertEquals(currentDb, resetDb)

    def resetResult = sql "select 1 union all select 2"
    assertEquals(2, resetResult.size())
    assertNull((sql "select @reset_connection_user_variable")[0][0])

    String url = getServerPrepareJdbcUrl(context.config.jdbcUrl, currentDb, false)
    connect(context.config.jdbcUser, context.config.jdbcPassword, url) {
        sql "set enable_server_side_prepared_statement = true"
        def stmt = prepareStatement "select 1"
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt.class)
        assertEquals(1, exec(stmt)[0][0])

        resetConnection()

        try {
            exec(stmt)
            assertTrue(false)
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Unknown prepared statement handler"))
        } finally {
            stmt.close()
        }
    }
}
