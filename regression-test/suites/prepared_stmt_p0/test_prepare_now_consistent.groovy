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

suite("test_prepare_now_consistent") {
    def user = context.config.jdbcUser
    def password = context.config.jdbcPassword
    def dbName = context.config.getDbNameByFile(context.file)
    def url = getServerPrepareJdbcUrl(context.config.jdbcUrl, dbName)
    def nowQueries = (1..100).collect {
        it % 2 == 0 ? "select now(6) as ts" : "select current_timestamp(6) as ts"
    }.join(" union all ")

    connect(user, password, url) {
        sql "set enable_fold_nondeterministic_fn=true"
        sql "set global enable_server_side_prepared_statement=true"

        def stmt = prepareStatement """
            select count(distinct ts), min(ts), max(ts)
            from (${nowQueries}) t
            where ? = 1
        """
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt.class)
        stmt.setInt(1, 1)

        def execute = {
            def result
            stmt.executeQuery().withCloseable { resultSet ->
                resultSet.next()
                result = [resultSet.getInt(1), resultSet.getTimestamp(2), resultSet.getTimestamp(3)]
            }
            assertEquals(1, result[0])
            assertEquals(result[1], result[2])
            return result[1]
        }

        def firstExecutionTime = execute()
        sleep(100)
        def secondExecutionTime = execute()
        assertNotEquals(firstExecutionTime, secondExecutionTime)
        stmt.close()
    }
}
