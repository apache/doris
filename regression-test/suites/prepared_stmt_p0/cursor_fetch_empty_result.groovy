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

suite("cursor_fetch_empty_result") {
    String url = getServerPrepareJdbcUrl(context.config.jdbcUrl, "regression_test_prepared_stmt_p0") +
            "&useCursorFetch=true&defaultFetchSize=10000&socketTimeout=10000"

    connect(context.config.jdbcUser, context.config.jdbcPassword, url) {
        // With a positive defaultFetchSize Connector/J also converts a plain Statement into a
        // server-prepared cursor execution, which is how BI tools commonly enter this path.
        assertEquals(0, sql("SELECT 1 AS c WHERE 1 = 2").size())
        assertEquals([[1]], sql("SELECT 1 AS c WHERE 1 = 1"))

        def emptyResult = prepareStatement "SELECT 1 AS c WHERE 1 = 2"
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, emptyResult.class)
        qe_empty_result emptyResult
        emptyResult.close()

        def nonEmptyResult = prepareStatement "SELECT 1 AS c WHERE 1 = 1"
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, nonEmptyResult.class)
        qe_non_empty_result nonEmptyResult
        nonEmptyResult.close()
    }

    String unidentifiedClientUrl = getServerPrepareJdbcUrl(
            context.config.jdbcUrl, "regression_test_prepared_stmt_p0") +
            "&useCursorFetch=true&defaultFetchSize=10000&connectionAttributes=none&socketTimeout=10000"
    connect(context.config.jdbcUser, context.config.jdbcPassword, unidentifiedClientUrl) {
        test {
            sql "SELECT 1 AS c WHERE 1 = 2"
            exception "Cannot safely execute cursor fetch because the client did not provide identifiable " +
                    "connection attributes"
        }
    }
}
