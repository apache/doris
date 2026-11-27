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

import org.apache.doris.regression.suite.client.FrontendClientImpl
import org.apache.doris.thrift.TFetchSchemaTableDataRequest
import org.apache.doris.thrift.TNetworkAddress
import org.apache.doris.thrift.TSchemaTableName
import org.apache.doris.thrift.TSchemaTableRequestParams
import org.apache.doris.thrift.TStatusCode

import java.util.concurrent.TimeUnit

suite("test_active_queries_auth", "p0,auth") {
    def owner = "active_queries_auth_owner"
    def reader = "active_queries_auth_reader"
    def password = "C123_567p"
    def clientHost = sql("select substring_index(user(), '@', -1)")[0][0].replace("'", "")
    def query = "select sleep(30), 'active_queries_auth_secret'"
    def countQuery = """select count(*) from information_schema.active_queries
                        where SQL = "${query}"
                     """
    def queryTask = null

    sql_return_maparray("show frontends").findAll { it.Alive.toBoolean() }.each { frontend ->
        def client = new FrontendClientImpl(new TNetworkAddress(frontend.Host, frontend.RpcPort.toInteger()))
        try {
            def request = new TFetchSchemaTableDataRequest()
            request.setSchemaTableName(TSchemaTableName.ACTIVE_QUERIES)
            request.setSchemaTableParams(new TSchemaTableRequestParams())
            def result = client.client.fetchSchemaTableData(request)
            assertEquals(TStatusCode.INTERNAL_ERROR, result.status.statusCode)
            assertTrue(result.status.errorMsgs.contains("current user ident is not set."))
            assertFalse(result.isSetDataBatch())
        } finally {
            client.close()
        }
    }

    sql "DROP USER IF EXISTS '${owner}'@'${clientHost}'"
    sql "DROP USER IF EXISTS '${owner}'"
    sql "DROP USER IF EXISTS '${reader}'"
    try {
        sql "CREATE USER '${owner}' IDENTIFIED BY '${password}'"
        sql "CREATE USER '${reader}' IDENTIFIED BY '${password}'"
        sql "GRANT SELECT_PRIV ON regression_test.* TO '${owner}'"
        sql "GRANT SELECT_PRIV ON regression_test.* TO '${reader}'"

        queryTask = thread("active_queries_auth_owner") {
            connect(owner, password, context.config.jdbcUrl) {
                sql query
            }
        }
        awaitUntil(10, 0.1) {
            sql(countQuery)[0][0] == 1
        }

        connect(owner, password, context.config.jdbcUrl) {
            assertEquals(1, sql(countQuery)[0][0])
        }
        connect(reader, password, context.config.jdbcUrl) {
            assertEquals(0, sql(countQuery)[0][0])
            def rows = sql "select USER, SQL from information_schema.active_queries"
            assertFalse(rows.isEmpty())
            assertTrue(rows.every { it[0] == reader })
        }

        sql "GRANT ADMIN_PRIV ON *.*.* TO '${reader}'"
        connect(reader, password, context.config.jdbcUrl) {
            assertEquals(1, sql(countQuery)[0][0])
        }

        sql "CREATE USER '${owner}'@'${clientHost}' IDENTIFIED BY '${password}'"
        sql "GRANT SELECT_PRIV ON regression_test.* TO '${owner}'@'${clientHost}'"
        connect(owner, password, context.config.jdbcUrl) {
            assertEquals("'${owner}'@'${clientHost}'".toString(), sql("select current_user()")[0][0])
            assertEquals(0, sql(countQuery)[0][0])
        }
        assertEquals(1, sql(countQuery)[0][0])
    } finally {
        try {
            if (queryTask != null) {
                queryTask.get(60, TimeUnit.SECONDS)
            }
        } finally {
            sql "DROP USER IF EXISTS '${owner}'@'${clientHost}'"
            sql "DROP USER IF EXISTS '${owner}'"
            sql "DROP USER IF EXISTS '${reader}'"
        }
    }
}
