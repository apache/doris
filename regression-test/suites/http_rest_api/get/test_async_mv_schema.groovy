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

suite("test_async_mv_schema") {
    sql "DROP MATERIALIZED VIEW IF EXISTS test_async_mv_schema"
    sql "DROP TABLE IF EXISTS test_async_mv_schema_base"
    sql """
        CREATE TABLE test_async_mv_schema_base (k INT, v INT)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE MATERIALIZED VIEW test_async_mv_schema
        BUILD DEFERRED REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS SELECT k AS mv_key, v AS mv_value FROM test_async_mv_schema_base
    """

    def db = context.config.getDbNameByFile(context.file)
    httpTest {
        basicAuthorization context.config.jdbcUser, context.config.jdbcPassword
        endpoint context.config.feHttpAddress
        uri "/api/meta/namespaces/default_cluster/databases/${db}/tables/test_async_mv_schema/schema"
        op "get"
        check { status, body ->
            assertEquals(200, status)
            def result = parseJson(body)
            assertEquals(0, result.code)
            def view = result.data.test_async_mv_schema
            assertEquals(true, view.is_base)
            assertEquals(["mv_key", "mv_value"], view.schema*.Field)
        }
    }
}
