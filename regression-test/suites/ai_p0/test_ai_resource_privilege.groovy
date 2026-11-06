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

suite("test_ai_resource_privilege", "p0,auth") {
    String resourceName = "test_ai_resource_privilege"
    String userName = "test_ai_resource_privilege_user"
    String password = "AiResource_12345"
    def adminConnection = context.getConnection()
    def queries = [
        "SELECT AI_GENERATE('${resourceName}', 'input')",
        "SELECT AI_AGG('${resourceName}', 'input', 'summarize')",
        "SELECT EMBED('${resourceName}', 'input')",
        "SELECT EMBED('${resourceName}', CAST('{\"text\":\"input\"}' AS JSON))",
        "SELECT AI_GENERATE('input')",
        "SELECT AI_AGG('input', 'summarize')",
        "SELECT EMBED('input')",
        "SELECT EMBED(CAST('{\"text\":\"input\"}' AS JSON))",
        "SELECT AI_GENERATE('${resourceName}', NULL)",
        "SELECT AI_GENERATE(CONCAT('${resourceName}', ''), 'input')",
        "SELECT AI_AGG(CONCAT('${resourceName}', ''), 'input', 'summarize')",
        "SELECT EMBED(CONCAT('${resourceName}', ''), 'input')"
    ]
    sql "DROP USER IF EXISTS '${userName}'"
    sql "DROP RESOURCE IF EXISTS '${resourceName}'"
    try {
        sql """CREATE RESOURCE '${resourceName}' PROPERTIES (
            'type' = 'ai',
            'ai.provider_type' = 'local',
            'ai.endpoint' = 'http://localhost:8000',
            'ai.model_name' = 'chat',
            'ai.embed.provider_type' = 'local',
            'ai.embed.endpoint' = 'http://localhost:8001',
            'ai.embed.model_name' = 'embed',
            'ai.embed.mm.provider_type' = 'local',
            'ai.embed.mm.endpoint' = 'http://localhost:8002',
            'ai.embed.mm.model_name' = 'multimodal'
        )"""
        sql "CREATE USER '${userName}' IDENTIFIED BY '${password}'"
        sql "GRANT SELECT_PRIV ON ${context.dbName}.* TO '${userName}'"
        if (isCloudMode()) {
            def clusters = sql "SHOW CLUSTERS"
            assertTrue(!clusters.isEmpty())
            sql "GRANT USAGE_PRIV ON CLUSTER `${clusters[0][0]}` TO '${userName}'"
        }
        connect(userName, password, adminConnection.getMetaData().getURL()) {
            sql "SET default_ai_resource = '${resourceName}'"
            def assertDenied = {
                queries.each { query ->
                    test {
                        sql "EXPLAIN ${query}"
                        exception "privilege(s) on resource ${resourceName}"
                    }
                }
            }
            assertDenied()
            sql_impl(adminConnection, "GRANT USAGE_PRIV ON RESOURCE '${resourceName}' TO '${userName}'")
            queries.each { query -> sql "EXPLAIN ${query}" }
            sql_impl(adminConnection, "REVOKE USAGE_PRIV ON RESOURCE '${resourceName}' FROM '${userName}'")
            assertDenied()

            sql "SET enable_sql_cache = true"
            sql "SELECT 20261104"
            explain {
                sql "physical plan SELECT 20261104"
                contains "PhysicalSqlCache"
            }
            def nullQueries = [
                "SELECT AI_GENERATE('${resourceName}', NULL)",
                "SELECT AI_GENERATE(NULL)",
                "SELECT AI_GENERATE(CONCAT('${resourceName}', ''), NULL)",
                "SELECT IF(TRUE, 'ok', AI_GENERATE('${resourceName}', NULL))",
                "SELECT EMBED('${resourceName}', CAST(NULL AS STRING))",
                "SELECT EMBED(CAST(NULL AS STRING))",
                "SELECT EMBED('${resourceName}', CAST(NULL AS JSON))",
                "SELECT EMBED(CAST(NULL AS JSON))"
            ]
            nullQueries.each { query ->
                sql_impl(adminConnection, "GRANT USAGE_PRIV ON RESOURCE '${resourceName}' TO '${userName}'")
                sql query
                sql query
                explain {
                    sql "physical plan ${query}"
                    notContains "PhysicalSqlCache"
                }
                sql_impl(adminConnection, "REVOKE USAGE_PRIV ON RESOURCE '${resourceName}' FROM '${userName}'")
                test {
                    sql query
                    exception "privilege(s) on resource ${resourceName}"
                }
                test {
                    sql "EXPLAIN ${query}"
                    exception "privilege(s) on resource ${resourceName}"
                }
            }
        }
    } finally {
        sql "DROP USER IF EXISTS '${userName}'"
        sql "DROP RESOURCE IF EXISTS '${resourceName}'"
    }
}
