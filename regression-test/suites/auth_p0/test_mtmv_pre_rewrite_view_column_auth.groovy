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

suite("test_mtmv_pre_rewrite_view_column_auth", "p0,auth") {
    String suiteName = "test_mtmv_pre_rewrite_view_column_auth"
    String dbName = "${suiteName}_db"
    String tableName = "${suiteName}_table"
    String viewName = "${suiteName}_view"
    String mvName = "${suiteName}_mv"
    String user = "${suiteName}_user"
    String password = "C123_567p"

    try_sql "DROP USER IF EXISTS ${user}"
    sql "DROP DATABASE IF EXISTS ${dbName}"

    try {
        sql "CREATE DATABASE ${dbName}"
        sql """
            CREATE TABLE ${dbName}.${tableName} (
                id INT,
                secret INT
            )
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ('replication_num' = '1')
        """
        sql "INSERT INTO ${dbName}.${tableName} VALUES (1, 10), (2, 20)"
        sql """
            CREATE VIEW ${dbName}.${viewName}
            AS SELECT id, secret FROM ${dbName}.${tableName}
        """
        sql "CREATE USER '${user}' IDENTIFIED BY '${password}'"
        sql "GRANT SELECT_PRIV(id) ON internal.${dbName}.${viewName} TO ${user}"
        sql """
            CREATE MATERIALIZED VIEW ${dbName}.${mvName}
            BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
            DISTRIBUTED BY RANDOM BUCKETS 1
            PROPERTIES ('replication_num' = '1')
            AS SELECT id, secret FROM ${dbName}.${viewName}
        """
        waitingMTMVTaskFinishedByMvName(mvName, dbName)

        if (isCloudMode()) {
            def clusters = sql "SHOW CLUSTERS"
            assertTrue(!clusters.isEmpty())
            sql "GRANT USAGE_PRIV ON CLUSTER `${clusters[0][0]}` TO ${user}"
        }

        String userJdbcUrl = org.apache.doris.regression.Config.buildUrlWithDb(
                context.config.jdbcUrl, dbName)
        connect(user, password, userJdbcUrl) {
            sql "SET enable_nereids_planner = true"
            sql "SET enable_fallback_to_original_planner = false"
            sql "SET enable_sql_cache = false"
            sql "SET enable_materialized_view_rewrite = true"

            test {
                sql """
                    SELECT secret FROM ${dbName}.${viewName}
                    ORDER BY secret
                """
                exception "Permission denied"
            }

            sql "SET pre_materialized_view_rewrite_strategy = 'TRY_IN_RBO'"
            test {
                sql """
                    WITH c AS (SELECT * FROM ${dbName}.${viewName})
                    SELECT a.id FROM c a JOIN c b ON a.id = b.id ORDER BY a.id
                """
                exception "Permission denied"
            }

            sql "SET pre_materialized_view_rewrite_strategy = 'FORCE_IN_RBO'"
            test {
                sql """
                    WITH c AS (SELECT * FROM ${dbName}.${viewName})
                    SELECT a.secret FROM c a JOIN c b ON a.id = b.id ORDER BY a.secret
                """
                exception "Permission denied"
            }
        }
    } finally {
        try_sql "DROP USER IF EXISTS ${user}"
        sql "DROP DATABASE IF EXISTS ${dbName}"
    }
}
