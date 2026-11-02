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

suite("test_hive_external_lookup_refresh_multi_fe", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Hive test.")
        return
    }

    def frontends = sql_return_maparray("SHOW FRONTENDS").findAll { it.Alive.toBoolean() }
    def master = frontends.find { it.IsMaster.toBoolean() }
    assertNotNull(master)
    def follower = frontends.find { !it.IsMaster.toBoolean() }
    def refreshEndpoints = follower == null ? [master] : [master, follower]
    def warmEndpoint = follower == null ? master : follower
    def onFrontend = { frontend, action ->
        String host = frontend.Host.contains(":") ? "[${frontend.Host}]" : frontend.Host
        String authority = "jdbc:mysql://${host}:${frontend.QueryPort}"
        int queryOffset = context.config.jdbcUrl.indexOf('?')
        String options = queryOffset < 0 ? "" : context.config.jdbcUrl.substring(queryOffset)
        String url = "${authority}/${options}"
        connect(context.config.jdbcUser, context.config.jdbcPassword, url) {
            sql "SET force_forward_all_queries = false"
            // Wait for catalog DDL and refresh journals without warming any external table on the master.
            sql "SYNC"
            action()
        }
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String hmsPort = context.config.otherConfigs.get("hive3HmsPort")
    String hdfsPort = context.config.otherConfigs.get("hive3HdfsPort")
    refreshEndpoints.eachWithIndex { endpoint, index ->
        String source = "hive_refresh_multi_source_${index}"
        String target = "hive_refresh_multi_target_${index}"
        String database = "hive_refresh_multi_db_${index}"
        try {
            onFrontend(master) {
                [source, target].each { catalog ->
                    sql "DROP CATALOG IF EXISTS ${catalog}"
                    sql """CREATE CATALOG ${catalog} PROPERTIES (
                        'type' = 'hms',
                        'hadoop.username' = 'hadoop',
                        'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}',
                        'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfsPort}'
                    )"""
                }
                sql "DROP DATABASE IF EXISTS ${source}.${database} FORCE"
                sql "CREATE DATABASE ${source}.${database}"
                sql "CREATE TABLE ${source}.${database}.refresh_target (before_col INT)"
                sql "CREATE TABLE ${source}.${database}.refresh_sibling (sibling_col INT)"
            }

            // Only the follower retains the old target object; REFRESH must also work on a cold master.
            onFrontend(warmEndpoint) {
                assertEquals([["refresh_sibling"], ["refresh_target"]],
                        sql("SHOW TABLES FROM ${target}.${database}").sort { it[0] })
                assertEquals("before_col", sql("DESC ${target}.${database}.refresh_target")[0][0])
            }
            onFrontend(master) {
                sql "DROP TABLE ${source}.${database}.refresh_target"
            }
            onFrontend(endpoint) {
                sql "REFRESH TABLE ${target}.${database}.refresh_target"
                // A second refresh must remain valid after all cached copies of the dropped table are gone.
                sql "REFRESH TABLE ${target}.${database}.refresh_target"
            }
            frontends.each { frontend ->
                onFrontend(frontend) {
                    // Check membership before a failed lookup can repair it as a side effect.
                    assertEquals([["refresh_sibling"]], sql("SHOW TABLES FROM ${target}.${database}"))
                    test {
                        sql "SELECT * FROM ${target}.${database}.refresh_target"
                        exception "does not exist"
                    }
                    assertEquals("sibling_col", sql("DESC ${target}.${database}.refresh_sibling")[0][0])
                }
            }

            onFrontend(master) {
                sql "CREATE TABLE ${source}.${database}.refresh_target (after_col BIGINT)"
            }
            onFrontend(endpoint) {
                sql "REFRESH TABLE ${target}.${database}.refresh_target"
            }
            frontends.each { frontend ->
                onFrontend(frontend) {
                    assertEquals([["refresh_sibling"], ["refresh_target"]],
                            sql("SHOW TABLES FROM ${target}.${database}").sort { it[0] })
                    assertEquals("after_col", sql("DESC ${target}.${database}.refresh_target")[0][0])
                }
            }
        } finally {
            onFrontend(master) {
                try_sql "DROP DATABASE IF EXISTS ${source}.${database} FORCE"
                try_sql "DROP CATALOG IF EXISTS ${target}"
                try_sql "DROP CATALOG IF EXISTS ${source}"
            }
        }
    }
}
