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

suite("test_non_nullable_trino_mtmv", "p0,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableHiveTest"))) {
        return
    }
    sql "SET materialized_view_rewrite_enable_contain_external_table = true"
    String host = context.config.otherConfigs.get("externalEnvIp")
    def hosts = (sql("SHOW BACKENDS").collect { it[1] } + sql("SHOW FRONTENDS").collect { it[1] }).unique()
    dispatchTrinoConnectors(hosts)
    String port = context.config.otherConfigs.get("hive2HmsPort")
    sql "DROP CATALOG IF EXISTS non_nullable_mv_trino"
    sql """CREATE CATALOG non_nullable_mv_trino PROPERTIES (
        "type" = "trino-connector", "trino.connector.name" = "hive",
        "trino.hive.metastore.uri" = "thrift://${host}:${port}"
    )"""
    // Quote the reserved schema name before the helper embeds it in DESC and MV queries.
    checkNonNullableMvRewrite("`non_nullable_mv_trino`.`default`.`mtmv_base1`", "id", "non_nullable_trino_mv")
}
