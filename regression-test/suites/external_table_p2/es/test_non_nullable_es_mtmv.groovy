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

suite("test_non_nullable_es_mtmv", "p2,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableEsTest"))) {
        return
    }
    sql "SET materialized_view_rewrite_enable_contain_external_table = true"
    String host = context.config.otherConfigs.get("externalEnvIp")
    for (int version : [5, 6, 7, 8]) {
        String port = context.config.otherConfigs.get("es_${version}_port")
        String catalog = "non_nullable_mv_es${version}"
        sql "DROP CATALOG IF EXISTS ${catalog}"
        sql """CREATE CATALOG ${catalog} PROPERTIES (
            "type" = "es", "hosts" = "http://${host}:${port}",
            "nodes_discovery" = "false", "enable_keyword_sniff" = "true"
        )"""
        checkNonNullableMvRewrite("${catalog}.default_db.test1", "test1", "non_nullable_es${version}_mv")
    }
}
