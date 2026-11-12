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

suite("test_non_nullable_hive_mtmv", "p0,external,hive,external_docker,external_docker_hive") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableHiveTest"))) {
        return
    }
    sql "SET materialized_view_rewrite_enable_contain_external_table = true"
    String host = context.config.otherConfigs.get("externalEnvIp")
    for (String version : ["hive2", "hive3"]) {
        String port = context.config.otherConfigs.get(version + "HmsPort")
        String catalog = "non_nullable_mv_${version}"
        sql "DROP CATALOG IF EXISTS ${catalog}"
        sql """CREATE CATALOG ${catalog} PROPERTIES (
            "type" = "hms", "hive.metastore.uris" = "thrift://${host}:${port}"
        )"""
        // The helper reuses this name in DESC and MV queries, where default is a reserved keyword.
        checkNonNullableMvRewrite("`${catalog}`.`default`.`mtmv_base1`", "id", "non_nullable_${version}_mv")
        for (String format : ["orc", "parquet"]) {
            String source = "`${catalog}`.`default`.`non_nullable_vector_${format}`"
            sql "DROP TABLE IF EXISTS ${source}"
            sql """CREATE TABLE ${source} (id INT, vec ARRAY<FLOAT>) ENGINE=HIVE
                PROPERTIES ("file_format" = "${format}")"""
            sql "INSERT INTO ${source} VALUES (1, [1.0, 0.0]), (2, [2.0, 0.0])"
            checkNonNullableMvRewrite(source, ["id", "vec"], "non_nullable_${version}_${format}_mv")
        }
    }
}
