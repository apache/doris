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

suite("test_non_nullable_paimon_mtmv", "p0,external,mtmv,external_docker,external_docker_doris") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enablePaimonTest"))) {
        return
    }
    sql "SET materialized_view_rewrite_enable_contain_external_table = true"
    String host = context.config.otherConfigs.get("externalEnvIp")
    String port = context.config.otherConfigs.get("iceberg_minio_port")
    sql "DROP CATALOG IF EXISTS non_nullable_mv_paimon"
    sql """CREATE CATALOG non_nullable_mv_paimon PROPERTIES (
        "type" = "paimon", "warehouse" = "s3://warehouse/wh/",
        "s3.endpoint" = "http://${host}:${port}", "s3.region" = "us-east-1",
        "s3.access_key" = "admin", "s3.secret_key" = "password"
    )"""
    // This nullable field is populated in both the ORC and Parquet fixtures.
    checkNonNullableMvRewrite("non_nullable_mv_paimon.test_paimon_spark.test_tb_mix_format",
            "value", "non_nullable_paimon_mv")
}
