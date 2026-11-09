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

suite("test_non_nullable_hudi_mtmv", "p2,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableHudiTest"))) {
        return
    }
    sql "SET materialized_view_rewrite_enable_contain_external_table = true"
    String host = context.config.otherConfigs.get("externalEnvIp")
    String hmsPort = context.config.otherConfigs.get("hudiHmsPort")
    String minioPort = context.config.otherConfigs.get("hudiMinioPort")
    String accessKey = context.config.otherConfigs.get("hudiMinioAccessKey")
    String secretKey = context.config.otherConfigs.get("hudiMinioSecretKey")
    sql "DROP CATALOG IF EXISTS non_nullable_mv_hudi"
    sql """CREATE CATALOG non_nullable_mv_hudi PROPERTIES (
        "type" = "hms", "hive.metastore.uris" = "thrift://${host}:${hmsPort}",
        "s3.endpoint" = "http://${host}:${minioPort}", "s3.region" = "us-east-1",
        "s3.access_key" = "${accessKey}", "s3.secret_key" = "${secretKey}", "use_path_style" = "true"
    )"""
    checkNonNullableMvRewrite("non_nullable_mv_hudi.hudi_mtmv_regression_test.hudi_table_1",
            "age", "non_nullable_hudi_mv")
}
