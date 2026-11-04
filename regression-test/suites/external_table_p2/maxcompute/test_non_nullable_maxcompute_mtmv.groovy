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

suite("test_non_nullable_maxcompute_mtmv", "p2,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableMaxComputeTest"))) {
        return
    }
    sql "SET materialized_view_rewrite_enable_contain_external_table = true"
    String project = context.config.otherConfigs.get("mcDefaultProject") ?: "mc_datalake"
    String endpoint = context.config.otherConfigs.get("mcEndpoint")
            ?: "https://service.cn-beijing.maxcompute.aliyun.com/api"
    String accessKey = context.config.otherConfigs.get("ak")
    String secretKey = context.config.otherConfigs.get("sk")
    sql "DROP CATALOG IF EXISTS non_nullable_mv_maxcompute"
    sql """CREATE CATALOG non_nullable_mv_maxcompute PROPERTIES (
        "type" = "max_compute", "mc.default.project" = "${project}",
        "mc.access_key" = "${accessKey}", "mc.secret_key" = "${secretKey}",
        "mc.endpoint" = "${endpoint}", "mc.quota" = "pay-as-you-go"
    )"""
    checkNonNullableMvRewrite("non_nullable_mv_maxcompute.${project}.mc_all_types",
            "id", "non_nullable_maxcompute_mv")
}
