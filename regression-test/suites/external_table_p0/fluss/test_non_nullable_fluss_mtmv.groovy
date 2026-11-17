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

suite("test_non_nullable_fluss_mtmv", "p0,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableFlussTest"))) {
        return
    }
    sql "SET materialized_view_rewrite_enable_contain_external_table = true"
    String host = context.config.otherConfigs.get("externalEnvIp")
    String port = context.config.otherConfigs.get("fluss_coordinator_port")
    sql "DROP CATALOG IF EXISTS non_nullable_mv_fluss"
    sql """CREATE CATALOG non_nullable_mv_fluss PROPERTIES (
        "type" = "fluss", "fluss.bootstrap.servers" = "${host}:${port}", "fluss.union_read.mode" = "disabled"
    )"""
    checkNonNullableMvRewrite("non_nullable_mv_fluss.fluss_test.log_basic", "id", "non_nullable_fluss_mv")
}
