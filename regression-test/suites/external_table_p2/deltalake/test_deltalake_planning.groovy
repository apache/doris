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

// The optional fixture contains one million AddFile actions and deliberately no data objects.
// Use metadata COUNT only; ordinary data queries are not meaningful on this planning fixture.
suite("test_deltalake_planning", "p2,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableDeltaLakeTest"))
            || !"true".equalsIgnoreCase(context.config.otherConfigs.get("enableDeltaLakePlanningTest"))) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("deltalakeMinioPort")
    String warehouse = context.config.otherConfigs.get("deltalakeWarehouse")
    sql "drop catalog if exists test_deltalake_planning"
    sql """create catalog test_deltalake_planning properties (
        'type'='deltalake', 'warehouse'='${warehouse}',
        's3.endpoint'='http://${host}:${minioPort}', 's3.region'='us-east-1',
        's3.access_key'='admin', 's3.secret_key'='password', 'use_path_style'='true'
    )"""
    sql "set enable_count_push_down_for_external_table=true"
    sql "set enable_external_table_batch_mode=true"
    sql "set num_files_in_batch_mode=1"
    sql "set enable_profile=true"
    sql "set enable_sql_cache=false"
    qt_cold_count "select count(*) from test_deltalake_planning.delta_bench.million_files"
    qt_warm_count "select count(*) from test_deltalake_planning.delta_bench.million_files"
    explain {
        sql "select count(*) from test_deltalake_planning.delta_bench.million_files"
        contains "Delta snapshot: 10"
        // The streaming range carries its row count; the scan node has no table-level count yet.
        contains "pushdown agg=COUNT"
    }
}
