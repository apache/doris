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

import org.apache.doris.regression.action.ProfileAction

suite("test_deltalake_scan_reuse", "p0,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableDeltaLakeTest"))) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("deltalakeMinioPort")
    String hmsPort = context.config.otherConfigs.get("deltalakeHmsPort")
    String warehouse = context.config.otherConfigs.get("deltalakeWarehouse")
    sql "set enable_sql_cache=false"
    sql "set enable_profile=true"
    sql "set num_files_in_batch_mode=1"
    def profiles = new ProfileAction(context)

    for (String source : ["filesystem", "hms", "gateway"]) {
        String catalog = "test_deltalake_scan_reuse_${source}"
        String type = source == "gateway" ? "hms" : "deltalake"
        String metastore = source == "filesystem" ? "filesystem" : "hms"
        sql "drop catalog if exists ${catalog}"
        sql """create catalog ${catalog} properties (
            'type'='${type}', 'deltalake.catalog.type'='${metastore}', 'warehouse'='${warehouse}',
            'hive.metastore.uris'='thrift://${host}:${hmsPort}',
            's3.endpoint'='http://${host}:${minioPort}', 's3.region'='us-east-1',
            's3.access_key'='admin', 's3.secret_key'='password', 'use_path_style'='true'
        )"""
        String table = "${catalog}.delta_m1.parquet_pruning_dv"
        for (String mode : ["off", "on", "stream"]) {
            sql "set enable_external_scan_task_reuse=${mode != 'off'}"
            sql "set enable_external_table_batch_mode=${mode == 'stream'}"
            // The first two scans share their typed predicate. The third must keep its own ranges.
            // Payload embeds the id, so this also detects DV/range descriptor corruption on reuse.
            String token = UUID.randomUUID().toString()
            "order_qt_${source}_${mode}_ranges"("""
                select /* ${token} */ 'first' as branch, id, payload from ${table} where id >= 100 and id < 107
                union all
                select 'second', id, payload from ${table} where id >= 100 and id < 107
                union all
                select 'different', id, payload from ${table} where id >= 195 and id < 201
            """)
            String profile = profiles.getProfileBySql(token, ["Delta Scan Metrics", "EnumerationComplete: true"])
            if (mode == "on") {
                assertTrue(profile.contains("PlanningReuseHit: true"), profile)
            } else {
                assertFalse(profile.contains("PlanningReuseHit: true"), profile)
            }
            "order_qt_${source}_${mode}_count"("""
                select 'first' as branch, count(*) from ${table}
                union all select 'second', count(*) from ${table}
                union all select 'filtered', count(*) from ${table} where id >= 100 and id < 107
            """)
            "order_qt_${source}_${mode}_mapping"("""
                select 'first' as branch, * from ${catalog}.delta_m1.mapping_id
                union all select 'second', * from ${catalog}.delta_m1.mapping_id
            """)
        }
    }
}
