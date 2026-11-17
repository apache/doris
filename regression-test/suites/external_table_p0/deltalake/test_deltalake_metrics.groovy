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

suite("test_deltalake_metrics", "p0,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableDeltaLakeTest"))) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("deltalakeMinioPort")
    String warehouse = context.config.otherConfigs.get("deltalakeWarehouse")
    sql "drop catalog if exists test_deltalake_metrics"
    sql """create catalog test_deltalake_metrics properties (
        'type'='deltalake', 'warehouse'='${warehouse}',
        's3.endpoint'='http://${host}:${minioPort}', 's3.region'='us-east-1',
        's3.access_key'='admin', 's3.secret_key'='password', 'use_path_style'='true'
    )"""
    sql "switch test_deltalake_metrics"
    sql "use delta_m1"
    sql "set enable_profile=true"
    sql "set enable_sql_cache=false"
    sql "set enable_count_push_down_for_external_table=true"
    sql "set num_files_in_batch_mode=1"
    def profiles = new ProfileAction(context)
    [false, true].each { boolean streaming ->
        sql "set enable_external_table_batch_mode=${streaming}"
        String token = UUID.randomUUID().toString()
        "order_qt_filter_${streaming}"("select /* ${token} */ id, s from skipping where id < 5 order by id")
        String profile = profiles.getProfileBySql(token, ["Delta Scan Metrics", "EnumerationComplete: true"])
        // Two files survive Kernel, including the all-null file with absent min/max statistics.
        ["TotalFiles: 3", "SelectedFiles: 2", "SelectedPartitions: 0", "GeneratedSplits: 2"].each {
            assertTrue(profile.contains(it), "Missing ${it}: ${profile}")
        }
        explain {
            sql "select id, s from skipping where id < 5"
            contains "Delta snapshot:"
            contains "Delta files:"
            if (!streaming) {
                contains "Delta files: 2/3, selected partitions: 0, splits: 2"
            }
        }
        token = UUID.randomUUID().toString()
        "qt_count_${streaming}"("select /* ${token} */ count(*) from skipping")
        profile = profiles.getProfileBySql(token, ["Delta Scan Metrics", "EnumerationComplete: true"])
        ["TotalFiles: 3", "SelectedFiles: 3", "GeneratedSplits: 1"].each {
            assertTrue(profile.contains(it), "Missing ${it}: ${profile}")
        }
        token = UUID.randomUUID().toString()
        "order_qt_partition_${streaming}"("""select /* ${token} */ renamed
            from mapping_name where partition_renamed='a/b' order by renamed""")
        profile = profiles.getProfileBySql(token, ["Delta Scan Metrics", "EnumerationComplete: true"])
        ["TotalFiles: 2", "SelectedFiles: 1", "SelectedPartitions: 1", "GeneratedSplits: 1"].each {
            assertTrue(profile.contains(it), "Missing ${it}: ${profile}")
        }
    }
}
