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

suite("test_deltalake_cast_pruning", "p0,external") {
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
        String catalog = "test_deltalake_cast_pruning_${source}"
        String connector = source == "gateway" ? "'type'='hms'"
                : "'type'='deltalake', 'deltalake.catalog.type'='${source}'"
        String location = source == "filesystem" ? "'warehouse'='${warehouse}'"
                : "'hive.metastore.uris'='thrift://${host}:${hmsPort}'"
        sql "drop catalog if exists ${catalog}"
        sql """create catalog ${catalog} properties (
            ${connector}, ${location}, 's3.endpoint'='http://${host}:${minioPort}', 's3.region'='us-east-1',
            's3.access_key'='admin', 's3.secret_key'='password', 'use_path_style'='true'
        )"""
        sql "switch ${catalog}"
        sql "use delta_golden"

        // The golden has two files. The first one's decimal min and max are both 123.45.
        String column = "decimal_decimal_same_scale"
        String castMatch = "cast(${column} as bigint) = 123"
        for (boolean batch : [false, true]) {
            sql "set enable_external_table_batch_mode=${batch}"
            String label = "${source}_${batch ? 'stream' : 'sync'}"
            "order_qt_${label}_cast"(
                    "select ${column}, cast(${column} as bigint) from type_widening where ${castMatch}")
            // Only the second file matches. A table-level count or a first-file-only fallback would be wrong.
            "qt_${label}_count"(
                    "select count(*) from type_widening where cast(${column} as bigint) = 12345678901234")
            "order_qt_${label}_try_or"("""select ${column} from type_widening
                    where try_cast(${column} as bigint) = 123 or ${column} < 0""")
            String mixed = "${castMatch} and ${column} < 124"
            String token = UUID.randomUUID().toString()
            "order_qt_${label}_and"("select /* ${token} */ ${column} from type_widening where ${mixed}")
            // The independent decimal condition still prunes the second file in both execution modes.
            String profile = profiles.getProfileBySql(token, ["Delta Scan Metrics", "EnumerationComplete: true"])
            assertTrue(profile.contains("TotalFiles: 2") && profile.contains("SelectedFiles: 1"), profile)
        }
    }
}
