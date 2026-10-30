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

suite("test_deltalake_parquet", "p0,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableDeltaLakeTest"))) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("deltalakeMinioPort")
    String hmsPort = context.config.otherConfigs.get("deltalakeHmsPort")
    String warehouse = context.config.otherConfigs.get("deltalakeWarehouse")
    def profileAction = new ProfileAction(context)
    def counterValues = { String profile, String counter ->
        def values = []
        def matcher = profile =~ ("(?m)^\\s*(?:-\\s*)?"
                + java.util.regex.Pattern.quote(counter) + ":\\s+([^\\n]+)")
        while (matcher.find()) {
            String text = matcher.group(1)
            def exact = text =~ /\(([0-9,]+)\)/
            def number = text =~ /([0-9,]+)/
            values.add(Long.parseLong((exact.find() ? exact.group(1) : number.find() ? number.group(1) : "0")
                    .replace(",", "")))
        }
        return values
    }
    sql "set enable_profile=true"
    sql "set enable_sql_cache=false"
    ["filesystem", "hms"].each { String metastore ->
        String catalog = "test_deltalake_parquet_${metastore}"
        sql "drop catalog if exists ${catalog}"
        sql """create catalog ${catalog} properties (
            'type'='deltalake', 'deltalake.catalog.type'='${metastore}', 'warehouse'='${warehouse}',
            'hive.metastore.uris'='thrift://${host}:${hmsPort}',
            's3.endpoint'='http://${host}:${minioPort}', 's3.region'='us-east-1',
            's3.access_key'='admin', 's3.secret_key'='password', 'use_path_style'='true'
        )"""
        ["none", "name", "id"].each { String mapping ->
            String column = mapping == "none" ? "id" : "renamed"
            String table = "${catalog}.delta_m1.parquet_pruning_${mapping}"
            String predicate = "${column} >= 100 and ${column} < 120"
            // The one file overlaps this predicate, so Kernel cannot prune the file itself.
            explain {
                sql "select ${column}, length(payload) from ${table} where ${predicate}"
                contains "inputSplitNum=1"
            }
            String token = UUID.randomUUID().toString()
            "order_qt_${metastore}_${mapping}"("""
                select /* ${token} */ ${column}, length(payload)
                from ${table} where ${predicate} order by ${column}
            """)
            String profile = profileAction.getProfileBySql(token,
                    ["RowGroupsFilteredByMinMax", "FilteredRowsByPage", "PageIndexReadCalls"])
            ["RowGroupsFilteredByMinMax", "FilteredRowsByPage", "PageIndexReadCalls"].each { String counter ->
                assertTrue(counterValues(profile, counter).sum(0L) > 0L,
                        "${metastore}/${mapping}: ${counter} did not show Parquet pruning; ${profile}")
            }
        }
    }
}
