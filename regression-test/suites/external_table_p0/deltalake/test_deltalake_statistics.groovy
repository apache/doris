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

suite("test_deltalake_statistics", "p0,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableDeltaLakeTest"))) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("deltalakeMinioPort")
    String hmsPort = context.config.otherConfigs.get("deltalakeHmsPort")
    String warehouse = context.config.otherConfigs.get("deltalakeWarehouse")
    for (String source : ["filesystem", "hms", "hive"]) {
        String catalog = "test_deltalake_statistics_${source}"
        sql "drop catalog if exists ${catalog}"
        String type = source == "hive" ? "hms" : "deltalake"
        String metastore = source == "filesystem" ? "'warehouse'='${warehouse}'"
                : "'hive.metastore.uris'='thrift://${host}:${hmsPort}'"
        String mode = source == "hive" ? "" : "'deltalake.catalog.type'='${source}',"
        sql """create catalog ${catalog} properties (
            'type'='${type}', ${mode} ${metastore},
            's3.endpoint'='http://${host}:${minioPort}', 's3.region'='us-east-1',
            's3.access_key'='admin', 's3.secret_key'='password', 'use_path_style'='true'
        )"""
        for (String table : ["delta_m1.basic", "delta_golden.dv_partitioned_with_checkpoint",
                "delta_golden.v2_checkpoint_json", "delta_golden.v2_checkpoint_parquet"]) {
            String label = "${source}_${table.replace('.', '_')}"
            "qt_${label}_count"("select count(*) from ${catalog}.${table}")
        }
        sql "refresh table ${catalog}.delta_m1.basic"
        "qt_${source}_refreshed_count"("select count(*) from ${catalog}.delta_m1.basic")
        "qt_${source}_empty_count"("select count(*) from ${catalog}.delta_m1.empty")
        sql "alter catalog ${catalog} set properties ('meta.cache.deltalake.statistics.enable'='false')"
        // With collection disabled, the three small Parquet files yield a CRC size estimate above three rows.
        explain {
            sql("select * from ${catalog}.delta_m1.basic")
            check { plan ->
                def rows = plan =~ /\bcardinality=(\d+)\b/
                rows.find() && rows.group(1).toLong() > 3
            }
        }
        // The golden table has no version checksum. CBO uses its unknown-row fallback and COUNT is exact.
        explain {
            sql("select * from ${catalog}.delta_golden.dv_partitioned_with_checkpoint")
            check { plan -> (plan =~ /\bcardinality=1\b/).find() }
        }
        "qt_${source}_disabled_count"("select count(*) from ${catalog}.delta_golden.dv_partitioned_with_checkpoint")
    }
}
