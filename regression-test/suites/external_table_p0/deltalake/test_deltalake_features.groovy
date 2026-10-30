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

suite("test_deltalake_features", "p0,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableDeltaLakeTest"))) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("deltalakeMinioPort")
    String hmsPort = context.config.otherConfigs.get("deltalakeHmsPort")
    String warehouse = context.config.otherConfigs.get("deltalakeWarehouse")
    sql "set time_zone='UTC'"
    for (String source : ["filesystem", "hms"]) {
        String catalog = "test_deltalake_features_${source}"
        sql "drop catalog if exists ${catalog}"
        String metastore = source == "filesystem" ? "'warehouse'='${warehouse}'"
                : "'hive.metastore.uris'='thrift://${host}:${hmsPort}'"
        sql """create catalog ${catalog} properties (
            'type'='deltalake', 'deltalake.catalog.type'='${source}', ${metastore},
            's3.endpoint'='http://${host}:${minioPort}', 's3.region'='us-east-1',
            's3.access_key'='admin', 's3.secret_key'='password', 'use_path_style'='true'
        )"""
        for (String mode : ["name", "id"]) {
            String table = "${catalog}.delta_m1.mapping_${mode}"
            "qt_${source}_${mode}_schema"("desc ${table}")
            "order_qt_${source}_${mode}_all"("select * from ${table}")
            "order_qt_${source}_${mode}_filter"("select * from ${table} where renamed=1")
            "order_qt_${source}_${mode}_partition"("select * from ${table} where partition_renamed='a/b'")
            "order_qt_${source}_${mode}_null_partition"("select * from ${table} where partition_renamed is null")
            "order_qt_${source}_${mode}_nested"("""select renamed,
                struct_element(nested, 'after'), struct_element(nested, 'keep'),
                struct_element(arr[1], 'after'), struct_element(m['k'], 'after') from ${table}
                where struct_element(nested, 'after')=10""")
            "order_qt_${source}_${mode}_golden"(
                    "select * from ${catalog}.delta_golden.table_with_columnmapping_mode_${mode}")
        }
        for (String zone : ["UTC", "Asia/Shanghai", "America/Los_Angeles"]) {
            sql "set time_zone='${zone}'"
            String label = "${source}_${zone.replace('/', '_')}"
            "order_qt_${label}_ntz"("select * from ${catalog}.delta_m1.ntz")
            "order_qt_${label}_ntz_filter"("""select * from ${catalog}.delta_m1.ntz
                where ts='2024-03-10 02:30:00.123456' and part='2024-03-10 02:30:00.123456'""")
            for (String suffix : ["", "_name_mode", "_id_mode"]) {
                "order_qt_${label}_golden_ntz${suffix}"(
                        "select * from ${catalog}.delta_golden.data_reader_timestamp_ntz${suffix}")
            }
        }
        sql "set time_zone='UTC'"
        "order_qt_${source}_widening"("select * from ${catalog}.delta_m1.widening")
        "order_qt_${source}_widening_filter"("""select * from ${catalog}.delta_m1.widening
            where small > 32767 and int_decimal < 200 and dt > '2024-02-29 00:00:00'""")
        "order_qt_${source}_golden_widening"("select * from ${catalog}.delta_golden.type_widening")
        "order_qt_${source}_golden_nested_widening"("select * from ${catalog}.delta_golden.type_widening_nested")
        for (boolean countPushdown : [true, false]) {
            sql "set enable_count_push_down_for_external_table=${countPushdown}"
            String label = "${source}_${countPushdown}"
            "qt_${label}_dv_count"("select count(*) from ${catalog}.delta_m1.dv")
            "qt_${label}_dv_column_count"("select count(value) from ${catalog}.delta_m1.dv")
            "qt_${label}_dv_filtered_count"("select count(*) from ${catalog}.delta_m1.dv where part=1 and id<20")
            for (String table : ["dv_partitioned_with_checkpoint", "dv_with_columnmapping", "log_replay_dv_key_cases"]) {
                "qt_${label}_${table}_count"("select count(*) from ${catalog}.delta_golden.${table}")
            }
        }
        for (boolean v2 : [true, false]) {
            sql "set enable_file_scanner_v2=${v2}"
            String label = "${source}_${v2}"
            "order_qt_${label}_dv_rows"("select * from ${catalog}.delta_m1.dv")
            "order_qt_${label}_dv_deleted"("select * from ${catalog}.delta_m1.dv where id%7=0")
            "order_qt_${label}_dv_partition"("select * from ${catalog}.delta_m1.dv where part=1 and id<20")
            "order_qt_${label}_dv_updates"("select * from ${catalog}.delta_m1.dv where value like 'updated-%'")
            for (String table : ["dv_partitioned_with_checkpoint", "dv_with_columnmapping", "log_replay_dv_key_cases"]) {
                "order_qt_${label}_${table}"("select * from ${catalog}.delta_golden.${table}")
            }
        }
        // Exercise file-global DV positions when a Parquet file spans many scan ranges.
        sql "set file_split_size=128"
        "order_qt_${source}_dv_byte_ranges"("select * from ${catalog}.delta_m1.dv")
        "order_qt_${source}_mapping_byte_ranges"("select * from ${catalog}.delta_m1.mapping_name")
        "order_qt_${source}_golden_dv_byte_ranges"("select * from ${catalog}.delta_golden.dv_with_columnmapping")
        sql "set file_split_size=0"
    }
}
