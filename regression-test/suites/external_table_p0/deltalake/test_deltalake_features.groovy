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
    String warehouse = context.config.otherConfigs.get("deltalakeWarehouse")
    sql "set time_zone='UTC'"
    String catalog = "test_deltalake_features_filesystem"
    sql "drop catalog if exists ${catalog}"
    sql """create catalog ${catalog} properties (
        'type'='deltalake', 'deltalake.catalog.type'='filesystem', 'warehouse'='${warehouse}',
        's3.endpoint'='http://${host}:${minioPort}', 's3.region'='us-east-1',
        's3.access_key'='admin', 's3.secret_key'='password', 'use_path_style'='true'
    )"""
    for (String mode : ["name", "id"]) {
        String table = "${catalog}.delta_m1.mapping_${mode}"
        "qt_filesystem_${mode}_schema"("desc ${table}")
        "order_qt_filesystem_${mode}_all"("select * from ${table}")
        "order_qt_filesystem_${mode}_filter"("select * from ${table} where renamed=1")
        "order_qt_filesystem_${mode}_partition"("select * from ${table} where partition_renamed='a/b'")
        "order_qt_filesystem_${mode}_null_partition"("select * from ${table} where partition_renamed is null")
        "order_qt_filesystem_${mode}_nested"("""select renamed,
            struct_element(nested, 'after'), struct_element(nested, 'keep'),
            struct_element(arr[1], 'after'), struct_element(m['k'], 'after') from ${table}
            where struct_element(nested, 'after')=10""")
        "order_qt_filesystem_${mode}_golden"(
                "select * from ${catalog}.delta_golden.table_with_columnmapping_mode_${mode}")
    }
    for (String zone : ["UTC", "Asia/Shanghai", "America/Los_Angeles"]) {
        sql "set time_zone='${zone}'"
        String label = "filesystem_${zone.replace('/', '_')}"
        "order_qt_${label}_ntz"("select * from ${catalog}.delta_m1.ntz")
        "order_qt_${label}_ntz_filter"("""select * from ${catalog}.delta_m1.ntz
            where ts='2024-03-10 02:30:00.123456' and part='2024-03-10 02:30:00.123456'""")
        for (String suffix : ["", "_name_mode", "_id_mode"]) {
            "order_qt_${label}_golden_ntz${suffix}"(
                    "select * from ${catalog}.delta_golden.data_reader_timestamp_ntz${suffix}")
        }
    }
    sql "set time_zone='UTC'"
    "order_qt_filesystem_widening"("select * from ${catalog}.delta_m1.widening")
    "order_qt_filesystem_widening_filter"("""select * from ${catalog}.delta_m1.widening
        where small > 32767 and int_decimal < 200 and dt > '2024-02-29 00:00:00'""")
    "order_qt_filesystem_golden_widening"("select * from ${catalog}.delta_golden.type_widening")
    "order_qt_filesystem_golden_nested_widening"("select * from ${catalog}.delta_golden.type_widening_nested")
    for (boolean countPushdown : [true, false]) {
        sql "set enable_count_push_down_for_external_table=${countPushdown}"
        String label = "filesystem_${countPushdown}"
        "qt_${label}_dv_count"("select count(*) from ${catalog}.delta_m1.dv")
        "qt_${label}_dv_column_count"("select count(value) from ${catalog}.delta_m1.dv")
        "qt_${label}_dv_filtered_count"("select count(*) from ${catalog}.delta_m1.dv where part=1 and id<20")
        for (String table : ["dv_partitioned_with_checkpoint", "dv_with_columnmapping", "log_replay_dv_key_cases"]) {
            "qt_${label}_${table}_count"("select count(*) from ${catalog}.delta_golden.${table}")
        }
    }
    String v2Label = "filesystem_true"
    "order_qt_${v2Label}_dv_rows"("select * from ${catalog}.delta_m1.dv")
    "order_qt_${v2Label}_dv_deleted"("select * from ${catalog}.delta_m1.dv where id%7=0")
    "order_qt_${v2Label}_dv_partition"("select * from ${catalog}.delta_m1.dv where part=1 and id<20")
    "order_qt_${v2Label}_dv_updates"("select * from ${catalog}.delta_m1.dv where value like 'updated-%'")
    for (String table : ["dv_partitioned_with_checkpoint", "dv_with_columnmapping", "log_replay_dv_key_cases"]) {
        "order_qt_${v2Label}_${table}"("select * from ${catalog}.delta_golden.${table}")
    }
    // Exercise file-global DV positions when a Parquet file spans many scan ranges.
    sql "set file_split_size=128"
    "order_qt_filesystem_dv_byte_ranges"("select * from ${catalog}.delta_m1.dv")
    "order_qt_filesystem_mapping_byte_ranges"("select * from ${catalog}.delta_m1.mapping_name")
    "order_qt_filesystem_golden_dv_byte_ranges"("select * from ${catalog}.delta_golden.dv_with_columnmapping")
    sql "set file_split_size=0"
}
