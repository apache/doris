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

import groovy.json.JsonSlurper

// Run the same results against one configured storage target at a time.
suite("test_deltalake_storage", "p2,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableDeltaLakeTest"))
            || !"true".equalsIgnoreCase(context.config.otherConfigs.get("enableDeltaLakeStorageTest"))) {
        return
    }
    String propertiesPath = context.config.otherConfigs.get("deltalakeStoragePropertiesFile")
    Map properties = new JsonSlurper().parse(new File(propertiesPath)) as Map
    def quote = { value -> "'" + value.toString().replace('\\', '\\\\').replace("'", "\\'") + "'" }
    String storageProperties = properties.collect { key, value -> "${quote(key)}=${quote(value)}" }.join(',')
    sql "drop catalog if exists test_deltalake_storage"
    sql """create catalog test_deltalake_storage properties (
        'type'='deltalake', 'deltalake.catalog.type'='filesystem', ${storageProperties}
    )"""
    sql "set time_zone='UTC'"
    sql "set enable_sql_cache=false"
    sql "set enable_count_push_down_for_external_table=true"
    for (boolean streaming : [false, true]) {
        sql "set enable_external_table_batch_mode=${streaming}"
        sql "set num_files_in_batch_mode=1"
        // Small byte ranges also exercise file-global DV positions on this storage backend.
        sql "set file_split_size=128"
        "order_qt_${streaming}_mapping"("""select * from
            test_deltalake_storage.delta_golden.table_with_columnmapping_mode_id""")
        "order_qt_${streaming}_dv"("""select * from
            test_deltalake_storage.delta_golden.dv_with_columnmapping""")
        "qt_${streaming}_dv_count"("""select count(*) from
            test_deltalake_storage.delta_golden.dv_with_columnmapping""")
        "order_qt_${streaming}_partitioned_dv"("""select * from
            test_deltalake_storage.delta_golden.dv_partitioned_with_checkpoint""")
        "order_qt_${streaming}_ntz"("""select * from
            test_deltalake_storage.delta_golden.data_reader_timestamp_ntz_name_mode""")
        "order_qt_${streaming}_widening"("""select * from
            test_deltalake_storage.delta_golden.type_widening_nested""")
        "order_qt_${streaming}_checkpoint"("""select * from
            test_deltalake_storage.delta_golden.v2_checkpoint_parquet""")
        "order_qt_${streaming}_checkpoint_json"("""select * from
            test_deltalake_storage.delta_golden.v2_checkpoint_json""")
    }
}
