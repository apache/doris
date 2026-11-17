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

suite("test_deltalake_column_mapping", "p0,external,deltalake,external_docker") {
    def config = context.config.otherConfigs
    def catalogs = [:]
    if ("true".equalsIgnoreCase(config.get("enableDeltaLakeTest"))) {
        String warehouse = config.get("deltalakeWarehouse")
        String storage = ""
        if (warehouse.startsWith("s3")) {
            storage = """, 's3.endpoint'='http://${config.get('externalEnvIp')}:${config.get('deltalakeMinioPort')}',
                's3.region'='us-east-1', 's3.access_key'='admin', 's3.secret_key'='password', 'use_path_style'='true'"""
        }
        catalogs.fs = "'deltalake.catalog.type'='filesystem', 'warehouse'='${warehouse}'${storage}"
    }
    if ("true".equalsIgnoreCase(config.get("enableDeltaLakeUcTest"))) {
        catalogs.uc = """'deltalake.catalog.type'='uc', 'deltalake.uc.uri'='${config.get('deltalakeUcUri')}',
            'deltalake.uc.catalog'='doris_delta'"""
    }
    catalogs.each { kind, properties ->
        String catalog = "test_deltalake_column_mapping_${kind}"
        sql "drop catalog if exists ${catalog}"
        sql """create catalog ${catalog} properties ('type'='deltalake', ${properties})"""
        String db = "${catalog}.delta_reader_edges"
        sql "set enable_sql_cache=false"
        sql "set num_files_in_batch_mode=1"
        for (boolean batch : [false, true]) {
            sql "set enable_external_table_batch_mode=${batch}"
            for (String mode : ["id", "name", "none"]) {
                // All three tables contain the same file: physical_x, field ID 3, value 703.
                String byName = mode == "none" ? "physical_x" : "name_match"
                String byId = mode == "none" ? "physical_y" : "id_match"
                String table = "${db}.mapping_cross_${mode}"
                "order_qt_${kind}_${batch}_${mode}"("select ${byName}, ${byId} from ${table}")
                "qt_${kind}_${batch}_${mode}_filter"("select count(*) from ${table} where ${byId}=703")
            }
            "order_qt_${kind}_${batch}_name_untagged"("select logical_x from ${db}.name_untagged")
            test {
                sql "select logical_x from ${db}.id_untagged"
                exception "Delta id mapping requires Parquet field IDs"
            }
        }
    }
}
