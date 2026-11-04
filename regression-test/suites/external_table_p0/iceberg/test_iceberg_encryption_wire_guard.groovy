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

suite("test_iceberg_encryption_wire_guard", "p0,external,nonConcurrent") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableIcebergTest"))) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    sql "DROP CATALOG IF EXISTS encryption_wire_regression"
    sql """CREATE CATALOG encryption_wire_regression PROPERTIES (
        'type' = 'iceberg', 'iceberg.catalog.type' = 'rest', 'uri' = 'http://${host}:${restPort}',
        's3.access_key' = 'admin', 's3.secret_key' = 'password',
        's3.endpoint' = 'http://${host}:${minioPort}', 's3.region' = 'us-east-1',
        'use_path_style' = 'true'
    )"""
    sql "CREATE DATABASE IF NOT EXISTS encryption_wire_regression.iceberg_encryption_wire_db"
    for (String version : ["2", "3"]) {
        sql "DROP TABLE IF EXISTS encryption_wire_regression.iceberg_encryption_wire_db.guard_data_v${version}"
        sql """CREATE TABLE encryption_wire_regression.iceberg_encryption_wire_db.guard_data_v${version} (id INT)
            PROPERTIES ('format-version' = '${version}')"""
        sql "INSERT INTO encryption_wire_regression.iceberg_encryption_wire_db.guard_data_v${version} VALUES (1), (2), (3)"
        order_qt_plain_before """SELECT id FROM encryption_wire_regression.iceberg_encryption_wire_db.guard_data_v${version}
            ORDER BY id"""
    }
    String point = "FileScanEncryptionGuard.inject"
    try {
        sql "SET num_files_in_batch_mode=1"
        for (String version : ["2", "3"]) {
            for (String batch : ["false", "true"]) {
                sql "SET enable_external_table_batch_mode=${batch}"
                // Current v3 planning is synchronous; v2 supplies the actual lazy split path.
                explain {
                    sql "SELECT id FROM encryption_wire_regression.iceberg_encryption_wire_db.guard_data_v${version}"
                    if (version == "2" && batch == "true") {
                        contains "(approximate)inputSplitNum"
                    } else {
                        notContains "(approximate)inputSplitNum"
                    }
                }
                for (String pushdown : ["false", "true"]) {
                    sql "SET enable_count_push_down_for_external_table=${pushdown}"
                    for (boolean delete : [false, true]) {
                        GetDebugPoint().enableDebugPointForAllFEs(point, [delete: delete,
                                path_contains: "iceberg_encryption_wire_db"])
                        // Injection is after Iceberg admission. These queries exercise real dispatch and lazy fetch guards.
                        for (int attempt = 0; attempt < 2; attempt++) {
                            test {
                                sql "SELECT id FROM encryption_wire_regression.iceberg_encryption_wire_db.guard_data_v${version} ORDER BY id"
                                exception "File encryption [READ_UNAVAILABLE]"
                            }
                            test {
                                sql "SELECT COUNT(*) FROM encryption_wire_regression.iceberg_encryption_wire_db.guard_data_v${version}"
                                exception "File encryption [READ_UNAVAILABLE]"
                            }
                        }
                        GetDebugPoint().disableDebugPointForAllFEs(point)
                    }
                    for (Map malformed : [[version: 9, expected: "UNSUPPORTED_VERSION"],
                            [invalid_key: true, expected: "INVALID_DESCRIPTOR"]]) {
                        GetDebugPoint().enableDebugPointForAllFEs(point, malformed.findAll { it.key != "expected" })
                        test {
                            sql "SELECT id FROM encryption_wire_regression.iceberg_encryption_wire_db.guard_data_v${version} ORDER BY id"
                            exception "File encryption [${malformed.expected}]"
                        }
                        GetDebugPoint().disableDebugPointForAllFEs(point)
                    }
                }
            }
        }
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs(point)
        sql "UNSET VARIABLE enable_external_table_batch_mode"
        sql "UNSET VARIABLE num_files_in_batch_mode"
        sql "UNSET VARIABLE enable_count_push_down_for_external_table"
    }
    for (String version : ["2", "3"]) {
        order_qt_plain_after """SELECT id FROM encryption_wire_regression.iceberg_encryption_wire_db.guard_data_v${version}
            ORDER BY id"""
        qt_plain_count "SELECT COUNT(*) FROM encryption_wire_regression.iceberg_encryption_wire_db.guard_data_v${version}"
    }
}
