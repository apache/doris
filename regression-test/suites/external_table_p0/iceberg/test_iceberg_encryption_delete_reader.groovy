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

suite("test_iceberg_encryption_delete_reader", "p0,external,nonConcurrent") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableIcebergTest"))) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String hms = context.config.otherConfigs.get("iceberg_encryption_metastore_port")
    String fixture = context.config.otherConfigs.get("iceberg_encryption_fixture_port")
    String minio = context.config.otherConfigs.get("iceberg_minio_port")
    def reference = { String path, String method ->
        def connection = (HttpURLConnection) new URL("http://${host}:${fixture}/native-deletes/${path}").openConnection()
        connection.setRequestMethod(method)
        connection.setConnectTimeout(10000)
        connection.setReadTimeout(180000)
        try {
            assertEquals(200, connection.responseCode, "Native delete reference ${path} failed")
            return connection.inputStream.withCloseable { new JsonSlurper().parse(it) }
        } finally {
            connection.disconnect()
        }
    }
    assertTrue(reference("reset", "POST").created)
    def verified = reference("verify", "GET")
    assertEquals(16, verified.tables)
    assertEquals([1, 3, 4, 6, 8], verified.rows)
    assertTrue(verified.independentFileKeys)
    sql "DROP CATALOG IF EXISTS encryption_delete_regression"
    sql """CREATE CATALOG encryption_delete_regression PROPERTIES (
        'type' = 'iceberg', 'iceberg.catalog.type' = 'hms',
        'hive.metastore.uris' = 'thrift://${host}:${hms}',
        's3.endpoint' = 'http://${host}:${minio}', 's3.region' = 'us-east-1',
        's3.access_key' = 'admin', 's3.secret_key' = 'password', 'use_path_style' = 'true',
        'meta.cache.iceberg.table.enable' = 'false'
    )"""
    def previous = sql """SELECT @@enable_file_scanner_v2, @@enable_external_table_batch_mode,
        @@num_files_in_batch_mode, @@enable_count_push_down_for_external_table"""
    try {
        sql "SET enable_file_scanner_v2=true"
        sql "SET num_files_in_batch_mode=1"
        for (String batch : ["false", "true"]) {
            sql "SET enable_external_table_batch_mode=${batch}"
            for (String pushdown : ["false", "true"]) {
                sql "SET enable_count_push_down_for_external_table=${pushdown}"
                for (int version : [2, 3]) {
                    for (int mode = 0; mode < 8; mode++) {
                        String table = "encryption_delete_regression.iceberg_encryption_delete_db.native_deletes_${mode}_v${version}"
                        if (mode == 0) {
                            quickTest("plain_rows_v${version}", "SELECT id FROM ${table} ORDER BY id")
                            quickTest("plain_count_v${version}", "SELECT COUNT(*) FROM ${table}")
                        } else {
                            // The physical reader is tested with real independent keys in UT. SQL
                            // must still stop before sending any key until the transport stage is ready.
                            test {
                                sql "SELECT id FROM ${table} ORDER BY id"
                                exception "File encryption [READ_UNAVAILABLE]"
                            }
                            // Equality deletes prevent metadata-only COUNT here; the query still
                            // reaches the dispatch fence when aggregate pushdown is requested.
                            test {
                                sql "SELECT COUNT(*) FROM ${table}"
                                exception "File encryption [READ_UNAVAILABLE]"
                            }
                        }
                        if ((mode & 2) == 0) {
                            // Encrypted data or equality deletes do not encrypt the separate plaintext
                            // position-delete file exposed by this metadata table.
                            quickTest("plain_positions_${mode}_v${version}",
                                    "SELECT pos FROM ${table}\$position_deletes ORDER BY pos")
                            quickTest("plain_position_count_${mode}_v${version}",
                                    "SELECT COUNT(*) FROM ${table}\$position_deletes")
                        } else {
                            test {
                                sql "SELECT pos FROM ${table}\$position_deletes ORDER BY pos"
                                exception "File encryption [READ_UNAVAILABLE]"
                            }
                            test {
                                sql "SELECT COUNT(*) FROM ${table}\$position_deletes"
                                exception "File encryption [READ_UNAVAILABLE]"
                            }
                        }
                    }
                }
            }
        }
    } finally {
        sql "SET enable_file_scanner_v2=${previous[0][0]}"
        sql "SET enable_external_table_batch_mode=${previous[0][1]}"
        sql "SET num_files_in_batch_mode=${previous[0][2]}"
        sql "SET enable_count_push_down_for_external_table=${previous[0][3]}"
    }
}
