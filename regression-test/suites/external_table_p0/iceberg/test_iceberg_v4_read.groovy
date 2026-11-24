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

suite("test_iceberg_v4_read", "p0,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableIcebergTest"))) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String hmsPort = context.config.otherConfigs.get("iceberg_encryption_metastore_port")
    String fixturePort = context.config.otherConfigs.get("iceberg_encryption_fixture_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    def connection = (HttpURLConnection) new URL("http://${host}:${fixturePort}/v4/reset").openConnection()
    connection.setRequestMethod("POST")
    connection.setConnectTimeout(10000)
    connection.setReadTimeout(120000)
    try {
        assertEquals(200, connection.responseCode)
        assertTrue(connection.inputStream.withCloseable { new JsonSlurper().parse(it) }.created)
    } finally {
        connection.disconnect()
    }

    for (String cached : ["false", "true"]) {
        String catalog = "iceberg_v4_cache_${cached}"
        sql "DROP CATALOG IF EXISTS ${catalog}"
        // Explicit SDK FileIO needs its own property dialect alongside the native reader's storage settings.
        sql """CREATE CATALOG ${catalog} PROPERTIES (
            'type' = 'iceberg', 'iceberg.catalog.type' = 'hms',
            'hive.metastore.uris' = 'thrift://${host}:${hmsPort}',
            'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
            'client.region' = 'us-east-1', 's3.path-style-access' = 'true',
            's3.access-key-id' = 'admin', 's3.secret-access-key' = 'password',
            's3.access_key' = 'admin', 's3.secret_key' = 'password',
            's3.endpoint' = 'http://${host}:${minioPort}', 's3.region' = 'us-east-1',
            'use_path_style' = 'true', 'hadoop.username' = 'hadoop',
            'meta.cache.iceberg.table.enable' = '${cached}',
            'meta.cache.iceberg.manifest.enable' = '${cached}'
        )"""
        // Both whole-table and row-level DELETE must refuse before entering an unadapted V4 write path.
        // The read matrix below also verifies that these refused statements preserve the fixture rows.
        for (String predicate : ["", " WHERE id = 1"]) {
            test {
                sql "DELETE FROM ${catalog}.iceberg_v4_db.relative_v4${predicate}"
                exception "Doris does not support writes to Iceberg format v4 tables"
            }
        }
        try {
            for (String batch : ["false", "true"]) {
                sql "SET enable_external_table_batch_mode=${batch}"
                sql "SET num_files_in_batch_mode=1"
                for (int attempt = 0; attempt < 2; attempt++) {
                    order_qt_fresh "SELECT id FROM ${catalog}.iceberg_v4_db.fresh_v4"
                    order_qt_relative "SELECT id FROM ${catalog}.iceberg_v4_db.relative_v4"
                    order_qt_upgraded_v2 "SELECT id FROM ${catalog}.iceberg_v4_db.upgraded_v2"
                    order_qt_upgraded_v3 "SELECT id FROM ${catalog}.iceberg_v4_db.upgraded_v3"
                    qt_filtered "SELECT COUNT(*), SUM(id) FROM ${catalog}.iceberg_v4_db.relative_v4 WHERE id < 100"
                    // Files metadata tasks open Parquet manifests inside the isolated JNI plugin.
                    order_qt_files """SELECT record_count FROM ${catalog}.iceberg_v4_db.`fresh_v4\$files`
                        WHERE content=0"""
                    for (String pushdown : ["false", "true"]) {
                        sql "SET enable_count_push_down_for_external_table=${pushdown}"
                        qt_fresh_count "SELECT COUNT(*) FROM ${catalog}.iceberg_v4_db.fresh_v4"
                        qt_upgrade_count "SELECT COUNT(*) FROM ${catalog}.iceberg_v4_db.upgraded_v2"
                    }
                }
            }
        } finally {
            sql "UNSET VARIABLE enable_external_table_batch_mode"
            sql "UNSET VARIABLE num_files_in_batch_mode"
            sql "UNSET VARIABLE enable_count_push_down_for_external_table"
        }
    }
}
