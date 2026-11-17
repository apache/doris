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

suite("test_iceberg_encryption_native_read_integration", "p0,external,nonConcurrent") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableIcebergTest"))
            || !"true".equalsIgnoreCase(context.config.otherConfigs.get("enableIcebergEncryptionNativeRead"))) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String hms = context.config.otherConfigs.get("iceberg_encryption_metastore_port")
    String fixturePort = context.config.otherConfigs.get("iceberg_encryption_fixture_port")
    String minio = context.config.otherConfigs.get("iceberg_minio_port")
    def fixture = { String path, String method ->
        def connection = (HttpURLConnection) new URL("http://${host}:${fixturePort}${path}").openConnection()
        connection.setRequestMethod(method)
        connection.setConnectTimeout(10000)
        connection.setReadTimeout(180000)
        try {
            assertEquals(200, connection.responseCode, "Encryption reference operation failed")
            return connection.inputStream.withCloseable { new JsonSlurper().parse(it) }
        } finally {
            connection.disconnect()
        }
    }
    assertTrue(fixture("/reset", "POST").created)
    assertTrue(fixture("/native-deletes/reset", "POST").created)
    def reference = fixture("/verify", "GET")
    assertEquals(reference.snapshots, reference.sparkSnapshots)
    assertTrue(reference.encryptedManifests)
    assertTrue(reference.encryptedManifestLists)
    def deletes = fixture("/native-deletes/verify", "GET")
    assertEquals(16, deletes.tables)
    assertEquals([1, 3, 4, 6, 8], deletes.rows)
    assertTrue(deletes.independentFileKeys)
    assertTrue(fixture("/benchmark/reset", "POST").created)
    def benchmark = fixture("/benchmark/verify", "GET")
    assertEquals(1048576, benchmark.rows)
    assertEquals(4, benchmark.files)
    assertTrue(benchmark.independentFileKeys)
    fixture("/kms/access/allow", "POST")

    def variables = ["enable_file_scanner_v2", "enable_external_table_batch_mode", "num_files_in_batch_mode",
                     "enable_count_push_down_for_external_table", "enable_parquet_file_page_cache",
                     "enable_parquet_filter_by_min_max", "enable_parquet_filter_by_bloom_filter", "enable_profile"]
    def previous = sql "SELECT ${variables.collect { '@@' + it }.join(', ')}"
    def catalogs = []
    try {
        sql "SET enable_file_scanner_v2=true"
        sql "SET num_files_in_batch_mode=1"
        sql "SET enable_profile=true"
        for (String cached : ["false", "true"]) {
            String catalog = "encryption_native_cache_${cached}"
            catalogs.add(catalog)
            sql "DROP CATALOG IF EXISTS ${catalog}"
            sql """CREATE CATALOG ${catalog} PROPERTIES (
                'type' = 'iceberg', 'iceberg.catalog.type' = 'hms',
                'hive.metastore.uris' = 'thrift://${host}:${hms}',
                'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
                'encryption.kms-type' = 'aws', 'kms.endpoint' = 'http://${host}:${fixturePort}/kms/runtime',
                'client.region' = 'us-east-1',
                'client.credentials-provider' = 'org.apache.doris.connector.iceberg.glue.ConfigurationAWSCredentialsProvider2x',
                'client.credentials-provider.glue.access_key' = 'admin',
                'client.credentials-provider.glue.secret_key' = 'password',
                's3.endpoint' = 'http://${host}:${minio}', 's3.region' = 'us-east-1',
                's3.access_key' = 'admin', 's3.secret_key' = 'password', 'use_path_style' = 'true',
                'hadoop.username' = 'hadoop', 'meta.cache.iceberg.table.enable' = '${cached}'
            )"""
            String base = "${catalog}.iceberg_encryption_metadata_db"
            String encrypted = "${base}.encrypted_v3"
            for (int index = 0; index < reference.snapshotIds.size(); index++) {
                def rows = sql "SELECT id FROM ${encrypted} FOR VERSION AS OF ${reference.snapshotIds[index]} ORDER BY id"
                assertEquals(reference.snapshots[index], rows.collect { it[0] as Integer })
            }
            sql "SET enable_parquet_file_page_cache=${cached}"
            for (String batch : ["false", "true"]) {
                sql "SET enable_external_table_batch_mode=${batch}"
                for (String pushdown : ["false", "true"]) {
                    sql "SET enable_count_push_down_for_external_table=${pushdown}"
                    for (String prune : ["false", "true"]) {
                        sql "SET enable_parquet_filter_by_min_max=${prune}"
                        sql "SET enable_parquet_filter_by_bloom_filter=${prune}"
                        for (int mode = 0; mode < 8; mode++) {
                            String table = "${catalog}.iceberg_encryption_delete_db.native_deletes_${mode}_v3"
                            assertEquals(deletes.rows, (sql "SELECT id FROM ${table} ORDER BY id").collect { it[0] as Integer })
                            assertEquals([4, 6, 8], (sql "SELECT id FROM ${table} WHERE id >= 4 ORDER BY id")
                                    .collect { it[0] as Integer })
                            def aggregate = sql "SELECT COUNT(*), SUM(id), MIN(id), MAX(id) FROM ${table}"
                            assertEquals([5L, 22L, 1L, 8L], aggregate[0].collect { it as Long })
                            assertEquals(5L, (sql "SELECT COUNT(*) FROM ${table}")[0][0] as Long)
                            assertEquals(3L, (sql "SELECT COUNT(*) FROM ${table} d JOIN ${base}.plain_v3 p ON d.id=p.id")[0][0] as Long)
                            assertEquals([1L, 6L], (sql "SELECT pos FROM ${table}\$position_deletes ORDER BY pos")
                                    .collect { it[0] as Long })
                        }
                        for (String kind : ["plain", "encrypted"]) {
                            String table = "${base}.benchmark_${kind}_v3"
                            assertEquals([1048576L, 549756338176L, 1L, 1048576L],
                                    (sql "SELECT COUNT(*), SUM(id), MIN(id), MAX(id) FROM ${table}")[0]
                                            .collect { it as Long })
                            assertEquals(1048576L, (sql "SELECT COUNT(*) FROM ${table}")[0][0] as Long)
                            assertEquals([1, 262144, 262145, 1048576],
                                    (sql "SELECT id FROM ${table} WHERE id IN (1, 262144, 262145, 1048576) ORDER BY id")
                                            .collect { it[0] as Integer })
                        }
                    }
                }
            }
            test {
                sql "SELECT COUNT(*) FROM ${encrypted}\$files"
                exception "Iceberg encryption [UNSUPPORTED_METADATA_TABLE]"
            }
            // Keep the catalog, endpoint and warmed cache unchanged: revoke only KMS decrypt permission.
            long before = fixture("/kms/access", "GET").decryptRequests as Long
            assertEquals([1, 2, 3, 4], (sql "SELECT id FROM ${encrypted} ORDER BY id").collect { it[0] as Integer })
            assertTrue((fixture("/kms/access", "GET").decryptRequests as Long) > before)
            try {
                fixture("/kms/access/deny", "POST")
                // SQL exposes the KMS service message/status rather than its Java exception type.
                for (int attempt = 0; attempt < 2; attempt++) {
                    test {
                        sql "SELECT id FROM ${encrypted} ORDER BY id"
                        exception "Fixture decrypt permission revoked (Service: Kms, Status Code: 403"
                    }
                    test {
                        sql "SELECT COUNT(*) FROM ${encrypted}"
                        exception "Fixture decrypt permission revoked (Service: Kms, Status Code: 403"
                    }
                }
            } finally {
                fixture("/kms/access/allow", "POST")
            }
            assertEquals([1, 2, 3, 4], (sql "SELECT id FROM ${encrypted} ORDER BY id").collect { it[0] as Integer })
            assertEquals(4L, (sql "SELECT COUNT(*) FROM ${encrypted}")[0][0] as Long)
        }
    } finally {
        fixture("/kms/access/allow", "POST")
        for (int index = 0; index < variables.size(); index++) {
            sql "SET ${variables[index]}=${previous[0][index]}"
        }
        catalogs.each { sql "DROP CATALOG IF EXISTS ${it}" }
    }
}
