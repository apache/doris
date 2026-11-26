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

suite("test_iceberg_encrypted_rest_metadata", "p0,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableIcebergTest"))) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String kmsPort = context.config.otherConfigs.get("iceberg_encryption_kms_port")
    String fixturePort = context.config.otherConfigs.get("iceberg_encryption_fixture_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    def fixtureRequest = { String method, String path ->
        def connection = (HttpURLConnection) new URL("http://${host}:${fixturePort}${path}").openConnection()
        connection.setRequestMethod(method)
        connection.setConnectTimeout(10000)
        connection.setReadTimeout(120000)
        try {
            assertEquals(200, connection.responseCode, "REST encryption fixture ${method} ${path} failed")
            return connection.inputStream.withCloseable { new JsonSlurper().parse(it) }
        } finally {
            connection.disconnect()
        }
    }
    assertTrue(fixtureRequest("POST", "/rest/reset").created)
    def verified = fixtureRequest("GET", "/rest/verify")
    assertEquals("rest", verified.catalog)
    assertEquals("1.12.0", verified.sdkVersion)
    assertEquals(3, verified.formatVersion)
    assertEquals([[1, 2], [1, 2, 3, 4]], verified.snapshots)
    assertTrue(verified.encryptedManifests)
    assertTrue(verified.encryptedManifestLists)
    for (String failure : ["missingKms", "wrongCredentials", "denied", "wrongKey"]) {
        assertEquals("REJECTED", verified[failure])
    }
    def metadata = fixtureRequest("GET",
            "/rest/v1/namespaces/iceberg_encryption_rest_db/tables/encrypted_v3").metadata
    def snapshots = metadata.snapshots.collect { it["snapshot-id"] }
    assertEquals(2, snapshots.size())

    String previousBatch = sql("SHOW VARIABLES LIKE 'enable_external_table_batch_mode'")[0][1]
    String previousCount = sql("SHOW VARIABLES LIKE 'enable_count_push_down_for_external_table'")[0][1]
    try {
        for (String cached : ["false", "true"]) {
            String catalog = "encryption_rest_cache_${cached}"
            sql "DROP CATALOG IF EXISTS ${catalog}"
            sql """CREATE CATALOG ${catalog} PROPERTIES (
                'type' = 'iceberg', 'iceberg.catalog.type' = 'rest',
                'iceberg.rest.uri' = 'http://${host}:${fixturePort}/rest', 'rest.auth.type' = 'none',
                'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
                'encryption.kms-type' = 'aws', 'kms.endpoint' = 'http://${host}:${kmsPort}',
                'client.region' = 'us-east-1',
                'client.credentials-provider' = 'org.apache.doris.connector.iceberg.glue.ConfigurationAWSCredentialsProvider2x',
                'client.credentials-provider.glue.access_key' = 'admin',
                'client.credentials-provider.glue.secret_key' = 'password',
                's3.access_key' = 'admin', 's3.secret_key' = 'password',
                's3.endpoint' = 'http://${host}:${minioPort}', 's3.region' = 'us-east-1',
                'use_path_style' = 'true', 'meta.cache.iceberg.table.enable' = '${cached}'
            )"""
            for (String batch : ["false", "true"]) {
                sql "SET enable_external_table_batch_mode=${batch}"
                for (String count : ["false", "true"]) {
                    sql "SET enable_count_push_down_for_external_table=${count}"
                    order_qt_plain_rows "SELECT id FROM ${catalog}.iceberg_encryption_rest_db.plain_v3 ORDER BY id"
                    qt_plain_count "SELECT COUNT(*) FROM ${catalog}.iceberg_encryption_rest_db.plain_v3"
                    for (int attempt = 0; attempt < 2; attempt++) {
                        // REST admission follows the same default-off contract as HMS and must run before
                        // encrypted metadata can enter COUNT, metadata-table or lazy split paths.
                        for (String statement : [
                            "SELECT id FROM ${catalog}.iceberg_encryption_rest_db.encrypted_v3 ORDER BY id",
                            "SELECT COUNT(*) FROM ${catalog}.iceberg_encryption_rest_db.encrypted_v3",
                            "DESC ${catalog}.iceberg_encryption_rest_db.encrypted_v3"
                        ]) {
                            test {
                                sql statement
                                exception "Iceberg encryption [READ_DISABLED]"
                            }
                        }
                        for (String systemTable : ["files", "snapshots", "position_deletes"]) {
                            test {
                                sql "SELECT COUNT(*) FROM ${catalog}.iceberg_encryption_rest_db.`encrypted_v3\$${systemTable}`"
                                exception "Iceberg encryption [READ_DISABLED]"
                            }
                        }
                        for (def snapshot : snapshots) {
                            test {
                                sql "SELECT id FROM ${catalog}.iceberg_encryption_rest_db.encrypted_v3 FOR VERSION AS OF ${snapshot}"
                                exception "Iceberg encryption [READ_DISABLED]"
                            }
                        }
                    }
                }
            }
            sql "REFRESH CATALOG ${catalog}"
            order_qt_plain_after_refresh "SELECT id FROM ${catalog}.iceberg_encryption_rest_db.plain_v3 ORDER BY id"
        }
    } finally {
        sql "SET enable_external_table_batch_mode=${previousBatch}"
        sql "SET enable_count_push_down_for_external_table=${previousCount}"
    }
}
