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

suite("test_iceberg_encrypted_hms_metadata", "p0,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableIcebergTest"))) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String hmsPort = context.config.otherConfigs.get("iceberg_encryption_metastore_port")
    String kmsPort = context.config.otherConfigs.get("iceberg_encryption_kms_port")
    String fixturePort = context.config.otherConfigs.get("iceberg_encryption_fixture_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    def fixtureRequest = { String method, String path ->
        def connection = (HttpURLConnection) new URL("http://${host}:${fixturePort}${path}").openConnection()
        connection.setRequestMethod(method)
        connection.setConnectTimeout(10000)
        connection.setReadTimeout(120000)
        try {
            assertEquals(200, connection.responseCode, "Encrypted metadata fixture ${method} ${path} failed")
            return connection.inputStream.withCloseable { new JsonSlurper().parse(it) }
        } finally {
            connection.disconnect()
        }
    }

    assertTrue(fixtureRequest("POST", "/reset").created)
    def verified = fixtureRequest("GET", "/verify")
    assertEquals("1.12.0", verified.sdkVersion)
    assertEquals(3, verified.formatVersion)
    assertEquals([[1, 2], [1, 2, 3, 4]], verified.snapshots)
    assertEquals([[1, 2], [1, 2, 3, 4]], verified.sparkSnapshots)
    assertEquals(2, verified.fileKeys)
    assertTrue(verified.encryptedManifests)
    assertTrue(verified.encryptedManifestLists)
    for (String failure : ["missingKms", "wrongCredentials", "denied", "wrongKey"]) {
        assertEquals("REJECTED", verified[failure], "The SDK metadata path must reject ${failure}")
    }

    // Each populated snapshot has real encrypted payloads; declarations alone cannot satisfy this fixture.
    for (String cacheEnabled : ["false", "true"]) {
        String catalog = "encryption_hms_cache_${cacheEnabled}"
        sql "DROP CATALOG IF EXISTS ${catalog}"
        sql """CREATE CATALOG ${catalog} PROPERTIES (
            'type' = 'iceberg', 'iceberg.catalog.type' = 'hms',
            'hive.metastore.uris' = 'thrift://${host}:${hmsPort}',
            'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
            'encryption.kms-type' = 'aws', 'kms.endpoint' = 'http://${host}:${kmsPort}',
            'client.region' = 'us-east-1',
            'client.credentials-provider' = 'org.apache.doris.connector.iceberg.glue.ConfigurationAWSCredentialsProvider2x',
            'client.credentials-provider.glue.access_key' = 'admin',
            'client.credentials-provider.glue.secret_key' = 'password',
            's3.access_key' = 'admin', 's3.secret_key' = 'password',
            's3.endpoint' = 'http://${host}:${minioPort}', 's3.region' = 'us-east-1',
            'use_path_style' = 'true', 'hadoop.username' = 'hadoop',
            'meta.cache.iceberg.table.enable' = '${cacheEnabled}'
        )"""
        order_qt_plain_rows "SELECT id FROM ${catalog}.iceberg_encryption_metadata_db.plain_v3 ORDER BY id"
        qt_plain_count "SELECT COUNT(*) FROM ${catalog}.iceberg_encryption_metadata_db.plain_v3"
        try {
            for (String pushdown : ["true", "false"]) {
                sql "SET enable_count_push_down_for_external_table=${pushdown}"
                for (int attempt = 0; attempt < 2; attempt++) {
                    test {
                        sql "SELECT id FROM ${catalog}.iceberg_encryption_metadata_db.encrypted_v3 ORDER BY id"
                        exception "Iceberg encryption [READ_DISABLED]"
                    }
                    test {
                        sql "SELECT COUNT(*) FROM ${catalog}.iceberg_encryption_metadata_db.encrypted_v3"
                        exception "Iceberg encryption [READ_DISABLED]"
                    }
                    test {
                        sql "DESC ${catalog}.iceberg_encryption_metadata_db.encrypted_v3"
                        exception "Iceberg encryption [READ_DISABLED]"
                    }
                    for (String systemTable : ["files", "snapshots", "position_deletes"]) {
                        test {
                            sql "SELECT COUNT(*) FROM ${catalog}.iceberg_encryption_metadata_db.`encrypted_v3\$${systemTable}`"
                            exception "Iceberg encryption [READ_DISABLED]"
                        }
                    }
                }
            }
        } finally {
            sql "UNSET VARIABLE enable_count_push_down_for_external_table"
        }
        order_qt_plain_after_rejections "SELECT id FROM ${catalog}.iceberg_encryption_metadata_db.plain_v3 ORDER BY id"
    }
}
