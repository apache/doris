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

suite("test_iceberg_encryption_native_read_guards", "p0,external,nonConcurrent") {
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
            assertEquals(200, connection.responseCode, "Encryption fixture operation failed")
            return connection.inputStream.withCloseable { new JsonSlurper().parse(it) }
        } finally {
            connection.disconnect()
        }
    }
    def variables = ["enable_sql_cache", "enable_hive_sql_cache", "enable_file_scanner_v2"]
    def previous = sql "SELECT ${variables.collect { '@@' + it }.join(', ')}"
    try {
        fixture("/kms/access/allow", "POST")
        fixture("/reset", "POST")
        sql "SET enable_sql_cache=true"
        sql "SET enable_hive_sql_cache=true"
        sql "SET enable_file_scanner_v2=true"
        sql "DROP CATALOG IF EXISTS encryption_native_read_guards"
        sql """CREATE CATALOG encryption_native_read_guards PROPERTIES (
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
            'hadoop.username' = 'hadoop', 'meta.cache.iceberg.table.enable' = 'true'
        )"""
        String plainQuery = "SELECT COUNT(*), SUM(id) FROM encryption_native_read_guards.iceberg_encryption_metadata_db.plain_v3"
        String encryptedCount = "SELECT COUNT(*) FROM encryption_native_read_guards.iceberg_encryption_metadata_db.encrypted_v3"
        String encryptedSum = "SELECT SUM(id) FROM encryption_native_read_guards.iceberg_encryption_metadata_db.encrypted_v3"
        // Pass the operator's quiet window without changing global FE settings shared by other suites.
        long interval = (sql "ADMIN SHOW FRONTEND CONFIG LIKE 'cache_last_version_interval_second'")[0][1] as Long
        sleep((interval + 3) * 1000L)
        qt_plain_before plainQuery
        for (int attempt = 0; attempt < 10; attempt++) {
            sql plainQuery
            if ((sql "EXPLAIN PHYSICAL PLAN ${plainQuery}").toString().contains("PhysicalSqlCache")) {
                break
            }
            sleep(1000)
        }
        explain {
            sql "physical plan ${plainQuery}"
            contains "PhysicalSqlCache"
        }
        for (int attempt = 0; attempt < 3; attempt++) {
            sql encryptedCount
            sql encryptedSum
        }
        qt_encrypted_count_before encryptedCount
        qt_encrypted_sum_before encryptedSum
        for (String query : [encryptedCount, encryptedSum]) {
            explain {
                sql "physical plan ${query}"
                notContains "PhysicalSqlCache"
            }
        }
        // Stable metadata and a warmed result must not replace the current KMS authorization.
        try {
            fixture("/kms/access/deny", "POST")
            for (int attempt = 0; attempt < 2; attempt++) {
                for (String query : [encryptedCount, encryptedSum]) {
                    test {
                        sql query
                        exception "Fixture decrypt permission revoked (Service: Kms, Status Code: 403"
                    }
                }
            }
            qt_plain_during_revocation plainQuery
            explain {
                sql "physical plan ${plainQuery}"
                contains "PhysicalSqlCache"
            }
        } finally {
            fixture("/kms/access/allow", "POST")
        }
        qt_encrypted_count_restored encryptedCount
        qt_encrypted_sum_restored encryptedSum
        // Native sinks do not encrypt physical objects; enabling reads cannot grant encrypted writes.
        test {
            sql "INSERT INTO encryption_native_read_guards.iceberg_encryption_metadata_db.encrypted_v3 VALUES (5)"
            exception "Iceberg encryption [WRITE_UNAVAILABLE]"
        }
        test {
            sql "INSERT OVERWRITE TABLE encryption_native_read_guards.iceberg_encryption_metadata_db.encrypted_v3 VALUES (5)"
            exception "Iceberg encryption [WRITE_UNAVAILABLE]"
        }
        qt_encrypted_after_rejected_writes """
            SELECT id FROM encryption_native_read_guards.iceberg_encryption_metadata_db.encrypted_v3 ORDER BY id
        """
        sql "INSERT INTO encryption_native_read_guards.iceberg_encryption_metadata_db.plain_v3 VALUES (5)"
        qt_plain_after_write """
            SELECT id FROM encryption_native_read_guards.iceberg_encryption_metadata_db.plain_v3 ORDER BY id
        """
    } finally {
        fixture("/kms/access/allow", "POST")
        for (int index = 0; index < variables.size(); index++) {
            sql "SET ${variables[index]}=${previous[0][index]}"
        }
    }
}
