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

import groovy.json.JsonOutput
import groovy.json.JsonSlurper

suite("test_iceberg_encryption_read_guard", "p0,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableIcebergTest"))) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String uri = "http://${host}:${restPort}"
    String db = "iceberg_encryption_read_guard_db"

    def restRequest = { String method, String path, Map body, List<Integer> statuses ->
        def connection = (HttpURLConnection) new URL("${uri}${path}").openConnection()
        connection.setRequestMethod(method)
        connection.setConnectTimeout(10000)
        connection.setReadTimeout(30000)
        try {
            if (body != null) {
                connection.setRequestProperty("Content-Type", "application/json")
                connection.setDoOutput(true)
                connection.outputStream.withCloseable {
                    it.write(JsonOutput.toJson(body).getBytes("UTF-8"))
                }
            }
            int status = connection.responseCode
            assertTrue(statuses.contains(status), "Iceberg fixture ${method} ${path} returned ${status}")
            // Released REST fixtures may return 200 with no body for DELETE instead of 204.
            if (status == 200 && method != "DELETE") {
                return connection.inputStream.withCloseable { new JsonSlurper().parse(it) }
            }
            return null
        } finally {
            connection.disconnect()
        }
    }

    def createCatalog = { String name, String cacheEnabled ->
        sql "DROP CATALOG IF EXISTS ${name}"
        sql """CREATE CATALOG ${name} PROPERTIES (
            'type' = 'iceberg', 'iceberg.catalog.type' = 'rest', 'uri' = '${uri}',
            's3.access_key' = 'admin', 's3.secret_key' = 'password',
            's3.endpoint' = 'http://${host}:${minioPort}', 's3.region' = 'us-east-1',
            'use_path_style' = 'true', 'meta.cache.iceberg.table.enable' = '${cacheEnabled}',
            'enable_encryption_read' = 'true'
        )"""
    }

    // Admission is determined by SDK metadata, before file decryption. Keep the files plaintext so
    // this regression needs no KMS and cannot mistake a late decryption error for the admission error.
    createCatalog("encryption_guard_seed", "false")
    sql "CREATE DATABASE IF NOT EXISTS encryption_guard_seed.${db}"
    for (String table : ["plain_data", "plain_empty", "declared_data", "declared_empty"]) {
        // Remove previous declarations through the fixture, since Doris correctly refuses to bind them.
        // Only this suite's tables are touched; keep all tables after the run for failure inspection.
        restRequest("DELETE", "/v1/namespaces/${db}/tables/${table}", null, [200, 204, 404])
    }
    sql """CREATE TABLE encryption_guard_seed.${db}.plain_data (id INT)
        PROPERTIES ('format-version' = '3')"""
    sql """CREATE TABLE encryption_guard_seed.${db}.plain_empty (id INT)
        PROPERTIES ('format-version' = '3')"""
    sql """CREATE TABLE encryption_guard_seed.${db}.declared_data (id INT)
        PROPERTIES ('format-version' = '3')"""
    sql """CREATE TABLE encryption_guard_seed.${db}.declared_empty (id INT)
        PROPERTIES ('format-version' = '3')"""
    sql "INSERT INTO encryption_guard_seed.${db}.plain_data VALUES (1), (2), (3)"
    sql "INSERT INTO encryption_guard_seed.${db}.declared_data VALUES (1), (2), (3)"
    order_qt_before_declaration "SELECT id FROM encryption_guard_seed.${db}.declared_data ORDER BY id"

    for (String table : ["declared_data", "declared_empty"]) {
        String path = "/v1/namespaces/${db}/tables/${table}"
        def loaded = restRequest("GET", path, null, [200])
        restRequest("POST", path, [
            requirements: [[type: "assert-table-uuid", uuid: loaded.metadata["table-uuid"]]],
            updates: [[action: "set-properties", updates: ["encryption.key-id": "test-key"]]]
        ], [200])
        def declared = restRequest("GET", path, null, [200])
        assertEquals("test-key", declared.metadata.properties["encryption.key-id"],
                "The remote table must actually declare encryption")
    }

    try {
        for (String cacheEnabled : ["false", "true"]) {
            String catalog = "encryption_guard_cache_${cacheEnabled}"
            // New catalogs isolate read admission from the pre-declaration binding and its schema cache.
            // The catalog property above must never override the plugin's deployment-level switch.
            createCatalog(catalog, cacheEnabled)
            order_qt_plain_rows "SELECT id FROM ${catalog}.${db}.plain_data ORDER BY id"
            qt_plain_empty "SELECT COUNT(*) FROM ${catalog}.${db}.plain_empty"
            for (String countPushdown : ["true", "false"]) {
                sql "SET enable_count_push_down_for_external_table=${countPushdown}"
                qt_plain_count "SELECT COUNT(*) FROM ${catalog}.${db}.plain_data"
                // Repeated failures exercise a warmed table cache as well as the initial acquisition.
                for (int attempt = 0; attempt < 2; attempt++) {
                    for (String table : ["declared_data", "declared_empty"]) {
                        // Assert the default-off deployment contract even when the catalog requests encryption.
                        test {
                            sql "SELECT id FROM ${catalog}.${db}.${table} ORDER BY id"
                            exception "Iceberg encryption [READ_DISABLED]"
                        }
                        test {
                            sql "SELECT COUNT(*) FROM ${catalog}.${db}.${table}"
                            exception "Iceberg encryption [READ_DISABLED]"
                        }
                        test {
                            sql "DESC ${catalog}.${db}.${table}"
                            exception "Iceberg encryption [READ_DISABLED]"
                        }
                        for (String systemTable : ["files", "snapshots", "position_deletes"]) {
                            test {
                                sql "SELECT COUNT(*) FROM ${catalog}.${db}.`${table}\$${systemTable}`"
                                exception "Iceberg encryption [READ_DISABLED]"
                            }
                        }
                    }
                }
            }
            order_qt_plain_after_rejections "SELECT id FROM ${catalog}.${db}.plain_data ORDER BY id"
        }
    } finally {
        sql "UNSET VARIABLE enable_count_push_down_for_external_table"
    }
}
