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

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.AmazonS3Exception
import com.amazonaws.services.s3.model.GetObjectRequest
import groovy.json.JsonSlurper
import org.junit.jupiter.api.Assertions

suite("test_iceberg_rest_server_planning_guard", "p0,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableIcebergTest"))) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String restPort = context.config.otherConfigs.get("iceberg_scan_planning_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String uri = "http://${host}:${restPort}"
    // The dedicated fixture is mandatory whenever Iceberg tests are enabled. Missing capabilities
    // must fail the suite, not silently turn this regression into a passing no-op.
    def config = new JsonSlurper().parseText(new URL("${uri}/v1/config").getText(
            connectTimeout: 10000, readTimeout: 10000))
    assertTrue(config.endpoints?.any { it.startsWith("POST ") && it.endsWith("/tables/{table}/plan") },
            "Dedicated Iceberg REST fixture must advertise scan planning")

    sql "DROP CATALOG IF EXISTS rest_guard_client"
    sql """CREATE CATALOG rest_guard_client PROPERTIES (
        'type' = 'iceberg', 'iceberg.catalog.type' = 'rest', 'uri' = '${uri}',
        's3.access_key' = 'admin', 's3.secret_key' = 'password',
        's3.endpoint' = 'http://${host}:${minioPort}', 's3.region' = 'us-east-1',
        'use_path_style' = 'true', 'scan-planning-mode' = 'client'
    )"""
    sql "CREATE DATABASE IF NOT EXISTS rest_guard_client.rest_guard_db"
    sql "DROP TABLE IF EXISTS rest_guard_client.rest_guard_db.populated"
    sql "DROP TABLE IF EXISTS rest_guard_client.rest_guard_db.empty_table"
    sql "CREATE TABLE rest_guard_client.rest_guard_db.populated (id INT)"
    sql "CREATE TABLE rest_guard_client.rest_guard_db.empty_table (id INT)"
    sql "INSERT INTO rest_guard_client.rest_guard_db.populated VALUES (1), (2), (3), (4)"
    order_qt_client "SELECT * FROM rest_guard_client.rest_guard_db.populated ORDER BY id"

    // Exercise the real /plan endpoint before testing rejection. A capability advertisement alone
    // would not prove the selected image can plan this table using its own storage credentials.
    String tablePath = "/v1/namespaces/rest_guard_db/tables/populated"
    def connection = (HttpURLConnection) new URL("${uri}${tablePath}/plan").openConnection()
    connection.setConnectTimeout(10000)
    connection.setReadTimeout(30000)
    connection.setRequestMethod("POST")
    connection.setRequestProperty("Content-Type", "application/json")
    connection.setDoOutput(true)
    def plan
    try {
        connection.outputStream.withCloseable { it.write("{}".getBytes("UTF-8")) }
        assertEquals(200, connection.responseCode, "Real server-side planning must succeed")
        connection.inputStream.withCloseable { plan = new JsonSlurper().parse(it) }
    } finally {
        connection.disconnect()
    }
    assertEquals("completed", plan.status)
    assertTrue(!plan["file-scan-tasks"].isEmpty(), "The real fixture must return data-file tasks")

    def loaded = new JsonSlurper().parseText(new URL("${uri}${tablePath}").getText(
            connectTimeout: 10000, readTimeout: 10000))
    def snapshot = loaded.metadata.snapshots.find {
        it["snapshot-id"] == loaded.metadata["current-snapshot-id"]
    }
    def restrictedStorage = AmazonS3ClientBuilder.standard()
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                    "http://${host}:${minioPort}", "us-east-1"))
            .withCredentials(new AWSStaticCredentialsProvider(
                    new BasicAWSCredentials("scan_data_reader", "ScanDataOnly2026")))
            .withPathStyleAccessEnabled(true)
            .build()
    try {
        // Verify both halves of the actual policy: data GET succeeds, metadata HEAD is forbidden.
        for (def task : plan["file-scan-tasks"]) {
            URI file = new URI(task["data-file"]["file-path"])
            def request = new GetObjectRequest(file.host, file.path.substring(1)).withRange(0, 0)
            restrictedStorage.getObject(request).withCloseable { obj ->
                assertTrue(obj.objectContent.read() >= 0, "Restricted client must be able to read data")
            }
        }
        URI manifest = new URI(snapshot["manifest-list"])
        try {
            restrictedStorage.getObjectMetadata(manifest.host, manifest.path.substring(1))
            Assertions.fail("Restricted client unexpectedly read manifest metadata")
        } catch (AmazonS3Exception denied) {
            assertEquals(403, denied.statusCode, "The failure must be a real storage permission denial")
        }
    } finally {
        restrictedStorage.shutdown()
    }
    // Cancel only after the storage checks succeed, so cancellation cannot mask their failure.
    // Completed plans may omit plan-id; only cancel when the server provides one.
    if (plan["plan-id"] != null) {
        def cancel = (HttpURLConnection) new URL("${uri}${tablePath}/plan/${plan['plan-id']}").openConnection()
        try {
            cancel.setRequestMethod("DELETE")
            cancel.setConnectTimeout(10000)
            cancel.setReadTimeout(10000)
            assertEquals(204, cancel.responseCode)
        } finally {
            cancel.disconnect()
        }
    }

    for (String cacheEnabled : ["true", "false"]) {
        String catalog = "rest_guard_server_${cacheEnabled}"
        sql "DROP CATALOG IF EXISTS ${catalog}"
        sql """CREATE CATALOG ${catalog} PROPERTIES (
            'type' = 'iceberg', 'iceberg.catalog.type' = 'rest', 'uri' = '${uri}',
            's3.access_key' = 'admin', 's3.secret_key' = 'password',
            's3.endpoint' = 'http://${host}:${minioPort}', 's3.region' = 'us-east-1',
            'use_path_style' = 'true', 'scan-planning-mode' = 'server',
            'meta.cache.iceberg.table.enable' = '${cacheEnabled}'
        )"""
        test {
            sql "SELECT * FROM ${catalog}.rest_guard_db.populated ORDER BY id"
            exception "Iceberg server-side scan planning is not supported"
        }
        test {
            sql "DESC ${catalog}.rest_guard_db.populated"
            exception "Iceberg server-side scan planning is not supported"
        }
        test {
            sql "SHOW CREATE TABLE ${catalog}.rest_guard_db.populated"
            exception "Iceberg server-side scan planning is not supported"
        }
        test {
            sql "INSERT INTO ${catalog}.rest_guard_db.populated VALUES (99)"
            exception "Iceberg server-side scan planning is not supported"
        }
        test {
            sql "SELECT * FROM ${catalog}.rest_guard_db.empty_table"
            exception "Iceberg server-side scan planning is not supported"
        }
        test {
            sql "SELECT * FROM ${catalog}.rest_guard_db.`populated\$files`"
            exception "Iceberg server-side scan planning is not supported"
        }
    }
    sql "DROP CATALOG IF EXISTS rest_guard_restricted"
    sql """CREATE CATALOG rest_guard_restricted PROPERTIES (
        'type' = 'iceberg', 'iceberg.catalog.type' = 'rest', 'uri' = '${uri}',
        's3.access_key' = 'scan_data_reader', 's3.secret_key' = 'ScanDataOnly2026',
        's3.endpoint' = 'http://${host}:${minioPort}', 's3.region' = 'us-east-1',
        'use_path_style' = 'true', 'scan-planning-mode' = 'server'
    )"""
    test {
        sql "SELECT * FROM rest_guard_restricted.rest_guard_db.populated ORDER BY id"
        exception "Iceberg server-side scan planning is not supported"
    }
    test {
        sql "SELECT * FROM rest_guard_restricted.rest_guard_db.`populated\$files`"
        exception "Iceberg server-side scan planning is not supported"
    }
    order_qt_client_after "SELECT * FROM rest_guard_client.rest_guard_db.populated ORDER BY id"
    sql "DROP CATALOG rest_guard_restricted"
    sql "DROP CATALOG rest_guard_server_true"
    sql "DROP CATALOG rest_guard_server_false"
    sql "DROP CATALOG rest_guard_client"
}
