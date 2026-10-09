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

suite("test_iceberg_write_uuid_inputs", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    if (!context.config.otherConfigs.get("enableIcebergTest")?.toString()?.equalsIgnoreCase("true")) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String endpoint = context.config.otherConfigs.get("iceberg_minio_endpoint") ?: "http://${host}:${minioPort}"
    String catalog = "test_iceberg_write_uuid_inputs"
    String database = "uuid_input_roundtrip"
    String canonical = "00112233-4455-6677-8899-aabbccddeeff"
    String compact = canonical.replace("-", "")
    try {
        sql "DROP CATALOG IF EXISTS ${catalog}"
        sql """CREATE CATALOG ${catalog} PROPERTIES (
            "type"="iceberg", "iceberg.catalog.type"="rest", "uri"="http://${host}:${restPort}",
            "s3.endpoint"="${endpoint}", "s3.access_key"="admin", "s3.secret_key"="password",
            "s3.region"="us-east-1", "use_path_style"="true")"""
        sql "SWITCH ${catalog}"
        sql "CREATE DATABASE IF NOT EXISTS ${database}"
        sql "USE ${database}"
        for (String format : ["parquet", "orc"]) {
            for (boolean partitioned : [false, true]) {
                String table = "uuid_${format}_${partitioned}"
                sql "DROP TABLE IF EXISTS ${table}"
                // Spark SQL has no UUID type; REST preserves the remote UUID schema under test.
                def request = [name: table, schema: [type: "struct", "schema-id": 0, fields: [
                        [id: 1, name: "id", required: false, type: "int"],
                        [id: 2, name: "u", required: false, type: "uuid"]]],
                        "partition-spec": ["spec-id": 0, fields: partitioned ? [
                                ["source-id": 2, "field-id": 1000, name: "u", transform: "identity"]] : []],
                        properties: ["format-version": "2", "write.format.default": format]]
                def connection = new URL("http://${host}:${restPort}/v1/namespaces/${database}/tables").openConnection()
                try {
                    connection.setConnectTimeout(10000)
                    connection.setReadTimeout(60000)
                    connection.setRequestMethod("POST")
                    connection.setRequestProperty("Content-Type", "application/json")
                    connection.setDoOutput(true)
                    connection.getOutputStream().withCloseable { it.write(JsonOutput.toJson(request).getBytes("UTF-8")) }
                    assertEquals(200, connection.getResponseCode())
                } finally {
                    connection.disconnect()
                }
                sql "INSERT INTO ${table} VALUES (1, '${canonical}'), (2, '${compact}'), (3, X'${compact}'), (4, NULL)"
                // Read text from a table to exercise row expressions, not only literal folding.
                sql "INSERT INTO ${table} SELECT id + 10, IF(id = 4, NULL, '${canonical}') FROM ${table}"
                if (partitioned) {
                    sql "INSERT INTO ${table} PARTITION(u='${canonical}') VALUES (21)"
                    sql "INSERT INTO ${table} PARTITION(u='${compact}') VALUES (22)"
                    sql "INSERT INTO ${table} PARTITION(u=X'${compact}') VALUES (23)"
                    sql "INSERT INTO ${table} PARTITION(u=NULL) VALUES (24)"
                }
                test {
                    sql "INSERT INTO ${table} VALUES (30, 'not-a-uuid')"
                    exception "UUID"
                }
                test {
                    sql "INSERT INTO ${table} SELECT id + 30, IF(id = 1, 'invalid-uuid', '${canonical}') FROM ${table}"
                    exception "parse uuid failed"
                }
                test {
                    sql "INSERT INTO ${table} VALUES (30, X'0011')"
                    exception format == "orc" && !partitioned ? "Invalid UUID string length" : "16 bytes"
                }
                "qt_${table}" "SELECT id, HEX(u) FROM ${table} ORDER BY id"
                if (partitioned) {
                    sql "INSERT OVERWRITE TABLE ${table} PARTITION(u='${compact}') VALUES (31)"
                    "qt_${table}_overwrite" "SELECT id, HEX(u) FROM ${table} ORDER BY id"
                }
            }
        }
    } finally {
        sql "SWITCH internal"
    }
}
