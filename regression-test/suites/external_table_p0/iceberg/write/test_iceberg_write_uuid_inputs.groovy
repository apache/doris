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
                sql "INSERT INTO ${table} VALUES (1, '${canonical}'), (2, '${compact}'), (3, CAST('${canonical}' AS UUID)), (4, NULL)"
                // Read text from a table to exercise row expressions, not only literal folding.
                sql "INSERT INTO ${table} SELECT id + 10, IF(id = 4, NULL, '${canonical}') FROM ${table}"
                sql "SWITCH internal"
                sql "CREATE DATABASE IF NOT EXISTS uuid_input_source"
                sql "USE uuid_input_source"
                sql "DROP TABLE IF EXISTS native_uuid_source"
                sql """CREATE TABLE native_uuid_source(id INT, u UUID) DUPLICATE KEY(id)
                    DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("replication_num"="1")"""
                sql "INSERT INTO native_uuid_source VALUES (51, '${canonical}'), (52, NULL)"
                sql "INSERT INTO ${catalog}.${database}.${table} SELECT * FROM native_uuid_source"
                sql "SWITCH ${catalog}"
                sql "USE ${database}"
                if (partitioned) {
                    sql "INSERT INTO ${table} PARTITION(u='${canonical}') VALUES (21)"
                    sql "INSERT INTO ${table} PARTITION(u='${compact}') VALUES (22)"
                    sql "INSERT INTO ${table} PARTITION(u='${canonical.toUpperCase()}') VALUES (23)"
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
                    exception "cast"
                }
                "qt_${table}" "SELECT id, u FROM ${table} ORDER BY id"
                if (partitioned) {
                    // Both the UUID and NULL identity partitions must survive FE partition parsing.
                    explain {
                        sql "SELECT id, u FROM ${table}"
                        contains "partition=2/2"
                    }
                    sql "INSERT OVERWRITE TABLE ${table} PARTITION(u='${compact}') VALUES (31)"
                    "qt_${table}_overwrite" "SELECT id, u FROM ${table} ORDER BY id"
                }
            }
            String nested = "uuid_nested_${format}"
            sql "DROP TABLE IF EXISTS ${nested}"
            // UUID semantics must survive ARRAY/MAP/STRUCT nesting, including null containers and elements.
            def nestedRequest = [name: nested, schema: [type: "struct", "schema-id": 0, fields: [
                    [id: 1, name: "id", required: false, type: "int"],
                    [id: 2, name: "items", required: false,
                            type: [type: "list", "element-id": 3, element: "uuid", "element-required": false]],
                    [id: 4, name: "record", required: false, type: [type: "struct", fields: [
                            [id: 5, name: "u", required: false, type: "uuid"],
                            [id: 6, name: "text", required: false, type: "string"]]]]]],
                    "partition-spec": ["spec-id": 0, fields: []],
                    properties: ["format-version": "2", "write.format.default": format]]
            def nestedConnection = new URL("http://${host}:${restPort}/v1/namespaces/${database}/tables").openConnection()
            try {
                nestedConnection.setConnectTimeout(10000)
                nestedConnection.setReadTimeout(60000)
                nestedConnection.setRequestMethod("POST")
                nestedConnection.setRequestProperty("Content-Type", "application/json")
                nestedConnection.setDoOutput(true)
                nestedConnection.getOutputStream().withCloseable {
                    it.write(JsonOutput.toJson(nestedRequest).getBytes("UTF-8"))
                }
                assertEquals(200, nestedConnection.getResponseCode())
            } finally {
                nestedConnection.disconnect()
            }
            sql """INSERT INTO ${nested} VALUES
                (1, ['${canonical}', NULL], named_struct('u', '${compact}', 'text', 'text')),
                (2, array(CAST('${canonical}' AS UUID), NULL), named_struct('u', CAST('${canonical}' AS UUID), 'text', 'bytes')),
                (3, NULL, NULL), (4, [], named_struct('u', NULL, 'text', NULL))"""
            sql """INSERT INTO ${nested} SELECT id + 10,
                IF(id = 3, NULL, ['${compact}', NULL]),
                IF(id = 3, NULL, named_struct('u', '${canonical}', 'text', 'dynamic')) FROM ${nested}"""
            "qt_${nested}" """SELECT id, items IS NULL, size(items), items[1], items[2],
                record IS NULL, record.u, record.text FROM ${nested} ORDER BY id"""
            def dataFile = sql("SELECT file_path FROM `${nested}\$files` ORDER BY file_path LIMIT 1")[0][0]
            for (String flag : ["unset", "false", "true"]) {
                String mapping = flag == "unset" ? "" : ", 'enable_mapping_varbinary'='${flag}'"
                // File TVFs and catalog scans must expose the same UUID types for every legacy flag value.
                "qt_${nested}_tvf_${flag}" """DESC FUNCTION s3(
                    'uri'='${dataFile}', 'format'='${format}', 's3.endpoint'='${endpoint}',
                    's3.access_key'='admin', 's3.secret_key'='password', 's3.region'='us-east-1',
                    'use_path_style'='true' ${mapping})"""
            }
            // VALUES has no source slot: each random choice must supply all fields of one record.
            String otherUuid = "ffffffff-ffff-ffff-ffff-ffffffffffff"
            String volatileRows = (100..<164).collect { id ->
                "(${id}, NULL, IF(RAND() < 0.5, " +
                        "named_struct('u', '${canonical}', 'text', 'first'), " +
                        "named_struct('u', '${otherUuid}', 'text', 'second')))"
            }.join(",")
            sql "INSERT INTO ${nested} VALUES ${volatileRows}"
            "qt_${nested}_volatile_struct" """SELECT COUNT(*), SUM(
                (record.u = CAST('${canonical}' AS UUID) AND record.text = 'first') OR
                (record.u = CAST('${otherUuid}' AS UUID) AND record.text = 'second'))
                FROM ${nested} WHERE id >= 100"""
            String maps = "uuid_maps_${format}"
            sql "DROP TABLE IF EXISTS ${maps}"
            sql """CREATE TABLE ${maps} (id INT, m MAP<UUID,UUID>, a ARRAY<MAP<STRING,UUID>>,
                    s STRUCT<m:MAP<UUID,UUID>>) PROPERTIES("write.format.default"="${format}")"""
            sql """INSERT INTO ${maps} VALUES
                (1, map('${canonical}', '${compact}'), array(map('k', '${canonical}')),
                    named_struct('m', map('${compact}', '${canonical}'))),
                (2, map('${compact}', NULL), [], named_struct('m', map())),
                (3, NULL, NULL, NULL)"""
            sql "INSERT INTO ${maps} SELECT id + 10, m, a, s FROM ${maps}"
            sql """INSERT INTO ${maps} SELECT id + 30,
                IF(id = 3, NULL, map('${compact}', '${canonical}')),
                IF(id = 3, NULL, array(map('k', '${compact}'))),
                IF(id = 3, NULL, named_struct('m', map('${canonical}', '${compact}')))
                FROM ${maps} WHERE id < 10"""
            "qt_${maps}" "SELECT * FROM ${maps} ORDER BY id"
            // A volatile source map must be evaluated once so each converted key retains its paired value.
            sql """INSERT INTO ${maps}(id, m) SELECT number + 100,
                MAP_APPLY((k, v) -> STRUCT(k, k), MAP(UUID(), 'unused')) FROM numbers("number"="8")"""
            "qt_${maps}_volatile" """SELECT COUNT(*), SUM(MAP_KEYS(m)[1] = MAP_VALUES(m)[1])
                FROM ${maps} WHERE id >= 100"""
            test {
                sql "INSERT INTO ${maps} VALUES (99, map('invalid-uuid', '${canonical}'), NULL, NULL)"
                exception "uuid"
            }
            test {
                sql "INSERT INTO ${nested} VALUES (30, ['invalid-uuid'], NULL)"
                exception "parse uuid failed"
            }
        }
    } finally {
        sql "SWITCH internal"
    }
}
