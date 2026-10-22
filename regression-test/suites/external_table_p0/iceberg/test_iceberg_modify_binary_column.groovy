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

// Doris reads an Iceberg BINARY column as VARBINARY and a FIXED(n) column as VARBINARY(n). MODIFY COLUMN carries
// the whole column definition, so changing the nullability, comment or position of such a column, or of such a
// STRUCT field, ARRAY element or MAP value, restates that VARBINARY type, and the Iceberg type must stay as it is.
suite("test_iceberg_modify_binary_column", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String catalogName = "test_iceberg_modify_binary_column"
    String dbName = "iceberg_modify_binary_column_db"
    String tableName = "binary_columns"
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    sql """drop catalog if exists ${catalogName}"""
    sql """
        CREATE CATALOG ${catalogName} PROPERTIES (
            'type'='iceberg',
            'iceberg.catalog.type'='rest',
            'uri' = 'http://${externalEnvIp}:${restPort}',
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
            "s3.region" = "us-east-1",
            "meta.cache.iceberg.table.ttl-second" = "0"
        );
    """
    sql """switch ${catalogName}"""
    sql """drop database if exists ${dbName} force"""
    sql """create database ${dbName}"""
    sql """use ${dbName}"""

    // Neither Doris (no Iceberg type for VARBINARY) nor Spark SQL (no FIXED type) can declare these columns, so
    // the table is created through the REST catalog.
    def request = [name: tableName, schema: [type: "struct", "schema-id": 0, fields: [
            [id: 1, name: "id", required: false, type: "int"],
            [id: 2, name: "payload", required: false, type: "binary"],
            [id: 3, name: "digest", required: true, type: "fixed[16]", doc: "sha"],
            [id: 4, name: "s", required: false, type: [type: "struct", fields: [
                    [id: 5, name: "a", required: false, type: "binary", doc: "raw"],
                    [id: 6, name: "b", required: false, type: "int"]]]],
            [id: 7, name: "arr", required: false,
                    type: [type: "list", "element-id": 8, element: "fixed[4]", "element-required": true]],
            [id: 9, name: "m", required: false, type: [type: "map", "key-id": 10, key: "string",
                    "value-id": 11, value: "binary", "value-required": true]]]],
            "partition-spec": ["spec-id": 0, fields: []],
            properties: ["format-version": "2"]]
    def connection = new URL("http://${externalEnvIp}:${restPort}/v1/namespaces/${dbName}/tables").openConnection()
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

    sql """INSERT INTO ${tableName} VALUES (1, X'0102', X'00112233445566778899AABBCCDDEEFF',
            named_struct('a', X'AB', 'b', 1), [X'01020304'], map('k', X'02'))"""
    // DESC shows column and STRUCT field comments too.
    sql """set show_column_comment_in_describe = true"""
    qt_desc_before """DESC ${tableName}"""
    // The required column, ARRAY element and MAP value reject NULL.
    test {
        sql """INSERT INTO ${tableName} VALUES (2, X'03', NULL, NULL, NULL, NULL)"""
        exception "Column 'digest' is declared non-nullable"
    }
    test {
        sql """INSERT INTO ${tableName} VALUES (2, X'03', X'00112233445566778899AABBCCDDEEFF', NULL, [NULL], NULL)"""
        exception "Column 'element' is declared non-nullable"
    }
    test {
        sql """INSERT INTO ${tableName} VALUES (2, X'03', X'00112233445566778899AABBCCDDEEFF', NULL, NULL,
                map('k', NULL))"""
        exception "Column 'value' is declared non-nullable"
    }

    // Each statement restates the type DESC shows, to change the position, nullability or comment.
    sql """ALTER TABLE ${tableName} MODIFY COLUMN payload VARBINARY NULL FIRST"""
    sql """ALTER TABLE ${tableName} MODIFY COLUMN digest VARBINARY(16) NULL COMMENT 'digest'"""
    sql """ALTER TABLE ${tableName} MODIFY COLUMN s.a VARBINARY COMMENT 'bytes'"""
    sql """ALTER TABLE ${tableName} MODIFY COLUMN arr.element VARBINARY(4) NULL"""
    sql """ALTER TABLE ${tableName} MODIFY COLUMN m.value VARBINARY NULL"""
    // A VARBINARY that is not how the column or field is read has no Iceberg type.
    test {
        sql """ALTER TABLE ${tableName} MODIFY COLUMN payload VARBINARY(16)"""
        exception "Unsupported type for Iceberg: VARBINARY"
    }
    test {
        sql """ALTER TABLE ${tableName} MODIFY COLUMN s.a VARBINARY(3)"""
        exception "Unsupported type for Iceberg: VARBINARY"
    }
    // BINARY, FIXED(16) and FIXED(4) are still read the same way; payload moved first and the comments changed.
    qt_desc_after """DESC ${tableName}"""

    // The column, ARRAY element and MAP value accept NULL now.
    sql """INSERT INTO ${tableName} VALUES (X'FF', 2, NULL, named_struct('a', NULL, 'b', 2), [NULL],
            map('k', NULL))"""
    String dorisQuery = """
        SELECT id, hex(payload), hex(digest), hex(struct_element(s, 'a')), struct_element(s, 'b'), hex(arr[1]),
               hex(m['k'])
        FROM ${tableName}
        ORDER BY id
    """
    qt_rows dorisQuery
    // Spark reads the evolved table back the same way.
    def sparkRows = spark_iceberg """
        SELECT id, hex(payload), hex(digest), hex(s.a), s.b, hex(arr[0]), hex(m['k'])
        FROM demo.${dbName}.${tableName}
        ORDER BY id
    """
    assertSparkDorisResultEquals(sparkRows, sql(dorisQuery))
}
