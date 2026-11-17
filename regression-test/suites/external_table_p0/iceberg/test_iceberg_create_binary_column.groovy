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

// CREATE TABLE and CTAS on an Iceberg catalog map every VARBINARY, bounded or not, to Iceberg BINARY at any
// depth. FIXED(n) holds exactly n bytes, so a shorter value in a VARBINARY(n) column must still be writable.
suite("test_iceberg_create_binary_column", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String catalogName = "test_iceberg_create_binary_column"
    String dbName = "iceberg_create_binary_column_db"
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

    // The physical Iceberg type, read from the REST catalog rather than through Doris's own mapping.
    def icebergTypes = { String table ->
        def connection = new URL("http://${externalEnvIp}:${restPort}/v1/namespaces/${dbName}/tables/${table}")
                .openConnection()
        try {
            connection.setConnectTimeout(10000)
            connection.setReadTimeout(30000)
            assertEquals(200, connection.getResponseCode())
            def metadata = connection.getInputStream().withCloseable { new JsonSlurper().parse(it) }.metadata
            def schema = metadata.schemas.find { it."schema-id" == metadata."current-schema-id" }
            return schema.fields.collectEntries { [(it.name): it.type] }
        } finally {
            connection.disconnect()
        }
    }

    sql """
        CREATE TABLE binary_columns (
            id INT,
            payload VARBINARY,
            digest VARBINARY(16),
            s STRUCT<a:VARBINARY, b:INT>,
            arr ARRAY<VARBINARY(4)>,
            m MAP<STRING, VARBINARY>
        ) PROPERTIES ("format-version" = "2", "write.merge.mode" = "merge-on-read")
    """
    def types = icebergTypes("binary_columns")
    assertEquals("binary", types.payload)
    assertEquals("binary", types.digest)
    assertEquals("binary", types.s.fields.find { it.name == "a" }.type)
    assertEquals("binary", types.arr.element)
    assertEquals("binary", types.m.value)
    // A bounded VARBINARY reads back unbounded, as any Iceberg BINARY column does.
    def desc = sql """DESC binary_columns"""
    assertEquals("varbinary(2147483647)", desc.find { it[0] == "digest" }[1])
    assertEquals("array<varbinary(2147483647)>", desc.find { it[0] == "arr" }[1])

    // Non-UTF-8 bytes, a value shorter than the declared bound and nested values all round-trip.
    sql """INSERT INTO binary_columns VALUES (1, X'FFFEF8', X'0102', named_struct('a', X'FF', 'b', 1),
            [X'FFFE'], map('k', X'F8'))"""
    def rows = sql """SELECT id, hex(payload), hex(digest), hex(struct_element(s, 'a')), hex(arr[1]), hex(m['k'])
            FROM binary_columns ORDER BY id"""
    assertEquals([[1, "FFFEF8", "0102", "FF", "FFFE", "F8"]], rows)

    // BINARY, unlike FIXED, can be truncate-partitioned.
    sql """
        CREATE TABLE binary_partitioned (id INT, b VARBINARY(16))
        PARTITION BY LIST (truncate(2, b)) ()
    """
    sql """INSERT INTO binary_partitioned VALUES (1, X'FFFEF8'), (2, X'01')"""
    assertEquals([[1, "FFFEF8"], [2, "01"]], sql("""SELECT id, hex(b) FROM binary_partitioned ORDER BY id"""))

    // CTAS from a table and from a constant.
    sql """CREATE TABLE binary_ctas AS SELECT id, payload, arr FROM binary_columns"""
    assertEquals("binary", icebergTypes("binary_ctas").payload)
    assertEquals([[1, "FFFEF8"]], sql("""SELECT id, hex(payload) FROM binary_ctas ORDER BY id"""))
    sql """CREATE TABLE binary_ctas_const AS SELECT 1 AS id, X'FFFEF8' AS b"""
    assertEquals("binary", icebergTypes("binary_ctas_const").b)

    // ADD COLUMN has no VARBINARY mapping yet.
    test {
        sql """ALTER TABLE binary_columns ADD COLUMN extra VARBINARY"""
        exception "Unsupported type for Iceberg: VARBINARY"
    }

    // An internal OLAP table still cannot store VARBINARY, top-level or nested, by DDL or by CTAS.
    String internalDb = "iceberg_create_binary_column_internal_db"
    sql """create database if not exists internal.${internalDb}"""
    test {
        sql """CREATE TABLE internal.${internalDb}.t (k INT, b VARBINARY)
                DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ("replication_num" = "1")"""
        exception "doris do not support varbinary create table"
    }
    test {
        sql """CREATE TABLE internal.${internalDb}.t (k INT, b ARRAY<VARBINARY>)
                DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ("replication_num" = "1")"""
        exception "unsupported sub-type"
    }
    test {
        sql """CREATE TABLE internal.${internalDb}.t DISTRIBUTED BY HASH(k) BUCKETS 1
                PROPERTIES ("replication_num" = "1") AS SELECT 1 AS k, X'FF' AS b"""
        exception "doris do not support varbinary create table"
    }
    sql """drop database if exists internal.${internalDb} force"""
}
