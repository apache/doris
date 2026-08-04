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

// WHY: an iceberg column WRITE default (v3) must be surfaced by the read path — parseSchema reads
// Types.NestedField.writeDefault() into the FE Column default — so that DESCRIBE shows it and an INSERT
// that omits the column applies it. Before the fix parseSchema hardcoded the default to null, so after a
// schema refresh the write default silently vanished (DESC showed no default, omitted INSERT wrote NULL).
suite("test_iceberg_write_default", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "test_iceberg_write_default"
    String db = "test_iceberg_write_default_db"
    String tbl = "t_write_default"

    sql """drop catalog if exists ${catalog_name}"""
    sql """
    CREATE CATALOG ${catalog_name} PROPERTIES (
        'type'='iceberg',
        'iceberg.catalog.type'='rest',
        'uri' = 'http://${externalEnvIp}:${rest_port}',
        "s3.access_key" = "admin",
        "s3.secret_key" = "password",
        "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
        "s3.region" = "us-east-1"
    );"""

    try {
        sql """switch ${catalog_name}"""
        sql """drop database if exists ${db} force"""
        sql """create database ${db}"""
        sql """use ${db}"""

        // format-version=3 so iceberg accepts a non-null column default.
        sql """
            CREATE TABLE ${tbl} (
                id INT
            ) PROPERTIES ('format-version' = '3');
        """
        // Doris ALTER ADD COLUMN ... DEFAULT wires the default into iceberg's write default (and initial
        // default) via updateSchema.addColumn(name, type, doc, literal).
        sql """ ALTER TABLE ${tbl} ADD COLUMN c INT DEFAULT 42 """

        // Force a fresh schema load so DESC / INSERT exercise parseSchema reading writeDefault (not any
        // in-memory schema retained from the ALTER).
        sql """refresh catalog ${catalog_name}"""
        sql """use ${db}"""

        // 1) Read path: DESC must surface the write default on column c.
        def descRows = sql """desc ${tbl}"""
        def cRow = descRows.find { it[0].toString().equalsIgnoreCase("c") }
        assertTrue(cRow != null, "column c must exist in DESC: " + descRows.toString())
        assertTrue(cRow.toString().contains("42"),
                "DESC must show column c write default 42, got: " + cRow.toString())

        // 2) INSERT with column c omitted must apply the write default.
        sql """ INSERT INTO ${tbl} (id) VALUES (1) """
        def rows = sql """ SELECT id, c FROM ${tbl} ORDER BY id """
        assertEquals(1, rows.size())
        assertEquals("42", rows[0][1].toString(),
                "an INSERT omitting column c must apply the iceberg write default 42, got: " + rows[0][1])
    } finally {
        sql """drop table if exists ${catalog_name}.${db}.${tbl}"""
        sql """drop database if exists ${catalog_name}.${db} force"""
        sql """drop catalog if exists ${catalog_name}"""
    }
}
