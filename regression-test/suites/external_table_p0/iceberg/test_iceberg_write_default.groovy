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

// WHY: an Iceberg write default belongs to a request-scoped writer schema. Cached catalog Columns must not
// expose it through DESCRIBE/SHOW CREATE, while INSERT analysis still needs it to fill omitted columns.
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

    sql """switch ${catalog_name}"""
    sql """drop database if exists ${db} force"""
    sql """create database ${db}"""
    sql """use ${db}"""

    // format-version=3 so iceberg accepts a non-null column default.
    sql """
        CREATE TABLE t_write_default (
            id INT
        ) PROPERTIES ('format-version' = '3');
    """
    // Doris ALTER ADD COLUMN ... DEFAULT wires the default into iceberg's write default (and initial
    // default) via updateSchema.addColumn(name, type, doc, literal).
    sql """ ALTER TABLE t_write_default ADD COLUMN c INT DEFAULT 42 """

    // Force a fresh schema load so DESC and INSERT exercise the cached/read schema versus the
    // request-scoped writer schema.
    sql """refresh catalog ${catalog_name}"""
    sql """use ${db}"""

    // 1) Read path: catalog Columns must not expose the connector-only write default.
    qt_desc_write_default """
        SELECT COLUMN_DEFAULT
        FROM ${catalog_name}.information_schema.columns
        WHERE TABLE_CATALOG = '${catalog_name}'
          AND TABLE_SCHEMA = '${db}'
          AND TABLE_NAME = 't_write_default'
          AND COLUMN_NAME = 'c'
    """

    // 2) INSERT with column c omitted must apply the write default.
    sql """ INSERT INTO t_write_default (id) VALUES (1) """
    order_qt_omitted_write_default """SELECT id, c FROM t_write_default ORDER BY id"""
}
