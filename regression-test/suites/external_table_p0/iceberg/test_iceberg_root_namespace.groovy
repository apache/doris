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

suite("test_iceberg_root_namespace", "p0,external,nonConcurrent") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableIcebergTest"))) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String previousNested = sql("SHOW GLOBAL VARIABLES LIKE 'enable_nested_namespace'")[0][1]
    def createCatalog = { String name, String options ->
        sql "DROP CATALOG IF EXISTS ${name}"
        sql """CREATE CATALOG ${name} PROPERTIES (
            'type' = 'iceberg', 'iceberg.catalog.type' = 'rest', 'uri' = 'http://${host}:${restPort}',
            's3.access_key' = 'admin', 's3.secret_key' = 'password',
            's3.endpoint' = 'http://${host}:${minioPort}', 's3.region' = 'us-east-1',
            'use_path_style' = 'true', ${options}
        )"""
    }
    try {
        sql "SET GLOBAL enable_nested_namespace = true"
        createCatalog("iceberg_root_namespace_seed", "'iceberg.rest.nested-namespace-enabled' = 'true'")
        sql "CREATE DATABASE IF NOT EXISTS iceberg_root_namespace_seed.root_namespace_scope"
        sql "CREATE DATABASE IF NOT EXISTS iceberg_root_namespace_seed.`root_namespace_scope.root_namespace_db`"
        sql "CREATE DATABASE IF NOT EXISTS iceberg_root_namespace_seed.root_namespace_db"
        sql "CREATE DATABASE IF NOT EXISTS iceberg_root_namespace_seed.`root_namespace_db.root_namespace_scope`"
        sql "CREATE DATABASE IF NOT EXISTS iceberg_root_namespace_seed.`root_namespace_scope.root_namespace_db.deep`"
        sql "DROP TABLE IF EXISTS iceberg_root_namespace_seed.`root_namespace_scope.root_namespace_db`.namespace_identity"
        sql "DROP TABLE IF EXISTS iceberg_root_namespace_seed.`root_namespace_db.root_namespace_scope`.namespace_identity"
        sql "DROP TABLE IF EXISTS iceberg_root_namespace_seed.`root_namespace_scope.root_namespace_db.deep`.namespace_identity"
        sql """CREATE TABLE iceberg_root_namespace_seed.`root_namespace_scope.root_namespace_db`.namespace_identity
            (original_marker BIGINT)"""
        sql """CREATE TABLE iceberg_root_namespace_seed.`root_namespace_db.root_namespace_scope`.namespace_identity
            (other_marker BIGINT)"""
        sql """CREATE TABLE iceberg_root_namespace_seed.`root_namespace_scope.root_namespace_db.deep`.namespace_identity
            (deep_marker BIGINT)"""

        createCatalog("iceberg_root_namespace_flat", """'external_catalog.name' = 'root_namespace_scope',
            'iceberg.rest.nested-namespace-enabled' = 'false'""")
        createCatalog("iceberg_root_namespace_nested", """'external_catalog.name' = 'root_namespace_scope',
            'iceberg.rest.nested-namespace-enabled' = 'true'""")

        // Each column exists only on the intended object. LIMIT 0 checks metadata identity without scanning rows.
        sql "SELECT original_marker FROM iceberg_root_namespace_flat.root_namespace_db.namespace_identity LIMIT 0"
        test {
            sql "SELECT other_marker FROM iceberg_root_namespace_flat.root_namespace_db.namespace_identity LIMIT 0"
            exception "Unknown column"
        }
        sql """SELECT original_marker
            FROM iceberg_root_namespace_nested.`root_namespace_scope.root_namespace_db`.namespace_identity LIMIT 0"""
        test {
            sql """SELECT other_marker
                FROM iceberg_root_namespace_nested.`root_namespace_scope.root_namespace_db`.namespace_identity LIMIT 0"""
            exception "Unknown column"
        }
        sql """SELECT deep_marker
            FROM iceberg_root_namespace_nested.`root_namespace_scope.root_namespace_db.deep`.namespace_identity LIMIT 0"""

        sql "REFRESH CATALOG iceberg_root_namespace_flat"
        sql "REFRESH CATALOG iceberg_root_namespace_nested"
        sql "SELECT original_marker FROM iceberg_root_namespace_flat.root_namespace_db.namespace_identity LIMIT 0"
        sql """SELECT original_marker
            FROM iceberg_root_namespace_nested.`root_namespace_scope.root_namespace_db`.namespace_identity LIMIT 0"""

        sql "DROP TABLE IF EXISTS iceberg_root_namespace_seed.`root_namespace_scope.root_namespace_db`.created_from_root"
        sql "DROP TABLE IF EXISTS iceberg_root_namespace_seed.`root_namespace_scope.root_namespace_db`.renamed_from_root"
        sql "DROP TABLE IF EXISTS iceberg_root_namespace_seed.`root_namespace_db.root_namespace_scope`.created_from_root"
        sql "DROP TABLE IF EXISTS iceberg_root_namespace_seed.`root_namespace_db.root_namespace_scope`.renamed_from_root"
        sql """CREATE TABLE iceberg_root_namespace_flat.root_namespace_db.created_from_root
            (created_marker BIGINT)"""
        sql """SELECT created_marker
            FROM iceberg_root_namespace_seed.`root_namespace_scope.root_namespace_db`.created_from_root LIMIT 0"""
        sql """ALTER TABLE iceberg_root_namespace_flat.root_namespace_db.created_from_root
            RENAME renamed_from_root"""
        sql """SELECT created_marker
            FROM iceberg_root_namespace_seed.`root_namespace_scope.root_namespace_db`.renamed_from_root LIMIT 0"""
    } finally {
        sql "SET GLOBAL enable_nested_namespace = ${previousNested}"
    }
}
