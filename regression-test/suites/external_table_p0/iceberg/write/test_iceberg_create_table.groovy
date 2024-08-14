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

suite("test_iceberg_create_table", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "test_iceberg_create_table"

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

    sql """ switch ${catalog_name} """

    String db1 = catalog_name + "_db1"
    String tb1 = db1 + "_tb1"
    String tb2 = db1 + "_tb2"

    sql """ drop table if exists ${db1}.${tb1} """
    sql """ drop table if exists ${db1}.${tb2} """
    sql """ drop database if exists ${db1} """

    sql """ create database ${db1} """

    test {
        sql """ create table ${db1}.${tb1} (id int) engine = olap """
        exception "Cannot create olap table out of internal catalog. Make sure 'engine' type is specified when use the catalog: ${catalog_name}"
    }

    test {
        sql """ create table ${db1}.${tb1} (id int) engine = hive """
        exception "java.sql.SQLException: errCode = 2, detailMessage = Iceberg type catalog can only use `iceberg` engine."
    }

    test {
        sql """ create table ${db1}.${tb1} (id int) engine = jdbc """
        exception "java.sql.SQLException: errCode = 2, detailMessage = Iceberg type catalog can only use `iceberg` engine."
    }

    sql """ create table ${db1}.${tb1} (id int) engine = iceberg """
    sql """ create table ${db1}.${tb2} (id int) """

    sql """ drop table ${db1}.${tb1} """
    sql """ drop table ${db1}.${tb2} """
    sql """ drop database ${db1} """

}
