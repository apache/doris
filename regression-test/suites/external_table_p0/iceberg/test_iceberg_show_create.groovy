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

suite("test_iceberg_show_create", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String catalog_name = "test_iceberg_show_create"
    String hivePrefix = "hive2";
    String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
    String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
    String iceberg_catalog_name = "test_iceberg_write_partitions_iceberg_${hivePrefix}"
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String default_fs = "hdfs://${externalEnvIp}:${hdfs_port}"
    String warehouse = "${default_fs}/warehouse"

    sql """drop catalog if exists ${catalog_name}"""
    sql """create catalog if not exists ${catalog_name} properties (
        'type'='iceberg',
        'iceberg.catalog.type'='hms',
        'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
        'fs.defaultFS' = '${default_fs}',
        'warehouse' = '${warehouse}',
        'use_meta_cache' = 'true'
    );"""

    sql """ switch ${catalog_name} """

    String db1 = "test_db1"
    String db2 = "test_db2"
    String tb1 = "test_tb1"

    sql """ drop table if exists ${db1}.${tb1} """
    sql """ drop database if exists ${db1} """
    sql """ drop database if exists ${db2} """

    sql """ create database ${db1} properties ('location'='${warehouse}/other_location') """
    sql """ create database ${db2} """

    String result = ""
    result = sql "show create database ${db1}"
    logger.info("${result}")
    assertTrue(result.toString().containsIgnoreCase("${warehouse}/other_location"))

    result = sql "show create database ${db2}"
    logger.info("${result}")
    assertTrue(result.toString().containsIgnoreCase("${warehouse}/${db2}"))

    sql """ create table ${db1}.${tb1} (id int) """
    result = sql "show create table ${db1}.${tb1}"
    logger.info("${result}")
    assertTrue(result.toString().containsIgnoreCase("${warehouse}/other_location/${tb1}"))

    sql """ drop table ${db1}.${tb1} """
    sql """ drop database ${db1} """
    sql """ drop database ${db2} """

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
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
    sql """ drop database if exists ${db1} """

    test {
        sql """ create database ${db1} properties ('location'='${warehouse}/other_location') """
        exception "Not supported: create database with properties for iceberg catalog type"
    }

    sql """ drop database if exists ${db1} """
    sql """drop catalog if exists ${catalog_name}"""
}
