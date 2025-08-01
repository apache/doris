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

suite("iceberg_schema_change2", "p0,external,doris,external_docker,external_docker_doris") {

    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String catalog_name = "iceberg_schema_change2"
    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

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

    logger.info("catalog " + catalog_name + " created")
    sql """switch ${catalog_name};"""
    logger.info("switched to catalog " + catalog_name)
    sql """ use test_db;""" 

    qt_parquet_1 """ select * from sc_drop_add_parquet order by id; """
    qt_parquet_2 """ select * from sc_drop_add_parquet where age is NULL order by id; """
    qt_parquet_3 """ select * from sc_drop_add_parquet where age is not NULL order by id; """
    qt_parquet_4 """ select * from sc_drop_add_parquet where age > 28 order by id; """
    qt_parquet_5 """ select * from sc_drop_add_parquet where age >= 28 order by id; """
    qt_parquet_6 """ select id, name from sc_drop_add_parquet where age >= 28 order by id; """
    qt_parquet_7 """ select id, age from sc_drop_add_parquet where name="Eve" order by id; """



    qt_orc_1 """ select * from sc_drop_add_orc order by id; """
    qt_orc_2 """ select * from sc_drop_add_orc where age is NULL order by id; """
    qt_orc_3 """ select * from sc_drop_add_orc where age is not NULL order by id; """
    qt_orc_4 """ select * from sc_drop_add_orc where age > 28 order by id; """
    qt_orc_5 """ select * from sc_drop_add_orc where age >= 28 order by id; """
    qt_orc_6 """ select id, name from sc_drop_add_orc where age >= 28 order by id; """
    qt_orc_7 """ select id, age from sc_drop_add_orc where name="Eve" order by id; """

}
