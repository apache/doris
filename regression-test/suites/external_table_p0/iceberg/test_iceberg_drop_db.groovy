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

suite("test_iceberg_drop_db", "p0,external,doris,external_docker,external_docker_doris") {

    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String catalog_name = "test_iceberg_drop_db_ctl"
    String db_name = "test_iceberg_drop_db"
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

    sql """switch ${catalog_name}"""

    sql """drop database if exists ${db_name} force"""
    sql """create database ${db_name}"""
    sql """use ${db_name}"""

    sql """ create table test_iceberg_drop_db_tbl1 (id int) """
    sql """insert into test_iceberg_drop_db_tbl1 values(1);"""
    qt_sql_tbl1 """select * from test_iceberg_drop_db_tbl1"""

    sql """ create table test_iceberg_drop_db_tbl2 (id int) """
    sql """insert into test_iceberg_drop_db_tbl2 values(2);"""
    qt_sql_tbl2 """select * from test_iceberg_drop_db_tbl2"""

    sql """ create table test_iceberg_drop_db_tbl3 (id int) """
    sql """insert into test_iceberg_drop_db_tbl3 values(3);"""
    qt_sql_tbl3 """select * from test_iceberg_drop_db_tbl3"""

    // drop db with tables
    test {
        sql """drop database ${db_name}"""
        exception """is not empty"""
    }

    // drop db froce with tables
    sql """drop database ${db_name} force"""

    // refresh catalog
    sql """refresh catalog ${catalog_name}"""
    // should be empty
    test {
        sql """show tables from ${db_name}"""
        exception "Unknown database"
    }

    // table should be deleted
    qt_test1 """
        select * from s3(
            "uri" = "s3://warehouse/wh/${db_name}/test_iceberg_drop_db_tbl1/data/*.parquet",
            "s3.endpoint"="http://${externalEnvIp}:${minio_port}",
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.region" = "us-east-1",
            "format" = "parquet",
            "use_path_style" = "true"
        )
    """
    qt_test2 """
        select * from s3(
            "uri" = "s3://warehouse/wh/${db_name}/test_iceberg_drop_db_tbl1/data/*.parquet",
            "s3.endpoint"="http://${externalEnvIp}:${minio_port}",
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.region" = "us-east-1",
            "format" = "parquet",
            "use_path_style" = "true"
        )
    """
    qt_test3 """
        select * from s3(
            "uri" = "s3://warehouse/wh/${db_name}/test_iceberg_drop_db_tbl1/data/*.parquet",
            "s3.endpoint"="http://${externalEnvIp}:${minio_port}",
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.region" = "us-east-1",
            "format" = "parquet",
            "use_path_style" = "true"
        )
    """
}
