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

suite("iceberg_drop_rest_table", "p0,external,doris,external_docker,external_docker_doris") {

    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String catalog_name = "iceberg_drop_rest_table"
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

    String db = "test_db"
    String tb = "need_drop_table"
    sql """ use ${catalog_name}.${db} """

    sql """ drop table if exists ${tb} """
    sql """ create table ${tb} (id int) """
    sql """ insert into ${tb} values (1) """

        
    qt_q0 """
        select * from s3(
            "uri" = "s3://warehouse/wh/${db}/${tb}/data/*.parquet",
            "s3.endpoint"="http://${externalEnvIp}:${minio_port}",
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.region" = "us-east-1",
            "format" = "parquet",
            "use_path_style" = "true"
        )
    """

    sql """drop user if exists user1"""
    sql """create user user1 identified by '12345' """
    sql """ grant all on internal.*.* to user1 """
    sql """ grant SELECT_PRIV on ${catalog_name}.${db}.${tb} to user1 """

    def result1 = connect('user1', '12345', context.config.jdbcUrl) {
        sql """ use ${catalog_name}.${db} """
        qt_q1 """ select * from ${tb} """
        test {
            sql """ drop table ${tb} """
            exception """Access denied"""
        }
    }

    sql """ drop table ${tb} """
    // data file should be deleted
    qt_q2 """
        select * from s3(
            "uri" = "s3://warehouse/wh/${db}/${tb}/data/*.parquet",
            "s3.endpoint"="http://${externalEnvIp}:${minio_port}",
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.region" = "us-east-1",
            "format" = "parquet",
            "use_path_style" = "true"
        )
    """
}