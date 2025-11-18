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

suite("test_iceberg_varbinary", "p0,external,doris,external_docker,external_docker_doris") {

    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String catalog_name_no_mapping = "test_iceberg_no_mapping"
    String catalog_name_with_mapping = "test_iceberg_with_mapping"
    String db_name = "transform_partition_db"
    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    
    sql """drop catalog if exists ${catalog_name_no_mapping}"""
    sql """
    CREATE CATALOG ${catalog_name_no_mapping} PROPERTIES (
        'type'='iceberg',
        'iceberg.catalog.type'='rest',
        'uri' = 'http://${externalEnvIp}:${rest_port}',
        "s3.access_key" = "admin",
        "s3.secret_key" = "password",
        "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
        "s3.region" = "us-east-1",
        "s3.path.style.access" = "true",
        "s3.connection.ssl.enabled" = "false",
        "enable.mapping.varbinary"="false"
    );"""

    sql """drop catalog if exists ${catalog_name_with_mapping}"""
    sql """
    CREATE CATALOG ${catalog_name_with_mapping} PROPERTIES (
        'type'='iceberg',
        'iceberg.catalog.type'='rest',
        'uri' = 'http://${externalEnvIp}:${rest_port}',
        "s3.access_key" = "admin",
        "s3.secret_key" = "password",
        "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
        "s3.region" = "us-east-1",
        "s3.path.style.access" = "true",
        "s3.connection.ssl.enabled" = "false",
        "enable.mapping.varbinary"="true"
    );"""

    sql """switch ${catalog_name_no_mapping}"""
    sql """use ${db_name}"""

    qt_select1 """
        select * from test_ice_uuid_orc order by id;
    """

    qt_select2 """
        select * from test_ice_uuid_parquet order by id;
    """


    sql """switch ${catalog_name_with_mapping}"""
    sql """use ${db_name}"""

    qt_select3 """
        select * from test_ice_uuid_orc order by id;
    """

    qt_select4 """
        select * from test_ice_uuid_parquet order by id;
    """
}
