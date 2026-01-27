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

suite("test_iceberg_timestamp_tz", "p0,external,doris,external_docker,external_docker_doris") {

    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String catalog_name_with_mapping = "test_iceberg_timestamp_tz_with_mapping"
    String db_name = "test_timestamp_tz"
    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    
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
        "enable.mapping.varbinary"="true",
        "enable.mapping.timestamp_tz"="true"
    );"""


    sql """switch ${catalog_name_with_mapping}"""
    sql """use ${db_name}"""
    sql """set time_zone = 'Asia/Shanghai';"""
    order_qt_desc_with_mapping1 """desc test_ice_timestamp_tz_orc;"""
    order_qt_desc_with_mapping2 """desc test_ice_timestamp_tz_parquet;"""

    qt_select_UTC81_mapping """
        select * from test_ice_timestamp_tz_orc order by id;
    """

    qt_select_UTC82_mapping """
        select * from test_ice_timestamp_tz_parquet order by id;
    """

    sql """set time_zone = 'UTC';"""

    qt_select_UTC03_mapping """
        select * from test_ice_timestamp_tz_orc order by id;
    """
    qt_select_UTC04_mapping """
        select * from test_ice_timestamp_tz_parquet order by id;
    """

    // with mapping orc
    sql """set time_zone = 'Asia/Shanghai';"""
    qt_select12 """
        insert into test_ice_timestamp_tz_orc_write_with_mapping select * from test_ice_timestamp_tz_orc;
    """

    qt_select13 """
        insert into test_ice_timestamp_tz_orc_write_with_mapping values(NULL,NULL);
    """

    qt_select14 """
        insert into test_ice_timestamp_tz_orc_write_with_mapping values(7,cast("2020-01-01 10:10:10 +08:00" as timestamptz));
    """   

    qt_select15 """
        select * from test_ice_timestamp_tz_orc_write_with_mapping order by id;
    """

    sql """set time_zone = 'UTC';"""
    qt_select16 """
        select * from test_ice_timestamp_tz_orc_write_with_mapping order by id;
    """

    // with mapping parquet
    sql """set time_zone = 'Asia/Shanghai';"""
    qt_select17 """
        insert into test_ice_timestamp_tz_parquet_write_with_mapping select * from test_ice_timestamp_tz_parquet;
    """

    qt_select18 """
        insert into test_ice_timestamp_tz_parquet_write_with_mapping values(NULL,NULL);
    """

    qt_select19 """
        insert into test_ice_timestamp_tz_parquet_write_with_mapping values(7,cast("2020-01-01 10:10:10 +08:00" as timestamptz));
    """

    qt_select20 """
        select * from test_ice_timestamp_tz_parquet_write_with_mapping order by id;
    """
    sql """set time_zone = 'UTC';"""
    qt_select21 """
        select * from test_ice_timestamp_tz_parquet_write_with_mapping order by id;
    """
}
