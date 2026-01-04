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

suite("test_iceberg_mixed_format", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Iceberg test")
        return
    }

    String hivePrefix = "hive2";
    String catalog_name = "test_iceberg_mixed_format"
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String hmsPort = context.config.otherConfigs.get(hivePrefix + "HmsPort")
    String hdfsPort = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
    String default_fs = "hdfs://${externalEnvIp}:${hdfsPort}"
    String warehouse = "${default_fs}/warehouse"

    hive_docker """create database if not exists test_mixed_format_db"""
    hive_docker """use test_mixed_format_db"""
    hive_docker """drop table if exists mixed_format_table"""
    hive_docker """
        CREATE TABLE mixed_format_table (
            id INT,
            data STRING
        ) USING iceberg
        PARTITIONED BY (id)
        PROPERTIES (
            'write-format'='parquet'
        )
    """

    hive_docker """INSERT INTO mixed_format_table VALUES (1, 'parquet_data')"""
    hive_docker """ALTER TABLE mixed_format_table SET TBLPROPERTIES ('write.format.default'='orc')"""
    hive_docker """INSERT INTO mixed_format_table VALUES (2, 'orc_data')"""

    sql """drop catalog if exists ${catalog_name}"""
    sql """create catalog if not exists ${catalog_name} properties (
        'type'='iceberg',
        'iceberg.catalog.type'='hms',
        'hive.metastore.uris'='thrift://${externalEnvIp}:${hmsPort}',
        'fs.defaultFS' = '${default_fs}',
        'warehouse' = '${warehouse}',
        'use_meta_cache' = 'true'
    )"""

    sql """switch ${catalog_name}"""
    sql """use test_mixed_format_db"""
    qt_select_mixed """SELECT * FROM mixed_format_table ORDER BY id"""

    sql """drop catalog ${catalog_name}"""
    hive_docker """drop test_mixed_format_db"""
}
