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

suite("test_iceberg_support_char_varchar", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        setHivePrefix(hivePrefix)
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String iceberg_catalog_name = "test_iceberg_support_char_varchar_iceberg_${hivePrefix}"
        String hive_catalog_name = "test_iceberg_support_char_varchar_hive_${hivePrefix}"

        String db = "write_test"
        String tb_iceberg = "tb_iceberg_support_char_varchar_iceberg"
        String tb_hive = "tb_iceberg_support_char_varchar_hive"
        String tb = "tb_iceberg_support_char_varchar_doris"

        try {

            sql """drop catalog if exists ${iceberg_catalog_name}"""
            sql """create catalog if not exists ${iceberg_catalog_name} properties (
                'type'='iceberg',
                'iceberg.catalog.type'='hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
                'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}',
                'use_meta_cache' = 'true'
            );"""
            sql """drop catalog if exists ${hive_catalog_name}"""
            sql """create catalog if not exists ${hive_catalog_name} properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
                'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}',
                'use_meta_cache' = 'true'
            );"""

            sql """set enable_fallback_to_original_planner=false;"""

            sql """ create database if not exists internal.${db}  """
            sql """ drop table if exists internal.${db}.${tb} """
            sql """ drop table if exists ${hive_catalog_name}.${db}.${tb_hive} """
            sql """ drop table if exists ${iceberg_catalog_name}.${db}.${tb_iceberg} """
            sql """ create table internal.${db}.${tb} (v1 varchar(20), v2 char(10), v3 int) DISTRIBUTED BY HASH(`v1`) BUCKETS 1 PROPERTIES ("replication_allocation" = "tag.location.default: 1") """
            sql """ create table ${hive_catalog_name}.${db}.${tb_hive} (v1 varchar(20), v2 char(10), v3 int); """
            sql """ create table ${iceberg_catalog_name}.${db}.${tb_iceberg} (v1 varchar(20), v2 char(10), v3 int); """

            sql """ insert into internal.${db}.${tb} values ('a', 'b', 1)"""
            sql """ insert into ${hive_catalog_name}.${db}.${tb_hive} values ('a', 'b', 1) """
            sql """ insert into ${iceberg_catalog_name}.${db}.${tb_iceberg} values ('a', 'b', 1) """

            qt_qt01 """ select * from ${iceberg_catalog_name}.${db}.${tb_iceberg} """

            // ctas from doris
            sql """ drop table ${iceberg_catalog_name}.${db}.${tb_iceberg} """
            sql """ create table ${iceberg_catalog_name}.${db}.${tb_iceberg} as select * from internal.${db}.${tb} """
            qt_qt02 """ select * from ${iceberg_catalog_name}.${db}.${tb_iceberg} """

            // ctas from hive
            sql """ drop table ${iceberg_catalog_name}.${db}.${tb_iceberg} """
            sql """ create table ${iceberg_catalog_name}.${db}.${tb_iceberg} as select * from ${hive_catalog_name}.${db}.${tb_hive} """
            qt_qt03 """ select * from ${iceberg_catalog_name}.${db}.${tb_iceberg} """

        } finally {
            sql """drop table if exists internal.${db}.${tb}"""
            sql """drop table if exists ${hive_catalog_name}.${db}.${tb_hive}"""
            sql """drop table if exists ${iceberg_catalog_name}.${db}.${tb_iceberg}"""
            sql """drop catalog if exists ${iceberg_catalog_name}"""
            sql """drop catalog if exists ${hive_catalog_name}"""
        }
    }
}
