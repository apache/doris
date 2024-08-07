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

suite("test_catalog_upgrade_test", "p0,external,hive,external_docker,external_docker_hive,restart_fe,upgrade_case") {

    // Hive
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        sql """switch test_catalog_upgrade_hive2"""
        order_qt_hive1 """select * from multi_catalog.test_chinese_orc limit 10""";
        sql """refresh catalog test_catalog_upgrade_hive2"""
        order_qt_hive2 """select * from multi_catalog.test_chinese_orc limit 10""";
    }

    // Iceberg rest catalog
    enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
        sql """switch test_catalog_upgrade_iceberg_rest"""
        order_qt_ice_rest1 """select * from format_v2.sample_cow_parquet order by id limit 10;"""
        sql """refresh catalog test_catalog_upgrade_iceberg_rest"""
        order_qt_ice_rest2 """select * from format_v2.sample_cow_parquet order by id limit 10;"""
    }

    // Iceberg hms catalog
    enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        sql """switch test_catalog_upgrade_iceberg_hms"""
        sql """drop database if exists ice_upgrade_db""";
        sql """create database ice_upgrade_db""";
        sql """use ice_upgrade_db"""
        sql """CREATE TABLE unpartitioned_table (
                  `col1` BOOLEAN COMMENT 'col1',
                  `col2` INT COMMENT 'col2'
                )  ENGINE=iceberg
                PROPERTIES (
                  'write-format'='parquet'
                );
        """
        sql """insert into unpartitioned_table values(true, 2)"""
        order_qt_ice_hms1 """select * from unpartitioned_table"""
    }

    // Paimon filesystem catalog
    enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        sql """switch test_catalog_upgrade_paimon_fs"""
        order_qt_paimon_fs1 """select * from db1.all_table limit 10""";
        sql """refresh catalog test_catalog_upgrade_paimon_fs"""
        order_qt_paimon_fs2 """select * from db1.all_table limit 10""";
    }

    // Kerberos hive catalog
    enabled = context.config.otherConfigs.get("enableKerberosTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String catalog_name = "test_catalog_upgrade_kerberos_hive"
        // TODO
    }

    // Jdbc MySQL catalog
    enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        sql """switch test_catalog_upgrade_jdbc_mysql"""
        order_qt_mysql1 """select * from doris_test.dt""";
        sql """refresh catalog test_catalog_upgrade_jdbc_mysql"""
        order_qt_mysql2 """select * from doris_test.dt""";
    }
}

