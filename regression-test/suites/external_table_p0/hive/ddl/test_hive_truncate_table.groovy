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

suite("test_hive_truncate_table", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String hms_port = context.config.otherConfigs.get("hive3HmsPort")
        String hdfs_port = context.config.otherConfigs.get("hive3HdfsPort")
        String catalog_name = "test_hive3_truncate_table"
        sql """drop catalog if exists ${catalog_name};"""

        sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
                'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}',
                'use_meta_cache' = 'true',
                'hive.version'='3.0'
            );
        """

        logger.info("catalog " + catalog_name + " created")
        sql """switch ${catalog_name};"""
        logger.info("switched to catalog " + catalog_name)
        sql """create database if not exists `hive_truncate`;"""
        sql """use `hive_truncate`;"""

        // 1. test no partition table
        sql """create table if not exists `table_no_pars`(col1 bigint, col2 string) engine=hive; """
        sql """truncate table table_no_pars;"""

        sql """insert into `table_no_pars` values(3234424, '44'); """
        sql """insert into `table_no_pars` values(222, 'aoe'); """
        order_qt_truncate_01_pre """ select * from table_no_pars;  """
        sql """truncate table table_no_pars;"""
        order_qt_truncate_01 """ select * from table_no_pars; """

        sql """insert into `table_no_pars` values(3234424, '44'); """
        sql """truncate table hive_truncate.table_no_pars;"""
        order_qt_truncate_02 """ select * from table_no_pars;  """

        sql """insert into `table_no_pars` values(222, 'aoe'); """
        sql """truncate table ${catalog_name}.hive_truncate.table_no_pars;"""
        order_qt_truncate_03 """ select * from table_no_pars; """
        sql """insert into `table_no_pars` values(222, 'aoe'); """
        sql """drop table table_no_pars """

        // 2. test partition table
        sql """create table if not exists `table_with_pars`(col1 bigint, col2 string, pt1 varchar, pt2 date) engine=hive
               partition by list(pt1, pt2)() """
        sql """truncate table table_with_pars;"""

        sql """insert into `table_with_pars` values(33, 'awe', 'wuu', '2023-02-04') """
        sql """insert into `table_with_pars` values(5535, '3', 'dre', '2023-04-04') """
        order_qt_truncate_04_pre """  select * from table_with_pars; """
        sql """truncate table table_with_pars;"""
        order_qt_truncate_04 """ select * from table_with_pars; """

        // 3. test partition table and truncate partitions
        sql """insert into `table_with_pars` values(44, 'etg', 'wuweu', '2022-02-04') """
        sql """insert into `table_with_pars` values(88, 'etg', 'wuweu', '2022-01-04') """
        sql """insert into `table_with_pars` values(095, 'etgf', 'hiyr', '2021-05-06') """
        sql """insert into `table_with_pars` values(555, 'etgf', 'wet', '2021-05-06') """
        // sql """truncate table hive_truncate.table_with_pars partition pt1;"""
        // order_qt_truncate_05 """ select * from table_with_pars; """
        // sql """truncate table hive_truncate.table_with_pars partition pt2;"""
        order_qt_truncate_06 """ select * from table_with_pars; """

        sql """insert into `table_with_pars` values(22, 'ttt', 'gggw', '2022-02-04')"""
        sql """insert into `table_with_pars` values(44, 'etg', 'wuweu', '2022-02-04') """
        sql """insert into `table_with_pars` values(88, 'etg', 'wuweu', '2022-01-04') """
        sql """insert into `table_with_pars` values(095, 'etgf', 'hiyr', '2021-05-06') """
        sql """insert into `table_with_pars` values(555, 'etgf', 'wet', '2021-05-06') """
        // sql """truncate table ${catalog_name}.hive_truncate.table_with_pars partition pt1;"""
        // order_qt_truncate_07 """ select * from table_with_pars; """
        // sql """truncate table ${catalog_name}.hive_truncate.table_with_pars partition pt2;"""
        // order_qt_truncate_08 """ select * from table_with_pars; """
        sql """truncate table table_with_pars"""
        order_qt_truncate_09 """ select * from table_with_pars; """

        sql """drop table table_with_pars """
        sql """drop database hive_truncate;"""
        sql """drop catalog ${catalog_name};"""
    }
}
