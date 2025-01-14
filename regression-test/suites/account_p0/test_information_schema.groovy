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
suite("test_information_schema") {
    def dbPrefix = "db_test_schema_"
    def tablePrefix = "tb_test_schema_"

    // create lots of dbs and tables to make rows in `information_schema.columns` more than 1024
    for (int i = 1; i <= 5; i++) {
        def dbName = dbPrefix + i.toString()
        sql "CREATE DATABASE IF NOT EXISTS `${dbName}`"
        sql "USE `${dbName}`"
        for (int j = i; j <= 18; j++) {
            def tableName = tablePrefix + j.toString();
            sql """
                CREATE TABLE IF NOT EXISTS `${tableName}` (
                `aaa` varchar(170) NOT NULL COMMENT "",
                `bbb` varchar(100) NOT NULL COMMENT "",
                `ccc` varchar(170) NULL COMMENT "",
                `ddd` varchar(120) NULL COMMENT "",
                `eee` varchar(120) NULL COMMENT "",
                `fff` varchar(130) NULL COMMENT "",
                `ggg` varchar(170) NULL COMMENT "",
                `hhh` varchar(170) NULL COMMENT "",
                `jjj` varchar(170) NULL COMMENT "",
                `kkk` varchar(170) NULL COMMENT "",
                `lll` varchar(170) NULL COMMENT "",
                `mmm` varchar(170) NULL COMMENT "",
                `nnn` varchar(70) NULL COMMENT "",
                `ooo` varchar(140) NULL COMMENT "",
                `ppp` varchar(70) NULL COMMENT "",
                `qqq` varchar(130) NULL COMMENT "",
                `rrr` bigint(20) NULL COMMENT "",
                `sss` bigint(20) NULL COMMENT "",
                `ttt` decimal(20, 2) NULL COMMENT "",
                `uuu` decimal(20, 2) NULL COMMENT "",
                `vvv` decimal(20, 2) NULL COMMENT "",
                `www` varchar(50) NULL COMMENT "",
                `xxx` varchar(190) NULL COMMENT "",
                `yyy` varchar(190) NULL COMMENT "",
                `zzz` varchar(100) NULL COMMENT "",
                `aa` bigint(20) NULL COMMENT "",
                `bb` bigint(20) NULL COMMENT "",
                `cc` bigint(20) NULL COMMENT "",
                `dd` varchar(60) NULL COMMENT "",
                `ee` varchar(60) NULL COMMENT "",
                `ff` varchar(60) NULL COMMENT "",
                `gg` varchar(50) NULL COMMENT "",
                `hh` bigint(20) NULL COMMENT "",
                `ii` bigint(20) NULL COMMENT ""
                ) ENGINE=OLAP
                DUPLICATE KEY(`aaa`)
                COMMENT "OLAP"
                DISTRIBUTED BY HASH(`aaa`) BUCKETS 1
                PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "in_memory" = "false",
                "storage_format" = "V2"
                )
            """
        }
    }

    for (int i = 1; i <= 5; i++) {
        def dbName = dbPrefix + i.toString()
        sql "USE information_schema"
        qt_sql "SELECT COUNT(*) FROM `columns` WHERE TABLE_SCHEMA='${dbName}'"
    }

    sql "USE information_schema"
    qt_sql "SELECT COLUMN_KEY FROM `columns` WHERE TABLE_SCHEMA='db_test_schema_1' and TABLE_NAME='tb_test_schema_1' and COLUMN_NAME='aaa'"

    for (int i = 1; i <= 5; i++) {
        def dbName = dbPrefix + i.toString()
        sql "DROP DATABASE `${dbName}`"
    }

    def dbName = dbPrefix + "default"
    def tableName = tablePrefix + "default"
    sql "CREATE DATABASE IF NOT EXISTS `${dbName}`"
    sql "USE `${dbName}`"
    sql """drop table if exists `${tableName}`"""
    sql """
        CREATE TABLE `${tableName}` (
          `id` largeint NULL COMMENT '用户ID',
          `name` varchar(20) NULL DEFAULT "无" COMMENT '用户姓名',
          `age` smallint NULL DEFAULT "0" COMMENT '用户年龄',
          `address` varchar(100) NULL DEFAULT "beijing" COMMENT '用户所在地区',
          `date` datetime NULL DEFAULT "20240101" COMMENT '数据导入时间'
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`, `name`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1")
    """
    qt_default "SELECT COLUMN_NAME as field,COLUMN_TYPE as type,IS_NULLABLE as isNullable, COLUMN_DEFAULT as defaultValue FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '${tableName}' AND TABLE_SCHEMA = '${dbName}'"
    sql "DROP DATABASE `${dbName}`"
}

