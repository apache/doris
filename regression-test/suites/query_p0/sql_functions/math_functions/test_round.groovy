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

    suite("test_round") {
    qt_select "SELECT round(10.12345)"
    qt_select "SELECT round(10.12345, 2)"
    qt_select "SELECT round_bankers(10.12345)"
    qt_select "SELECT round_bankers(10.12345, 2)"

    // test tie case 1: float, banker's rounding
    qt_select "SELECT number*10/100 AS x, ROUND(number * 10 / 100) from NUMBERS(\"number\"=\"50\") where number % 5 = 0 ORDER BY x;"
    // test tie case 2: decimal, rounded up when tie
    qt_select "SELECT number*10/100 AS x, ROUND(CAST(number * 10 AS DECIMALV3(10,2)) / 100) from NUMBERS(\"number\"=\"50\") where number % 5 = 0 ORDER BY x;"

    def tableTest = "test_query_db.test"
    qt_truncate "select truncate(k1, 1), truncate(k2, 1), truncate(k3, 1), truncate(k5, 1), truncate(k8, 1), truncate(k9, 1) from ${tableTest} order by 1;"

    def tableName = "test_round"
    sql """DROP TABLE IF EXISTS `${tableName}`"""
    sql """ CREATE TABLE `${tableName}` (
        `col1` DECIMALV3(6,3) COMMENT "",
        `col2` DECIMALV3(16,5) COMMENT "",
        `col3` DECIMALV3(32,5) COMMENT "")
        DUPLICATE KEY(`col1`) DISTRIBUTED BY HASH(`col1`)
        PROPERTIES ( "replication_num" = "1" ); """

    sql """ insert into `${tableName}` values(16.025, 16.025, 16.025); """
    qt_select """ SELECT round(col1), round(col2), round(col3) FROM `${tableName}`; """
    qt_select """ SELECT floor(col1), floor(col2), floor(col3) FROM `${tableName}`; """
    qt_select """ SELECT ceil(col1), ceil(col2), ceil(col3) FROM `${tableName}`; """
    qt_select """ SELECT round_bankers(col1), round_bankers(col2), round_bankers(col3) FROM `${tableName}`; """

    qt_select """ SELECT round(col1, 2), round(col2, 2), round(col3, 2) FROM `${tableName}`; """
    qt_select """ SELECT floor(col1, 2), floor(col2, 2), floor(col3, 2) FROM `${tableName}`; """
    qt_select """ SELECT ceil(col1, 2), ceil(col2, 2), ceil(col3, 2) FROM `${tableName}`; """
    qt_select """ SELECT truncate(col1, 2), truncate(col2, 2), truncate(col3, 2) FROM `${tableName}`; """
    qt_select """ SELECT round_bankers(col1, 2), round_bankers(col2, 2), round_bankers(col3, 2) FROM `${tableName}`; """

    qt_select """ SELECT round(col1, -1), round(col2, -1), round(col3, -1) FROM `${tableName}`; """
    qt_select """ SELECT floor(col1, -1), floor(col2, -1), floor(col3, -1) FROM `${tableName}`; """
    qt_select """ SELECT ceil(col1, -1), ceil(col2, -1), ceil(col3, -1) FROM `${tableName}`; """
    qt_select """ SELECT truncate(col1, -1), truncate(col2, -1), truncate(col3, -1) FROM `${tableName}`; """
    qt_select """ SELECT round_bankers(col1, -1), round_bankers(col2, -1), round_bankers(col3, -1) FROM `${tableName}`; """

    qt_select """ SELECT round(col1, 7), round(col2, 7), round(col3, 7) FROM `${tableName}`; """
    qt_select """ SELECT floor(col1, 7), floor(col2, 7), floor(col3, 7) FROM `${tableName}`; """
    qt_select """ SELECT ceil(col1, 7), ceil(col2, 7), ceil(col3, 7) FROM `${tableName}`; """
    qt_select """ SELECT truncate(col1, 7), truncate(col2, 7), truncate(col3, 7) FROM `${tableName}`; """
    qt_select """ SELECT round_bankers(col1, 7), round_bankers(col2, 7), round_bankers(col3, 7) FROM `${tableName}`; """

    sql """ DROP TABLE IF EXISTS `${tableName}` """

    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    qt_nereids_round_arg1 "SELECT round(10.12345)"
    qt_nereids_round_arg2 "SELECT round(10.12345, 2)"
    qt_nereids_round_bankers_arg1 "SELECT round_bankers(10.12345)"
    qt_nereids_round_bankers_arg2 "SELECT round_bankers(10.12345, 2)"

    def tableName1 = "test_round1"
    sql """ DROP TABLE IF EXISTS `${tableName1}` """
    sql """ CREATE TABLE `${tableName1}` (
          `TENANT_ID` varchar(50) NOT NULL,
          `PUBONLN_PRC` decimalv3(18, 4) NULL,
          `PRODENTP_CODE` varchar(50) NULL,
          `ORD_SUMAMT` decimalv3(18, 4) NULL,
          `PURC_CNT` decimalv3(12, 2) NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`TENANT_ID`)
        DISTRIBUTED BY HASH(`TENANT_ID`) BUCKETS 16
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "enable_unique_key_merge_on_write" = "true",
        "disable_auto_compaction" = "false"
        ); """

    def tableName2 = "test_round2"
    sql """ DROP TABLE IF EXISTS `${tableName2}` """
    sql """ CREATE TABLE `${tableName2}` (
          `tenant_id` varchar(50) NOT NULL COMMENT '租户ID',
          `prodentp_code` varchar(50) NULL COMMENT '生产企业代码',
          `delv_amt` decimalv3(18, 4) NULL DEFAULT "0" COMMENT '配送金额',
          `ord_sumamt` decimalv3(18, 4) NULL COMMENT '订单总金额'
        ) ENGINE=OLAP
        UNIQUE KEY(`tenant_id`, `prodentp_code`)
        COMMENT '订单明细配送统计'
        DISTRIBUTED BY HASH(`prodentp_code`) BUCKETS 16
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "enable_unique_key_merge_on_write" = "true",
        "disable_auto_compaction" = "false"
        );                """

    sql """ insert into ${tableName1} values ('111', 1.2432, '001', 0.2341, 12.1234123); """
    sql """ insert into ${tableName2} select  TENANT_ID,PRODENTP_CODE,ROUND((MAX(PURC_CNT)*MAX(PUBONLN_PRC)),2) delv_amt,ROUND(SUM(ORD_SUMAMT),2) from ${tableName1} GROUP BY TENANT_ID,PRODENTP_CODE; """
    qt_query """ select * from ${tableName2} """


    def tableName3 = "test_round_decimal"
    sql """ DROP TABLE IF EXISTS `${tableName3}` """
    sql """ CREATE TABLE `${tableName3}` (
          `id` int NOT NULL COMMENT 'id',
          `d1` decimalv3(9, 4) NULL COMMENT '',
          `d2` decimalv3(27, 4)  NULL DEFAULT "0" ,
          `d3` decimalv3(38, 4)  NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );                """

    sql """ insert into ${tableName3} values (1, 123.56789, 234.67895, 345.78956); """
    sql """ insert into ${tableName3} values (2, 123.56789, 234.67895, 345.78956); """

    qt_query """ select d1, d2, d3 from ${tableName3} order by d1 """

    qt_query """ select cast(round(sum(d1), 6) as double), cast(round(sum(d2), 6) as double), cast(round(sum(d3), 6) as double) from ${tableName3} """
    qt_query """ select cast(round(sum(d1), 2) as double), cast(round(sum(d2), 2) as double), cast(round(sum(d3),2) as double) from ${tableName3} """
    qt_query """ select cast(round(sum(d1), -2) as double), cast(round(sum(d2), -2) as double), cast(round(sum(d3), -2) as double) from ${tableName3} """
    qt_query """ select cast(round(sum(d1), -4) as double), cast(round(sum(d2), -4) as double), cast(round(sum(d3), -4) as double) from ${tableName3} """

    qt_query """ select cast(round(sum(d1), 6) as decimalv3(27, 3)), cast(round(sum(d2), 6) as decimalv3(27, 3)), cast(round(sum(d3), 6) as decimalv3(27, 3)) from ${tableName3} """
    qt_query """ select cast(round(sum(d1), 2) as decimalv3(27, 3)), cast(round(sum(d2), 2) as decimalv3(27, 3)), cast(round(sum(d3),2) as decimalv3(27, 3)) from ${tableName3} """
    qt_query """ select cast(round(sum(d1), -2) as decimalv3(27, 3)), cast(round(sum(d2), -2) as decimalv3(27, 3)), cast(round(sum(d3), -2) as decimalv3(27, 3)) from ${tableName3} """
    qt_query """ select cast(round(sum(d1), -4) as decimalv3(27, 3)), cast(round(sum(d2), -4) as decimalv3(27, 3)), cast(round(sum(d3), -4) as decimalv3(27, 3)) from ${tableName3} """
}
