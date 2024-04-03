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

suite("test_nested_lateral_view") {
    sql """set enable_nereids_planner=true;"""
    sql "SET enable_fallback_to_original_planner=false"
    sql """drop table if exists ods_42378777c342d2da36d05db875393811;"""
    sql """CREATE TABLE `ods_42378777c342d2da36d05db875393811` (
                `57CE5E02901753F7_06a28` varchar(*) NULL,
                `57CE5E02520653F7_06a28` varchar(*) NULL,
                `65F695F4519253F7_06a28` varchar(*) NULL,
                `65E5671F659C6760_06a28` varchar(*) NULL,
                `57CE5E027A7A683C_06a28` varchar(*) NULL,
                `57CE5E02767E520653F7_06a28a4` varchar(*) NULL,
                `65705B575C0F657070B9_06a28a4` varchar(*) NULL,
                `qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq_` DECIMAL(38, 10) NULL,
                `5F88591A7B2653F7_06a28` varchar(*) NULL
                ) ENGINE=OLAP
                DUPLICATE KEY(`57CE5E02901753F7_06a28`)
                COMMENT 'OLAP'
                DISTRIBUTED BY RANDOM BUCKETS AUTO
                PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "is_being_synced" = "false",
                "storage_format" = "V2",
                "light_schema_change" = "true",
                "disable_auto_compaction" = "false",
                "enable_single_replica_compaction" = "false"
                );"""
    sql """INSERT INTO ods_42378777c342d2da36d05db875393811 (`57CE5E02901753F7_06a28`,`57CE5E02520653F7_06a28`,`65F695F4519253F7_06a28`,`65E5671F659C6760_06a28`,`57CE5E027A7A683C_06a28`,`57CE5E02767E520653F7_06a28a4`,`65705B575C0F657070B9_06a28a4`,qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq_,`5F88591A7B2653F7_06a28`) VALUES
         ('a,a,a,a','a;a;a;a','1899-12-31 15:11:22','2024-01-12 00:00:00','a a a a','a%a%a%a','33..33.*.@3',3.3300000000,''),
         ('a,a,a,a','a;a;a;a','1899-12-31 00:11:22','2024-01-10 00:00:00','a a a a','a%a%a%a','99..99.*.@9',1.1100000000,'~！@#￥%……&*（）——+=【】；‘，。、、'),
         ('a,a,a,a','a;a;a;a','1899-12-31 12:11:22','2024-01-11 00:00:00','a a a a','a%a%a%a','22..22.*.@2',2.2200000000,'131.0');
    """
    qt_select_default """select * from (SELECT   `t1`.`k0`,  `f2`.`lt2`
                        FROM
                        (
                        SELECT    `t0`.`57CE5E02901753F7_06a28` as `k0`  FROM    `ods_42378777c342d2da36d05db875393811` `t0`
                        LATERAL VIEW explode(ARRAY(1,2)) `f1` AS `lt1`)`t1`
                        LATERAL VIEW explode(ARRAY(1,2)) `f2` AS `lt2` ) ttt order by 1,2; """
}
