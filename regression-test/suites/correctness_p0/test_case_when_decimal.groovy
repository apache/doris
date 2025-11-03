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

suite("test_case_when_decimal") {
    sql """ DROP TABLE IF EXISTS `decimal_to_double_table1`; """
    sql """ DROP TABLE IF EXISTS `decimal_to_double_table2`; """
    sql """
        CREATE TABLE `decimal_to_double_table1` (
        `cust_no` varchar(40) NULL,
        `bar_averageday_asset` decimal(18, 4) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`cust_no`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`cust_no`) BUCKETS AUTO
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false"
        );
    """

    sql """
        CREATE TABLE `decimal_to_double_table2` (
        `cust_no` varchar(54) NULL DEFAULT ""
        ) ENGINE=OLAP
        DUPLICATE KEY(`cust_no`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`cust_no`) BUCKETS AUTO
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );
    """

    sql "INSERT INTO `decimal_to_double_table1` VALUES ('1', 2.1);"
    sql "INSERT INTO `decimal_to_double_table2` VALUES ('1');"
    qt_sql1 """
            SELECT sum(v24)
                FROM ( select
                    CASE
                    WHEN 1=0 THEN
                    0.00
                    ELSE round((CASE
                    WHEN T001.cust_no = '86193' THEN
                    0
                    ELSE COALESCE(T101.bar_averageday_asset,0.0) END) * T101.bar_averageday_asset,2)
                    END v24
                FROM decimal_to_double_table2 T001, decimal_to_double_table1 T101 ) t;
    """
    sql "set enable_nereids_planner=true;"
    qt_sql2 """
            SELECT sum(v24)
                FROM ( select
                    CASE
                    WHEN 1=0 THEN
                    0.00
                    ELSE round((CASE
                    WHEN T001.cust_no = '86193' THEN
                    0
                    ELSE COALESCE(T101.bar_averageday_asset,0.0) END) * T101.bar_averageday_asset,2)
                    END v24
                FROM decimal_to_double_table2 T001, decimal_to_double_table1 T101 ) t;
    """

    sql "set enable_dphyp_optimizer=true;"
    qt_sql3 """
            SELECT sum(v24)
                FROM ( select
                    CASE
                    WHEN 1=0 THEN
                    0.00
                    ELSE round((CASE
                    WHEN T001.cust_no = '86193' THEN
                    0
                    ELSE COALESCE(T101.bar_averageday_asset,0.0) END) * T101.bar_averageday_asset,2)
                    END v24
                FROM decimal_to_double_table2 T001, decimal_to_double_table1 T101 ) t;
    """

    sql """ DROP TABLE IF EXISTS `decimal_to_double_table1`; """
    sql """ DROP TABLE IF EXISTS `decimal_to_double_table2`; """

    sql """ set debug_skip_fold_constant = true;  """

    qt_sql4 """
        select ifnull(cast(123 as time) , cast(300 as time)) , coalesce(cast(123 as time) , cast(300 as time)) , if(true ,cast(123 as time) , cast(300 as time)) , nullif(cast(123 as time) , cast(300 as time));
    """

    sql """ set debug_skip_fold_constant = false;  """

    qt_sql5 """
        select ifnull(cast(123 as time) , cast(300 as time)) , coalesce(cast(123 as time) , cast(300 as time)) , if(true ,cast(123 as time) , cast(300 as time)) , nullif(cast(123 as time) , cast(300 as time));
    """
}
