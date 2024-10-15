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
    sql "set enable_fold_constant_by_be=false;"
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    qt_select "SELECT round(123.123, 1.123);"
    qt_select """SELECT round(123.123, 1.123) FROM numbers("number"="10");"""
    qt_select """SELECT round(123.123, -1.123) FROM numbers("number"="10");"""
    qt_select """SELECT truncate(123.123, 1.123) FROM numbers("number"="10");"""
    qt_select """SELECT truncate(123.123, -1.123) FROM numbers("number"="10");"""
    qt_select """SELECT ceil(123.123, 1.123) FROM numbers("number"="10");"""
    qt_select """SELECT ceil(123.123, -1.123) FROM numbers("number"="10");"""
    qt_select """SELECT round_bankers(123.123, 1.123) FROM numbers("number"="10");"""
    qt_select """SELECT round_bankers(123.123, -1.123) FROM numbers("number"="10");"""
    sql """drop table if exists test_round_1; """
    sql """
        create table test_round_1(big_key bigint not NULL)
                DISTRIBUTED BY HASH(big_key) BUCKETS 1 PROPERTIES ("replication_num" = "1");
    """
    qt_select """SELECT truncate(cast(round(8990.65 - 4556.2354, 2.4652) as Decimal(9,4)), 2);"""
    qt_select """SELECT cast(round(round(465.56,min(-5.987)),2) as DECIMAL)"""
    qt_select """
        SELECT truncate(100,2)<-2308.57 , cast(round(round(465.56,min(-5.987)),2) as DECIMAL) , cast(truncate(round(8990.65-4556.2354,2.4652),2)as DECIMAL) from test_round_1;
    """

    qt_select """
        SELECT truncate(123456789.123456789, -9);
    """

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

    qt_select_fix """ SELECT round(col1, 6.234), round(col2, 6.234), round(col3, 6.234) FROM `${tableName}`; """
    qt_select_fix """ SELECT floor(col1, 6.234), floor(col2, 6.234), floor(col3, 6.234) FROM `${tableName}`; """
    qt_select_fix """ SELECT truncate(col1, 6.234), truncate(col2, 6.234), truncate(col3, 6.234) FROM `${tableName}`; """
    qt_select_fix """ SELECT round_bankers(col1, 6.234), round_bankers(col2, 6.234), round_bankers(col3, 6.234) FROM `${tableName}`; """

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

    /// Testing with enhanced round function, which can deal with scale being a column, like this:
    /// func(Column, Column), func(ColumnConst, Column).
    /// Consider truncate() has been tested in test_function_truncate.groovy, so we focus on the rest here.
    sql """DROP TABLE IF EXISTS test_enhanced_round;"""
    sql """
        CREATE TABLE test_enhanced_round (
            rid int, flo float, dou double,
            dec90 decimal(9, 0), dec91 decimal(9, 1), dec99 decimal(9, 9),
            dec100 decimal(10,0), dec109 decimal(10,9), dec1010 decimal(10,10),
            number int DEFAULT 1)
        DISTRIBUTED BY HASH(rid)
        PROPERTIES("replication_num" = "1" );
    """
    sql """
        INSERT INTO test_enhanced_round
        VALUES
        (1, 12345.123, 123456789.123456789,
            123456789, 12345678.1, 0.123456789,
            123456789.1, 1.123456789, 0.123456789, 1);
    """
    sql """
        INSERT INTO test_enhanced_round
        VALUES
        (2, 12345.123, 123456789.123456789,
            123456789, 12345678.1, 0.123456789,
            123456789.1, 1.123456789, 0.123456789, 1);
    """
    qt_floor_dec9 """
        SELECT number, dec90, floor(dec90, number), dec91, floor(dec91, number), dec99, floor(dec99, number) FROM test_enhanced_round order by rid;
    """
    qt_floor_dec10 """
        SELECT number, dec100, floor(dec100, number), dec109, floor(dec109, number), dec1010, floor(dec1010, number) FROM test_enhanced_round order by rid;
    """
    qt_floor_flo """
        SELECT number, flo, floor(flo, number + 1), dou, floor(dou, number + 2) FROM test_enhanced_round order by rid;
    """
    qt_ceil_dec9 """
        SELECT number, dec90, ceil(dec90, number), dec91, ceil(dec91, number), dec99, ceil(dec99, number) FROM test_enhanced_round order by rid;
    """
    qt_ceil_dec10 """
        SELECT number, dec100, ceil(dec100, number), dec109, ceil(dec109, number), dec1010, ceil(dec1010, number) FROM test_enhanced_round order by rid;
    """
    qt_ceil_flo """
        SELECT number, flo, ceil(flo, number - 1), dou, ceil(dou, number - 2) FROM test_enhanced_round order by rid;
    """
    qt_round_dec9 """
        SELECT number, dec90, round(dec90, number), dec91, round(dec91, number), dec99, round(dec99, number) FROM test_enhanced_round order by rid;
    """
    qt_round_dec10 """
        SELECT number, dec100, round(dec100, number), dec109, round(dec109, number), dec1010, round(dec1010, number) FROM test_enhanced_round order by rid;
    """
    qt_round_flo """
        SELECT number, flo, round(flo, number - 2), dou, round(dou, number - 3) FROM test_enhanced_round order by rid;
    """
    qt_round_bankers_dec9 """
        SELECT number, dec90, round_bankers(dec90, number), dec91, round_bankers(dec91, number), dec99, round_bankers(dec99, number) FROM test_enhanced_round order by rid;
    """
    qt_round_bankers_dec10 """
        SELECT number, dec100, round_bankers(dec100, number), dec109, round_bankers(dec109, number), dec1010, round_bankers(dec1010, number) FROM test_enhanced_round order by rid;
    """
    qt_round_bankers_flo """
        SELECT number, flo, round_bankers(flo, number - 2), dou, round_bankers(dou, number - 3) FROM test_enhanced_round order by rid;
    """

    qt_all_funcs_compare_dec """
        SELECT number + 4 as new_number, dec109, truncate(dec109, number + 4) as t_res, floor(dec109, number + 4) as f_res, ceil(dec109, number + 4) as c_res, round(dec109, number + 4) as r_res, 
                round_bankers(dec109, number + 4) as rb_res FROM test_enhanced_round where rid = 1;
    """
    qt_bankers_compare """
        SELECT number * 2.5 as input1, number - 1 as input2, round(number * 2.5, number - 1) as r_res, round_bankers(number * 2.5, number - 1) as rb_res FROM test_enhanced_round where rid = 1;
    """
    qt_nested_func """
        SELECT number, floor(floor(number * floor(number) + 1), ceil(floor(number))) as nested_col FROM test_enhanced_round where rid = 1;
    """
    qt_pos_zero_neg_compare """
        SELECT number, dou, ceil(dou, (-2) * number), ceil(dou, 0 * number), ceil(dou, 2 * number) FROM test_enhanced_round;
    """
    qt_cast_dec """
        SELECT round(cast(0 as Decimal(9,8)), 10), round(cast(0 as Decimal(9,8)), 2);
    """
    //For func(x, d), if d is a column and x has Decimal type, scale of result Decimal will always be same with input Decimal.
    qt_col_const_compare """
        SELECT number, dec109, floor(dec109, number) as f_col_col, floor(dec109, 1) as f_col_const, 
        floor(1.123456789, number) as f_const_col, floor(1.123456789, 1) as f_const_const FROM test_enhanced_round limit 1;
    """

    sql """DROP TABLE IF EXISTS test_enhanced_round_dec128;"""
        sql """
        CREATE TABLE test_enhanced_round_dec128 (
            rid int, dec190 decimal(19,0), dec199 decimal(19,9), dec1919 decimal(19,19),
                     dec380 decimal(38,0), dec3819 decimal(38,19), dec3838 decimal(38,38),
                     number int DEFAULT 1
        )
        DISTRIBUTED BY HASH(rid)
        PROPERTIES("replication_num" = "1" );
    """
    sql """
        INSERT INTO test_enhanced_round_dec128
            VALUES
        (1, 1234567891234567891.0, 1234567891.123456789, 0.1234567891234567891,
            12345678912345678912345678912345678912.0, 
            1234567891234567891.1234567891234567891,
            0.12345678912345678912345678912345678912345678912345678912345678912345678912, 1);
    """
    qt_floor_dec128 """
        SELECT number, dec190, floor(dec190, 0), floor(dec190, number), dec199, floor(dec199, 0), floor(dec199, number), 
            dec1919, floor(dec1919, 0), floor(dec1919, number) FROM test_enhanced_round_dec128 order by rid;
    """
    qt_ceil_dec128 """
        SELECT number, dec190, ceil(dec190, 0), ceil(dec190, number), dec199, ceil(dec199, 0), ceil(dec199, number), 
            dec1919, ceil(dec1919, 0), ceil(dec1919, number) FROM test_enhanced_round_dec128 order by rid;
    """
    qt_round_dec128 """
        SELECT number, dec190, round(dec190, 0), round(dec190, number), dec199, round(dec199, 0), round(dec199, number), 
            dec1919, round(dec1919, 0), round(dec1919, number) FROM test_enhanced_round_dec128 order by rid;
    """
    qt_round_bankers_dec128 """
        SELECT number, dec190, round_bankers(dec190, 0), round_bankers(dec190, number), dec199, round_bankers(dec199, 0), round_bankers(dec199, number), 
            dec1919, round_bankers(dec1919, 0), round_bankers(dec1919, number) FROM test_enhanced_round_dec128 order by rid;
    """
}
