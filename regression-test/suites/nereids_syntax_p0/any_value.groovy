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

suite("any_value") {
    // enable nereids
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    test {
        sql "select any(s_suppkey), any(s_name), any_value(s_address) from supplier;"
    }
    qt_sql_max """select max(cast(concat(number, ":00:00") as time)) from numbers("number" = "100");"""
    qt_sql_min """select min(cast(concat(number, ":00:00") as time)) from numbers("number" = "100");"""
    sql """select any(cast(concat(number, ":00:00") as time)) from numbers("number" = "100");"""

    sql """DROP TABLE IF EXISTS any_test"""
    sql """
    CREATE TABLE `any_test` (
        `id` int(11) NULL,
        `c_array1` ARRAY<int(11)> NULL,
        `c_array2` ARRAY<int(11)> NOT NULL,
        `c_array3` ARRAY<string> NULL,
        `c_array4` ARRAY<string> NOT NULL,
        `s_info1` STRUCT<s_id:int(11), s_name:string, s_address:string> NULL,
        `s_info2` STRUCT<s_id:int(11), s_name:string, s_address:string> NOT NULL,
        `m1` Map<STRING, INT> NULL,
        `m2` Map<STRING, INT> NOT NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`id`)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`id`) BUCKETS 10
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "in_memory" = "false",
    "storage_format" = "V2"
    );
    """

    qt_sql_any1 """ select any(c_array1),any(c_array2),any(c_array3),any(c_array4),any(s_info1),any(s_info2),any(m1),any(m2) from any_test; """       
    qt_sql_any2 """ select any(c_array1),any(c_array2),any(c_array3),any(c_array4),any(s_info1),any(s_info2),any(m1),any(m2) from any_test group by id; """       
    sql """ insert into any_test values(1, array(1,2,3), array(4,5,6), array('a','b','c'), array('d','e','f'), named_struct('s_id', 1, 's_name', 'a', 's_address', 'b'), named_struct('s_id', 2, 's_name', 'c', 's_address', 'd'), map('a', 1, 'b', 2), map('c', 3, 'd', 4)); """
    qt_sql_any3 """ select any(c_array1),any(c_array2),any(c_array3),any(c_array4),any(s_info1),any(s_info2),any(m1),any(m2) from any_test; """       
    sql """ insert into any_test values(2, array(4,5,6), array(7,8,9), array('d','e','f'), array('g','h','i'), named_struct('s_id', 3, 's_name', 'e', 's_address', 'f'), named_struct('s_id', 4, 's_name', 'g', 's_address', 'h'), map('e', 5, 'f', 6), map('g', 7, 'h', 8)); """
    qt_sql_any4 """ select any(c_array1),any(c_array2),any(c_array3),any(c_array4),any(s_info1),any(s_info2),any(m1),any(m2) from any_test group by id order by id; """       

    sql """DROP TABLE IF EXISTS baseall_any"""
    sql """
        CREATE TABLE `baseall_any` (
        `k1` tinyint NULL,
        `k2` smallint NULL,
        `k3` int NULL,
        `k4` bigint NULL,
        `k5` decimal(9,3) NULL,
        `k6` char(5) NULL,
        `k10` date NULL,
        `k11` datetime NULL,
        `k7` varchar(20) NULL,
        `k8` double MAX NULL,
        `k9` float SUM NULL
        ) ENGINE=OLAP
        AGGREGATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`, `k6`, `k10`, `k11`, `k7`)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 5
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    qt_sql_any5 """ select any(k1),any(k2),any(k3),any(k4),any(k5),any(k6),any(k10),any(k11),any(k7),any(k8),any(k9) from baseall_any; """
    qt_sql_any6 """ select any(k1),any(k2),any(k3),any(k4),any(k5),any(k6),any(k10),any(k11),any(k7),any(k8),any(k9) from baseall_any group by k1; """       
    sql """
        insert into baseall_any values(1, 1, 1, 1, 1.1, 'a', '2021-01-01', '2021-01-01 00:00:00', 'a', 1.1, 1.1);
    """
    qt_sql_any7 """ select any(k1),any(k2),any(k3),any(k4),any(k5),any(k6),any(k10),any(k11),any(k7),any(k8),any(k9) from baseall_any; """
    sql """
        insert into baseall_any values(2, 2, 2, 2, 2.2, 'b', '2021-02-02', '2021-02-02 00:00:00', 'b', 2.2, 2.2);
    """
    qt_sql_any8 """ select any(k1),any(k2),any(k3),any(k4),any(k5),any(k6),any(k10),any(k11),any(k7),any(k8),any(k9) from baseall_any group by k1 order by k1; """

     sql """DROP TABLE IF EXISTS pv_bitmap_any"""
    sql """
    CREATE TABLE `pv_bitmap_any` (
        `dt` int(11) NULL COMMENT "",
        `page` varchar(10) NULL COMMENT "",
        `user_id` bitmap BITMAP_UNION NOT NULL COMMENT ""
        ) ENGINE=OLAP
        AGGREGATE KEY(`dt`, `page`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`dt`) BUCKETS 2
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """
    qt_sql_any9 """ select bitmap_to_string(any(user_id)) from pv_bitmap_any; """
    qt_sql_any10 """ select dt, bitmap_to_string(any(user_id)) from pv_bitmap_any group by dt order by dt; """

    sql """
        insert into pv_bitmap_any values(20230101, 'a', bitmap_from_string('1,2,3'));
    """
    qt_sql_any11 """ select bitmap_to_string(any(user_id)) from pv_bitmap_any; """
    sql """
        insert into pv_bitmap_any values(20230102, 'b', bitmap_from_string('4,5,6'));
    """
    qt_sql_any12 """ select dt, bitmap_to_string(any(user_id)) from pv_bitmap_any group by dt order by dt; """

    sql """DROP TABLE IF EXISTS test_hll_any"""
    sql """
    create table test_hll_any(
        id int,
        uv hll hll_union
    )
    Aggregate KEY (id)
    distributed by hash(id) buckets 10
    PROPERTIES(
            "replication_num" = "1"
    );
    """
    qt_sql_any13 """ select HLL_CARDINALITY(any(uv)) from test_hll_any; """
    qt_sql_any14 """ select id, HLL_CARDINALITY(any(uv)) from test_hll_any group by id order by id; """
    sql """
        insert into test_hll_any values(1, hll_hash(1));
    """
    qt_sql_any15 """ select HLL_CARDINALITY(any(uv)) from test_hll_any; """
    sql """
        insert into test_hll_any values(2, hll_hash(2));
    """
    qt_sql_any16 """ select id, HLL_CARDINALITY(any(uv)) from test_hll_any group by id order by id; """


    sql """DROP TABLE IF EXISTS quantile_state_agg_test"""
    sql """
        CREATE TABLE IF NOT EXISTS quantile_state_agg_test (
         `dt` int(11) NULL COMMENT "",
         `id` int(11) NULL COMMENT "",
         `price` quantile_state QUANTILE_UNION NOT NULL COMMENT ""
        ) ENGINE=OLAP
        AGGREGATE KEY(`dt`, `id`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`dt`) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """
    qt_sql_any17 """ select quantile_percent(any(price),1) from quantile_state_agg_test; """
    qt_sql_any18 """ select dt, quantile_percent(any(price),1) from quantile_state_agg_test group by dt order by dt; """
    sql """
        insert into quantile_state_agg_test values(20230101, 1, to_quantile_state(1,2048));
    """
    qt_sql_any19 """ select quantile_percent(any(price),1) from quantile_state_agg_test; """
    sql """
        insert into quantile_state_agg_test values(20230102, 2, to_quantile_state(200,2048));
    """
    qt_sql_any20 """ select dt, quantile_percent(any(price),1) from quantile_state_agg_test group by dt order by dt; """

    sql """ set enable_agg_state=true"""
    sql """DROP TABLE IF EXISTS a_table_any"""
    sql """
        create table a_table_any(
            k1 int null,
            k2 agg_state<max_by(int not null,int)> generic,
            k3 agg_state<group_concat(string)> generic
        )
        aggregate key (k1)
        distributed BY hash(k1) buckets 3
        properties("replication_num" = "1");
    """
    qt_sql_any21 """ select any(k1),any(k2),any(k3) from a_table_any; """
    qt_sql_any22 """ select k1,any(k2),any(k3) from a_table_any group by k1 order by k1; """
    sql """
        insert into a_table_any values(1, max_by_state(1,1), group_concat_state('a'));
    """
    qt_sql_any23 """ select max_by_merge(u2),group_concat_merge(u3) from (
        select k1,max_by_union(k2) as u2,group_concat_union(k3) u3 from a_table_any group by k1 order by k1
        ) t; """
    sql """
        insert into a_table_any values(2, max_by_state(2,2), group_concat_state('a'));
    """
    qt_sql_any24 """ select max_by_merge(u2),group_concat_merge(u3) from (
        select k1,max_by_union(k2) as u2,group_concat_union(k3) u3 from a_table_any group by k1 order by k1
        ) t; """

    sql """drop table if exists test_table_any;"""
    sql """CREATE TABLE `test_table_any` (
            `ordernum` varchar(65533) NOT NULL ,
            `dnt` datetime NOT NULL ,
            `data` json NULL 
            ) ENGINE=OLAP
            DUPLICATE KEY(`ordernum`, `dnt`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`ordernum`) BUCKETS 3
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );"""

    sql """insert into test_table_any values('cib2205045_1_1s','2023/6/10 3:55:33','{"DB1":168939,"DNT":"2023-06-10 03:55:33"}');"""
    qt_sql_any25 """ select any(data) from test_table_any; """

}