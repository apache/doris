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

suite("test_aggregate_all_functions2") {
    
    def dbName = "agg_func_db"
    sql "DROP DATABASE IF EXISTS ${dbName}"
    sql "CREATE DATABASE ${dbName}"
    sql "USE $dbName"

    sql """
        CREATE TABLE IF NOT EXISTS `baseall` (
            `k0` boolean null comment "",
            `k1` tinyint(4) null comment "",
            `k2` smallint(6) null comment "",
            `k3` int(11) null comment "",
            `k4` bigint(20) null comment "",
            `k5` decimal(9, 3) null comment "",
            `k6` char(5) null comment "",
            `k10` date null comment "",
            `k11` datetime null comment "",
            `k7` varchar(20) null comment "",
            `k8` double max null comment "",
            `k9` float sum null comment "",
            `k12` string replace null comment "",
            `k13` largeint(40) replace null comment ""
        ) engine=olap
        DISTRIBUTED BY HASH(`k1`) BUCKETS 5 properties("replication_num" = "1")
        """

    streamLoad {
        table "baseall"
        db dbName
        set 'column_separator', ','
        file "../../baseall.txt"
    }

    sql "sync"

    qt_select_approx_count_distinct1 """ SELECT approx_count_distinct(k0) FROM baseall """
    qt_select_approx_count_distinct2 """ SELECT approx_count_distinct(k1) FROM baseall """
    qt_select_collect_set1 """ SELECT size(collect_set(k10,5)) FROM baseall """
    qt_select_collect_set2 """ SELECT size(collect_set(k11,5)) FROM baseall """
    qt_select_collect_list1 """ SELECT size(collect_list(k10,5)) FROM baseall """
    qt_select_collect_list2 """ SELECT size(collect_list(k11,5)) FROM baseall """
    qt_select_histogram """SELECT histogram(k7, 5) FROM baseall"""
    qt_select_max_by1 """ select max_by(k1,k10) from baseall; """
    qt_select_max_by2 """ select max_by(k1,k11) from baseall; """
    qt_select_max_by3 """ select max_by(k1,k7) from baseall; """
    qt_select_min_by1 """ select min_by(k1,k10) from baseall; """
    qt_select_min_by2 """ select min_by(k1,k11) from baseall; """    
    qt_select_min_by3 """ select min_by(k1,cast(k1 as string)) from baseall; """ 
    qt_select_intersect_count_1 """ select intersect_count(bitmap_from_array(array(1,2,3,4,5)),cast(k1 as string),1,2) from baseall; """ 
    qt_select_intersect_count_2 """ select intersect_count(bitmap_from_array(array(1,2,3,4,5)),k1,1,2) from baseall; """ 
    qt_select_percentile_approx1 """ select percentile_approx(k2,10001) from baseall; """ 
    qt_select_percentile_array """ select percentile_array(k2,[0.3,0.5,0.9]) from baseall; """ 
    qt_select_array_product """ select array_product(array(cast(k5 as decimalv3(30,10)))) from baseall order by k1; """ 
    qt_select_quantile_percent """ select QUANTILE_PERCENT(QUANTILE_UNION(TO_QUANTILE_STATE(k1,2048)),0.5) from baseall;  """ 
    qt_select_sum """ select sum(cast(k5 as decimalv3(38,18))) from baseall; """ 
    qt_select_topn_weighted1 """ select topn_weighted(k2,k1,3) from baseall; """ 
    qt_select_topn_weighted2 """ select topn_weighted(k2,k1,3,100) from baseall; """ 
    qt_select_topn_array1 """ select topn_array(k7,3) from baseall; """ 
    qt_select_topn_array2 """ select topn_array(k7,3,100) from baseall; """ 
    qt_select_topn_array3 """ select topn_array(k10,3) from baseall; """ 
    qt_select_topn_array4 """ select topn_array(k10,3,100) from baseall; """ 
    qt_select_topn_array5 """ select topn_array(k11,3) from baseall; """ 
    qt_select_topn_array6 """ select topn_array(k11,3,100) from baseall; """ 
    qt_select_count1 """ select count(distinct k1,k2,k5) from baseall; """ 
    qt_select_count2 """ select count(distinct k1,k2,cast(k5 as decimalv3(38,18))) from baseall; """ 


    sql "DROP DATABASE IF EXISTS metric_table"
    sql """
        CREATE TABLE `metric_table` (
        `datekey` int(11) NULL,
        `hour` int(11) NULL,
        `device_id` bitmap BITMAP_UNION NOT NULL
        ) ENGINE=OLAP
        AGGREGATE KEY(`datekey`, `hour`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`datekey`, `hour`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "is_being_synced" = "false",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false"
        ); 
    """
    sql """
        insert into metric_table values
        (20200622, 1, to_bitmap(243)),
        (20200622, 2, bitmap_from_array([1,2,3,4,5,434543])),
        (20200622, 3, to_bitmap(287667876573));
    """

    qt_select_minmax1 """ select * from metric_table order by hour; """
    qt_select_minmax2 """ select max_by(datekey,hour) from metric_table; """
    qt_select_minmax3 """ select bitmap_to_string(max_by(device_id,hour)) from metric_table; """
    qt_select_minmax4 """ select bitmap_to_string(min_by(device_id,hour)) from metric_table; """
}
