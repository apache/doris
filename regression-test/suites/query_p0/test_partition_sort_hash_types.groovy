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

suite("test_partition_sort_hash_types") {
    def dbName = "test_partition_sort_hash_types_db"
    sql "DROP DATABASE IF EXISTS ${dbName}"
    sql "CREATE DATABASE ${dbName}"
    sql "USE $dbName"
    sql "set enable_partition_topn = true;"
    sql "set enable_decimal256 = true;"

    sql "DROP TABLE IF EXISTS test_partition_sort_hash_types"
    sql """
        CREATE TABLE IF NOT EXISTS `test_partition_sort_hash_types` (
            `k0` boolean null comment "",
            `k1` tinyint(4) null comment "",
            `k2` smallint(6) null comment "",
            `k3` int(11) null comment "",
            `k4` bigint(20) null comment "",
            `k5` decimal(40, 6) null comment "",
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
        table "test_partition_sort_hash_types"
        db dbName
        set 'column_separator', ','
        file "baseall.txt"
    }

    sql "sync"
    sql """ delete from test_partition_sort_hash_types where k1 is null; """
    qt_select_0 """ select * from test_partition_sort_hash_types order by 2;"""

    qt_select_1 """ select * from (select row_number() over(partition by k1 order by k1) as row_num from test_partition_sort_hash_types)t where row_num = 1; """
    qt_select_2 """ select * from (select row_number() over(partition by k4 order by k1) as row_num from test_partition_sort_hash_types)t where row_num = 1; """
    qt_select_3 """ select * from (select row_number() over(partition by k13 order by k1) as row_num from test_partition_sort_hash_types)t where row_num = 1; """
    qt_select_4 """ select * from (select row_number() over(partition by k5 order by k1) as row_num from test_partition_sort_hash_types)t where row_num = 1; """
    qt_select_5 """ select * from (select row_number() over(partition by non_nullable(k12) order by k1) as row_num from test_partition_sort_hash_types)t where row_num = 1; """
    qt_select_6 """ select * from (select row_number() over(partition by k2,k3 order by k1) as row_num from test_partition_sort_hash_types)t where row_num = 1; """
    qt_select_7 """ select * from (select row_number() over(partition by non_nullable(k0),non_nullable(k13) order by k1) as row_num from test_partition_sort_hash_types)t where row_num = 1; """
}
