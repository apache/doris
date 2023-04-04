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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases/aggregate
// and modified by Doris.

suite("sort") {
    qt_sort_string_single_column """ select * from ( select '汇总' as a union all select '2022-01-01' as a ) a order by 1 """
    qt_sort_string_multiple_columns """ select * from ( select '汇总' as a,1 as b union all select '2022-01-01' as a,1 as b ) a order by 1,2 """
    qt_sort_string_on_fe """ select '汇总' > '2022-01-01' """

    sql """ DROP TABLE if exists `sort_non_overlap`; """
    sql """ CREATE TABLE `sort_non_overlap` (
      `time_period` datetime NOT NULL,
      `area_name` varchar(255) NOT NULL,
      `province` varchar(255) NOT NULL,
      `res_name` varchar(255) NOT NULL,
      `dev` varchar(255) NOT NULL,
      `dec0` decimal(10, 3) REPLACE_IF_NOT_NULL NULL,
      `dec1` decimal(10, 3) REPLACE_IF_NOT_NULL NULL,
      `dec2` decimal(10, 3) REPLACE_IF_NOT_NULL NULL,
      `dec3` decimal(10, 3) REPLACE_IF_NOT_NULL NULL,
      `update_time` datetime REPLACE NULL
    ) ENGINE=OLAP
    AGGREGATE KEY(`time_period`, `area_name`, `province`, `res_name`, `dev`)
    DISTRIBUTED BY HASH(`area_name`) BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "disable_auto_compaction" = "true"
    );
    """

    sql """ insert into sort_non_overlap values
        ('2023-03-21 06:00:00', 'area1', 'p0', 'aaaaa', 'ddddd1', 100, 100, 100, 100, '2023-03-21 17:00:00'),
        ('2023-03-21 07:00:00', 'area1', 'p0', 'aaaaa', 'ddddd2', 100, 100, 100, 100, '2023-03-21 17:00:00');
    """

    sql """ insert into sort_non_overlap values
                ('2023-03-21 08:00:00', 'area1', 'p0', 'aaaaa', 'ddddd5', 100, 100, 100, 100, '2023-03-21 17:00:00'),
                ('2023-03-21 09:00:00', 'area1', 'p0', 'aaaaa', 'ddddd6', 100, 100, 100, 100, '2023-03-21 17:00:00');
    """

    qt_sql_orderby_non_overlap_desc """ select * from sort_non_overlap order by time_period desc limit 4; """
}

