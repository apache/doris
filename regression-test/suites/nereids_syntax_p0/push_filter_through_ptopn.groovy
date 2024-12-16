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

suite("push_filter_through_ptopn") {
    sql """set enable_nereids_planner=true"""
    sql """set enable_partition_topn=true"""
    sql "set enable_parallel_result_sink=false;"
    sql """
        DROP TABLE IF EXISTS push_filter_through_ptopn_tbl
    """
    sql """
        CREATE TABLE `push_filter_through_ptopn_tbl` (
            `a` int(11) NULL,
            `b` int NULL
            ) ENGINE=OLAP
            duplicate KEY(`a`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`a`) BUCKETS 4
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
        insert into push_filter_through_ptopn_tbl values  (1, 2), (1, 3), (2, 4), (2, 5);
    """
 
    qt_1 """select * from (select a, row_number() over (partition by a) rn from push_filter_through_ptopn_tbl) T where a=1 and rn<=2;"""
    qt_shape_1 """explain shape plan select * from (select a, row_number() over (partition by a) rn from push_filter_through_ptopn_tbl) T where a=1 and rn<=2;"""
    qt_2 """select * from (select a, row_number() over (partition by b, a) rn from push_filter_through_ptopn_tbl) T where a=1 and rn <=2;"""
    qt_shape_2 """explain shape plan select * from (select a, row_number() over (partition by b, a) rn from push_filter_through_ptopn_tbl) T where a=1 and rn<=2;"""

    qt_3 """explain shape plan select * from (select a, b, row_number() over (partition by a) rn from push_filter_through_ptopn_tbl) T where b=2 and rn<=2;"""
    qt_4 """explain shape plan select * from (select a, b, row_number() over (partition by a+b) rn from push_filter_through_ptopn_tbl) T where b=2 and rn<=2;"""

}
