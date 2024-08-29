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

suite("insert_cte") {
    def t1 = "insert_cte_tbl_1"
    def t2 = "insert_cte_tbl_2"
    def t3 = "insert_cte_tbl_3"

    sql " SET enable_nereids_planner = true; "

    sql """ DROP TABLE IF EXISTS $t1 """
    sql """
        create table $t1 (
            k int
        ) engine = OLAP
        unique key(k)
        partition by list (k)
        (
        partition p1 values in ("1", "2", "3"),
        partition p2 values in ("4", "5", "6")
        )
        distributed by hash(k) buckets 3
        properties("replication_num" = "1");
    """
    sql """ DROP TABLE IF EXISTS $t2 """
    sql """
        create table $t2 (
            k int
        ) properties("replication_num" = "1");
    """

    sql """ insert into $t2 values (1), (2), (3), (4), (5), (6); """

    // test for InsertIntoTableCommand
    // 1. insert into values
    sql """ with cte1 as (select * from $t2 where k < 4) insert into $t1 values (4); """
    order_qt_select1 """ select * from $t1; """
    // 2. insert into select
    sql """ with cte1 as (select * from $t2 where k < 4) insert into $t1 select * from cte1; """
    order_qt_select2 """ select * from $t1; """
    // 3. insert into partition select
    sql """ with cte1 as (select * from $t2 where k >= 4) insert into $t1 partition(p2) select * from cte1; """
    order_qt_select3 """ select * from $t1; """

    // test for InsertOverwriteTableCommand
    // 1. insert overwrite table select
    sql """ with cte1 as (select * from $t2 where k < 4) insert overwrite table $t1 select * from cte1; """
    order_qt_select4 """ select * from $t1; """
    // 2. insert overwrite table partition select
    sql """ with cte1 as (select 4) insert overwrite table $t1 partition(p2) select * from cte1; """
    order_qt_select5 """ select * from $t1; """
    // 3. overwrite auto detect partition
    sql """ with cte1 as (select 1) insert overwrite table $t1 partition(*) select * from cte1; """
    order_qt_select6 """ select * from $t1; """

}
