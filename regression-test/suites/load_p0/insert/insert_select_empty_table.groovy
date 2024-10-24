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

suite("insert_select_empty_table") {
    multi_sql """
    SET enable_nereids_planner=true;
    SET enable_fallback_to_original_planner=false;

    DROP TABLE IF EXISTS insert_select_empty_table1;

    create table insert_select_empty_table1(pk int, a int, b int) distributed by hash(pk) buckets 10
    properties('replication_num' = '1'); 

    DROP TABLE IF EXISTS insert_select_empty_table2;

    create table insert_select_empty_table2(pk int, a int, b int) distributed by hash(pk) buckets 10
    properties('replication_num' = '1'); 
    
    insert into insert_select_empty_table1 select * from insert_select_empty_table2;
    """
    qt_test_shape "explain shape plan insert into insert_select_empty_table1 select * from insert_select_empty_table2;"

    sql """insert into insert_select_empty_table1 select * from insert_select_empty_table2 
        union all select * from insert_select_empty_table2"""
    qt_test_shape_union """explain shape plan insert into insert_select_empty_table1 select * from insert_select_empty_table2 
        union all select * from insert_select_empty_table2 """

    sql """insert into insert_select_empty_table1 select t1.* from insert_select_empty_table2 t1
        inner join insert_select_empty_table2 t2 on t1.a=t2.a"""
    qt_test_shape_join """explain shape plan insert into insert_select_empty_table1 select t1.* from insert_select_empty_table2 t1
        inner join insert_select_empty_table2 t2 on t1.a=t2.a"""

    sql """
    insert into insert_select_empty_table1 select pk,a,b from insert_select_empty_table2 where a > 10 group by pk,a,b  limit 10;
    """
    qt_test_shape_agg_filter_limit """ explain shape plan
    insert into insert_select_empty_table1 select pk,a,b from insert_select_empty_table2 where a > 10 group by pk,a,b  limit 10;
    """

    qt_test_insert_empty_agg "insert into insert_select_empty_table1(a) select count(*) from insert_select_empty_table1"
    qt_test_insert_empty_agg_select "select * from insert_select_empty_table1"

}