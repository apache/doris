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

suite("sort_spill") {
    sql """
        drop table if exists d_table;
    """
    sql """
       create table d_table(
        k1 int,
        k2 int,
        ) distributed by random buckets 10
        properties ("replication_num"="1");
    """
    sql """
        insert into d_table select e1,e1 from (select 1 k1) as t lateral view explode_numbers(10000) tmp1 as e1;
    """
    sql """ set parallel_pipeline_task_num = 2; """
    sql """ set batch_size = 100; """
    sql """ set enable_force_spill=true; """
    sql """ set enable_topn_lazy_materialization=false;"""
    sql """ set enable_reserve_memory=true; """
    sql """ set force_sort_algorithm = "full"; """
    sql """ set enable_parallel_result_sink=true; """
    qt_select_1 "select k1,row_number () over (ORDER BY k2 DESC) from d_table order by k1 limit 10 offset 9900;"
    qt_select_2 "select k1,row_number () over (ORDER BY k2 DESC) from d_table order by k1 limit 10;"
}