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

suite("simplify_window_expression") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql """
          DROP TABLE IF EXISTS mal_test_simplify_window
         """

    sql """
         create table mal_test_simplify_window(a int, b int, c int) unique key(a,b) distributed by hash(a) buckets 10
         properties('replication_num' = '1'); 
         """

    sql """
         insert into mal_test_simplify_window values(2,1,3),(1,1,2),(3,5,6),(6,null,6),(4,5,6),(2,1,4),(2,3,5),(1,1,4)
        ,(3,5,6),(3,5,null),(6,7,1),(2,1,7),(2,4,2),(2,3,9),(1,3,6),(3,5,8),(3,2,8),(null,null,3);
      """

    sql "sync"

    qt_select_count_col "select a,count(a) over (partition by a,b) c1 from mal_test_simplify_window order by 1,2;"
    qt_select_rank "select a,rank() over (partition by a,b) c1 from mal_test_simplify_window order by 1,2;"
    qt_select_dense_rank "select a,dense_rank() over (partition by a,b) c1 from mal_test_simplify_window order by 1,2;"
    qt_select_row_number "select a,row_number() over (partition by a,b) c1 from mal_test_simplify_window order by 1,2;"
    qt_select_first_value "select a,first_value(a) over (partition by a,b) c1 from mal_test_simplify_window order by 1,2;"
    qt_select_last_value "select a,last_value(a) over (partition by a,b) c1 from mal_test_simplify_window order by 1,2;"
    qt_select_min "select b,min(b) over (partition by a,b) c1 from mal_test_simplify_window order by 1,2;"
    qt_select_max "select b,max(b) over (partition by a,b) c1 from mal_test_simplify_window order by 1,2;"
    qt_select_sum "select a,sum(a) over (partition by a,b) c1 from mal_test_simplify_window order by 1,2;"
    qt_select_avg "select b,avg(b) over (partition by a,b) c1 from mal_test_simplify_window order by 1,2;"

    qt_select_last_value_shape "explain shape plan select a,last_value(a) over (partition by a,b) c1 from mal_test_simplify_window order by a;"
    qt_select_min_shape "explain shape plan select b,min(b) over (partition by a,b) c1 from mal_test_simplify_window order by a;"
}