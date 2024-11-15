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
suite("eliminate_group_by_key_by_uniform") {
    sql "drop table if exists eli_gbk_by_uniform_t"
    sql """create table eli_gbk_by_uniform_t(a int null, b int not null, c varchar(10) null, d date, dt datetime)
    distributed by hash(a) properties("replication_num"="1");
    """
    qt_empty_tranform_not_to_scalar_agg "select a, min(a), sum(a), count(a) from eli_gbk_by_uniform_t where a = 1 group by a"
    qt_empty_tranform_multi_column "select a,b, min(a), sum(a), count(a) from eli_gbk_by_uniform_t where a = 1 and b=2 group by a,b"

    sql """
    INSERT INTO eli_gbk_by_uniform_t (a, b, c, d, dt) VALUES
    (1, 100, 'apple', '2023-01-01', '2023-01-01 10:00:00'),
    (1, 100, 'apple', '2023-01-01', '2023-01-01 10:00:00'),
    (2, 101, 'banana', '2023-01-02', '2023-01-02 11:00:00'),
    (3, 102, 'cherry', '2023-01-03', '2023-01-03 12:00:00'),
    (3, 102, 'cherry', '2023-01-03', '2023-01-03 12:00:00'), 
    (NULL, 103, 'date', '2023-01-04', '2023-01-04 13:00:00'),
    (4, 104, 'elderberry', '2023-01-05', '2023-01-05 14:00:00'),
    (5, 105, NULL, '2023-01-06', '2023-01-06 15:00:00'),
    (5, 105, NULL, '2023-01-06', '2023-01-06 15:00:00'),
    (6, 106, 'fig', '2023-01-07', '2023-01-07 16:00:00'),
    (NULL, 107, 'grape', '2023-01-08', '2023-01-08 17:00:00');
    """
    qt_empty_tranform_multi_column "select a, min(a), sum(a), count(a) from eli_gbk_by_uniform_t where a = 1 group by a, b,'abc' order by 1,2,3,4"
    qt_tranform_to_scalar_agg_not_null_column "select b, min(a), sum(a), count(a) from eli_gbk_by_uniform_t where b = 1 group by a, b order by 1,2,3,4"

    qt_project_const "select sum(c1), c2 from (select a c1,1 c2, d c3 from eli_gbk_by_uniform_t) t group by c2,c3 order by 1,2;"
    qt_project_slot_uniform "select max(c3), c1,c2,c3 from (select a c1,1 c2, d c3 from eli_gbk_by_uniform_t where a=1) t group by c1,c2,c3 order by 1,2,3,4;"

    qt_upper_refer "select b from (select b, min(a), sum(a), count(a) from eli_gbk_by_uniform_t where b = 1 group by a, b) t order by b"
    qt_upper_refer_varchar_alias "select c1,c2 from (select c as c1, min(a) c2, sum(a), count(a) from eli_gbk_by_uniform_t where c = 'cherry' group by a, b,c) t order  by c1,c2"
    qt_upper_refer_date "select d from (select d, min(a), sum(a), count(a) from eli_gbk_by_uniform_t where d = '2023-01-06' group by d,a) t order by  1"
    qt_upper_refer_datetime_not_to_scalar_agg "select dt from (select dt, min(a), sum(a), count(a) from eli_gbk_by_uniform_t where dt = '2023-01-06 15:00:00' group by dt) t order by 1"
    qt_upper_refer_datetime "select dt from (select dt, min(a), sum(a), count(a) from eli_gbk_by_uniform_t where dt = '2023-01-06 15:00:00' group by dt, a) t order by 1"

    qt_project_no_other_agg_func "select c2 from (select a c1,1 c2, d c3 from eli_gbk_by_uniform_t) t group by c2,c3 order by 1;"
    qt_project_const_not_to_scalar_agg_multi "select c2 from (select a c1,1 c2, 3 c3 from eli_gbk_by_uniform_t) t group by c2,c3 order by 1;"
    qt_not_to_scalar_agg_multi "select a, min(a), sum(a), count(a) from eli_gbk_by_uniform_t where a = 1 and b=100 group by a, b,'abc' order by 1,2,3,4"
    qt_conflict_equal_value "select a, min(a), sum(a), count(a) from eli_gbk_by_uniform_t where a = 1 and a=2 group by a, b,'abc' order by 1,2,3,4"
    qt_project_slot_uniform_confict_value "select max(c3), c1,c2,c3 from (select a c1,1 c2, d c3 from eli_gbk_by_uniform_t where a=1) t where c2=2 group by c1,c2,c3 order by 1,2,3,4;"

    // test join
    qt_inner_join_left_has_filter "select t1.b,t2.b from eli_gbk_by_uniform_t t1 inner join eli_gbk_by_uniform_t t2 on t1.b=t2.b and t1.b=100 group by t1.b,t2.b,t2.c order by 1,2"
    qt_inner_join_right_has_filter "select t1.b,t2.b from eli_gbk_by_uniform_t t1 inner join eli_gbk_by_uniform_t t2 on t1.b=t2.b and t2.b=100 group by t1.b,t2.b,t2.c order by 1,2"
    qt_left_join_right_has_filter "select t1.b,t2.b from eli_gbk_by_uniform_t t1 left join eli_gbk_by_uniform_t t2 on t1.b=t2.b and t2.b=100 group by t1.b,t2.b,t2.c order by 1,2"
    qt_left_join_left_has_filter "select t1.b,t2.b from eli_gbk_by_uniform_t t1 left join eli_gbk_by_uniform_t t2 on t1.b=t2.b and t1.b=100 group by t1.b,t2.b,t2.c order by 1,2"
    qt_right_join_right_has_filter "select t1.b,t2.b from eli_gbk_by_uniform_t t1 right join eli_gbk_by_uniform_t t2 on t1.b=t2.b and t2.b=100 group by t1.b,t2.b,t2.c order by 1,2"
    qt_right_join_left_has_filter "select t1.b,t2.b from eli_gbk_by_uniform_t t1 right join eli_gbk_by_uniform_t t2 on t1.b=t2.b and t1.b=100 group by t1.b,t2.b,t2.c order by 1,2"
    qt_left_semi_join_right_has_filter "select t1.b from eli_gbk_by_uniform_t t1 left semi join eli_gbk_by_uniform_t t2 on t1.b=t2.b and t2.b=100 group by t1.b,t1.a order by 1"
    qt_left_semi_join_left_has_filter "select t1.b from eli_gbk_by_uniform_t t1 left semi join eli_gbk_by_uniform_t t2 on t1.b=t2.b and t1.b=100 group by  t1.b,t1.a order by 1"
    qt_left_anti_join_right_has_on_filter "select t1.b from eli_gbk_by_uniform_t t1 left anti join eli_gbk_by_uniform_t t2 on t1.b=t2.b and t2.b=100 group by t1.b,t1.a order by 1"
    qt_left_anti_join_left_has_on_filter "select t1.b from eli_gbk_by_uniform_t t1 left anti join eli_gbk_by_uniform_t t2 on t1.b=t2.b and t1.b=100 group by t1.b,t1.a order by 1"
    qt_left_anti_join_left_has_where_filter "select t1.b from eli_gbk_by_uniform_t t1 left anti join eli_gbk_by_uniform_t t2 on t1.b=t2.b where t1.b=100 group by t1.b,t1.a order by 1"
    qt_right_semi_join_right_has_filter "select t2.b from eli_gbk_by_uniform_t t1 right semi join eli_gbk_by_uniform_t t2 on t1.b=t2.b and t2.b=100 group by t2.b,t2.c order by 1"
    qt_right_semi_join_left_has_filter "select t2.b from eli_gbk_by_uniform_t t1 right semi join eli_gbk_by_uniform_t t2 on t1.b=t2.b and t1.b=100 group by t2.b,t2.c order by 1"
    qt_right_anti_join_right_has_on_filter "select t2.b from eli_gbk_by_uniform_t t1 right anti join eli_gbk_by_uniform_t t2 on t1.b=t2.b and t2.b=100 group by t2.b,t2.c order by 1"
    qt_right_anti_join_left_has_on_filter "select t2.b from eli_gbk_by_uniform_t t1 right anti join eli_gbk_by_uniform_t t2 on t1.b=t2.b and t1.b=100 group by t2.b,t2.c order by 1"
    qt_right_anti_join_right_has_where_filter "select t2.b from eli_gbk_by_uniform_t t1 right anti join eli_gbk_by_uniform_t t2 on t1.b=t2.b where t2.b=100 group by t2.b,t2.c order by 1"
    qt_cross_join_left_has_filter "select t1.b,t2.b from eli_gbk_by_uniform_t t1 cross join eli_gbk_by_uniform_t t2 where t1.b=100 group by t1.b,t2.b,t2.c order by 1,2"
    qt_cross_join_right_has_filter "select t1.b,t2.b from eli_gbk_by_uniform_t t1 cross join eli_gbk_by_uniform_t t2 where t2.b=100 group by t1.b,t2.b,t2.c order by 1,2"

    //test union
    qt_union "select * from (select a,b from eli_gbk_by_uniform_t where a=1 group by a,b union select a,b from eli_gbk_by_uniform_t where b=100 group by a,b union select a,b from eli_gbk_by_uniform_t where a=5 group by a,b) t order by 1,2,3,4,5"
    qt_union_all "select * from (select a,b from eli_gbk_by_uniform_t where a=1 group by a,b union all select a,b from eli_gbk_by_uniform_t where b=100 group by a,b union all select a,b from eli_gbk_by_uniform_t where a=5 group by a,b) t order by 1,2,3,4,5"
    qt_intersect "select * from (select a,b from eli_gbk_by_uniform_t where a=1 group by a,b intersect select a,b from eli_gbk_by_uniform_t where b=100 group by a,b intersect select a,b from eli_gbk_by_uniform_t where a=5 group by a,b) t order by 1,2,3,4,5"
    qt_except "select * from (select a,b from eli_gbk_by_uniform_t where a=1 group by a,b except select a,b from eli_gbk_by_uniform_t where b=100 group by a,b except select a,b from eli_gbk_by_uniform_t where a=5 group by a,b) t order by 1,2,3,4,5"
    qt_set_op_mixed "select * from (select a,b from eli_gbk_by_uniform_t where a=1 group by a,b union select a,b from eli_gbk_by_uniform_t where b=100 group by a,b except select a,b from eli_gbk_by_uniform_t where a=5 group by a,b) t order by 1,2,3,4,5"

    //test window
    qt_window "select max(a) over(partition by a order by a) from eli_gbk_by_uniform_t where a=10 group by a,b order by 1"
    //test partition topn
    qt_partition_topn "select r from (select rank(a) over(partition by a order by a) r from eli_gbk_by_uniform_t where a=10 group by a,b) t where r<2 order by 1"
    qt_partition_topn_qualifiy "select rank() over(partition by a order by a) r from eli_gbk_by_uniform_t where a=10 group by a,b qualify r<2 order by 1"
    //test cte
    qt_cte_producer "with t as (select a,b,count(*) from eli_gbk_by_uniform_t where a=1 group by a,b) select t1.a,t2.a,t2.b from t t1 inner join t t2 on t1.a=t2.a order by 1,2,3"
    qt_cte_multi_producer "with t as (select a,b,count(*) from eli_gbk_by_uniform_t where a=1 group by a,b), tt as (select a,b,count(*) from eli_gbk_by_uniform_t where b=10 group by a,b) select t1.a,t2.a,t2.b from t t1 inner join tt t2 on t1.a=t2.a order by 1,2,3"
    qt_cte_consumer "with t as (select * from eli_gbk_by_uniform_t) select t1.a,t2.b from t t1 inner join t t2 on t1.a=t2.a where t1.a=10 group by t1.a,t2.b order by 1,2 "

    //test filter
    qt_filter "select * from (select a,b from eli_gbk_by_uniform_t where a=1 group by a,b) t where a>0 order by 1,2"

    //test topn
    qt_topn "select a,b from eli_gbk_by_uniform_t where a=1 group by a,b order by a limit 10 offset 0"
}