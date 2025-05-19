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

suite("eliminate_order_by_key") {
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"
    sql "drop table if exists eliminate_order_by_constant_t"
    sql """create table eliminate_order_by_constant_t(a int null, b int not null, c varchar(10) null, d date, dt datetime, id int)
    distributed by hash(a) properties("replication_num"="1");
    """
//    sql "set disable_nereids_rules='eliminate_order_by_key'"
    sql """
    INSERT INTO eliminate_order_by_constant_t (a, b, c, d, dt,id) VALUES
    (1, 100, 'apple', '2023-01-01', '2023-01-01 10:00:00',1),
    (1, 100, 'apple', '2023-01-01', '2023-01-01 10:00:00',2),
    (2, 101, 'banana', '2023-01-02', '2023-01-02 11:00:00',3),
    (3, 102, 'cherry', '2023-01-03', '2023-01-03 12:00:00',4),
    (3, 102, 'cherry', '2023-01-03', '2023-01-03 12:00:00',5), 
    (NULL, 103, 'date', '2023-01-04', '2023-01-04 13:00:00',6),
    (4, 104, 'elderberry', '2023-01-05', '2023-01-05 14:00:00',7),
    (5, 105, NULL, '2023-01-06', '2023-01-06 15:00:00',8),
    (5, 105, NULL, '2023-01-06', '2023-01-06 15:00:00',9),
    (6, 106, 'fig', '2023-01-07', '2023-01-07 16:00:00',10),
    (NULL, 107, 'grape', '2023-01-08', '2023-01-08 17:00:00',11);
    """
    qt_predicate "select 1 as c1,a from eliminate_order_by_constant_t where a=1 order by a"
    qt_predicate_order_by_two "select 1 as c1,a from eliminate_order_by_constant_t where a=1 order by a,c1"
    qt_with_group_by """select 1 as c1,a from eliminate_order_by_constant_t where a=1 group by c1,a order by a"""
    qt_predicate_multi_other """select 1 as c1,a,b,c from eliminate_order_by_constant_t where a=1 order by a,'abc',b,c"""
    qt_with_group_by_shape """explain shape plan select 1 as c1,a from eliminate_order_by_constant_t where a=1 group by c1,a order by a"""

    // fd
    qt_fd "select a,b,c,d,dt from eliminate_order_by_constant_t order by a,abs(a),a+1,id"
    qt_fd_duplicate "select a,b,c,d,dt from eliminate_order_by_constant_t order by a,abs(a),a,abs(a),a+1,id"
    qt_fd_topn "select a,b,c,d,dt from eliminate_order_by_constant_t order by a,abs(a),a+1,id limit 5"
    qt_fd_duplicate_topn "select a,b,c,d,dt from eliminate_order_by_constant_t order by a,abs(a),a,abs(a),a+1,id limit 5"
    qt_fd_multi_column "select a,b,c,d,dt from eliminate_order_by_constant_t order by a,b,a+b"
    qt_fd_desc "select a,b,c,d,dt from eliminate_order_by_constant_t order by a desc,abs(a) asc,a+1 desc,id"
    qt_fd_multi_column_desc "select a,b,c,d,dt from eliminate_order_by_constant_t order by a,b desc,a+b"
    qt_fd_multi_column_desc_with_other_in_middle "select a,b,c,d,dt from eliminate_order_by_constant_t order by a,c,b desc,a+b asc"
    qt_equalset_fd "select a,b,c,d,dt from eliminate_order_by_constant_t  where a=b order by a,a+b, b"
    qt_uniform_fd "select a,b from eliminate_order_by_constant_t where a=b and a=1 order by a,b,a+b"
    qt_fd_valid_slot_add_equalset "select c,d,a,a+100,b+a+100,b from eliminate_order_by_constant_t where b=a order by c,d,a,a+100,b+a+100"

    // duplicate
    qt_dup_shape "select a,b,c,d,dt from eliminate_order_by_constant_t order by a,a,id"
    qt_dup_expr_shape "select a,b,c,d,dt from eliminate_order_by_constant_t order by a+1,a+1,id"

    // window
    qt_dup_window "select sum(a) over (partition by a order by a,a)  from eliminate_order_by_constant_t order by 1"
    qt_fd_window "select sum(a) over (partition by a order by a,a+1,abs(a),1-a,b)  from eliminate_order_by_constant_t order by 1"
    qt_uniform_window "select sum(a) over (partition by a order by b)  from eliminate_order_by_constant_t where b=100 order by 1"
    qt_uniform_window "select first_value(c) over (partition by a order by b)  from eliminate_order_by_constant_t where b=100 order by 1"
    qt_multi_window """select sum(a) over (partition by a order by a,a+1,abs(a),1-a,b), max(a) over (partition by a order by b,b+1,b,abs(b)) 
                        from eliminate_order_by_constant_t order by 1,2"""
    qt_multi_window_desc """select sum(a) over (partition by a order by a desc,a+1 asc,abs(a) desc,1-a,b), max(a) over (partition by a order by b desc,b+1 desc,b asc,abs(b) desc) 
                        from eliminate_order_by_constant_t order by 1,2"""


    sql "drop table if exists eliminate_order_by_constant_t2"
    sql """create table eliminate_order_by_constant_t2(a int, b int, c int, d int) distributed by hash(a) properties("replication_num"="1");"""
    sql """INSERT INTO eliminate_order_by_constant_t2 (a, b, c, d)
    VALUES(1, 2, 3, 4),(2, 3, 3, 5),(3, 4, 5, 6),(4, 5, 6, 7),(5, 6, 7, 8),(6, 7, 8, 9),(7, 8, 9, 10),(8, 9, 10, 11),(9, 10, 11, 12),(10, 11, 12, 13);"""
    qt_equal_set_uniform """select * from eliminate_order_by_constant_t2 where b=a and a=c and d=1 order by d,a,b,c,c,b,a,d,d"""
    qt_equal_set_uniform2 """select * from eliminate_order_by_constant_t2 where b=a and a=d and a=c and d=1 order by d,a,b,c,c,b,a,d,d"""
    qt_equal_set_uniform_fd """select * from eliminate_order_by_constant_t2 where b=a and a=d and a=c and d=1 order by d,a,a+1,b+1,c+1,c,b,a,d,d"""
    qt_fd_uniform """select * from eliminate_order_by_constant_t2 where d=1 order by d,a,b,c,d+b+a-100,d+b+a,b,a,d,d"""
    qt_equalset_fd "select * from eliminate_order_by_constant_t2 where d=b and a=d order by a,c,d+b+a,b,a,d,d+1,abs(d)"

    // other operator
    // join
    qt_join_inner_order_by """
    select t1.a, t1.b, t2.c, t2.d from eliminate_order_by_constant_t t1 
    inner join eliminate_order_by_constant_t2 t2 on t1.a = t2.a order by t1.a, t2.a, t2.c, t1.a+t2.a, t1.b, t2.d, t1.a+1, t2.c;
    """

    qt_join_left_outer_order_by """
    select t1.a, t1.b, t2.c, t2.d from eliminate_order_by_constant_t t1 
    left outer join eliminate_order_by_constant_t2 t2 on t1.a = t2.a order by t1.a, t2.a, t2.c, t1.a+t2.a, t1.b, t2.d, t1.a+1, t2.c;
    """

    qt_join_right_outer_order_by """
    select t1.a, t1.b, t2.c, t2.d from eliminate_order_by_constant_t t1 
    right outer join eliminate_order_by_constant_t2 t2 on t1.a = t2.a order by t1.a, t2.a, t2.c, t1.a+t2.a, t1.b, t2.d, t1.a+1, t2.c;
    """

    qt_join_right_outer_order_by_predicate """select t1.a, t1.b, t2.c, t2.d from eliminate_order_by_constant_t t1
    right outer join eliminate_order_by_constant_t2 t2 on t1.a = t2.a where t2.a=1 order by t1.a, t2.a, t2.c, t1.a+t2.a, t1.b, t2.d, t1.a+1, t2.c;"""

    qt_join_left_semi_order_by """
    select t1.a, t1.b from eliminate_order_by_constant_t t1 
    left semi join eliminate_order_by_constant_t2 t2 on t1.a = t2.a order by t1.a, t1.b, t1.a+1,t1.a+t1.b;
    """

    qt_join_right_semi_order_by """
    select t2.a, t2.b 
    from eliminate_order_by_constant_t t1 
    right semi join eliminate_order_by_constant_t2 t2 on t1.a = t2.a 
    order by t2.a, t2.b, t2.a+1,t2.a+t2.b;
    """

    qt_join_left_anti_order_by """
    select t1.a, t1.b 
    from eliminate_order_by_constant_t t1 
    left anti join eliminate_order_by_constant_t2 t2 on t1.a = t2.a 
    order by t1.a, t1.b, t1.a+1,t1.a+t1.b;
    """

    qt_join_right_anti_order_by """
    select t2.a, t2.b 
    from eliminate_order_by_constant_t t1 
    right anti join eliminate_order_by_constant_t2 t2 on t1.a = t2.a 
    order by t2.a, t2.b, t2.a+1,t2.a+t2.b;
    """
    // agg
    qt_agg_order_by """
    select a, count(b) as cnt 
    from eliminate_order_by_constant_t2 
    group by a 
    order by a, cnt, a,a+cnt,a+100;
    """
    // agg+grouping
    qt_agg_grouping_order_by """
    select a, b, count(c) as cnt 
    from eliminate_order_by_constant_t2 
    group by cube(a, b) 
    order by a, b, cnt, a, b+1;
    """
    // join+window
    qt_join_window_order_by """
    select t1.a, t1.b, t2.c, t2.d, t2.a,
           row_number() over (partition by t1.a order by t1.b) as rn 
    from eliminate_order_by_constant_t t1 
    inner join eliminate_order_by_constant_t2 t2 on t1.a = t2.a 
    order by t1.a, t2.c, t1.b, t2.d, abs(t1.a), abs(t2.a), t2.c,rn,rn+100;
    """
    // agg+window
    qt_agg_window_order_by """
    select a, b, count(c) as cnt, 
           row_number() over (partition by a order by b) as rn 
    from eliminate_order_by_constant_t2 
    group by a, b 
    order by a, b, cnt, a+100, b, rn, rn+cnt, abs(rn+cnt);
    """
    // join + agg+ window
    qt_join_agg_window_order_by """
    select t1.a, t1.b, count(t2.c) as cnt, 
           row_number() over (partition by t1.a order by t1.b) as rn 
    from eliminate_order_by_constant_t t1 
    inner join eliminate_order_by_constant_t2 t2 on t1.a = t2.a 
    group by t1.a, t1.b ,t2.a
    order by t1.a,t2.a,t1.b, cnt, -t1.a, -t1.b-1000,rn, cnt, rn+111;
    """
    // union all + order by
    qt_union_all_order_by """
    select * from (
    select a, b from eliminate_order_by_constant_t2 
    union all 
    select a, b from eliminate_order_by_constant_t ) t
    order by a, b, abs(a),abs(a)+b,a+b,a,b;
    """
    // union + join + order by
    qt_union_join_order_by """
    select * from (select t1.a, t1.b 
    from eliminate_order_by_constant_t t1 
    inner join eliminate_order_by_constant_t2 t2 on t1.a = t2.a 
    union 
    select t1.a, t1.b 
    from eliminate_order_by_constant_t t1 
    left join eliminate_order_by_constant_t2 t2 on t1.a = t2.a ) t
    where a=1
    order by a, b, a+100,abs(a)+b;
    """

    // test composite key
    sql "drop table if exists test_unique_order_by2 "
    sql """create table test_unique_order_by2(a int not null, b int not null, c int, d int) unique key(a,b) distributed by hash(a) properties('replication_num'='1');"""
    sql """INSERT INTO test_unique_order_by2 (a, b, c, d)
    VALUES(1, 2, 3, 4),(2, 3, 3, 5),(3, 4, 5, 6),(4, 5, 6, 7),(5, 6, 7, 8),(6, 7, 8, 9),(7, 8, 9, 10),(8, 9, 10, 11),(9, 10, 11, 12),(10, 11, 12, 13);"""
    qt_composite_key """select * from test_unique_order_by2 order by a,'abc',d,b,d,c;"""
}