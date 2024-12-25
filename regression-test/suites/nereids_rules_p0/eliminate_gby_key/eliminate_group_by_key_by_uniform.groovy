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
    sql "set enable_nereids_rules = 'ELIMINATE_GROUP_BY_KEY_BY_UNIFORM'"
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
    qt_partition_topn "select r from (select rank() over(partition by a order by a) r from eli_gbk_by_uniform_t where a=10 group by a,b) t where r<2 order by 1"
    qt_partition_topn_qualifiy "select rank() over(partition by a order by a) r from eli_gbk_by_uniform_t where a=10 group by a,b qualify r<2 order by 1"
    //test cte
    qt_cte_producer "with t as (select a,b,count(*) from eli_gbk_by_uniform_t where a=1 group by a,b) select t1.a,t2.a,t2.b from t t1 inner join t t2 on t1.a=t2.a order by 1,2,3"
    qt_cte_multi_producer "with t as (select a,b,count(*) from eli_gbk_by_uniform_t where a=1 group by a,b), tt as (select a,b,count(*) from eli_gbk_by_uniform_t where b=10 group by a,b) select t1.a,t2.a,t2.b from t t1 inner join tt t2 on t1.a=t2.a order by 1,2,3"
    qt_cte_consumer "with t as (select * from eli_gbk_by_uniform_t) select t1.a,t2.b from t t1 inner join t t2 on t1.a=t2.a where t1.a=10 group by t1.a,t2.b order by 1,2 "

    //test filter
    qt_filter "select * from (select a,b from eli_gbk_by_uniform_t where a=1 group by a,b) t where a>0 order by 1,2"

    //test topn
    qt_topn "select a,b from eli_gbk_by_uniform_t where a=1 group by a,b order by a limit 10 offset 0"

    //olap table sink
    sql "insert into eli_gbk_by_uniform_t select a,b,c,d,dt from eli_gbk_by_uniform_t where a = 1 group by a,b,c,d,dt"
    qt_sink "select * from eli_gbk_by_uniform_t order by 1,2,3,4,5"

    sql """
    drop table if exists orders_inner_1
    """

    sql """CREATE TABLE `orders_inner_1` (
      `o_orderkey` BIGINT not NULL,
      `o_custkey` INT NULL,
      `o_orderstatus` VARCHAR(1) NULL,
      `o_totalprice` DECIMAL(15, 2)  NULL,
      `o_orderpriority` VARCHAR(15) NULL,
      `o_clerk` VARCHAR(15) NULL,
      `o_shippriority` INT NULL,
      `o_comment` VARCHAR(79) NULL,
      `o_orderdate` DATE NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`o_orderkey`, `o_custkey`)
    COMMENT 'OLAP'
    PARTITION BY list(o_orderkey) (
    PARTITION p1 VALUES in ('1'),
    PARTITION p2 VALUES in ('2'),
    PARTITION p3 VALUES in ('3'),
    PARTITION p4 VALUES in ('4')
    )
    DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    drop table if exists lineitem_inner_1
    """

    sql """CREATE TABLE `lineitem_inner_1` (
      `l_orderkey` BIGINT not NULL,
      `l_linenumber` INT NULL,
      `l_partkey` INT NULL,
      `l_suppkey` INT NULL,
      `l_quantity` DECIMAL(15, 2) NULL,
      `l_extendedprice` DECIMAL(15, 2) NULL,
      `l_discount` DECIMAL(15, 2) NULL,
      `l_tax` DECIMAL(15, 2) NULL,
      `l_returnflag` VARCHAR(1) NULL,
      `l_linestatus` VARCHAR(1) NULL,
      `l_commitdate` DATE NULL,
      `l_receiptdate` DATE NULL,
      `l_shipinstruct` VARCHAR(25) NULL,
      `l_shipmode` VARCHAR(10) NULL,
      `l_comment` VARCHAR(44) NULL,
      `l_shipdate` DATE NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(l_orderkey, l_linenumber, l_partkey, l_suppkey )
    COMMENT 'OLAP'
    PARTITION BY list(l_orderkey) (
    PARTITION p1 VALUES in ('1'),
    PARTITION p2 VALUES in ('2'),
    PARTITION p3 VALUES in ('3')
    )
    DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    insert into orders_inner_1 values 
    (2, 1, 'o', 99.5, 'a', 'b', 1, 'yy', '2023-10-17'),
    (1, null, 'k', 109.2, 'c','d',2, 'mm', '2023-10-17'),
    (3, 3, null, 99.5, 'a', 'b', 1, 'yy', '2023-10-19'),
    (1, 2, 'o', null, 'a', 'b', 1, 'yy', '2023-10-20'),
    (2, 3, 'k', 109.2, null,'d',2, 'mm', '2023-10-21'),
    (3, 1, 'o', 99.5, 'a', null, 1, 'yy', '2023-10-22'),
    (1, 3, 'k', 99.5, 'a', 'b', null, 'yy', '2023-10-19'),
    (2, 1, 'o', 109.2, 'c','d',2, null, '2023-10-18'),
    (3, 2, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-17'),
    (4, 5, 'o', 99.5, 'a', 'b', 1, 'yy', '2023-10-19'); 
    """

    sql """
    insert into lineitem_inner_1 values 
    (2, 1, 2, 3, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (1, null, 3, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (3, 3, null, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
    (1, 2, 3, null, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (2, 3, 2, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-18'),
    (3, 1, 1, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', null, 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
    (1, 3, 2, 2, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17');
    """

    qt_nest_exprid_replace """
    select l_shipdate, l_orderkey, t.O_ORDERDATE, t.o_orderkey,
            count(t.O_ORDERDATE) over (partition by lineitem_inner_1.l_orderkey order by lineitem_inner_1.l_orderkey) as window_count
    from lineitem_inner_1
    inner join (select O_ORDERDATE, o_orderkey, count(O_ORDERDATE) over (partition by O_ORDERDATE order by o_orderkey ) from orders_inner_1 where o_orderkey=2 group by O_ORDERDATE, o_orderkey) as t
    on lineitem_inner_1.l_orderkey = t.o_orderkey
    where t.o_orderkey=2
    group by l_shipdate, l_orderkey, t.O_ORDERDATE, t.o_orderkey
    order by 1,2,3,4,5
    """
    sql "drop table if exists test1"
    sql "drop table if exists test2"
    sql "create table test1(a int, b int) distributed by hash(a) properties('replication_num'='1');"
    sql "insert into test1 values(1,1),(2,1),(3,1);"
    sql "create table test2(a int, b int) distributed by hash(a) properties('replication_num'='1');"
    sql "insert into test2 values(1,105),(2,105);"
    qt_full_join_uniform_should_not_eliminate_group_by_key "select t2.b,t1.b from test1 t1 full join (select * from test2 where b=105)  t2 on t1.a=t2.a group by t2.b,t1.b order by 1,2;"
    qt_full2 "select t2.b,t1.b from (select * from test2 where b=105)  t1  full join test1 t2 on t1.a=t2.a group by t2.b,t1.b order by 1,2;"

    qt_left_join_right_side_should_not_eliminate_group_by_key "select t2.b,t1.b from test1 t1 left join (select * from test2 where b=105)  t2 on t1.a=t2.a group by t2.b,t1.b order by 1,2;"
    qt_left_join_left_side_should_eliminate_group_by_key "select t2.b,t1.b from test1 t1 left join (select * from test2 where b=105)  t2 on t1.a=t2.a where t1.b=1 group by t2.b,t1.b order by 1,2;"

    qt_right_join_left_side_should_not_eliminate_group_by_key "select t2.b,t1.b from (select * from test2 where b=105)  t1  right join test1 t2 on t1.a=t2.a group by t2.b,t1.b order by 1,2;"
    qt_right_join_right_side_should_eliminate_group_by_key "select t2.b,t1.b from (select * from test2 where b=105)  t1  right join test1 t2 on t1.a=t2.a where t2.b=1 group by t2.b,t1.b order by 1,2;"

    qt_left_semi_left_side "select t1.b from test1 t1 left semi join (select * from test2 where b=105)  t2 on t1.a=t2.a where t1.b=1 group by t1.b,t1.a order by 1;"
    qt_left_anti_left_side "select t1.b from test1 t1 left anti join (select * from test2 where b=105)  t2 on t1.a=t2.a where t1.b=1 group by t1.b,t1.a order by 1;"
    qt_right_semi_right_side "select t2.b from test1 t1 right semi join (select * from test2 where b=105)  t2 on t1.a=t2.a  group by t2.b,t2.a order by 1;"
    qt_right_anti_right_side "select t2.b from test1 t1 right anti join (select * from test2 where b=105)  t2 on t1.a=t2.a  group by t2.b,t2.a order by 1;"
}