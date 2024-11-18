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

suite("create_view_use_mv") {
    sql "drop table if exists orders"
    sql """create table orders ( o_orderkey bigint null, o_custkey int null, o_orderstatus varchar(1) null,
            o_totalprice decimal(15,2) null, o_orderpriority varchar(15) null, o_clerk varchar(15) null, o_shippriority int null,
            o_comment varchar(79) null, o_orderdate date not null) engine=olap duplicate key(o_orderkey,o_custkey)
    comment 'olap' distributed by hash(o_orderkey) buckets 96 properties("replication_num"="1")"""

    sql "drop table if exists lineitem"
    sql """create table lineitem (
            l_orderkey bigint null, l_linenumber int null, l_partkey int null, l_suppkey int null, l_quantity decimal(15,2) null,l_extendedprice decimal(15,2) null,
            l_discount decimal(15,2) null, l_tax decimal(15,2) null, l_returnflag varchar(1) null, l_linestatus varchar(1) null, l_commitdate date null, l_receiptdate date null,
            l_shipnstruct varchar(25) null, l_shipmode varchar(10) null, l_comment varchar(44) null,l_shipdate date not null) engine=olap
    duplicate key(l_orderkey, l_linenumber,l_partkey, l_suppkey)
    distributed by hash(l_orderkey) buckets 96
    properties("replication_num"="1");"""

    sql """insert into orders values (null, 1, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-17'),(1, null, 'o', 109.2, 'c','d',2, 'mm', '2023-10-17'),
    (3, 3, null, 99.5, 'a', 'b', 1, 'yy', '2023-10-19'),
    (1, 2, 'o', null, 'a', 'b', 1, 'yy', '2023-10-20'),
    (2, 3, 'k', 109.2, null,'d',2, 'mm', '2023-10-21'),
    (3, 1, 'k', 99.5, 'a', null, 1, 'yy', '2023-10-22'),
    (1, 3, 'o', 99.5, 'a', 'b', null, 'yy', '2023-10-19'),
    (2, 1, 'o', 109.2, 'c','d',2, null, '2023-10-18'),
    (3, 2, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-17'),
    (4, 5, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-19');"""

    sql """insert into lineitem values(null, 1, 2, 3, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),(1, null, 3, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (3, 3, null, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
    (1, 2, 3, null, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (2, 3, 2, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-18'),
    (3, 1, 1, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', null, 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
    (1, 3, 2, 2, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17');"""

    createMV("""
    CREATE MATERIALIZED VIEW t_mv_mv AS select
    o_orderkey,
    sum(o_totalprice) as sum_total,
    max(o_totalprice) as max_total,
    min(o_totalprice) as min_total,
    count(*) as count_all,
    bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1,
    bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2
    from orders group by o_orderkey
    """)

    sql "drop view if exists t_mv_v_view"
    sql """CREATE VIEW t_mv_v_view (k1, k2, k3, k4, k5, k6, v1, v2, v3, v4, v5, v6) as
            select `mv_o_orderkey` as k1, `mva_SUM__``o_totalprice``` as k2, `mva_MAX__``o_totalprice``` as k3,
    `mva_MIN__``o_totalprice``` as k4, `mva_SUM__CASE WHEN 1 IS NULL THEN 0 ELSE 1 END` as k5, l_orderkey,
    sum(`mv_o_orderkey`) as sum_total,
    max(`mv_o_orderkey`) as max_total,
    min(`mv_o_orderkey`) as min_total,
    count(`mva_SUM__``o_totalprice```) as count_all,
    bitmap_union(to_bitmap(case when mv_o_orderkey > 1 then `mva_SUM__``o_totalprice``` else null end)) cnt_1,
    bitmap_union(to_bitmap(case when mv_o_orderkey > 2 then `mva_MAX__``o_totalprice``` else null end)) as cnt_2
    from orders index t_mv_mv
    left join lineitem on lineitem.l_orderkey = orders.mv_o_orderkey
    group by
    k1, k2, k3, k4, k5, l_orderkey, mv_o_orderkey"""
    qt_create_view_from_mv "select * from t_mv_v_view order by 1"

    sql "drop view if exists v_for_alter"
    sql "CREATE VIEW v_for_alter AS SELECT * FROM orders"
    sql """ALTER VIEW v_for_alter as
            select `mv_o_orderkey` as k1, `mva_SUM__``o_totalprice``` as k2, `mva_MAX__``o_totalprice``` as k3,
    `mva_MIN__``o_totalprice``` as k4, `mva_SUM__CASE WHEN 1 IS NULL THEN 0 ELSE 1 END` as k5, l_orderkey,
    sum(`mv_o_orderkey`) as sum_total,
    max(`mv_o_orderkey`) as max_total,
    min(`mv_o_orderkey`) as min_total,
    count(`mva_SUM__``o_totalprice```) as count_all,
    bitmap_union(to_bitmap(case when mv_o_orderkey > 1 then `mva_SUM__``o_totalprice``` else null end)) cnt_1,
    bitmap_union(to_bitmap(case when mv_o_orderkey > 2 then `mva_MAX__``o_totalprice``` else null end)) as cnt_2
    from orders index t_mv_mv
    left join lineitem on lineitem.l_orderkey = orders.mv_o_orderkey
    group by
    k1, k2, k3, k4, k5, l_orderkey, mv_o_orderkey"""
    qt_alter_view_from_mv "select * from v_for_alter order by 1"
}