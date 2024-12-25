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

/*
This file is used specifically to test the presence of AGGs under joins.
 */
suite("dimension_2_join_agg_replenish") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"

    sql """
    drop table if exists orders
    """

    sql """CREATE TABLE `orders` (
      `o_orderkey` BIGINT NULL,
      `o_custkey` INT NULL,
      `o_orderstatus` VARCHAR(1) NULL,
      `o_totalprice` DECIMAL(15, 2)  NULL,
      `o_orderpriority` VARCHAR(15) NULL,
      `o_clerk` VARCHAR(15) NULL,
      `o_shippriority` INT NULL,
      `o_comment` VARCHAR(79) NULL,
      `o_orderdate` DATE not NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`o_orderkey`, `o_custkey`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    drop table if exists lineitem
    """

    sql """CREATE TABLE `lineitem` (
      `l_orderkey` BIGINT NULL,
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
      `l_shipdate` DATE not NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(l_orderkey, l_linenumber, l_partkey, l_suppkey )
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql"""
    insert into orders values 
    (null, 1, 'ok', 99.5, 'a', 'b', 1, 'yy', '2023-10-17'),
    (1, null, 'ok', 109.2, 'c','d',2, 'mm', '2023-10-17'),
    (3, 3, null, 99.5, 'a', 'b', 1, 'yy', '2023-10-19'),
    (1, 2, 'ok', null, 'a', 'b', 1, 'yy', '2023-10-20'),
    (2, 3, 'ok', 109.2, null,'d',2, 'mm', '2023-10-21'),
    (3, 1, 'ok', 99.5, 'a', null, 1, 'yy', '2023-10-22'),
    (1, 3, 'ok', 99.5, 'a', 'b', null, 'yy', '2023-10-19'),
    (2, 1, 'ok', 109.2, 'c','d',2, null, '2023-10-18'),
    (3, 2, 'ok', 99.5, 'a', 'b', 1, 'yy', '2023-10-17'),
    (4, 5, 'ok', 99.5, 'a', 'b', 1, 'yy', '2023-10-19');
    """

    sql """
    insert into lineitem values 
    (null, 1, 2, 3, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (1, null, 3, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (3, 3, null, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
    (1, 2, 3, null, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (2, 3, 2, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-18'),
    (3, 1, 1, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', null, 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
    (1, 3, 2, 2, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17');
    """

    sql """analyze table orders with sync;"""
    sql """analyze table lineitem with sync;"""

    sql """alter table orders modify column o_comment set stats ('row_count'='10');"""
    sql """alter table lineitem modify column l_comment set stats ('row_count'='7');"""

    def compare_res = { def stmt ->
        sql "SET enable_materialized_view_rewrite=false"
        def origin_res = sql stmt
        logger.info("origin_res: " + origin_res)
        sql "SET enable_materialized_view_rewrite=true"
        def mv_origin_res = sql stmt
        logger.info("mv_origin_res: " + mv_origin_res)
        assertTrue((mv_origin_res == [] && origin_res == []) || (mv_origin_res.size() == origin_res.size()))
        for (int row = 0; row < mv_origin_res.size(); row++) {
            assertTrue(mv_origin_res[row].size() == origin_res[row].size())
            for (int col = 0; col < mv_origin_res[row].size(); col++) {
                assertTrue(mv_origin_res[row][col] == origin_res[row][col])
            }
        }
    }

    // left join + agg (function + group by + +-*/ + filter)
    def left_mv_stmt_1 = """select t1.o_orderdate, t1.o_shippriority, t1.o_orderkey 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_shippriority from orders group by o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_shippriority) as t1
            left join lineitem on lineitem.l_orderkey = t1.o_orderkey
            group by
            t1.o_orderdate, t1.o_shippriority, t1.o_orderkey"""

    def left_mv_stmt_2 = """select t1.o_orderdate, t1.o_orderkey, t1.col1 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1 from orders group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            left join lineitem on lineitem.l_orderkey = t1.o_orderkey
            group by 
            t1.o_orderdate,t1.o_orderkey, t1.col1 
            """
    def left_mv_stmt_3 = """select t1.o_orderdate, t1.o_orderkey, t1.col1 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1 from orders where o_orderkey > 1 + 1 group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            left join lineitem on lineitem.l_orderkey = t1.o_orderkey
            group by 
            t1.o_orderdate,t1.o_orderkey, t1.col1 
            """
    def left_mv_stmt_4 = """select t1.o_orderdate, (t1.o_orderkey+t1.col2) as col3, t1.col1 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1, count(*) as col2 from orders where o_orderkey >= 1 + 1 group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            left join lineitem on lineitem.l_orderkey = t1.o_orderkey
            group by 
            t1.o_orderdate,col3, t1.col1 
            """
    def left_mv_stmt_5 = """select t1.sum_total, max_total+min_total as col3, count_all 
            from (select o_orderkey, sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            sum(o_totalprice) + count(*) as col5, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as cnt_1,
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
            from orders where o_orderkey >= 1 + 1 group by o_orderkey) as t1
            left join lineitem on lineitem.l_orderkey = t1.o_orderkey 
            group by t1.sum_total, col3, count_all       
            """

    def left_mv_stmt_6 = """select t1.l_shipdate, t1.l_quantity, t1.l_orderkey 
            from orders 
            left join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate from lineitem group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = orders.o_orderkey
            group by
            t1.l_shipdate, t1.l_quantity, t1.l_orderkey """

    def left_mv_stmt_7 = """select t1.l_shipdate, t1.col1, t1.l_orderkey 
            from orders 
            left join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1 from lineitem group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = orders.o_orderkey
            group by
            t1.l_shipdate, t1.col1, t1.l_orderkey 
            """
    def left_mv_stmt_8 = """select t1.l_shipdate, t1.col1, t1.l_orderkey 
            from orders 
            left join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1 from lineitem where l_orderkey > 1 + 1 group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = orders.o_orderkey
            group by
            t1.l_shipdate, t1.col1, t1.l_orderkey
            """
    def left_mv_stmt_9 = """select t1.l_shipdate, t1.col1, t1.l_orderkey + t1.col2 as col3
            from orders 
            left join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1, count(*) as col2 from lineitem where l_orderkey > 1 + 1 group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = orders.o_orderkey
            group by
            t1.l_shipdate, t1.col1, col3
            """
    def left_mv_stmt_10 = """select t1.sum_total, max_total+min_total as col3, count_all  
            from orders 
            left join (select l_orderkey, sum(l_quantity) as sum_total,
            max(l_quantity) as max_total,
            min(l_quantity) as min_total,
            count(*) as count_all,
            sum(l_quantity) + count(*) as col5, 
            bitmap_union(to_bitmap(case when l_quantity > 1 and l_orderkey IN (1, 3) then l_partkey else null end)) as cnt_1,
            bitmap_union(to_bitmap(case when l_quantity > 2 and l_orderkey IN (2) then l_partkey else null end)) as cnt_2  
            from lineitem where l_orderkey > 1 + 1 group by l_orderkey) as t1 
            on t1.l_orderkey = orders.o_orderkey
            group by
            t1.sum_total, col3, count_all
            """

    // inner join + agg (function + group by + +-*/ + filter)
    def inner_mv_stmt_1 = """select t1.o_orderdate, t1.o_shippriority, t1.o_orderkey 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_shippriority from orders group by o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_shippriority) as t1
            inner join lineitem on lineitem.l_orderkey = t1.o_orderkey
            group by
            t1.o_orderdate, t1.o_shippriority, t1.o_orderkey"""

    def inner_mv_stmt_2 = """select t1.o_orderdate, t1.o_orderkey, t1.col1 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1 from orders group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            inner join lineitem on lineitem.l_orderkey = t1.o_orderkey
            group by 
            t1.o_orderdate,t1.o_orderkey, t1.col1 
            """
    def inner_mv_stmt_3 = """select t1.o_orderdate, t1.o_orderkey, t1.col1 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1 from orders where o_orderkey > 1 + 1 group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            inner join lineitem on lineitem.l_orderkey = t1.o_orderkey
            group by 
            t1.o_orderdate,t1.o_orderkey, t1.col1 
            """
    def inner_mv_stmt_4 = """select t1.o_orderdate, (t1.o_orderkey+t1.col2) as col3, t1.col1 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1, count(*) as col2 from orders where o_orderkey >= 1 + 1 group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            inner join lineitem on lineitem.l_orderkey = t1.o_orderkey
            group by 
            t1.o_orderdate,col3, t1.col1 
            """
    def inner_mv_stmt_5 = """select t1.sum_total, max_total+min_total as col3, count_all 
            from (select o_orderkey, sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            sum(o_totalprice) + count(*) as col5, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as cnt_1,
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
            from orders where o_orderkey >= 1 + 1 group by o_orderkey) as t1
            inner join lineitem on lineitem.l_orderkey = t1.o_orderkey 
            group by t1.sum_total, col3, count_all       
            """

    def inner_mv_stmt_6 = """select t1.l_shipdate, t1.l_quantity, t1.l_orderkey 
            from orders 
            inner join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate from lineitem group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = orders.o_orderkey
            group by
            t1.l_shipdate, t1.l_quantity, t1.l_orderkey """

    def inner_mv_stmt_7 = """select t1.l_shipdate, t1.col1, t1.l_orderkey 
            from orders 
            inner join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1 from lineitem group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = orders.o_orderkey
            group by
            t1.l_shipdate, t1.col1, t1.l_orderkey 
            """
    def inner_mv_stmt_8 = """select t1.l_shipdate, t1.col1, t1.l_orderkey 
            from orders 
            inner join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1 from lineitem where l_orderkey > 1 + 1 group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = orders.o_orderkey
            group by
            t1.l_shipdate, t1.col1, t1.l_orderkey
            """
    def inner_mv_stmt_9 = """select t1.l_shipdate, t1.col1, t1.l_orderkey + t1.col2 as col3
            from orders 
            inner join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1, count(*) as col2 from lineitem where l_orderkey > 1 + 1 group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = orders.o_orderkey
            group by
            t1.l_shipdate, t1.col1, col3
            """
    def inner_mv_stmt_10 = """select t1.sum_total, max_total+min_total as col3, count_all  
            from orders 
            inner join (select l_orderkey, sum(l_quantity) as sum_total,
            max(l_quantity) as max_total,
            min(l_quantity) as min_total,
            count(*) as count_all,
            sum(l_quantity) + count(*) as col5, 
            bitmap_union(to_bitmap(case when l_quantity > 1 and l_orderkey IN (1, 3) then l_partkey else null end)) as cnt_1,
            bitmap_union(to_bitmap(case when l_quantity > 2 and l_orderkey IN (2) then l_partkey else null end)) as cnt_2  
            from lineitem where l_orderkey > 1 + 1 group by l_orderkey) as t1 
            on t1.l_orderkey = orders.o_orderkey
            group by
            t1.sum_total, col3, count_all
            """

    // right join + agg (function + group by + +-*/ + filter)
    def right_mv_stmt_1 = """select t1.o_orderdate, t1.o_shippriority, t1.o_orderkey 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_shippriority from orders group by o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_shippriority) as t1
            right join lineitem on lineitem.l_orderkey = t1.o_orderkey
            group by
            t1.o_orderdate, t1.o_shippriority, t1.o_orderkey"""

    def right_mv_stmt_2 = """select t1.o_orderdate, t1.o_orderkey, t1.col1 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1 from orders group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            right join lineitem on lineitem.l_orderkey = t1.o_orderkey
            group by 
            t1.o_orderdate,t1.o_orderkey, t1.col1 
            """
    def right_mv_stmt_3 = """select t1.o_orderdate, t1.o_orderkey, t1.col1 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1 from orders where o_orderkey > 1 + 1 group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            right join lineitem on lineitem.l_orderkey = t1.o_orderkey
            group by 
            t1.o_orderdate,t1.o_orderkey, t1.col1 
            """
    def right_mv_stmt_4 = """select t1.o_orderdate, (t1.o_orderkey+t1.col2) as col3, t1.col1 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1, count(*) as col2 from orders where o_orderkey >= 1 + 1 group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            right join lineitem on lineitem.l_orderkey = t1.o_orderkey
            group by 
            t1.o_orderdate,col3, t1.col1 
            """
    def right_mv_stmt_5 = """select t1.sum_total, max_total+min_total as col3, count_all 
            from (select o_orderkey, sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            sum(o_totalprice) + count(*) as col5, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as cnt_1,
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
            from orders where o_orderkey >= 1 + 1 group by o_orderkey) as t1
            right join lineitem on lineitem.l_orderkey = t1.o_orderkey 
            group by t1.sum_total, col3, count_all       
            """

    def right_mv_stmt_6 = """select t1.l_shipdate, t1.l_quantity, t1.l_orderkey 
            from orders 
            right join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate from lineitem group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = orders.o_orderkey
            group by
            t1.l_shipdate, t1.l_quantity, t1.l_orderkey """

    def right_mv_stmt_7 = """select t1.l_shipdate, t1.col1, t1.l_orderkey 
            from orders 
            right join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1 from lineitem group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = orders.o_orderkey
            group by
            t1.l_shipdate, t1.col1, t1.l_orderkey 
            """
    def right_mv_stmt_8 = """select t1.l_shipdate, t1.col1, t1.l_orderkey 
            from orders 
            right join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1 from lineitem where l_orderkey > 1 + 1 group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = orders.o_orderkey
            group by
            t1.l_shipdate, t1.col1, t1.l_orderkey
            """
    def right_mv_stmt_9 = """select t1.l_shipdate, t1.col1, t1.l_orderkey + t1.col2 as col3
            from orders 
            right join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1, count(*) as col2 from lineitem where l_orderkey > 1 + 1 group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = orders.o_orderkey
            group by
            t1.l_shipdate, t1.col1, col3
            """
    def right_mv_stmt_10 = """select t1.sum_total, max_total+min_total as col3, count_all  
            from orders 
            right join (select l_orderkey, sum(l_quantity) as sum_total,
            max(l_quantity) as max_total,
            min(l_quantity) as min_total,
            count(*) as count_all,
            sum(l_quantity) + count(*) as col5, 
            bitmap_union(to_bitmap(case when l_quantity > 1 and l_orderkey IN (1, 3) then l_partkey else null end)) as cnt_1,
            bitmap_union(to_bitmap(case when l_quantity > 2 and l_orderkey IN (2) then l_partkey else null end)) as cnt_2  
            from lineitem where l_orderkey > 1 + 1 group by l_orderkey) as t1 
            on t1.l_orderkey = orders.o_orderkey
            group by
            t1.sum_total, col3, count_all
            """

    // Todo: full/cross/semi/anti join + agg (function + group by + +-*/ + filter)
    def full_mv_stmt_1 = """select t1.o_orderdate, t1.o_shippriority, t1.o_orderkey 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_shippriority from orders group by o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_shippriority) as t1
            full join lineitem on lineitem.l_orderkey = t1.o_orderkey
            group by
            t1.o_orderdate, t1.o_shippriority, t1.o_orderkey"""

    def full_mv_stmt_2 = """select t1.o_orderdate, t1.o_orderkey, t1.col1 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1 from orders group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            full join lineitem on lineitem.l_orderkey = t1.o_orderkey
            group by 
            t1.o_orderdate,t1.o_orderkey, t1.col1 
            """
    def full_mv_stmt_3 = """select t1.o_orderdate, t1.o_orderkey, t1.col1 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1 from orders where o_orderkey > 1 + 1 group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            full join lineitem on lineitem.l_orderkey = t1.o_orderkey
            group by 
            t1.o_orderdate,t1.o_orderkey, t1.col1 
            """
    def full_mv_stmt_4 = """select t1.o_orderdate, (t1.o_orderkey+t1.col2) as col3, t1.col1 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1, count(*) as col2 from orders where o_orderkey >= 1 + 1 group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            full join lineitem on lineitem.l_orderkey = t1.o_orderkey
            group by 
            t1.o_orderdate,col3, t1.col1 
            """
    def full_mv_stmt_5 = """select t1.sum_total, max_total+min_total as col3, count_all 
            from (select o_orderkey, sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            sum(o_totalprice) + count(*) as col5, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as cnt_1,
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
            from orders where o_orderkey >= 1 + 1 group by o_orderkey) as t1
            full join lineitem on lineitem.l_orderkey = t1.o_orderkey 
            group by t1.sum_total, col3, count_all       
            """

    def full_mv_stmt_6 = """select t1.l_shipdate, t1.l_quantity, t1.l_orderkey 
            from orders 
            full join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate from lineitem group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = orders.o_orderkey
            group by
            t1.l_shipdate, t1.l_quantity, t1.l_orderkey """

    def full_mv_stmt_7 = """select t1.l_shipdate, t1.col1, t1.l_orderkey 
            from orders 
            full join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1 from lineitem group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = orders.o_orderkey
            group by
            t1.l_shipdate, t1.col1, t1.l_orderkey 
            """
    def full_mv_stmt_8 = """select t1.l_shipdate, t1.col1, t1.l_orderkey 
            from orders 
            full join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1 from lineitem where l_orderkey > 1 + 1 group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = orders.o_orderkey
            group by
            t1.l_shipdate, t1.col1, t1.l_orderkey
            """
    def full_mv_stmt_9 = """select t1.l_shipdate, t1.col1, t1.l_orderkey + t1.col2 as col3
            from orders 
            full join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1, count(*) as col2 from lineitem where l_orderkey > 1 + 1 group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = orders.o_orderkey
            group by
            t1.l_shipdate, t1.col1, col3
            """
    def full_mv_stmt_10 = """select t1.sum_total, max_total+min_total as col3, count_all  
            from orders 
            full join (select l_orderkey, sum(l_quantity) as sum_total,
            max(l_quantity) as max_total,
            min(l_quantity) as min_total,
            count(*) as count_all,
            sum(l_quantity) + count(*) as col5, 
            bitmap_union(to_bitmap(case when l_quantity > 1 and l_orderkey IN (1, 3) then l_partkey else null end)) as cnt_1,
            bitmap_union(to_bitmap(case when l_quantity > 2 and l_orderkey IN (2) then l_partkey else null end)) as cnt_2  
            from lineitem where l_orderkey > 1 + 1 group by l_orderkey) as t1 
            on t1.l_orderkey = orders.o_orderkey
            group by
            t1.sum_total, col3, count_all
            """

    def cross_mv_stmt_1 = """select t1.o_orderdate, t1.o_shippriority, t1.o_orderkey 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_shippriority from orders group by o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_shippriority) as t1
            cross join lineitem 
            group by
            t1.o_orderdate, t1.o_shippriority, t1.o_orderkey"""

    def cross_mv_stmt_2 = """select t1.o_orderdate, t1.o_orderkey, t1.col1 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1 from orders group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            cross join lineitem 
            group by 
            t1.o_orderdate,t1.o_orderkey, t1.col1 
            """
    def cross_mv_stmt_3 = """select t1.o_orderdate, t1.o_orderkey, t1.col1 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1 from orders where o_orderkey > 1 + 1 group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            cross join lineitem 
            group by 
            t1.o_orderdate,t1.o_orderkey, t1.col1 
            """
    def cross_mv_stmt_4 = """select t1.o_orderdate, (t1.o_orderkey+t1.col2) as col3, t1.col1 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1, count(*) as col2 from orders where o_orderkey >= 1 + 1 group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            cross join lineitem 
            group by 
            t1.o_orderdate,col3, t1.col1 
            """
    def cross_mv_stmt_5 = """select t1.sum_total, max_total+min_total as col3, count_all 
            from (select o_orderkey, sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            sum(o_totalprice) + count(*) as col5, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as cnt_1,
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
            from orders where o_orderkey >= 1 + 1 group by o_orderkey) as t1
            cross join lineitem 
            group by t1.sum_total, col3, count_all       
            """

    def cross_mv_stmt_6 = """select t1.l_shipdate, t1.l_quantity, t1.l_orderkey 
            from orders 
            cross join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate from lineitem group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            group by
            t1.l_shipdate, t1.l_quantity, t1.l_orderkey """

    def cross_mv_stmt_7 = """select t1.l_shipdate, t1.col1, t1.l_orderkey 
            from orders 
            cross join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1 from lineitem group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            group by
            t1.l_shipdate, t1.col1, t1.l_orderkey 
            """
    def cross_mv_stmt_8 = """select t1.l_shipdate, t1.col1, t1.l_orderkey 
            from orders 
            cross join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1 from lineitem where l_orderkey > 1 + 1 group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            group by
            t1.l_shipdate, t1.col1, t1.l_orderkey
            """
    def cross_mv_stmt_9 = """select t1.l_shipdate, t1.col1, t1.l_orderkey + t1.col2 as col3
            from orders 
            cross join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1, count(*) as col2 from lineitem where l_orderkey > 1 + 1 group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            group by
            t1.l_shipdate, t1.col1, col3
            """
    def cross_mv_stmt_10 = """select t1.sum_total, max_total+min_total as col3, count_all  
            from orders 
            cross join (select l_orderkey, sum(l_quantity) as sum_total,
            max(l_quantity) as max_total,
            min(l_quantity) as min_total,
            count(*) as count_all,
            sum(l_quantity) + count(*) as col5, 
            bitmap_union(to_bitmap(case when l_quantity > 1 and l_orderkey IN (1, 3) then l_partkey else null end)) as cnt_1,
            bitmap_union(to_bitmap(case when l_quantity > 2 and l_orderkey IN (2) then l_partkey else null end)) as cnt_2  
            from lineitem where l_orderkey > 1 + 1 group by l_orderkey) as t1 
            group by
            t1.sum_total, col3, count_all
            """

    def left_semi_mv_stmt_1 = """select t1.o_orderdate, t1.o_shippriority, t1.o_orderkey 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_shippriority from orders group by o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_shippriority) as t1
            left semi join lineitem on lineitem.l_orderkey = t1.o_orderkey
            group by
            t1.o_orderdate, t1.o_shippriority, t1.o_orderkey"""

    def left_semi_mv_stmt_2 = """select t1.o_orderdate, t1.o_orderkey, t1.col1 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1 from orders group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            left semi join lineitem on lineitem.l_orderkey = t1.o_orderkey
            group by 
            t1.o_orderdate,t1.o_orderkey, t1.col1 
            """
    def left_semi_mv_stmt_3 = """select t1.o_orderdate, t1.o_orderkey, t1.col1 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1 from orders where o_orderkey > 1 + 1 group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            left semi join lineitem on lineitem.l_orderkey = t1.o_orderkey
            group by 
            t1.o_orderdate,t1.o_orderkey, t1.col1 
            """
    def left_semi_mv_stmt_4 = """select t1.o_orderdate, (t1.o_orderkey+t1.col2) as col3, t1.col1 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1, count(*) as col2 from orders where o_orderkey >= 1 + 1 group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            left semi join lineitem on lineitem.l_orderkey = t1.o_orderkey
            group by 
            t1.o_orderdate,col3, t1.col1 
            """
    def left_semi_mv_stmt_5 = """select t1.sum_total, max_total+min_total as col3, count_all 
            from (select o_orderkey, sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            sum(o_totalprice) + count(*) as col5, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as cnt_1,
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
            from orders where o_orderkey >= 1 + 1 group by o_orderkey) as t1
            left semi join lineitem on lineitem.l_orderkey = t1.o_orderkey 
            group by t1.sum_total, col3, count_all       
            """

    def left_semi_mv_stmt_6 = """select  orders.o_orderdate, orders.o_shippriority, orders.o_orderkey 
            from orders 
            left semi join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate from lineitem group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = orders.o_orderkey
            group by
            orders.o_orderdate, orders.o_shippriority, orders.o_orderkey  """

    def left_semi_mv_stmt_7 = """select orders.o_orderdate, orders.o_shippriority, orders.o_orderkey 
            from orders 
            left semi join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1 from lineitem group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = orders.o_orderkey
            group by
            orders.o_orderdate, orders.o_shippriority, orders.o_orderkey 
            """
    def left_semi_mv_stmt_8 = """select orders.o_orderdate, orders.o_shippriority, orders.o_orderkey 
            from orders 
            left semi join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1 from lineitem where l_orderkey > 1 + 1 group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = orders.o_orderkey
            group by
            orders.o_orderdate, orders.o_shippriority, orders.o_orderkey 
            """
    def left_semi_mv_stmt_9 = """select orders.o_orderdate, orders.o_shippriority, orders.o_orderkey 
            from orders 
            left semi join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1, count(*) as col2 from lineitem where l_orderkey > 1 + 1 group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = orders.o_orderkey
            group by
            orders.o_orderdate, orders.o_shippriority, orders.o_orderkey 
            """
    def left_semi_mv_stmt_10 = """select orders.o_orderdate, orders.o_shippriority, orders.o_orderkey 
            from orders 
            left semi join (select l_orderkey, sum(l_quantity) as sum_total,
            max(l_quantity) as max_total,
            min(l_quantity) as min_total,
            count(*) as count_all,
            sum(l_quantity) + count(*) as col5, 
            bitmap_union(to_bitmap(case when l_quantity > 1 and l_orderkey IN (1, 3) then l_partkey else null end)) as cnt_1,
            bitmap_union(to_bitmap(case when l_quantity > 2 and l_orderkey IN (2) then l_partkey else null end)) as cnt_2  
            from lineitem where l_orderkey > 1 + 1 group by l_orderkey) as t1 
            on t1.l_orderkey = orders.o_orderkey
            group by
            orders.o_orderdate, orders.o_shippriority, orders.o_orderkey 
            """

    def right_semi_mv_stmt_1 = """select l_orderkey, l_shipdate, l_partkey 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_shippriority from orders group by o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_shippriority) as t1
            right semi join lineitem on lineitem.l_orderkey = t1.o_orderkey
            group by
            l_orderkey, l_shipdate, l_partkey """

    def right_semi_mv_stmt_2 = """select l_orderkey, l_shipdate, l_partkey 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1 from orders group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            right semi join lineitem on lineitem.l_orderkey = t1.o_orderkey
            group by 
            l_orderkey, l_shipdate, l_partkey 
            """
    def right_semi_mv_stmt_3 = """select l_orderkey, l_shipdate, l_partkey 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1 from orders where o_orderkey > 1 + 1 group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            right semi join lineitem on lineitem.l_orderkey = t1.o_orderkey
            group by 
            l_orderkey, l_shipdate, l_partkey 
            """
    def right_semi_mv_stmt_4 = """select l_orderkey, l_shipdate, l_partkey 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1, count(*) as col2 from orders where o_orderkey >= 1 + 1 group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            right semi join lineitem on lineitem.l_orderkey = t1.o_orderkey
            group by 
            l_orderkey, l_shipdate, l_partkey 
            """
    def right_semi_mv_stmt_5 = """select l_orderkey, l_shipdate, l_partkey 
            from (select o_orderkey, sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            sum(o_totalprice) + count(*) as col5, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as cnt_1,
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
            from orders where o_orderkey >= 1 + 1 group by o_orderkey) as t1
            right semi join lineitem on lineitem.l_orderkey = t1.o_orderkey 
            group by l_orderkey, l_shipdate, l_partkey 
            """

    def right_semi_mv_stmt_6 = """select t1.l_orderkey, t1.l_shipdate, t1.l_partkey 
            from orders 
            right semi join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate from lineitem group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = orders.o_orderkey
            group by
            t1.l_orderkey, t1.l_shipdate, t1.l_partkey  """

    def right_semi_mv_stmt_7 = """select t1.l_orderkey, t1.l_shipdate, t1.l_partkey 
            from orders 
            right semi join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1 from lineitem group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = orders.o_orderkey
            group by
            t1.l_orderkey, t1.l_shipdate, t1.l_partkey 
            """
    def right_semi_mv_stmt_8 = """select t1.l_orderkey, t1.l_shipdate, t1.l_partkey 
            from orders 
            right semi join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1 from lineitem where l_shipdate >= "2023-10-17" group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = orders.o_orderkey
            group by
            t1.l_orderkey, t1.l_shipdate, t1.l_partkey 
            """
    def right_semi_mv_stmt_9 = """select t1.l_orderkey, t1.l_shipdate, t1.l_partkey 
            from orders 
            right semi join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1, count(*) as col2 from lineitem where l_shipdate >= "2023-10-17" group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = orders.o_orderkey
            group by
            t1.l_orderkey, t1.l_shipdate, t1.l_partkey 
            """
    def right_semi_mv_stmt_10 = """select t1.sum_total, max_total+min_total as col3, count_all  
            from orders 
            right semi join (select l_orderkey, sum(l_quantity) as sum_total,
            max(l_quantity) as max_total,
            min(l_quantity) as min_total,
            count(*) as count_all,
            sum(l_quantity) + count(*) as col5, 
            bitmap_union(to_bitmap(case when l_quantity > 1 and l_orderkey IN (1, 3) then l_partkey else null end)) as cnt_1,
            bitmap_union(to_bitmap(case when l_quantity > 2 and l_orderkey IN (2) then l_partkey else null end)) as cnt_2  
            from lineitem where l_shipdate >= "2023-10-17" group by l_orderkey) as t1 
            on t1.l_orderkey = orders.o_orderkey
            group by
            t1.sum_total, col3, count_all 
            """

    def left_anti_mv_stmt_1 = """select t1.o_orderdate, t1.o_shippriority, t1.o_orderkey 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_shippriority from orders group by o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_shippriority) as t1
            left anti join lineitem on lineitem.l_orderkey = t1.o_orderkey
            group by
            t1.o_orderdate, t1.o_shippriority, t1.o_orderkey"""

    def left_anti_mv_stmt_2 = """select t1.o_orderdate, t1.o_orderkey, t1.col1 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1 from orders group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            left anti join lineitem on lineitem.l_orderkey = t1.o_orderkey
            group by 
            t1.o_orderdate,t1.o_orderkey, t1.col1 
            """
    def left_anti_mv_stmt_3 = """select t1.o_orderdate, t1.o_orderkey, t1.col1 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1 from orders where o_orderkey > 1 + 1 group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            left anti join lineitem on lineitem.l_orderkey = t1.o_orderkey
            group by 
            t1.o_orderdate,t1.o_orderkey, t1.col1 
            """
    def left_anti_mv_stmt_4 = """select t1.o_orderdate, (t1.o_orderkey+t1.col2) as col3, t1.col1 
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1, count(*) as col2 from orders where o_orderkey >= 1 + 1 group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            left anti join lineitem on lineitem.l_orderkey = t1.o_orderkey
            group by 
            t1.o_orderdate,col3, t1.col1 
            """
    def left_anti_mv_stmt_5 = """select t1.sum_total, max_total+min_total as col3, count_all 
            from (select o_orderkey, sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            sum(o_totalprice) + count(*) as col5, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as cnt_1,
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
            from orders where o_orderkey >= 1 + 1 group by o_orderkey) as t1
            left anti join lineitem on lineitem.l_orderkey = t1.o_orderkey 
            group by t1.sum_total, col3, count_all       
            """

    def left_anti_mv_stmt_6 = """select orders.o_orderdate, orders.o_shippriority, orders.o_orderkey 
            from orders 
            left anti join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate from lineitem group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = orders.o_orderkey
            group by
            orders.o_orderdate, orders.o_shippriority, orders.o_orderkey"""

    def left_anti_mv_stmt_7 = """select orders.o_orderdate, orders.o_shippriority, orders.o_orderkey  
            from orders 
            left anti join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1 from lineitem group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = orders.o_orderkey
            group by
            orders.o_orderdate, orders.o_shippriority, orders.o_orderkey 
            """
    def left_anti_mv_stmt_8 = """select orders.o_orderdate, orders.o_shippriority, orders.o_orderkey  
            from orders 
            left anti join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1 from lineitem where l_orderkey > 1 + 1 group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = orders.o_orderkey
            group by
            orders.o_orderdate, orders.o_shippriority, orders.o_orderkey
            """
    def left_anti_mv_stmt_9 = """select orders.o_orderdate, orders.o_shippriority, orders.o_orderkey
            from orders 
            left anti join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1, count(*) as col2 from lineitem where l_orderkey > 1 + 1 group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = orders.o_orderkey
            group by
            orders.o_orderdate, orders.o_shippriority, orders.o_orderkey
            """
    def left_anti_mv_stmt_10 = """select orders.o_orderdate, orders.o_shippriority, orders.o_orderkey   
            from orders 
            left anti join (select l_orderkey, sum(l_quantity) as sum_total,
            max(l_quantity) as max_total,
            min(l_quantity) as min_total,
            count(*) as count_all,
            sum(l_quantity) + count(*) as col5, 
            bitmap_union(to_bitmap(case when l_quantity > 1 and l_orderkey IN (1, 3) then l_partkey else null end)) as cnt_1,
            bitmap_union(to_bitmap(case when l_quantity > 2 and l_orderkey IN (2) then l_partkey else null end)) as cnt_2  
            from lineitem where l_orderkey > 1 + 1 group by l_orderkey) as t1 
            on t1.l_orderkey = orders.o_orderkey
            group by
            orders.o_orderdate, orders.o_shippriority, orders.o_orderkey
            """

    def right_anti_mv_stmt_1 = """select l_orderkey, l_shipdate, l_partkey
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_shippriority from orders group by o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_shippriority) as t1
            right anti join lineitem on lineitem.l_orderkey = t1.o_orderkey
            group by
            l_orderkey, l_shipdate, l_partkey"""

    def right_anti_mv_stmt_2 = """select l_orderkey, l_shipdate, l_partkey
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1 from orders group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            right anti join lineitem on lineitem.l_orderkey = t1.o_orderkey
            group by 
            l_orderkey, l_shipdate, l_partkey
            """
    def right_anti_mv_stmt_3 = """select l_orderkey, l_shipdate, l_partkey
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1 from orders where o_orderkey > 1 + 1 group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            right anti join lineitem on lineitem.l_orderkey = t1.o_orderkey
            group by 
            l_orderkey, l_shipdate, l_partkey
            """
    def right_anti_mv_stmt_4 = """select l_orderkey, l_shipdate, l_partkey
            from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, sum(o_shippriority) as col1, count(*) as col2 from orders where o_orderkey >= 1 + 1 group by o_orderkey, o_custkey, o_orderstatus, o_orderdate) as t1
            right anti join lineitem on lineitem.l_orderkey = t1.o_orderkey
            group by 
            l_orderkey, l_shipdate, l_partkey
            """
    def right_anti_mv_stmt_5 = """select l_orderkey, l_shipdate, l_partkey
            from (select o_orderkey, sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            sum(o_totalprice) + count(*) as col5, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as cnt_1,
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
            from orders where o_orderkey >= 1 + 1 group by o_orderkey) as t1
            right anti join lineitem on lineitem.l_orderkey = t1.o_orderkey 
            group by l_orderkey, l_shipdate, l_partkey 
            """

    def right_anti_mv_stmt_6 = """select t1.l_shipdate, t1.l_quantity, t1.l_orderkey 
            from orders 
            right anti join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate from lineitem group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = orders.o_orderkey
            group by
            t1.l_shipdate, t1.l_quantity, t1.l_orderkey """

    def right_anti_mv_stmt_7 = """select t1.l_shipdate, t1.col1, t1.l_orderkey 
            from orders 
            right anti join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1 from lineitem group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = orders.o_orderkey
            group by
            t1.l_shipdate, t1.col1, t1.l_orderkey 
            """
    def right_anti_mv_stmt_8 = """select t1.l_shipdate, t1.col1, t1.l_orderkey 
            from orders 
            right anti join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1 from lineitem where l_shipdate >= "2023-10-17" group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = orders.o_orderkey
            group by
            t1.l_shipdate, t1.col1, t1.l_orderkey
            """
    def right_anti_mv_stmt_9 = """select t1.l_shipdate, t1.col1, t1.l_orderkey + t1.col2 as col3
            from orders 
            right anti join (select l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate, sum(l_quantity) as col1, count(*) as col2 from lineitem where l_shipdate >= "2023-10-17" group by l_orderkey, l_partkey, l_suppkey, l_quantity, l_shipdate) as t1 
            on t1.l_orderkey = orders.o_orderkey
            group by
            t1.l_shipdate, t1.col1, col3
            """
    def right_anti_mv_stmt_10 = """select t1.sum_total, max_total+min_total as col3, count_all  
            from orders 
            right anti join (select l_orderkey, sum(l_quantity) as sum_total,
            max(l_quantity) as max_total,
            min(l_quantity) as min_total,
            count(*) as count_all,
            sum(l_quantity) + count(*) as col5, 
            bitmap_union(to_bitmap(case when l_quantity > 1 and l_orderkey IN (1, 3) then l_partkey else null end)) as cnt_1,
            bitmap_union(to_bitmap(case when l_quantity > 2 and l_orderkey IN (2) then l_partkey else null end)) as cnt_2  
            from lineitem where l_shipdate >= "2023-10-17" group by l_orderkey) as t1 
            on t1.l_orderkey = orders.o_orderkey
            group by
            t1.sum_total, col3, count_all
            """

    def sql_list = [
            left_mv_stmt_1,left_mv_stmt_2,left_mv_stmt_3,left_mv_stmt_4,left_mv_stmt_5,left_mv_stmt_6,left_mv_stmt_7,left_mv_stmt_8,left_mv_stmt_9,left_mv_stmt_10,
                     right_mv_stmt_1,right_mv_stmt_2,right_mv_stmt_3,right_mv_stmt_4,right_mv_stmt_5,right_mv_stmt_6,right_mv_stmt_7,right_mv_stmt_8,right_mv_stmt_9,right_mv_stmt_10,
                     inner_mv_stmt_1,inner_mv_stmt_2,inner_mv_stmt_3,inner_mv_stmt_4,inner_mv_stmt_5,inner_mv_stmt_6,inner_mv_stmt_7,inner_mv_stmt_8,inner_mv_stmt_9,inner_mv_stmt_10,
                     full_mv_stmt_1,full_mv_stmt_2,full_mv_stmt_3,full_mv_stmt_4,full_mv_stmt_5,full_mv_stmt_6,full_mv_stmt_7,full_mv_stmt_8,full_mv_stmt_9,full_mv_stmt_10,

                    // agg pulled up, and the struct info is invalid. need to support aggregate merge then support following query rewriting
                    // left_semi_mv_stmt_1,left_semi_mv_stmt_2,left_semi_mv_stmt_3,left_semi_mv_stmt_4,left_semi_mv_stmt_5,left_semi_mv_stmt_6,left_semi_mv_stmt_7,left_semi_mv_stmt_8,left_semi_mv_stmt_9,left_semi_mv_stmt_10,
                    // left_anti_mv_stmt_1,left_anti_mv_stmt_2,left_anti_mv_stmt_3,left_anti_mv_stmt_4,left_anti_mv_stmt_5,left_anti_mv_stmt_6,left_anti_mv_stmt_7,left_anti_mv_stmt_8,left_anti_mv_stmt_9,left_anti_mv_stmt_10,
                    // right_semi_mv_stmt_1,right_semi_mv_stmt_2,right_semi_mv_stmt_3,right_semi_mv_stmt_4,right_semi_mv_stmt_5,right_semi_mv_stmt_6,right_semi_mv_stmt_7,right_semi_mv_stmt_8,right_semi_mv_stmt_9,right_semi_mv_stmt_10,
                    // right_anti_mv_stmt_1,right_anti_mv_stmt_2,right_anti_mv_stmt_3,right_anti_mv_stmt_4,right_anti_mv_stmt_5,right_anti_mv_stmt_6,right_anti_mv_stmt_7,right_anti_mv_stmt_8,right_anti_mv_stmt_9,right_anti_mv_stmt_10

                    // query rewrite by materialized view doesn't support cross join currently
                    // cross_mv_stmt_1,cross_mv_stmt_2,cross_mv_stmt_3,cross_mv_stmt_4,cross_mv_stmt_5,cross_mv_stmt_6,cross_mv_stmt_7,cross_mv_stmt_8,cross_mv_stmt_9,cross_mv_stmt_10,
    ]
    for (int i = 0; i < sql_list.size(); i++) {
        def origin_res = sql sql_list[i]
        assert (origin_res.size() > 0)
    }

    for (int i = 0; i < sql_list.size(); i++) {
        logger.info("sql_list current index: " + (i + 1))

        def mv_name = "mv_" + (i + 1)

        create_async_mv(db, mv_name, sql_list[i])

        mv_rewrite_success(sql_list[i], mv_name)

        compare_res(sql_list[i] + " order by 1,2,3")
        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name};"""
    }

}
