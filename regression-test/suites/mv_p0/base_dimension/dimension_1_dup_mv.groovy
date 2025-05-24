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
This suite is a one dimensional test case file.
 */
suite("partition_mv_rewrite_dimension_1_dup_mv", "partition_mv_rewrite_dimension") {
    String db = context.config.getDbNameByFile(context.file)
    String order_tb = "orders_dup"
    String lineitem_tb = "lineitem_dup"
    sql "use ${db}"

    sql """
    drop table if exists orders_dup
    """

    sql """CREATE TABLE `orders_dup` (
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
    drop table if exists lineitem_dup
    """

    sql """CREATE TABLE `lineitem_dup` (
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

    sql """
    insert into orders_dup values 
    (null, 1, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-17'),
    (1, null, 'o', 109.2, 'c','d',2, 'mm', '2023-10-17'),
    (3, 3, null, 99.5, 'a', 'b', 1, 'yy', '2023-10-19'),
    (1, 2, 'o', null, 'a', 'b', 1, 'yy', '2023-10-20'),
    (2, 3, 'k', 109.2, null,'d',2, 'mm', '2023-10-21'),
    (3, 1, 'k', 99.5, 'a', null, 1, 'yy', '2023-10-22'),
    (1, 3, 'o', 99.5, 'a', 'b', null, 'yy', '2023-10-19'),
    (2, 1, 'o', 109.2, 'c','d',2, null, '2023-10-18'),
    (3, 2, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-17'),
    (4, 5, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-19'); 
    """

    sql """
    insert into lineitem_dup values 
    (null, 1, 2, 3, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (1, null, 3, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (3, 3, null, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
    (1, 2, 3, null, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (2, 3, 2, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-18'),
    (3, 1, 1, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', null, 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
    (1, 3, 2, 2, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17');
    """


    sql """alter table orders_dup modify column o_orderkey set stats ('row_count'='50');"""
    sql """alter table orders_dup modify column o_custkey set stats ('row_count'='50');"""
    sql """alter table orders_dup modify column o_orderdate set stats ('row_count'='50');"""
    sql """alter table orders_dup modify column o_orderstatus set stats ('row_count'='50');"""
    sql """alter table orders_dup modify column o_totalprice set stats ('row_count'='50');"""
    sql """alter table orders_dup modify column o_orderpriority set stats ('row_count'='50');"""
    sql """alter table orders_dup modify column o_clerk set stats ('row_count'='50');"""
    sql """alter table orders_dup modify column o_shippriority set stats ('row_count'='50');"""
    sql """alter table orders_dup modify column o_comment set stats ('row_count'='50');"""

    sql """alter table lineitem_dup modify column l_orderkey set stats ('row_count'='50');"""
    sql """alter table lineitem_dup modify column l_linenumber set stats ('row_count'='50');"""
    sql """alter table lineitem_dup modify column l_partkey set stats ('row_count'='50');"""
    sql """alter table lineitem_dup modify column l_suppkey set stats ('row_count'='50');"""
    sql """alter table lineitem_dup modify column l_quantity set stats ('row_count'='50');"""
    sql """alter table lineitem_dup modify column l_extendedprice set stats ('row_count'='50');"""
    sql """alter table lineitem_dup modify column l_discount set stats ('row_count'='50');"""
    sql """alter table lineitem_dup modify column l_tax set stats ('row_count'='50');"""
    sql """alter table lineitem_dup modify column l_returnflag set stats ('row_count'='50');"""
    sql """alter table lineitem_dup modify column l_linestatus set stats ('row_count'='50');"""
    sql """alter table lineitem_dup modify column l_commitdate set stats ('row_count'='50');"""
    sql """alter table lineitem_dup modify column l_receiptdate set stats ('row_count'='50');"""
    sql """alter table lineitem_dup modify column l_shipinstruct set stats ('row_count'='50');"""
    sql """alter table lineitem_dup modify column l_shipmode set stats ('row_count'='50');"""
    sql """alter table lineitem_dup modify column l_comment set stats ('row_count'='50');"""
    sql """alter table lineitem_dup modify column l_shipdate set stats ('row_count'='50');"""


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

    // agg
    // agg + without group by + with agg function
    def agg_mv_name_1 = "agg_mv_name_1"
    sql """DROP MATERIALIZED VIEW IF EXISTS ${agg_mv_name_1} ON orders_dup;"""
    sql """DROP TABLE IF EXISTS ${agg_mv_name_1}"""
    def agg_mv_stmt_1 = """select
            o_orderkey,
            sum(O_TOTALPRICE) as sum_total,
            max(o_totalprice) as max_total, 
            min(o_totalprice) as min_total, 
            count(*) as count_all, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1, 
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
            from orders_dup
            group by o_orderkey"""
    createMV(getMVStmt(agg_mv_name_1, agg_mv_stmt_1))

    def agg_sql_1 = """select 
        count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) as cnt_1, 
        count(distinct case when O_SHIPPRIORITY > 2 and o_orderkey IN (2) then o_custkey else null end) as cnt_2, 
        sum(O_totalprice), 
        max(o_totalprice), 
        min(o_totalprice), 
        count(*) 
        from orders_dup
        """
    mv_rewrite_success(agg_sql_1, agg_mv_name_1)
    compare_res(agg_sql_1 + " order by 1,2,3,4,5,6")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${agg_mv_name_1} ON orders_dup;"""

    // agg + with group by + without agg function
    def agg_mv_name_2 = "agg_mv_name_2"
    def agg_mv_stmt_2 = """
        select o_orderdatE, O_SHIPPRIORITY, o_comment  
            from orders_dup 
            group by 
            o_orderdate, 
            o_shippriority, 
            o_comment  
        """
    createMV(getMVStmt(agg_mv_name_2, agg_mv_stmt_2))

    def agg_sql_2 = """select O_shippriority, o_commenT 
            from orders_dup 
            group by 
            o_shippriority, 
            o_comment 
        """
    mv_rewrite_success(agg_sql_2, agg_mv_name_2)
    sql """DROP MATERIALIZED VIEW IF EXISTS ${agg_mv_name_2} ON orders_dup;"""

    // agg + with group by + with agg function
    def agg_mv_name_3 = "agg_mv_name_3"
    def agg_mv_stmt_3 = """
        select o_orderdatE, o_shippriority, o_comment, 
            sum(o_totalprice) as sum_total, 
            max(o_totalpricE) as max_total, 
            min(o_totalprice) as min_total, 
            count(*) as count_all, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1, 
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
            from orders_dup 
            group by 
            o_orderdatE, 
            o_shippriority, 
            o_comment 
        """
    createMV(getMVStmt(agg_mv_name_3, agg_mv_stmt_3))

    def agg_sql_3 = """select o_shipprioritY, o_comment, 
            count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) as cnt_1,
            count(distinct case when O_SHIPPRIORITY > 2 and o_orderkey IN (2) then o_custkey else null end) as cnt_2, 
            sum(o_totalprice), 
            max(o_totalprice), 
            min(o_totalprice), 
            count(*) 
            from orders_dup 
            group by 
            o_shippriority, 
            o_commenT 
        """
    mv_rewrite_success(agg_sql_3, agg_mv_name_3)
    sql """DROP MATERIALIZED VIEW IF EXISTS ${agg_mv_name_3} ON orders_dup;"""

    // view partital rewriting
    def view_partition_mv_name_1 = "view_partition_mv_name_1"
    def view_partition_mv_stmt_1 = """
        select l_shipdatE, l_partkey, l_orderkey from lineitem_dup group by l_shipdate, l_partkey, l_orderkeY"""
    createMV(getMVStmt(view_partition_mv_name_1, view_partition_mv_stmt_1))

    def view_partition_sql_1 = """select t.l_shipdate, t.l_partkey 
        from (select l_shipdate, l_partkey, l_orderkey from lineitem_dup group by l_shipdate, l_partkey, l_orderkey) t 
        group by t.l_shipdate, t.l_partkey
        """
    mv_rewrite_success(view_partition_sql_1, view_partition_mv_name_1)
    compare_res(view_partition_sql_1 + " order by 1,2,3")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${view_partition_mv_name_1} ON lineitem_dup;"""

    // predicate compensate
    def predicate_mv_name_1 = "predicate_mv_name_1"
    def predicate_mv_stmt_1 = """
        select l_shipdatE, l_partkey 
        from lineitem_dup 
        where l_shipdate >= "2023-10-17"
        """
    createMV(getMVStmt(predicate_mv_name_1, predicate_mv_stmt_1))

    def predicate_sql_1 = """
        select l_shipdate, l_partkeY 
        from lineitem_dup 
        where l_shipdate >= "2023-10-17" and l_partkey = 1
        """
    mv_rewrite_success(predicate_sql_1, predicate_mv_name_1)
    compare_res(predicate_sql_1 + " order by 1,2,3")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${predicate_mv_name_1} on lineitem_dup;"""

    // Todo: project rewriting
    def rewriting_mv_name_1 = "rewriting_mv_name_1"
    def rewriting_mv_stmt_1 = """
        select o_orderdate, o_shippriority, o_comment, o_orderkey, o_shippriority + o_custkey,
        case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end cnt_1,
        case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end as cnt_2
        from orders_dup
        where  o_orderkey > 1 + 1
        """
    createMV(getMVStmt(rewriting_mv_name_1, rewriting_mv_stmt_1))

    def rewriting_sql_1 = """select o_shippriority, o_comment, o_shippriority + o_custkey  + o_orderkey,
            case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end cnt_1,
        case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end as cnt_2
            from orders_dup
           where  o_orderkey > (-3) + 5
        """
    mv_rewrite_success(rewriting_sql_1, rewriting_mv_name_1)
    compare_res(rewriting_sql_1 + " order by 1,2,3,4,5")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${rewriting_mv_name_1} on orders_dup;"""

    // single table
    def mv_name_1 = "single_tb_mv_1"
    def single_table_mv_stmt_1 = """
        select l_Shipdate, l_partkey, l_suppkey 
        from lineitem_dup 
        where l_commitdate like '2023-10-%'
        """
    createMV(getMVStmt(mv_name_1, single_table_mv_stmt_1))

    def single_table_query_stmt_1 = """
        select l_Shipdate, l_partkey, l_suppkey 
        from lineitem_dup 
        where l_commitdate like '2023-10-%'
        """
    def single_table_query_stmt_2 = """
        select l_Shipdate, l_partkey, l_suppkey 
        from lineitem_dup 
        where l_commitdate like '2023-10-%' and l_partkey > 0 + 1
        """

    mv_rewrite_success(single_table_query_stmt_1, mv_name_1)
    compare_res(single_table_query_stmt_1 + " order by 1,2,3")

    mv_rewrite_success(single_table_query_stmt_2, mv_name_1)
    compare_res(single_table_query_stmt_2 + " order by 1,2,3")


    single_table_mv_stmt_1 = """
        select o_orderkey, sum(o_totalprice) as sum_total, 
            max(o_totalpricE) as max_total, 
            min(o_totalprice) as min_total, 
            count(*) as count_all, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1, 
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
            from orders_dup where o_orderdate >= '2022-10-17' + interval '1' year group by o_orderkey
        """
    createMV(getMVStmt(mv_name_1, single_table_mv_stmt_1))

    // not support currently
//    single_table_query_stmt_1 = """
//        select sum(o_totalprice) as sum_total,
//            max(o_totalpricE) as max_total,
//            min(o_totalprice) as min_total,
//            count(*) as count_all,
//            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1,
//            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2
//            from orders_dup where o_orderdate >= '2022-10-17' + interval '1' year
//        """
//    single_table_query_stmt_2 = """
//        select sum(o_totalprice) as sum_total,
//            max(o_totalpricE) as max_total,
//            min(o_totalprice) as min_total,
//            count(*) as count_all,
//            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1,
//            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2
//            from orders_dup where o_orderdate > '2022-10-17' + interval '1' year
//        """
//    mv_rewrite_success(single_table_query_stmt_1, mv_name_1)
//    compare_res(single_table_query_stmt_1 + " order by 1,2,3,4")

//    mv_rewrite_success(single_table_query_stmt_2, mv_name_1)
//    compare_res(single_table_query_stmt_2 + " order by 1,2,3,4")

    // mv do not support sub-query
//    single_table_mv_stmt_1 = """
//        select l_Shipdate, l_partkey, l_suppkey
//        from lineitem_dup
//        where l_commitdate in (select l_commitdate from lineitem_dup)
//        """
//
//    createMV(getMVStmt(mv_name_1, single_table_mv_stmt_1))
//
//    single_table_query_stmt_1 = """
//        select l_Shipdate, l_partkey, l_suppkey
//        from lineitem_dup
//        where l_commitdate in (select l_commitdate from lineitem_dup)
//        """
//    explain {
//        sql("${single_table_query_stmt_1}")
//        contains "(${mv_name_1})"
//    }
//    mv_rewrite_success(single_table_query_stmt_1, mv_name_1)
//    compare_res(single_table_query_stmt_1 + " order by 1,2,3")

// not supported currently
//    single_table_mv_stmt_1 = """
//        select l_Shipdate, l_partkey, l_suppkey
//        from lineitem_dup
//        where exists (select l_commitdate from lineitem_dup where l_commitdate like "2023-10-17")
//        """
//
//    createMV(getMVStmt(mv_name_1, single_table_mv_stmt_1))
//
//    single_table_query_stmt_1 = """
//        select l_Shipdate, l_partkey, l_suppkey
//        from lineitem_dup
//        where exists (select l_commitdate from lineitem_dup where l_commitdate like "2023-10-17")
//        """
//    mv_rewrite_success(single_table_query_stmt_1, mv_name_1)
//    compare_res(single_table_query_stmt_1 + " order by 1,2,3")
//
//
//    single_table_mv_stmt_1 = """
//        select t.l_Shipdate, t.l_partkey, t.l_suppkey
//        from (select * from lineitem_dup) as t
//        where exists (select l_commitdate from lineitem_dup where l_commitdate like "2023-10-17")
//        """
//
//    createMV(getMVStmt(mv_name_1, single_table_mv_stmt_1))
//
//    single_table_query_stmt_1 = """
//        select t.l_Shipdate, t.l_partkey, t.l_suppkey
//        from (select * from lineitem_dup) as t
//        where exists (select l_commitdate from lineitem_dup where l_commitdate like "2023-10-17")
//        """
//    explain {
//        sql("${single_table_query_stmt_1}")
//        contains "${mv_name_1}(${mv_name_1})"
//    }
//    mv_rewrite_success(single_table_query_stmt_1, mv_name_1)
//    compare_res(single_table_query_stmt_1 + " order by 1,2,3")
}
