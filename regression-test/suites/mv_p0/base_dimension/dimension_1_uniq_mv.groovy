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
suite("partition_mv_rewrite_dimension_1_uniq_mv", "partition_mv_rewrite_dimension") {
    String db = context.config.getDbNameByFile(context.file)
    String order_tb = "orders_uniq"
    String lineitem_tb = "lineitem_uniq"
    sql "use ${db}"

    sql """
    drop table if exists orders_uniq
    """

    sql """CREATE TABLE `orders_uniq` (
      `o_orderkey` BIGINT not NULL,
      `o_custkey` INT not NULL,
      `o_orderdate` DATE not null,
      `o_orderstatus` VARCHAR(1) null,
      `o_totalprice` DECIMAL(15, 2) null,
      `o_orderpriority` VARCHAR(15) null,
      `o_clerk` VARCHAR(15) null,
      `o_shippriority` INT null,
      `o_comment` VARCHAR(79) null
    ) ENGINE=OLAP
    unique KEY(`o_orderkey`, `o_custkey`, `o_orderdate`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    drop table if exists lineitem_uniq
    """

    sql """CREATE TABLE `lineitem_uniq` (
      `l_orderkey` BIGINT not NULL,
      `l_linenumber` INT not NULL,
      `l_partkey` INT not NULL,
      `l_suppkey` INT not NULL,
      `l_shipdate` DATE not null,
      `l_quantity` DECIMAL(15, 2) null,
      `l_extendedprice` DECIMAL(15, 2) null,
      `l_discount` DECIMAL(15, 2) null,
      `l_tax` DECIMAL(15, 2) null,
      `l_returnflag` VARCHAR(1) null,
      `l_linestatus` VARCHAR(1) null,
      `l_commitdate` DATE null,
      `l_receiptdate` DATE null,
      `l_shipinstruct` VARCHAR(25) null,
      `l_shipmode` VARCHAR(10) null,
      `l_comment` VARCHAR(44) null
    ) ENGINE=OLAP
    unique KEY(l_orderkey, l_linenumber, l_partkey, l_suppkey, l_shipdate)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    insert into orders_uniq values 
    (2, 1, '2023-10-17', 'k', 99.5, 'a', 'b', 1, 'yy'),
    (1, 2, '2023-10-17', 'o', 109.2, 'c','d',2, 'mm'),
    (3, 3, '2023-10-19', null, 99.5, 'a', 'b', 1, 'yy'),
    (1, 2, '2023-10-20', 'o', null, 'a', 'b', 1, 'yy'),
    (2, 3, '2023-10-21', 'k', 109.2, null,'d',2, 'mm'),
    (3, 1, '2023-10-22', 'k', 99.5, 'a', null, 1, 'yy'),
    (1, 3, '2023-10-19', 'o', 99.5, 'a', 'b', null, 'yy'),
    (2, 1, '2023-10-18', 'o', 109.2, 'c','d',2, null),
    (3, 2, '2023-10-17', 'k', 99.5, 'a', 'b', 1, 'yy'),
    (4, 5, '2023-10-19', 'k', 99.5, 'a', 'b', 1, 'yy'); 
    """

    sql """
    insert into lineitem_uniq values 
    (2, 1, 2, 3, '2023-10-17', 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 1, '2023-10-17', 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy'),
    (3, 3, 1, 2, '2023-10-19', 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx'),
    (1, 2, 3, 3, '2023-10-17', 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (2, 3, 2, 1, '2023-10-18', 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy'),
    (3, 1, 1, 2, '2023-10-19', 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', null, 'c', 'd', 'xxxxxxxxx'),
    (1, 3, 2, 2, '2023-10-17', 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy');
    """


    sql """alter table orders_uniq modify column o_orderkey set stats ('row_count'='50');"""
    sql """alter table orders_uniq modify column o_custkey set stats ('row_count'='50');"""
    sql """alter table orders_uniq modify column o_orderdate set stats ('row_count'='50');"""
    sql """alter table orders_uniq modify column o_orderstatus set stats ('row_count'='50');"""
    sql """alter table orders_uniq modify column o_totalprice set stats ('row_count'='50');"""
    sql """alter table orders_uniq modify column o_orderpriority set stats ('row_count'='50');"""
    sql """alter table orders_uniq modify column o_clerk set stats ('row_count'='50');"""
    sql """alter table orders_uniq modify column o_shippriority set stats ('row_count'='50');"""
    sql """alter table orders_uniq modify column o_comment set stats ('row_count'='50');"""

    sql """alter table lineitem_uniq modify column l_orderkey set stats ('row_count'='50');"""
    sql """alter table lineitem_uniq modify column l_linenumber set stats ('row_count'='50');"""
    sql """alter table lineitem_uniq modify column l_partkey set stats ('row_count'='50');"""
    sql """alter table lineitem_uniq modify column l_suppkey set stats ('row_count'='50');"""
    sql """alter table lineitem_uniq modify column l_quantity set stats ('row_count'='50');"""
    sql """alter table lineitem_uniq modify column l_extendedprice set stats ('row_count'='50');"""
    sql """alter table lineitem_uniq modify column l_discount set stats ('row_count'='50');"""
    sql """alter table lineitem_uniq modify column l_tax set stats ('row_count'='50');"""
    sql """alter table lineitem_uniq modify column l_returnflag set stats ('row_count'='50');"""
    sql """alter table lineitem_uniq modify column l_linestatus set stats ('row_count'='50');"""
    sql """alter table lineitem_uniq modify column l_commitdate set stats ('row_count'='50');"""
    sql """alter table lineitem_uniq modify column l_receiptdate set stats ('row_count'='50');"""
    sql """alter table lineitem_uniq modify column l_shipinstruct set stats ('row_count'='50');"""
    sql """alter table lineitem_uniq modify column l_shipmode set stats ('row_count'='50');"""
    sql """alter table lineitem_uniq modify column l_comment set stats ('row_count'='50');"""
    sql """alter table lineitem_uniq modify column l_shipdate set stats ('row_count'='50');"""


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
    sql """DROP MATERIALIZED VIEW IF EXISTS ${agg_mv_name_1} ON orders_uniq;"""
    sql """DROP TABLE IF EXISTS ${agg_mv_name_1}"""
    def agg_mv_stmt_1 = """select 
            o_orderkey, o_custkey, o_orderdate, o_clerk, 
            case when o_shippriority > 1 then 1 else 2 end cnt_1
            from orders_uniq"""
    createMV(getMVStmt(agg_mv_name_1, agg_mv_stmt_1))

    def agg_sql_1 = """select 
            o_orderkey, o_clerk, 
            case when o_shippriority > 1 then 1 else 2 end cnt_1 
            from orders_uniq
        """
    mv_rewrite_success(agg_sql_1, agg_mv_name_1)
    compare_res(agg_sql_1 + " order by 1,2,3,4,5,6")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${agg_mv_name_1} ON orders_uniq;"""

    // agg + with group by + without agg function
    def agg_mv_name_2 = "agg_mv_name_2"
    def agg_mv_stmt_2 = """
        select o_orderkey, o_custkey, o_orderdate   
            from orders_uniq 
        """
    createMV(getMVStmt(agg_mv_name_2, agg_mv_stmt_2))

    def agg_sql_2 = """select o_orderkey, o_custkey  
            from orders_uniq 
        """
    mv_rewrite_success(agg_sql_2, agg_mv_name_2)
    compare_res(agg_sql_2 + " order by 1,2")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${agg_mv_name_2} ON orders_uniq;"""

    // view partital rewriting
    def view_partition_mv_name_1 = "view_partition_mv_name_1"
    def view_partition_mv_stmt_1 = """
        select l_orderkey, l_linenumber, l_shipdatE, l_partkey, l_suppkey from lineitem_uniq"""
    createMV(getMVStmt(view_partition_mv_name_1, view_partition_mv_stmt_1))

    def view_partition_sql_1 = """select t.l_shipdate, t.l_partkey 
        from (select l_orderkey, l_shipdatE, l_partkey from lineitem_uniq group by l_orderkey, l_shipdatE, l_partkey, l_suppkey) t 
        group by t.l_shipdate, t.l_partkey
        """
    mv_rewrite_success(view_partition_sql_1, view_partition_mv_name_1)
    compare_res(view_partition_sql_1 + " order by 1,2,3")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${view_partition_mv_name_1} ON lineitem_uniq;"""

    // predicate compensate
    def predicate_mv_name_1 = "predicate_mv_name_1"
    def predicate_mv_stmt_1 = """
        select l_orderkey, l_linenumber, l_shipdatE, l_partkey, l_suppkey  
        from lineitem_uniq 
        where l_shipdate >= "2023-10-17"
        """
    createMV(getMVStmt(predicate_mv_name_1, predicate_mv_stmt_1))

    def predicate_sql_1 = """
        select l_orderkey, l_linenumber, l_shipdatE, l_partkey, l_suppkey  
        from lineitem_uniq 
        where l_shipdate >= "2023-10-17" and l_partkey = 1
        """
    mv_rewrite_success(predicate_sql_1, predicate_mv_name_1)
    compare_res(predicate_sql_1 + " order by 1,2,3")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${predicate_mv_name_1} on lineitem_uniq;"""

    def rewriting_mv_name_1 = "rewriting_mv_name_1"
    def rewriting_mv_stmt_1 = """
            select 
            o_orderkey, o_custkey, o_orderdate, 
            case when o_shippriority > 1 then 1 else 2 end cnt_1, o_clerk, o_shippriority + o_shippriority
            from orders_uniq
            where  o_orderkey > 1 + 1
            """
    createMV(getMVStmt(rewriting_mv_name_1, rewriting_mv_stmt_1))

    def rewriting_sql_1 = """select o_orderkey, o_orderdate, o_shippriority + o_shippriority + o_custkey,
            case when o_shippriority > 1 then 1 else 2 end cnt_1 
            from orders_uniq
           where  o_orderkey > (-3) + 5
        """
    mv_rewrite_success(rewriting_sql_1, rewriting_mv_name_1)
    compare_res(rewriting_sql_1 + " order by 1,2,3,4,5")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${rewriting_mv_name_1} on orders_uniq;"""

    // single table
    def mv_name_1 = "single_tb_mv_1"
    def single_table_mv_stmt_1 = """
        select 
        o_orderkey, o_custkey, o_orderdate, 
        case when o_shippriority > 1 then 1 else 2 end cnt_1, o_clerk, o_shippriority + o_shippriority
        from orders_uniq
        where o_orderdate like '2023-10-%'
        """

    createMV(getMVStmt(mv_name_1, single_table_mv_stmt_1))

    def single_table_query_stmt_1 = """
        select 
        o_orderkey, o_custkey, o_orderdate, 
        case when o_shippriority > 1 then 1 else 2 end cnt_1, o_clerk, o_shippriority + o_shippriority
        from orders_uniq
        where o_orderdate like '2023-10-%'
        """
    def single_table_query_stmt_2 = """
        select 
        o_orderkey, o_custkey, o_orderdate, 
        case when o_shippriority > 1 then 1 else 2 end cnt_1, o_clerk, o_shippriority + o_shippriority
        from orders_uniq
        where o_orderdate like '2023-10-%' and o_custkey > o_orderkey
        """

    mv_rewrite_success(single_table_query_stmt_1, mv_name_1)
    compare_res(single_table_query_stmt_1 + " order by 1,2,3")

    mv_rewrite_success(single_table_query_stmt_2, mv_name_1)
    compare_res(single_table_query_stmt_2 + " order by 1,2,3")
}
