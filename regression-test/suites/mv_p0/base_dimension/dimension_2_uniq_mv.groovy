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

suite("partition_mv_rewrite_dimension_2_uniq_mv", "partition_mv_rewrite_dimension") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"

    sql """
    drop table if exists orders_2_uniq
    """

    sql """CREATE TABLE `orders_2_uniq` (
      `o_orderkey` BIGINT NULL,
      `o_custkey` INT NULL,
      `o_orderdate` DATE not NULL,
      `o_orderstatus` VARCHAR(1) NULL,
      `o_totalprice` DECIMAL(15, 2)  NULL,
      `o_orderpriority` VARCHAR(15) NULL,
      `o_clerk` VARCHAR(15) NULL,
      `o_shippriority` INT NULL,
      `o_comment` VARCHAR(79) NULL
    ) ENGINE=OLAP
    unique KEY(`o_orderkey`, `o_custkey`, `o_orderdate`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    drop table if exists lineitem_2_uniq
    """

    sql """CREATE TABLE `lineitem_2_uniq` (
      `l_orderkey` BIGINT NULL,
      `l_linenumber` INT NULL,
      `l_partkey` INT NULL,
      `l_suppkey` INT NULL,
      `l_shipdate` DATE not NULL,
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
      `l_comment` VARCHAR(44) NULL
    ) ENGINE=OLAP
    unique KEY(l_orderkey, l_linenumber, l_partkey, l_suppkey, l_shipdate )
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    insert into orders_2_uniq values 
    (null, 1, '2023-10-17', 'o', 99.5, 'a', 'b', 1, 'yy'),
    (1, null, '2023-10-17', 'k', 109.2, 'c','d',2, 'mm'),
    (3, 3, '2023-10-19', null, 99.5, 'a', 'b', 1, 'yy'),
    (1, 2, '2023-10-20', 'o', null, 'a', 'b', 1, 'yy'),
    (2, 3, '2023-10-21', 'k', 109.2, null,'d',2, 'mm'),
    (3, 1, '2023-10-22', 'o', 99.5, 'a', null, 1, 'yy'),
    (1, 3, '2023-10-19', 'k', 99.5, 'a', 'b', null, 'yy'),
    (2, 1, '2023-10-18', 'o', 109.2, 'c','d',2, null),
    (3, 2, '2023-10-17', 'k', 99.5, 'a', 'b', 1, 'yy'),
    (4, 5, '2023-10-19', 'o', 99.5, 'a', 'b', 1, 'yy');
    """

    sql """
    insert into lineitem_2_uniq values 
    (null, 1, 2, 3, '2023-10-17', 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, null, 3, 1, '2023-10-17', 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy'),
    (3, 3, null, 2, '2023-10-19', 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx'),
    (1, 2, 3, null, '2023-10-17', 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (2, 3, 2, 1, '2023-10-18', 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy'),
    (3, 1, 1, 2, '2023-10-19', 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', null, 'c', 'd', 'xxxxxxxxx'),
    (1, 3, 2, 2, '2023-10-17', 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy');
    """


    sql """alter table orders_2_uniq modify column o_orderkey set stats ('row_count'='50');"""
    sql """alter table orders_2_uniq modify column o_custkey set stats ('row_count'='50');"""
    sql """alter table orders_2_uniq modify column o_orderdate set stats ('row_count'='50');"""
    sql """alter table orders_2_uniq modify column o_orderstatus set stats ('row_count'='50');"""
    sql """alter table orders_2_uniq modify column o_totalprice set stats ('row_count'='50');"""
    sql """alter table orders_2_uniq modify column o_orderpriority set stats ('row_count'='50');"""
    sql """alter table orders_2_uniq modify column o_clerk set stats ('row_count'='50');"""
    sql """alter table orders_2_uniq modify column o_shippriority set stats ('row_count'='50');"""
    sql """alter table orders_2_uniq modify column o_comment set stats ('row_count'='50');"""

    sql """alter table lineitem_2_uniq modify column l_orderkey set stats ('row_count'='50');"""
    sql """alter table lineitem_2_uniq modify column l_linenumber set stats ('row_count'='50');"""
    sql """alter table lineitem_2_uniq modify column l_partkey set stats ('row_count'='50');"""
    sql """alter table lineitem_2_uniq modify column l_suppkey set stats ('row_count'='50');"""
    sql """alter table lineitem_2_uniq modify column l_quantity set stats ('row_count'='50');"""
    sql """alter table lineitem_2_uniq modify column l_extendedprice set stats ('row_count'='50');"""
    sql """alter table lineitem_2_uniq modify column l_discount set stats ('row_count'='50');"""
    sql """alter table lineitem_2_uniq modify column l_tax set stats ('row_count'='50');"""
    sql """alter table lineitem_2_uniq modify column l_returnflag set stats ('row_count'='50');"""
    sql """alter table lineitem_2_uniq modify column l_linestatus set stats ('row_count'='50');"""
    sql """alter table lineitem_2_uniq modify column l_commitdate set stats ('row_count'='50');"""
    sql """alter table lineitem_2_uniq modify column l_receiptdate set stats ('row_count'='50');"""
    sql """alter table lineitem_2_uniq modify column l_shipinstruct set stats ('row_count'='50');"""
    sql """alter table lineitem_2_uniq modify column l_shipmode set stats ('row_count'='50');"""
    sql """alter table lineitem_2_uniq modify column l_comment set stats ('row_count'='50');"""
    sql """alter table lineitem_2_uniq modify column l_shipdate set stats ('row_count'='50');"""


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

    // join + agg function
    def mv_name_1 = "mv_name_2_3_1"
    def mv_stmt_1 = """
            select 
            o_orderkey, o_custkey, o_orderdate,
            case when o_shippriority > 1 then 1 else 2 end cnt_1
            from orders_2_uniq
            """
    createMV(getMVStmt(mv_name_1, mv_stmt_1))

    def sql_stmt_1 = """select 
            case when o_shippriority > 1 then 1 else 2 end cnt_1
            from orders_2_uniq
            """
    mv_rewrite_success(sql_stmt_1, mv_name_1)
    compare_res(sql_stmt_1 + " order by 1,2,3,4,5,6")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name_1} on orders_2_uniq;"""

    // join + group by
    def mv_name_2 = "mv_name_2_3_2"
    def mv_stmt_2 = """select o_orderkey, o_custkey, o_orderdate 
            from orders_2_uniq
            """
    createMV(getMVStmt(mv_name_2, mv_stmt_2))

    def sql_stmt_2 = """select o_orderkey, o_custkey 
            from orders_2_uniq
            group by
            o_orderkey, o_custkey """
    mv_rewrite_success(sql_stmt_2, mv_name_2)
    compare_res(sql_stmt_2 + " order by 1,2")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name_2} on orders_2_uniq;"""

    // view partial
    def mv_name_5 = "mv_name_2_3_5"
    def mv_stmt_5 = """select l_orderkey, l_linenumber, l_shipdatE, l_partkey, l_suppkey  
        from lineitem_2_uniq"""
    createMV(getMVStmt(mv_name_5, mv_stmt_5))

    def sql_stmt_5 = """select l_shipdate, l_partkey 
        from lineitem_2_uniq"""
    mv_rewrite_success(sql_stmt_5, mv_name_5)
    compare_res(sql_stmt_5 + " order by 1,2,3")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name_5} on lineitem_2_uniq;"""

    // predicate compensate
    def mv_name_7 = "mv_name_2_3_7"
    def mv_stmt_7 = """select l_orderkey, l_linenumber, l_shipdatE, l_partkey, l_suppkey 
        from lineitem_2_uniq 
        where l_shipdate >= '2023-10-17'"""
    createMV(getMVStmt(mv_name_7, mv_stmt_7))

    def sql_stmt_7 = """select l_shipdate, l_partkey 
        from lineitem_2_uniq 
        where l_shipdate >= "2023-10-17" and l_partkey = 3"""
    mv_rewrite_success(sql_stmt_7, mv_name_7)
    compare_res(sql_stmt_7 + " order by 1,2,3")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name_7} on lineitem_2_uniq;"""


    // project rewriting
    def mv_name_8 = "mv_name_2_3_8"
    def mv_stmt_8 = """select o_orderkey, o_custkey, o_orderdate,  
            case when o_shippriority > 1 then 1 else 2 end cnt_1, o_shippriority + o_totalprice 
            from orders_2_uniq  
            where  o_orderkey > 1 + 1 """
    createMV(getMVStmt(mv_name_8, mv_stmt_8))

    def sql_stmt_8 = """select o_orderkey, o_orderdate, o_shippriority + o_totalprice + o_custkey, 
            case when o_shippriority > 1 then 1 else 2 end cnt_1  
            from orders_2_uniq 
           where  o_orderkey > (-3) + 5 """
    mv_rewrite_success(sql_stmt_8, mv_name_8)
    compare_res(sql_stmt_8 + " order by 1,2,3,4,5")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name_8} on orders_2_uniq;"""

    // predicate compensate
    // agg function + predicate compensate
    def mv_name_10 = "mv_name_2_4_10"
    def mv_stmt_10 = """select o_orderkey, o_custkey, o_orderdate, 
            case when o_shippriority > 1 then 1 else 2 end cnt_1 
            from orders_2_uniq 
            where o_orderdate >= '2023-10-17'"""
    createMV(getMVStmt(mv_name_10, mv_stmt_10))

    def sql_stmt_10 = """select t.o_orderkey from (select o_orderkey,
            case when o_shippriority > 1 then 1 else 2 end cnt_1 
            from orders_2_uniq where o_orderdate >= "2023-10-17" )  as t
            where t.o_orderkey = 3"""
    mv_rewrite_success(sql_stmt_10, mv_name_10)
    compare_res(sql_stmt_10 + " order by 1")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name_10} on orders_2_uniq;"""


    // group by + predicate compensate

    def mv_name_11 = "mv_name_2_4_11"
    def mv_stmt_11 = """select o_orderkey, o_custkey, o_orderdate, o_shippriority, o_comment 
            from orders_2_uniq 
            where o_orderdate >= '2023-10-17'
            """
    createMV(getMVStmt(mv_name_11, mv_stmt_11))

    def sql_stmt_11 = """select o_orderkey, o_custkey, o_orderdate 
            from orders_2_uniq
            where o_orderdate >= "2023-10-17" and o_totalprice = 1
            group by o_orderkey, o_custkey, o_orderdate
            """
    mv_rewrite_fail(sql_stmt_11, mv_name_11)
    compare_res(sql_stmt_11 + " order by 1,2,3")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name_11} on orders_2_uniq;"""

    def mv_name_16 = "mv_name_2_4_16"
    def mv_stmt_16 = """select o_orderkey, o_custkey, o_orderdate, o_totalprice
            from orders_2_uniq 
            where o_orderdate >= '2023-10-17'
            """
    createMV(getMVStmt(mv_name_16, mv_stmt_16))

    def sql_stmt_16 = """select o_orderkey, o_custkey, o_orderdate 
            from orders_2_uniq
            where o_orderdate >= "2023-10-17" and o_totalprice = 1
            group by o_orderkey, o_custkey, o_orderdate, o_totalprice """

    mv_rewrite_success(sql_stmt_16, mv_name_16)
    compare_res(sql_stmt_16 + " order by 1,2,3")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name_16} on orders_2_uniq;"""

    // agg function + group by + predicate compensate
    def mv_name_12 = "mv_name_2_4_12"
    def mv_stmt_12 = """select o_orderkey, o_custkey, o_orderdate,  
            case when o_shippriority > 1 then 1 else 2 end cnt_1 
            from orders_2_uniq 
            where o_orderdate >= "2023-10-17" 
            """
    createMV(getMVStmt(mv_name_12, mv_stmt_12))

    def sql_stmt_12 = """select t.o_orderkey, t.o_custkey 
            from  (
            select o_orderkey, o_custkey,  
            case when o_shippriority > 1 then 1 else 2 end cnt_1  
            from orders_2_uniq where o_orderdate >= "2023-10-17" 
            group by 
            o_orderkey, o_custkey , case when o_shippriority > 1 then 1 else 2 end 
            ) as t 
            where t.o_orderkey = 1 
             """
    mv_rewrite_success(sql_stmt_12, mv_name_12)
    compare_res(sql_stmt_12 + " order by 1,2,3,4,5,6,7")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name_12} on orders_2_uniq;"""


    // project rewriting
    // agg function + group by + project rewriting
    def mv_name_13 = "mv_name_2_4_13"
    def mv_stmt_13 = """select o_orderkey, o_custkey, o_orderdate,  
            case when o_shippriority > 1 then 1 else 2 end cnt_1  
            from orders_2_uniq  
            where  o_orderkey > 1 + 1 """
    createMV(getMVStmt(mv_name_13, mv_stmt_13))

    def sql_stmt_13 = """select o_orderkey + o_custkey , 
            case when o_shippriority > 1 then 1 else 2 end cnt_1 
            from orders_2_uniq  
            where o_orderkey > (-3) + 5 
            group by o_orderdate, o_orderkey + o_custkey, case when o_shippriority > 1 then 1 else 2 end """
    mv_rewrite_success(sql_stmt_13, mv_name_13)
    compare_res(sql_stmt_13 + " order by 1")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name_13} on orders_2_uniq;"""


    // group by + project rewriting
    def mv_name_14 = "mv_name_2_4_14"
    def mv_stmt_14 = """select o_orderkey, o_custkey, o_orderdate  
            from orders_2_uniq 
            where o_orderkey > 1 + 1"""
    createMV(getMVStmt(mv_name_14, mv_stmt_14))

    def sql_stmt_14 = """select o_orderkey, o_custkey, o_orderdate  
            from orders_2_uniq 
            where o_orderkey > (-3) + 5 
            group by 
            o_orderkey, o_custkey, o_orderdate """
    mv_rewrite_success(sql_stmt_14, mv_name_14)
    compare_res(sql_stmt_14 + " order by 1,2")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name_14} on orders_2_uniq;"""


    // agg function + group by + project rewriting
    def mv_name_15 = "mv_name_2_4_15"
    def mv_stmt_15 = """select o_orderkey, o_custkey, o_orderdate, o_shippriority,  
            case when o_shippriority > 1 then 1 else 2 end cnt_1  
            from orders_2_uniq 
            where o_orderkey > 1 + 1 
            """
    createMV(getMVStmt(mv_name_15, mv_stmt_15))

    def sql_stmt_15 = """select o_shippriority, o_custkey, o_shippriority + o_orderkey, 
            case when o_shippriority > 1 then 1 else 2 end as cnt_1 
            from orders_2_uniq 
            where  o_orderkey > (-3) + 5 
            group by o_shippriority, o_custkey, o_shippriority + o_orderkey, 
            case when o_shippriority > 1 then 1 else 2 end, 
            o_orderdate
            """
    mv_rewrite_success(sql_stmt_15, mv_name_15)
    compare_res(sql_stmt_15 + " order by 1,2,3,4,5")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name_15} on orders_2_uniq;"""


    // predicate compensate
    mv_name_5 = "mv_name_2_6_5"
    mv_stmt_5 = """select l_orderkey, l_linenumber, l_shipdatE, l_partkey, l_suppkey  
        from lineitem_2_uniq 
        where l_shipdate >= "2023-10-17"
        """
    createMV(getMVStmt(mv_name_5, mv_stmt_5))

    sql_stmt_5 = """select t.l_shipdate, o_orderdate, t.l_partkey 
        from (
            select l_shipdatE, l_partkey, l_suppkey 
            from lineitem_2_uniq 
            group by l_orderkey, l_linenumber, l_shipdatE, l_partkey, l_suppkey) t 
        left join orders_2_uniq   
        on t.l_partkey = orders_2_uniq.o_orderkey 
        where t.l_shipdate >= "2023-10-17" and t.l_suppkey  > 1 + 1 
        group by t.l_shipdate, o_orderdate, t.l_partkey"""
    mv_rewrite_success(sql_stmt_5, mv_name_5)
    compare_res(sql_stmt_5 + " order by 1,2,3")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name_5} on lineitem_2_uniq;"""

    // project rewriting
    def mv_name_6 = "mv_name_2_6_6"
    def mv_stmt_6 = """select l_orderkey, l_linenumber, l_shipdatE, l_partkey, l_suppkey 
        from lineitem_2_uniq
        where l_partkey  > 1 + 1
        """
    createMV(getMVStmt(mv_name_6, mv_stmt_6))

    def sql_stmt_6 = """select t.l_shipdate, o_orderdate, t.l_partkey * 2
        from (
            select l_orderkey, l_linenumber, l_shipdatE, l_partkey, l_partkey + l_suppkey 
            from lineitem_2_uniq 
            group by l_orderkey, l_linenumber, l_partkey, l_suppkey, l_partkey + l_suppkey, l_shipdatE) t
        left join orders_2_uniq
        on t.l_orderkey = orders_2_uniq.o_orderkey
        where  t.l_partkey  > (-3) + 5
        group by t.l_shipdate, o_orderdate, t.l_partkey"""
    mv_rewrite_success(sql_stmt_6, mv_name_6)
    compare_res(sql_stmt_6 + " order by 1,2,3")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name_6} on lineitem_2_uniq;"""

    // predicate compensate
    // project rewriting
    def mv_name_9 = "mv_name_2_6_9"
    def mv_stmt_9 = """ select o_orderkey, o_custkey, o_orderdate, o_shippriority, o_comment,
            case when o_shippriority > 1 then 1 else 2 end cnt_1 
            from orders_2_uniq 
            where  o_custkey > 1 + 1"""
    createMV(getMVStmt(mv_name_9, mv_stmt_9))

    def sql_stmt_9 = """select o_orderdate, o_shippriority, o_comment, o_shippriority + o_custkey,
            case when o_shippriority > 1 then 1 else 2 end cnt_1 
            from orders_2_uniq 
            where  o_custkey > (-3) + 5 and o_orderdate >= '2023-10-17'  """
    mv_rewrite_success(sql_stmt_9, mv_name_9)
    compare_res(sql_stmt_9 + " order by 1,2,3,4,5,6")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name_9} on orders_2_uniq;"""
}
