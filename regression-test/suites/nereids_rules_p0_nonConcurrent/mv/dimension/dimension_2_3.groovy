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
This suite is a two dimensional test case file.
It mainly tests the agg function, etc
 */
suite("partition_mv_rewrite_dimension_2_3") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"

    sql """
    drop table if exists orders_2_3
    """

    sql """CREATE TABLE `orders_2_3` (
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
    drop table if exists lineitem_2_3
    """

    sql """CREATE TABLE `lineitem_2_3` (
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
    insert into orders_2_3 values 
    (null, 1, 'o', 99.5, 'a', 'b', 1, 'yy', '2023-10-17'),
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
    insert into lineitem_2_3 values 
    (null, 1, 2, 3, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (1, null, 3, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (3, 3, null, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
    (1, 2, 3, null, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (2, 3, 2, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-18'),
    (3, 1, 1, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', null, 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
    (1, 3, 2, 2, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17');
    """

    sql """analyze table orders_2_3 with sync;"""
    sql """analyze table lineitem_2_3 with sync;"""

    sql """alter table orders_2_3 modify column o_comment set stats ('row_count'='10');"""
    sql """alter table lineitem_2_3 modify column l_comment set stats ('row_count'='7');"""

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
    def mv_stmt_1 = """select
            sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1,
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2
            from orders_2_3
            left join lineitem_2_3 on lineitem_2_3.l_orderkey = orders_2_3.o_orderkey"""
    create_async_mv(db, mv_name_1, mv_stmt_1)
    def job_name_1 = getJobName(db, mv_name_1)
    waitingMTMVTaskFinished(job_name_1)

    def sql_stmt_1 = """select
            count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) as cnt_1,
            count(distinct case when O_SHIPPRIORITY > 2 and o_orderkey IN (2) then o_custkey else null end) as cnt_2,
            sum(o_totalprice),
            max(o_totalprice),
            min(o_totalprice),
            count(*)
            from orders_2_3
            left join lineitem_2_3 on lineitem_2_3.l_orderkey = orders_2_3.o_orderkey"""
    mv_rewrite_success(sql_stmt_1, mv_name_1)
    compare_res(sql_stmt_1 + " order by 1,2,3,4,5,6")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name_1};"""

    // join + group by
    def mv_name_2 = "mv_name_2_3_2"
    def mv_stmt_2 = """select o_orderdate, o_shippriority, o_comment
            from orders_2_3
            left join lineitem_2_3 on lineitem_2_3.l_orderkey = orders_2_3.o_orderkey
            group by
            o_orderdate,
            o_shippriority,
            o_comment """
    create_async_mv(db, mv_name_2, mv_stmt_2)
    def job_name_2 = getJobName(db, mv_name_2)
    waitingMTMVTaskFinished(job_name_2)

    def sql_stmt_2 = """select o_shippriority, o_comment
            from orders_2_3
            left join lineitem_2_3 on lineitem_2_3.l_orderkey = orders_2_3.o_orderkey
            group by
            o_shippriority,
            o_comment """
    mv_rewrite_success(sql_stmt_2, mv_name_2)
    compare_res(sql_stmt_2 + " order by 1,2")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name_2};"""

    // join + group by + agg function
    def mv_name_3 = "mv_name_2_3_3"
    def mv_stmt_3 = """select o_orderdate, o_shippriority, o_comment, 
            sum(o_totalprice) as sum_total, 
            max(o_totalprice) as max_total, 
            min(o_totalprice) as min_total, 
            count(*) as count_all, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1, 
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
            from orders_2_3 
            left join lineitem_2_3 on lineitem_2_3.l_orderkey = orders_2_3.o_orderkey
            group by 
            o_orderdate, 
            o_shippriority, 
            o_comment """
    create_async_mv(db, mv_name_3, mv_stmt_3)
    def job_name_3 = getJobName(db, mv_name_3)
    waitingMTMVTaskFinished(job_name_3)

    def sql_stmt_3 = """select o_shippriority, o_comment, 
            count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) as cnt_1,
            count(distinct case when O_SHIPPRIORITY > 2 and o_orderkey IN (2) then o_custkey else null end) as cnt_2, 
            sum(o_totalprice), 
            max(o_totalprice), 
            min(o_totalprice), 
            count(*) 
            from orders_2_3 
            left join lineitem_2_3 on lineitem_2_3.l_orderkey = orders_2_3.o_orderkey
            group by 
            o_shippriority, 
            o_comment """
    mv_rewrite_success(sql_stmt_3, mv_name_3)
    compare_res(sql_stmt_3 + " order by 1,2,3,4,5,6,7,8")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name_3};"""


    // Todo: query partial
//    def mv_name_4 = "mv_name_2_3_4"
//    def mv_stmt_4 = """select l_shipdate, o_orderdate, l_partkey, l_suppkey
//        from lineitem_2_3
//        left join orders_2_3
//        on lineitem_2_3.l_orderkey = orders_2_3.o_orderkey """
//    create_async_mv(db, mv_name_4, mv_stmt_4)
//    def job_name_4 = getJobName(db, mv_name_4)
//    waitingMTMVTaskFinished(job_name_4)
//
//    def sql_stmt_4 = """select l_shipdate, o_orderdate, l_partkey, l_suppkey from lineitem_2_3 """
//    explain {
//        sql("${sql_stmt_4}")
//        contains "${mv_name_4}(${mv_name_4})"
//    }
//    sql_stmt_4 = """select o_orderdate from orders_2_3 """
//    explain {
//        sql("${sql_stmt_4}")
//        contains "${mv_name_4}(${mv_name_4})"
//    }

    // view partial
    def mv_name_5 = "mv_name_2_3_5"
    def mv_stmt_5 = """select l_shipdate, l_partkey, l_orderkey, o_orderdate 
        from lineitem_2_3 
        left join orders_2_3 
        on lineitem_2_3.l_orderkey = orders_2_3.o_orderkey"""
    create_async_mv(db, mv_name_5, mv_stmt_5)
    def job_name_5 = getJobName(db, mv_name_5)
    waitingMTMVTaskFinished(job_name_5)

    def sql_stmt_5 = """select l_shipdate, o_orderdate, l_partkey 
        from lineitem_2_3 
        left join orders_2_3  on lineitem_2_3.l_orderkey = orders_2_3.o_orderkey"""
    mv_rewrite_success(sql_stmt_5, mv_name_5)
    compare_res(sql_stmt_5 + " order by 1,2,3")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name_5};"""


    // Todo: union rewriting
//    def mv_name_6 = "mv_name_2_3_6"
//    def mv_stmt_6 = """select l_shipdate, o_orderdate, l_partkey
//        from lineitem_2_3
//        left join orders_2_3
//        on lineitem_2_3.l_orderkey = orders_2_3.o_orderkey
//        where l_shipdate >= '2023-10-17'"""
//    create_async_mv(db, mv_name_6, mv_stmt_6)
//    def job_name_6 = getJobName(db, mv_name_6)
//    waitingMTMVTaskFinished(job_name_6)
//
//    def sql_stmt_6 = """select l_shipdate, o_orderdate, l_partkey
//        from lineitem_2_3
//        left join orders_2_3
//        on lineitem_2_3.l_orderkey = orders_2_3.o_orderkey
//        where l_shipdate >= '2023-10-15'"""
//    explain {
//        sql("${sql_stmt_6}")
//        contains "${mv_name_6}(${mv_name_6})"
//    }

    // predicate compensate
    def mv_name_7 = "mv_name_2_3_7"
    def mv_stmt_7 = """select l_shipdate, o_orderdate, l_partkey 
        from lineitem_2_3 
        left join orders_2_3   
        on lineitem_2_3.l_orderkey = orders_2_3.o_orderkey
        where l_shipdate >= '2023-10-17'"""
    create_async_mv(db, mv_name_7, mv_stmt_7)
    def job_name_7 = getJobName(db, mv_name_7)
    waitingMTMVTaskFinished(job_name_7)

    def sql_stmt_7 = """select l_shipdate, o_orderdate, l_partkey 
        from lineitem_2_3 
        left join orders_2_3   
        on lineitem_2_3.l_orderkey = orders_2_3.o_orderkey
        where l_shipdate >= "2023-10-17" and l_partkey = 3"""
    mv_rewrite_success(sql_stmt_7, mv_name_7)
    compare_res(sql_stmt_7 + " order by 1,2,3")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name_7};"""


    // project rewriting
    def mv_name_8 = "mv_name_2_3_8"
    def mv_stmt_8 = """select o_orderdate, o_shippriority, o_comment, l_suppkey, o_shippriority + o_custkey, 
           case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end cnt_1, 
            case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end as cnt_2 
            from orders_2_3  left join lineitem_2_3   on lineitem_2_3.l_orderkey = orders_2_3.o_orderkey
            where  o_orderkey > 1 + 1 """
    create_async_mv(db, mv_name_8, mv_stmt_8)
    def job_name_8 = getJobName(db, mv_name_8)
    waitingMTMVTaskFinished(job_name_8)

    def sql_stmt_8 = """select o_shippriority, o_comment, o_shippriority + o_custkey  + l_suppkey, 
            case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end as cnt_1,
            case when O_SHIPPRIORITY > 2 and o_orderkey IN (2) then o_custkey else null end as cnt_2 
            from orders_2_3  left join lineitem_2_3   on lineitem_2_3.l_orderkey = orders_2_3.o_orderkey
           where  o_orderkey > (-3) + 5 """
    mv_rewrite_success(sql_stmt_8, mv_name_8)
    compare_res(sql_stmt_8 + " order by 1,2,3,4,5")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name_8};"""
}
