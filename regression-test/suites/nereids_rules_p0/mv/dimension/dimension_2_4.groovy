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
It mainly tests the query partial, view partial, union rewriting, predicate compensate, project rewriting.
 */
suite("partition_mv_rewrite_dimension_2_4") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"

    sql """
    drop table if exists orders_2_4
    """

    sql """CREATE TABLE `orders_2_4` (
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
    auto partition by range (date_trunc(`o_orderdate`, 'day')) ()
    DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    drop table if exists lineitem_2_4
    """

    sql """CREATE TABLE `lineitem_2_4` (
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
    auto partition by range (date_trunc(`l_shipdate`, 'day')) ()
    DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    drop table if exists partsupp_2_4
    """

    sql """CREATE TABLE `partsupp_2_4` (
      `ps_partkey` INT NULL,
      `ps_suppkey` INT NULL,
      `ps_availqty` INT NULL,
      `ps_supplycost` DECIMAL(15, 2) NULL,
      `ps_comment` VARCHAR(199) NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`ps_partkey`, `ps_suppkey`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`ps_partkey`) BUCKETS 24
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    insert into orders_2_4 values 
    (null, 1, 'o', 99.5, 'a', 'b', 1, 'yy', '2023-10-17'),
    (1, null, 'k', 109.2, 'c','d',2, 'mm', '2023-10-17'),
    (3, 3, null, 99.5, 'a', 'b', 1, 'yy', '2023-10-19'),
    (1, 2, 'o', null, 'a', 'b', 1, 'yy', '2023-10-20'),
    (2, 3, 'k', 109.2, null,'d',2, 'mm', '2023-10-21'),
    (3, 1, 'o', 99.5, 'a', null, 1, 'yy', '2023-10-22'),
    (1, 3, 'k', 99.5, 'a', 'b', null, 'yy', '2023-10-19'),
    (2, 1, 'o', 109.2, 'c','d',2, null, '2023-10-18'),
    (3, 2, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-17'),
    (4, 5, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-19'); 
    """

    sql """
    insert into lineitem_2_4 values 
    (null, 1, 2, 3, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (1, null, 3, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (3, 3, null, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
    (1, 2, 3, null, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (2, 3, 2, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-18'),
    (3, 1, 1, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', null, 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
    (1, 3, 2, 2, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17');
    """

    sql"""
    insert into partsupp_2_4 values 
    (1, 1, 1, 99.5, 'yy'),
    (null, 2, 2, 109.2, 'mm'),
    (3, null, 1, 99.5, 'yy'); 
    """

    sql """analyze table orders_2_4 with sync;"""
    sql """analyze table lineitem_2_4 with sync;"""
    sql """analyze table partsupp_2_4 with sync;"""

    def create_mv_lineitem = { mv_name, mv_sql ->
        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name};"""
        sql """DROP TABLE IF EXISTS ${mv_name}"""
        sql"""
        CREATE MATERIALIZED VIEW ${mv_name} 
        BUILD IMMEDIATE REFRESH AUTO ON MANUAL 
        partition by(l_shipdate) 
        DISTRIBUTED BY RANDOM BUCKETS 2 
        PROPERTIES ('replication_num' = '1')  
        AS  
        ${mv_sql}
        """
    }

    def create_mv_orders = { mv_name, mv_sql ->
        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name};"""
        sql """DROP TABLE IF EXISTS ${mv_name}"""
        sql"""
        CREATE MATERIALIZED VIEW ${mv_name} 
        BUILD IMMEDIATE REFRESH AUTO ON MANUAL 
        partition by(o_orderdate) 
        DISTRIBUTED BY RANDOM BUCKETS 2 
        PROPERTIES ('replication_num' = '1') 
        AS  
        ${mv_sql}
        """
    }

    def create_all_mv = { mv_name, mv_sql ->
        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name};"""
        sql """DROP TABLE IF EXISTS ${mv_name}"""
        sql"""
        CREATE MATERIALIZED VIEW ${mv_name} 
        BUILD IMMEDIATE REFRESH AUTO ON MANUAL 
        DISTRIBUTED BY RANDOM BUCKETS 2 
        PROPERTIES ('replication_num' = '1') 
        AS  
        ${mv_sql}
        """
    }

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

    // Todo: query partial
    // agg function + query partial
//    def mv_name_1 = "mv_name_2_4_1"
//    def mv_stmt_1 = """select
//            sum(o_totalprice) as sum_total,
//            max(o_totalprice) as max_total,
//            min(o_totalprice) as min_total,
//            count(*) as count_all,
//            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1,
//            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2
//            from orders_2_4
//            left join lineitem_2_4 on lineitem_2_4.l_orderkey = orders_2_4.o_orderkey"""
//    create_all_mv(mv_name_1, mv_stmt_1)
//    def job_name_1 = getJobName(db, mv_name_1)
//    waitingMTMVTaskFinished(job_name_1)
//
//    def sql_stmt_1 = """select
//            count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) as cnt_1,
//            count(distinct case when O_SHIPPRIORITY > 2 and o_orderkey IN (2) then o_custkey else null end) as cnt_2,
//            sum(o_totalprice),
//            max(o_totalprice),
//            min(o_totalprice),
//            count(*)
//            from orders_2_4 """
//    explain {
//        sql("${sql_stmt_1}")
//        contains "${mv_name_1}(${mv_name_1})"
//    }


    // group by + query partial
//    def mv_name_2 = "mv_name_2_4_2"
//    def mv_stmt_2 = """select o_orderdate, o_shippriority, o_comment
//            from orders_2_4
//            left join lineitem_2_4 on lineitem_2_4.l_orderkey = orders_2_4.o_orderkey
//            group by
//            o_orderdate,
//            o_shippriority,
//            o_comment """
//    create_mv_orders(mv_name_2, mv_stmt_2)
//    def job_name_2 = getJobName(db, mv_name_2)
//    waitingMTMVTaskFinished(job_name_2)
//
//    def sql_stmt_2 = """select o_shippriority, o_comment
//            from orders_2_4
//            group by
//            o_shippriority,
//            o_comment """
//    explain {
//        sql("${sql_stmt_2}")
//        contains "${mv_name_2}(${mv_name_2})"
//    }

    // agg function + group by + query partial
//    def mv_name_3 = "mv_name_2_4_3"
//    def mv_stmt_3 = """select  o_orderdate, o_shippriority, o_comment,
//            sum(o_totalprice) as sum_total,
//            max(o_totalprice) as max_total,
//            min(o_totalprice) as min_total,
//            count(*) as count_all,
//            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1,
//            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2
//            from orders_2_4
//            left join lineitem_2_4 on lineitem_2_4.l_orderkey = orders_2_4.o_orderkey
//            group by
//            o_orderdate,
//            o_shippriority,
//            o_comment  """
//    create_mv_orders(mv_name_3, mv_stmt_3)
//    def job_name_3 = getJobName(db, mv_name_3)
//    waitingMTMVTaskFinished(job_name_3)
//
//    def sql_stmt_3 = """select  o_shippriority, o_comment,
//            count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) as cnt_1,
//            count(distinct case when O_SHIPPRIORITY > 2 and o_orderkey IN (2) then o_custkey else null end) as cnt_2,
//            sum(o_totalprice),
//            max(o_totalprice),
//            min(o_totalprice),
//            count(*)
//            from orders_2_4
//            group by
//            o_shippriority,
//            o_comment """
//    explain {
//        sql("${sql_stmt_3}")
//        contains "${mv_name_3}(${mv_name_3})"
//    }

    // view partial
    // agg function + view partial
    def mv_name_4 = "mv_name_2_4_4"
    def mv_stmt_4 = """select 
            o_totalprice, 
            o_shippriority,
            o_orderkey,
            l_orderkey,
            o_custkey 
            from orders_2_4 
            left join lineitem_2_4
            on lineitem_2_4.l_orderkey = orders_2_4.o_orderkey """
    create_all_mv(mv_name_4, mv_stmt_4)
    def job_name_4 = getJobName(db, mv_name_4)
    waitingMTMVTaskFinished(job_name_4)

    def sql_stmt_4 = """select 
            o_totalprice, 
            o_shippriority,
            o_orderkey,
            l_orderkey,
            o_custkey 
            from orders_2_4 
            left join lineitem_2_4
            on lineitem_2_4.l_orderkey = orders_2_4.o_orderkey
            left join partsupp_2_4 on partsupp_2_4.ps_partkey = lineitem_2_4.l_orderkey"""
    explain {
        sql("${sql_stmt_4}")
        contains "${mv_name_4}(${mv_name_4})"
    }
    compare_res(sql_stmt_4 + " order by 1,2,3,4,5")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name_4};"""


    // group by + query partial
    def mv_name_5 = "mv_name_2_4_5"
    def mv_stmt_5 = """select o_orderdate, o_shippriority, o_comment 
            from orders_2_4 
            left join lineitem_2_4 on lineitem_2_4.l_orderkey = orders_2_4.o_orderkey
            group by 
            o_orderdate, 
            o_shippriority, 
            o_comment  """
    create_mv_orders(mv_name_5, mv_stmt_5)
    def job_name_5 = getJobName(db, mv_name_5)
    waitingMTMVTaskFinished(job_name_5)

    def sql_stmt_5 = """select o_orderdate, o_shippriority, o_comment
            from orders_2_4
            left join lineitem_2_4 on lineitem_2_4.l_orderkey = orders_2_4.o_orderkey
            left join partsupp_2_4 on partsupp_2_4.ps_partkey = lineitem_2_4.l_orderkey
            group by
            o_orderdate,
            o_shippriority,
            o_comment """
    explain {
        sql("${sql_stmt_5}")
        notContains "${mv_name_5}(${mv_name_5})"
    }
    compare_res(sql_stmt_5 + " order by 1,2,3")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name_5};"""


    // agg function + group by + view partial
    def mv_name_6 = "mv_name_2_4_6"
    def mv_stmt_6 = """select  o_orderdate, o_shippriority, o_comment,
            sum(o_totalprice) as sum_total, 
            max(o_totalprice) as max_total, 
            min(o_totalprice) as min_total, 
            count(*) as count_all, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1, 
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
            from orders_2_4 
            left join lineitem_2_4 on lineitem_2_4.l_orderkey = orders_2_4.o_orderkey
            group by 
            o_orderdate, 
            o_shippriority, 
            o_comment """
    create_mv_orders(mv_name_6, mv_stmt_6)
    def job_name_6 = getJobName(db, mv_name_6)
    waitingMTMVTaskFinished(job_name_6)

    def sql_stmt_6 = """select  o_orderdate, o_shippriority, o_comment,
            sum(o_totalprice) as sum_total, 
            max(o_totalprice) as max_total, 
            min(o_totalprice) as min_total, 
            count(*) as count_all, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1, 
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
            from orders_2_4 
            left join lineitem_2_4 on lineitem_2_4.l_orderkey = orders_2_4.o_orderkey
            left join partsupp_2_4 on partsupp_2_4.ps_partkey = lineitem_2_4.l_orderkey 
            group by 
            o_orderdate, 
            o_shippriority, 
            o_comment """
    explain {
        sql("${sql_stmt_6}")
        notContains "${mv_name_6}(${mv_name_6})"
    }
    compare_res(sql_stmt_6 + " order by 1,2,3,4,5,6,7")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name_6};"""



    // Todo: union rewriting
    // agg function + union rewriting
//    def mv_name_7 = "mv_name_2_4_7"
//    def mv_stmt_7 = """select
//            sum(o_totalprice) as sum_total,
//            max(o_totalprice) as max_total,
//            min(o_totalprice) as min_total,
//            count(*) as count_all,
//            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1,
//            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2
//            from orders_2_4
//            where o_orderdate >= '2023-10-17'"""
//    create_mv_orders(mv_name_7, mv_stmt_7)
//    def job_name_7 = getJobName(db, mv_name_7)
//    waitingMTMVTaskFinished(job_name_7)
//
//    def sql_stmt_7 = """select
//            sum(o_totalprice) as sum_total,
//            max(o_totalprice) as max_total,
//            min(o_totalprice) as min_total,
//            count(*) as count_all,
//            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1,
//            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2
//            from orders_2_4
//            where o_orderdate >= "2023-10-15" """
//    explain {
//        sql("${sql_stmt_7}")
//        contains "${mv_name_7}(${mv_name_7})"
//    }
//
//    // group by + union rewriting
//    def mv_name_8 = "mv_name_2_4_8"
//    def mv_stmt_8 = """select o_orderdate, o_shippriority, o_comment
//            from orders_2_4
//            left join lineitem_2_4 on lineitem_2_4.l_orderkey = orders_2_4.o_orderkey
//            where l_shipdate >= "2023-10-17"
//            group by
//            o_orderdate,
//            o_shippriority,
//            o_comment """
//    create_mv_orders(mv_name_8, mv_stmt_8)
//    def job_name_8 = getJobName(db, mv_name_8)
//    waitingMTMVTaskFinished(job_name_8)
//
//    def sql_stmt_8 = """select o_orderdate, o_shippriority, o_comment
//            from orders_2_4
//            left join lineitem_2_4 on lineitem_2_4.l_orderkey = orders_2_4.o_orderkey
//            where l_shipdate >= "2023-10-15"
//            group by
//            o_orderdate,
//            o_shippriority,
//            o_comment """
//    explain {
//        sql("${sql_stmt_8}")
//        contains "${mv_name_8}(${mv_name_8})"
//    }
//
//    // agg function + group by + union rewriting
//    def mv_name_9 = "mv_name_2_4_9"
//    def mv_stmt_9 = """select  o_orderdate, o_shippriority, o_comment,
//            sum(o_totalprice) as sum_total,
//            max(o_totalprice) as max_total,
//            min(o_totalprice) as min_total,
//            count(*) as count_all,
//            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1,
//            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2
//            from orders_2_4
//            left join lineitem_2_4 on lineitem_2_4.l_orderkey = orders_2_4.o_orderkey
//            where l_shipdate >= "2023-10-17"
//            group by
//            o_orderdate,
//            o_shippriority,
//            o_comment """
//    create_mv_orders(mv_name_9, mv_stmt_9)
//    def job_name_9 = getJobName(db, mv_name_9)
//    waitingMTMVTaskFinished(job_name_9)
//
//    def sql_stmt_9 = """select  o_orderdate, o_shippriority, o_comment,
//            sum(o_totalprice) as sum_total,
//            max(o_totalprice) as max_total,
//            min(o_totalprice) as min_total,
//            count(*) as count_all,
//            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1,
//            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2
//            from orders_2_4
//            left join lineitem_2_4 on lineitem_2_4.l_orderkey = orders_2_4.o_orderkey
//            left join partsupp_2_4 on partsupp_2_4.ps_partkey = lineitem_2_4.l_orderkey
//            where l_shipdate >= "2023-10-15"
//            group by
//            o_orderdate,
//            o_shippriority,
//            o_comment  """
//    explain {
//        sql("${sql_stmt_9}")
//        contains "${mv_name_9}(${mv_name_9})"
//    }

    // predicate compensate
    // agg function + predicate compensate
    def mv_name_10 = "mv_name_2_4_10"
    def mv_stmt_10 = """select 
            sum(o_totalprice) as sum_total, 
            max(o_totalprice) as max_total, 
            min(o_totalprice) as min_total, 
            count(*) as count_all, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1, 
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
            from orders_2_4 
            where o_orderdate >= '2023-10-17'"""
    create_all_mv(mv_name_10, mv_stmt_10)
    def job_name_10 = getJobName(db, mv_name_10)
    waitingMTMVTaskFinished(job_name_10)

    def sql_stmt_10 = """select t.sum_total from (select 
            sum(o_totalprice) as sum_total, 
            max(o_totalprice) as max_total, 
            min(o_totalprice) as min_total, 
            count(*) as count_all, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1, 
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
            from orders_2_4 where o_orderdate >= "2023-10-17" )  as t
            where t.count_all = 3"""
    explain {
        sql("${sql_stmt_10}")
        contains "${mv_name_10}(${mv_name_10})"
    }
    compare_res(sql_stmt_10 + " order by 1")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name_10};"""


    // group by + predicate compensate

    def mv_name_11 = "mv_name_2_4_11"
    def mv_stmt_11 = """select o_orderdate, o_shippriority, o_comment 
            from orders_2_4 
            where o_orderdate >= "2023-10-17"
            group by 
            o_orderdate, 
            o_shippriority, 
            o_comment """
    create_all_mv(mv_name_11, mv_stmt_11)
    def job_name_11 = getJobName(db, mv_name_11)
    waitingMTMVTaskFinished(job_name_11)

    def sql_stmt_11 = """select o_orderdate, o_shippriority, o_comment
            from orders_2_4
            where o_orderdate >= "2023-10-17" and o_totalprice = 1
            group by
            o_orderdate,
            o_shippriority,
            o_comment """
    explain {
        sql("${sql_stmt_11}")
        notContains "${mv_name_11}(${mv_name_11})"
    }
    compare_res(sql_stmt_11 + " order by 1,2,3")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name_11};"""

    def mv_name_16 = "mv_name_2_4_16"
    def mv_stmt_16 = """select o_orderdate, o_shippriority, o_comment, o_totalprice 
            from orders_2_4 
            where o_orderdate >= "2023-10-17"
            group by 
            o_orderdate, 
            o_shippriority, 
            o_comment,
            o_totalprice """
    create_all_mv(mv_name_16, mv_stmt_16)
    def job_name_16 = getJobName(db, mv_name_16)
    waitingMTMVTaskFinished(job_name_16)

    def sql_stmt_16 = """select o_orderdate, o_shippriority, o_comment
            from orders_2_4
            where o_orderdate >= "2023-10-17" and o_totalprice = 1
            group by
            o_orderdate,
            o_shippriority,
            o_comment """

    def agg_sql_explain_1 = sql """explain ${sql_stmt_16};"""
    def mv_index_1 = agg_sql_explain_1.toString().indexOf("MaterializedViewRewriteFail:")
    assert(mv_index_1 != -1)
    assert(agg_sql_explain_1.toString().substring(0, mv_index_1).indexOf(mv_name_16) != -1)

    compare_res(sql_stmt_16 + " order by 1,2,3")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name_16};"""

    // agg function + group by + predicate compensate
    def mv_name_12 = "mv_name_2_4_12"
    def mv_stmt_12 = """select o_orderdate, o_shippriority, o_comment , o_totalprice, 
            sum(o_totalprice) as sum_total, 
            max(o_totalprice) as max_total, 
            min(o_totalprice) as min_total, 
            count(*) as count_all, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1, 
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
            from orders_2_4 
            where o_orderdate >= "2023-10-17" 
            group by 
            o_orderdate, 
            o_shippriority, 
            o_comment,
            o_totalprice """
    create_all_mv(mv_name_12, mv_stmt_12)
    def job_name_12 = getJobName(db, mv_name_12)
    waitingMTMVTaskFinished(job_name_12)

    def sql_stmt_12 = """select t.o_orderdate, t.o_shippriority, t.o_comment, 
            t.sum_total, t.max_total, t.min_total, t.count_all 
            from  (
            select o_orderdate, o_shippriority, o_comment , o_totalprice, 
            sum(o_totalprice) as sum_total, 
            max(o_totalprice) as max_total, 
            min(o_totalprice) as min_total, 
            count(*) as count_all, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1, 
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
            from orders_2_4 where o_orderdate >= "2023-10-17" 
            group by 
            o_orderdate, 
            o_shippriority, 
            o_comment,
            o_totalprice 
            ) as t 
            where t.o_totalprice = 1 
             """
    explain {
        sql("${sql_stmt_12}")
        contains "${mv_name_12}(${mv_name_12})"
    }
    compare_res(sql_stmt_12 + " order by 1,2,3,4,5,6,7")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name_12};"""


    // project rewriting
    // agg function + group by + project rewriting
    def mv_name_13 = "mv_name_2_4_13"
    def mv_stmt_13 = """select sum(o_totalprice) as sum_total, 
            max(o_totalprice) as max_total, 
            min(o_totalprice) as min_total, 
            count(*) as count_all, 
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1, 
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2 
            from orders_2_4  
            where  o_orderkey > 1 + 1  """
    create_all_mv(mv_name_13, mv_stmt_13)
    def job_name_13 = getJobName(db, mv_name_13)
    waitingMTMVTaskFinished(job_name_13)

    def sql_stmt_13 = """select sum(o_totalprice) + count(*) , 
            count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) as cnt_1,
            count(distinct case when O_SHIPPRIORITY > 2 and o_orderkey IN (2) then o_custkey else null end) as cnt_2 
            from orders_2_4  
            where o_orderkey > (-3) + 5 """
    explain {
        sql("${sql_stmt_13}")
        contains "${mv_name_13}(${mv_name_13})"
    }
    compare_res(sql_stmt_13 + " order by 1")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name_13};"""


    // group by + project rewriting
    def mv_name_14 = "mv_name_2_4_14"
    def mv_stmt_14 = """select o_orderdate, o_shippriority, o_comment 
            from orders_2_4 
            where o_orderkey > 1 + 1 
            group by 
            o_orderdate, 
            o_shippriority, 
            o_comment """
    create_all_mv(mv_name_14, mv_stmt_14)
    def job_name_14 = getJobName(db, mv_name_14)
    waitingMTMVTaskFinished(job_name_14)

    def sql_stmt_14 = """select o_orderdate + o_shippriority, o_comment 
            from orders_2_4 
            where o_orderkey > (-3) + 5 
            group by 
            o_orderdate, 
            o_shippriority, 
            o_comment """
    explain {
        sql("${sql_stmt_14}")
        contains "${mv_name_14}(${mv_name_14})"
    }
    compare_res(sql_stmt_14 + " order by 1,2")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name_14};"""


    // agg function + group by + project rewriting
    def mv_name_15 = "mv_name_2_4_15"
    def mv_stmt_15 = """select o_orderdate, o_shippriority, o_comment, o_custkey, 
            case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end as cnt_1, 
            case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end as cnt_2 
            from orders_2_4 
            where o_orderkey > 1 + 1 
            group by 
            o_orderdate, 
            o_shippriority, 
            o_comment,
            o_shippriority,
            o_custkey,
            case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end,
            case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end """
    create_all_mv(mv_name_15, mv_stmt_15)
    def job_name_15 = getJobName(db, mv_name_15)
    waitingMTMVTaskFinished(job_name_15)

    def sql_stmt_15 = """select o_shippriority, o_comment, o_shippriority + o_custkey, 
            case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end as cnt_1,
            case when O_SHIPPRIORITY > 2 and o_orderkey IN (2) then o_custkey else null end as cnt_2 
            from orders_2_4 
            where  o_orderkey > (-3) + 5 
            group by 
            o_orderdate, 
            o_shippriority, 
            o_comment,
            o_custkey,
            case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end,
            case when O_SHIPPRIORITY > 2 and o_orderkey IN (2) then o_custkey else null end   """
    explain {
        sql("${sql_stmt_15}")
        contains "${mv_name_15}(${mv_name_15})"
    }
    compare_res(sql_stmt_15 + " order by 1,2,3,4,5")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name_15};"""

}
