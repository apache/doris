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
mtmv include left,right,full join, and query is inner join
 */
suite("inner_join_infer_and_derive") {

    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET enable_materialized_view_rewrite=true"
    sql "SET enable_nereids_timeout = false"

    sql """
    drop table if exists orders_inner
    """

    sql """CREATE TABLE `orders_inner` (
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
    AUTO PARTITION BY range (date_trunc(`o_orderdate`, 'day')) ()
    DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    drop table if exists lineitem_inner
    """

    sql """CREATE TABLE `lineitem_inner` (
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
    AUTO PARTITION BY range (date_trunc(`l_shipdate`, 'day')) ()
    DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    insert into orders_inner values 
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
    insert into lineitem_inner values 
    (null, 1, 2, 3, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (1, null, 3, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (3, 3, null, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
    (1, 2, 3, null, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (2, 3, 2, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-18'),
    (3, 1, 1, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', null, 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
    (1, 3, 2, 2, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17');
    """

    sql """analyze table orders_inner with sync;"""
    sql """analyze table lineitem_inner with sync;"""

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

    def mtmv_left_join_stmt = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey, o_orderkey
        from lineitem_inner
        left join orders_inner
        on lineitem_inner.l_orderkey = orders_inner.o_orderkey"""
    def mtmv_right_join_stmt = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey, o_orderkey
        from lineitem_inner
        right join orders_inner
        on lineitem_inner.l_orderkey = orders_inner.o_orderkey"""
    def mtmv_full_join_stmt = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey, o_orderkey
        from lineitem_inner
        full join orders_inner
        on lineitem_inner.l_orderkey = orders_inner.o_orderkey"""
    def mv_name_1 = "mv_inner"
    def mtmv_stmt_list = [mtmv_left_join_stmt, mtmv_right_join_stmt, mtmv_full_join_stmt]
    def query_stmt_0 = """select t.l_shipdate, o_orderdate, t.l_partkey, t.l_suppkey, orders_inner.o_orderkey 
        from (select l_shipdate, l_partkey, l_suppkey, l_orderkey from lineitem_inner where l_shipdate = '2023-10-17') t
        inner join orders_inner 
        on t.l_orderkey = orders_inner.o_orderkey"""
    def query_stmt_1 = """select l_shipdate, t.o_orderdate, l_partkey, l_suppkey, t.o_orderkey
        from lineitem_inner  
        inner join (select o_orderdate,o_orderkey from orders_inner where o_orderdate = '2023-10-17' ) t 
        on lineitem_inner.l_orderkey = t.o_orderkey"""
    def query_stmt_2 = """select l_shipdate, o_orderdate, l_partkey, l_suppkey, orders_inner.o_orderkey 
        from lineitem_inner  
        inner join orders_inner 
        on lineitem_inner.l_orderkey = orders_inner.o_orderkey 
        where l_shipdate = '2023-10-17'"""
    def query_stmt_3 = """select l_shipdate, o_orderdate, l_partkey, l_suppkey, orders_inner.o_orderkey 
        from lineitem_inner  
        inner join orders_inner 
        on lineitem_inner.l_orderkey = orders_inner.o_orderkey 
        where o_orderdate = '2023-10-17'"""
    def query_stmt_4 = """select l_shipdate, o_orderdate, l_partkey, l_suppkey, orders_inner.o_orderkey 
        from lineitem_inner  
        inner join orders_inner 
        on lineitem_inner.l_orderkey = orders_inner.o_orderkey 
        where o_orderkey = 1"""
    def query_stmt_5 = """select l_shipdate, o_orderdate, l_partkey, l_suppkey, orders_inner.o_orderkey 
        from lineitem_inner  
        inner join orders_inner 
        on lineitem_inner.l_orderkey = orders_inner.o_orderkey 
        where l_suppkey = 2"""
    def query_stmt_6 = """select t.l_shipdate, o_orderdate, t.l_partkey, t.l_suppkey, orders_inner.o_orderkey 
        from orders_inner 
        inner join  (select l_shipdate, l_orderkey, l_partkey, l_suppkey  from lineitem_inner  where l_shipdate = '2023-10-17') t 
        on t.l_orderkey = orders_inner.o_orderkey"""
    def query_stmt_7 = """select l_shipdate, t.o_orderdate, l_partkey, l_suppkey, t.o_orderkey 
        from (select o_orderdate, o_orderkey from orders_inner where o_orderdate = '2023-10-17' ) t 
        inner join lineitem_inner   
        on lineitem_inner.l_orderkey = t.o_orderkey"""
    def query_stmt_8 = """select l_shipdate, o_orderdate, l_partkey, l_suppkey, orders_inner.o_orderkey 
        from orders_inner  
        inner join lineitem_inner  
        on lineitem_inner.l_orderkey = orders_inner.o_orderkey 
        where l_shipdate = '2023-10-17' """
    def query_stmt_9 = """select l_shipdate, o_orderdate, l_partkey, l_suppkey, orders_inner.o_orderkey 
        from orders_inner 
        inner join lineitem_inner  
        on lineitem_inner.l_orderkey = orders_inner.o_orderkey 
        where o_orderdate = '2023-10-17'  """
    def query_stmt_10 = """select l_shipdate, o_orderdate, l_partkey, l_suppkey, orders_inner.o_orderkey 
        from orders_inner  
        inner join lineitem_inner  
        on lineitem_inner.l_orderkey = orders_inner.o_orderkey
        where o_orderkey = 1"""
    def query_stmt_11 = """select l_shipdate, o_orderdate, l_partkey, l_suppkey, orders_inner.o_orderkey 
        from orders_inner  
        inner join lineitem_inner  
        on lineitem_inner.l_orderkey = orders_inner.o_orderkey
        where l_suppkey = 2"""
    def query_stmt_12 = """select l_shipdate, o_orderdate, l_partkey, l_suppkey, orders_inner.o_orderkey 
        from orders_inner  
        inner join lineitem_inner  
        on lineitem_inner.l_orderkey = orders_inner.o_orderkey
        where l_suppkey = 2 and o_orderdate = '2023-10-17'"""
    def query_stmt_13 = """select l_shipdate, o_orderdate, l_partkey, l_suppkey, orders_inner.o_orderkey 
        from lineitem_inner  
        inner join orders_inner
        on lineitem_inner.l_orderkey = orders_inner.o_orderkey
        where l_suppkey = 2 and o_orderdate = '2023-10-17'"""

    def query_list = [query_stmt_0, query_stmt_1, query_stmt_2, query_stmt_3, query_stmt_4, query_stmt_5,
                      query_stmt_6, query_stmt_7, query_stmt_8, query_stmt_9, query_stmt_10, query_stmt_11, query_stmt_12, query_stmt_13]
    def order_stmt = " order by 1, 2, 3, 4, 5"
    for (int mtmv_it = 0; mtmv_it < mtmv_stmt_list.size(); mtmv_it++) {
        logger.info("mtmv_it:" + mtmv_it)
        create_all_mv(mv_name_1, mtmv_stmt_list[mtmv_it])
        def job_name_1 = getJobName(db, mv_name_1)
        waitingMTMVTaskFinished(job_name_1)
        if (mtmv_it == 0) {
            for (int i = 0; i < query_list.size(); i++) {
                logger.info("i: " + i)
                if (i in [0, 2, 5, 6, 8, 11]) {
                    explain {
                        sql("${query_list[i]}")
                        notContains "${mv_name_1}(${mv_name_1})"
                    }
                } else {
                    explain {
                        sql("${query_list[i]}")
                        contains "${mv_name_1}(${mv_name_1})"
                    }
                    compare_res(query_list[i] + order_stmt)
                }
            }
        } else if (mtmv_it == 1) {
            for (int i = 0; i < query_list.size(); i++) {
                logger.info("i: " + i)
                if (i in [1, 3, 4, 7, 9, 10]) {
                    explain {
                        sql("${query_list[i]}")
                        notContains "${mv_name_1}(${mv_name_1})"
                    }
                } else {
                    explain {
                        sql("${query_list[i]}")
                        contains "${mv_name_1}(${mv_name_1})"
                    }
                    compare_res(query_list[i] + order_stmt)
                }
            }
        } else if (mtmv_it == 2) {
            for (int i = 0; i < query_list.size(); i++) {
                logger.info("i: " + i)
                if (i in [12, 13]) {
                    explain {
                        sql("${query_list[i]}")
                        contains "${mv_name_1}(${mv_name_1})"
                    }
                    compare_res(query_list[i] + order_stmt)
                } else {
                    explain {
                        sql("${query_list[i]}")
                        notContains "${mv_name_1}(${mv_name_1})"
                    }
                }
            }
        }
        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name_1};"""
    }

}
