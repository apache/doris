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
It mainly tests the left semi join and filter positions.
 */
suite("partition_mv_rewrite_dimension_2_left_semi_join") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"

    sql """
    drop table if exists orders_2_left_semi_join
    """

    sql """CREATE TABLE `orders_2_left_semi_join` (
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
    drop table if exists lineitem_2_left_semi_join
    """

    sql """CREATE TABLE `lineitem_2_left_semi_join` (
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
    insert into orders_2_left_semi_join values 
    (null, 1, 'o', 99.5, 'a', 'b', 1, 'yy', '2023-10-17'),
    (1, null, 'k', 109.2, 'c','d',2, 'mm', '2023-10-17'),
    (3, 3, null, 99.5, 'a', 'b', 1, 'yy', '2023-10-19'),
    (1, 2, 'o', null, 'a', 'b', 1, 'yy', '2023-10-20'),
    (2, 3, 'k', 109.2, null,'d',2, 'mm', '2023-10-21'),
    (3, 1, 'o', 99.5, 'a', null, 1, 'yy', '2023-10-22'),
    (1, 3, 'o', 99.5, 'a', 'b', null, 'yy', '2023-10-19'),
    (2, 1, 'k', 109.2, 'c','d',2, null, '2023-10-18'),
    (3, 2, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-17'),
    (4, 5, 'o', 99.5, 'a', 'b', 1, 'yy', '2023-10-19'); 
    """

    sql """
    insert into lineitem_2_left_semi_join values 
    (null, 1, 2, 3, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (1, null, 3, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (3, 3, null, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
    (1, 2, 3, null, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (2, 3, 2, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-18'),
    (3, 1, 1, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', null, 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
    (1, 3, 2, 2, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17');
    """

    sql """analyze table orders_2_left_semi_join with sync;"""
    sql """analyze table lineitem_2_left_semi_join with sync;"""

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

    // left semi join + filter on different position
    def mv_stmt_0 = """select t.l_shipdate, t.l_partkey, t.l_suppkey  
        from (select l_shipdate, l_partkey, l_suppkey, l_orderkey from lineitem_2_left_semi_join where l_shipdate = '2023-10-17') t
        left semi join orders_2_left_semi_join 
        on t.l_orderkey = orders_2_left_semi_join.o_orderkey"""

    def mv_stmt_1 = """select l_shipdate, l_partkey, l_suppkey 
        from lineitem_2_left_semi_join  
        left semi join (select o_orderdate,o_orderkey from orders_2_left_semi_join where o_orderdate = '2023-10-17' ) t 
        on lineitem_2_left_semi_join.l_orderkey = t.o_orderkey"""

    def mv_stmt_2 = """select l_shipdate, l_partkey, l_suppkey  
        from lineitem_2_left_semi_join  
        left semi join orders_2_left_semi_join 
        on lineitem_2_left_semi_join.l_orderkey = orders_2_left_semi_join.o_orderkey 
        where l_shipdate = '2023-10-17'"""

    def mv_stmt_3 = """select o_orderkey, o_orderdate, o_custkey  
        from orders_2_left_semi_join 
        left semi join  (select l_shipdate, l_orderkey, l_partkey, l_suppkey  from lineitem_2_left_semi_join  where l_shipdate = '2023-10-17') t 
        on t.l_orderkey = orders_2_left_semi_join.o_orderkey"""

    def mv_stmt_4 = """select t.o_orderkey, t.o_orderdate, t.o_custkey  
        from (select o_orderkey, o_orderdate, o_custkey from orders_2_left_semi_join where o_orderdate = '2023-10-17' ) t 
        left semi join lineitem_2_left_semi_join   
        on lineitem_2_left_semi_join.l_orderkey = t.o_orderkey"""

    def mv_stmt_5 = """select o_orderkey, o_orderdate, o_custkey   
        from orders_2_left_semi_join 
        left semi join lineitem_2_left_semi_join  
        on lineitem_2_left_semi_join.l_orderkey = orders_2_left_semi_join.o_orderkey 
        where o_orderdate = '2023-10-17'  """

    def mv_list_1 = [mv_stmt_0, mv_stmt_1, mv_stmt_2, mv_stmt_3, mv_stmt_4, mv_stmt_5]
    def order_by_stmt = " order by 1,2,3"
    for (int i = 0; i < mv_list_1.size(); i++) {
        logger.info("i:" + i)
        def mv_name = """mv_name_2_left_semi_join_${i}"""
        if (i < 3) {
            create_mv_lineitem(mv_name, mv_list_1[i])
        } else {
            create_mv_orders(mv_name, mv_list_1[i])
        }
        def job_name = getJobName(db, mv_name)
        waitingMTMVTaskFinished(job_name)
        if (i == 0) {
            for (int j = 0; j < mv_list_1.size(); j++) {
                logger.info("j:" + j)
                if (j in [ 0, 2]) {
                    explain {
                        sql("${mv_list_1[j]}")
                        contains "${mv_name}(${mv_name})"
                    }
                    compare_res(mv_list_1[j] + order_by_stmt)
                } else {
                    explain {
                        sql("${mv_list_1[j]}")
                        notContains "${mv_name}(${mv_name})"
                    }
                }
            }
        } else if (i == 1) {
            for (int j = 0; j < mv_list_1.size(); j++) {
                logger.info("j:" + j)
                if (j in [1]) {
                    explain {
                        sql("${mv_list_1[j]}")
                        contains "${mv_name}(${mv_name})"
                    }
                    compare_res(mv_list_1[j] + order_by_stmt)
                } else {
                    explain {
                        sql("${mv_list_1[j]}")
                        notContains "${mv_name}(${mv_name})"
                    }
                }
            }
        } else if (i == 2) {
            for (int j = 0; j < mv_list_1.size(); j++) {
                logger.info("j:" + j)
                if (j in [0, 2]) {
                    explain {
                        sql("${mv_list_1[j]}")
                        contains "${mv_name}(${mv_name})"
                    }
                    compare_res(mv_list_1[j] + order_by_stmt)
                } else {
                    explain {
                        sql("${mv_list_1[j]}")
                        notContains "${mv_name}(${mv_name})"
                    }
                }
            }
        } else if (i == 3) {
            for (int j = 0; j < mv_list_1.size(); j++) {
                logger.info("j:" + j)
                if (j in [3]) {
                    explain {
                        sql("${mv_list_1[j]}")
                        contains "${mv_name}(${mv_name})"
                    }
                    compare_res(mv_list_1[j] + order_by_stmt)
                } else {
                    explain {
                        sql("${mv_list_1[j]}")
                        notContains "${mv_name}(${mv_name})"
                    }
                }
            }
        } else if (i == 4) {
            for (int j = 0; j < mv_list_1.size(); j++) {
                logger.info("j:" + j)
                if (j in [4, 5]) {
                    explain {
                        sql("${mv_list_1[j]}")
                        contains "${mv_name}(${mv_name})"
                    }
                    compare_res(mv_list_1[j] + order_by_stmt)
                } else {
                    explain {
                        sql("${mv_list_1[j]}")
                        notContains "${mv_name}(${mv_name})"
                    }
                }
            }
        } else if (i == 5) {
            for (int j = 0; j < mv_list_1.size(); j++) {
                logger.info("j:" + j)
                if (j in [4, 5]) {
                    explain {
                        sql("${mv_list_1[j]}")
                        contains "${mv_name}(${mv_name})"
                    }
                    compare_res(mv_list_1[j] + order_by_stmt)
                } else {
                    explain {
                        sql("${mv_list_1[j]}")
                        notContains "${mv_name}(${mv_name})"
                    }
                }
            }
        }
        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name};"""
    }
}
