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

suite("two_table_join_elim_null_col") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET enable_materialized_view_rewrite=true"
    sql "SET enable_nereids_timeout = false"

    sql """
    drop table if exists orders_1_null
    """

    sql """CREATE TABLE `orders_1_null` (
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
    DUPLICATE KEY(`o_orderkey`)
    COMMENT 'OLAP'
    auto partition by range (date_trunc(`o_orderdate`, 'day')) ()
    DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    drop table if exists lineitem_1_null
    """

    sql """CREATE TABLE `lineitem_1_null` (
      `l_linenumber` INT NULL,
      `l_partkey` INT NULL,
      `l_orderkey` BIGINT NULL,
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
    DUPLICATE KEY(l_linenumber)
    COMMENT 'OLAP'
    auto partition by range (date_trunc(`l_shipdate`, 'day')) ()
    DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    drop table if exists partsupp_1_null
    """

    sql """CREATE TABLE `partsupp_1_null` (
      `ps_partkey` INT NULL,
      `ps_suppkey` INT NULL,
      `ps_availqty` INT NULL,
      `ps_supplycost` DECIMAL(15, 2) NULL,
      `ps_comment` VARCHAR(199) NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`ps_partkey`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`ps_partkey`) BUCKETS 24
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    insert into orders_1_null values 
    (1, null, 'o', 109.2, 'c','d',2, 'mm', '2023-10-17'),
    (3, 3, null, 99.5, 'a', 'b', 1, 'yy', '2023-10-19'),
    (2, 3, 'k', 109.2, null,'d',2, 'mm', '2023-10-21'),
    (null, 5, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-18'),
    (4, 5, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-19'); 
    """

    sql """
    insert into lineitem_1_null values 
    (null, 1, 2, 3, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (1, null, 3, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (3, 3, null, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
    (1, 2, 3, null, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (2, 3, null, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-18'),
    (3, 1, 1, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', null, 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
    (1, 3, null, 2, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17');
    """

    sql"""
    insert into partsupp_1_null values 
    (1, 1, 1, 99.5, 'yy'),
    (2, 2, 2, 109.2, 'mm'),
    (3, null, 1, 99.5, 'yy'); 
    """

    sql """analyze table lineitem_1_null with sync;"""
    sql """analyze table orders_1_null with sync;"""
    sql """analyze table partsupp_1_null with sync;"""

    def create_mv_all = { mv_name, mv_sql ->
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


    def mv_name = "mv_elim_null"

    def mv_inner = """
        select * from lineitem_1_null inner join orders_1_null on orders_1_null.o_orderkey = lineitem_1_null.l_orderkey
        """
    def mv_left = """
        select * from lineitem_1_null left join orders_1_null on orders_1_null.o_orderkey = lineitem_1_null.l_orderkey
        """
    def mv_inner_group = """
        select lineitem_1_null.l_shipdate, t1.o_orderdate from lineitem_1_null inner join (select o_orderdate from orders_1_null group by o_orderdate) t1 on t1.o_orderdate = lineitem_1_null.l_shipdate
        """
    def mv_left_group = """
        select lineitem_1_null.l_shipdate, t1.o_orderdate from lineitem_1_null left join (select o_orderdate from orders_1_null group by o_orderdate) t1 on t1.o_orderdate = lineitem_1_null.l_shipdate
        """
    def mv_inner_dist = """
        select lineitem_1_null.l_shipdate, t1.o_orderdate from lineitem_1_null inner join (select distinct(o_orderdate) as o_orderdate from orders_1_null) t1 on t1.o_orderdate = lineitem_1_null.l_shipdate
        """
    def mv_left_dist = """
        select lineitem_1_null.l_shipdate, t1.o_orderdate from lineitem_1_null left join (select distinct(o_orderdate) as o_orderdate from orders_1_null) t1 on t1.o_orderdate = lineitem_1_null.l_shipdate
        """

    def query_fact = """select lineitem_1_null.l_shipdate from lineitem_1_null;"""
    def query_dimens = """select orders_1_null.o_orderdate from orders_1_null;"""
    def query_fact_notnull = """select lineitem_1_null.l_shipdate from lineitem_1_null where lineitem_1_null.l_orderkey is not null group by l_shipdate"""
    def query_dimens_notnull = """select orders_1_null.o_orderdate from lineitem_1_null where orders_1_null.o_orderkey is not null group by o_orderdate"""
    def query_fact_filter = """select lineitem_1_null.l_shipdate from lineitem_1_null where lineitem_1_null.l_orderkey = 1 group by l_shipdate"""
    def query_dimens_filter = """select orders_1_null.o_orderdate from lineitem_1_null where orders_1_null.o_orderkey = 1 group by o_orderdate"""

    def mtmv_list = [mv_inner, mv_left, mv_inner_group, mv_left_group, mv_inner_dist, mv_left_dist]
    def query_list = [query_fact, query_dimens, query_fact_notnull, query_dimens_notnull, query_fact_filter, query_dimens_filter]
    for (int i = 0; i < mtmv_list.size(); i++) {
        def res = sql mtmv_list[i]
        assertTrue(res.size() > 0)
    }
    for (int i = 0; i < query_list.size(); i++) {
        def res = sql mtmv_list[i]
        assertTrue(res.size() > 0)
    }
    for (int i = 0; i < mtmv_list.size(); i++) {
        logger.info("i: " + i)
        create_mv_all(mv_name, mtmv_list[i])
        def job_name = getJobName(db, mv_name)
        waitingMTMVTaskFinished(job_name)
        if (i == 0) {
            for (int j = 0; j < query_list.size(); j++) {
                logger.info("j: " + j)
                if (j in []) {
                    explain {
                        sql("${mtmv_list[j]}")
                        contains "${mv_name}(${mv_name})"
                    }
                    compare_res(mtmv_list[j] + " order by 1")
                } else {
                    explain {
                        sql("${mtmv_list[j]}")
                        notContains "${mv_name}(${mv_name})"
                    }
                }
            }
        } else if (i == 1) {
            for (int j = 0; j < query_list.size(); j++) {
                logger.info("j: " + j)
                if (j in []) {
                    explain {
                        sql("${mtmv_list[j]}")
                        contains "${mv_name}(${mv_name})"
                    }
                    compare_res(mtmv_list[j] + " order by 1")
                } else {
                    explain {
                        sql("${mtmv_list[j]}")
                        notContains "${mv_name}(${mv_name})"
                    }
                }
            }
        } else if (i == 2) {
            for (int j = 0; j < query_list.size(); j++) {
                logger.info("j: " + j)
                if (j in []) {
                    explain {
                        sql("${mtmv_list[j]}")
                        contains "${mv_name}(${mv_name})"
                    }
                    compare_res(mtmv_list[j] + " order by 1")
                } else {
                    explain {
                        sql("${mtmv_list[j]}")
                        notContains "${mv_name}(${mv_name})"
                    }
                }
            }
        } else if (i == 3) {
            for (int j = 0; j < query_list.size(); j++) {
                logger.info("j: " + j)
                if (j in []) {
                    explain {
                        sql("${mtmv_list[j]}")
                        contains "${mv_name}(${mv_name})"
                    }
                    compare_res(mtmv_list[j] + " order by 1")
                } else {
                    explain {
                        sql("${mtmv_list[j]}")
                        notContains "${mv_name}(${mv_name})"
                    }
                }
            }
        } else if (i == 4) {
            for (int j = 0; j < query_list.size(); j++) {
                logger.info("j: " + j)
                if (j in []) {
                    explain {
                        sql("${mtmv_list[j]}")
                        contains "${mv_name}(${mv_name})"
                    }
                    compare_res(mtmv_list[j] + " order by 1")
                } else {
                    explain {
                        sql("${mtmv_list[j]}")
                        notContains "${mv_name}(${mv_name})"
                    }
                }
            }
        } else if (i == 5) {
            for (int j = 0; j < query_list.size(); j++) {
                logger.info("j: " + j)
                if (j in []) {
                    explain {
                        sql("${mtmv_list[j]}")
                        contains "${mv_name}(${mv_name})"
                    }
                    compare_res(mtmv_list[j] + " order by 1")
                } else {
                    explain {
                        sql("${mtmv_list[j]}")
                        notContains "${mv_name}(${mv_name})"
                    }
                }
            }
        }
        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name};"""
    }


}
