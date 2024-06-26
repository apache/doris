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

suite("filter_equal_or_notequal_case") {

    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"

    sql """
    drop table if exists orders_1
    """

    sql """CREATE TABLE `orders_1` (
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
    drop table if exists lineitem_1
    """

    sql """CREATE TABLE `lineitem_1` (
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
    insert into orders_1 values 
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
    insert into lineitem_1 values 
    (null, 1, 2, 3, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (1, null, 3, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (3, 3, null, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
    (1, 2, 3, null, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (2, 3, 2, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-18'),
    (3, 1, 1, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', null, 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
    (1, 3, 2, 2, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17');
    """

    sql """analyze table orders_1 with sync;"""
    sql """analyze table lineitem_1 with sync;"""

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

    def mv_name = "mv_1"
    def mtmv_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey, o_orderkey
        from lineitem_1 
        left join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        where l_shipdate = '2023-10-17'
        """

    create_mv_lineitem(mv_name, mtmv_sql)
    def job_name = getJobName(db, mv_name)
    waitingMTMVTaskFinished(job_name)

    // mv equal and sql equal
    def query_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey  
        from lineitem_1 
        left join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        where l_shipdate = '2023-10-17'
        """
    def res_tmp = sql """explain ${query_sql}"""
    logger.info("res_temp:" + res_tmp)
    explain {
        sql("${query_sql}")
        contains "${mv_name}(${mv_name})"
    }
    compare_res(query_sql + " order by 1,2,3,4")

    // mv equal and sql not equal
    query_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey 
        from lineitem_1 
        left join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        where l_shipdate != '2023-10-17'
        """
    explain {
        sql("${query_sql}")
        notContains "${mv_name}(${mv_name})"
    }

    // mv equal and sql equal and number is difference
    query_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey 
        from lineitem_1 
        left join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        where l_shipdate = '2023-10-19'
        """
    explain {
        sql("${query_sql}")
        notContains "${mv_name}(${mv_name})"
    }

    mtmv_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey, o_orderkey 
        from lineitem_1 
        left join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        where l_shipdate != '2023-10-17'
        """

    create_mv_lineitem(mv_name, mtmv_sql)
    job_name = getJobName(db, mv_name)
    waitingMTMVTaskFinished(job_name)

    // mv not equal and sql equal
    query_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey 
        from lineitem_1 
        left join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        where l_shipdate = '2023-10-17'
        """
    explain {
        sql("${query_sql}")
        notContains "${mv_name}(${mv_name})"
    }

    // mv not equal and sql not equal
    query_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey 
        from lineitem_1 
        left join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        where l_shipdate != '2023-10-17'
        """
    explain {
        sql("${query_sql}")
        contains "${mv_name}(${mv_name})"
    }
    compare_res(query_sql + " order by 1,2,3,4")


    mtmv_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey, o_orderkey  
        from lineitem_1 
        left join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        where l_shipdate > '2023-10-17'
        """

    create_mv_lineitem(mv_name, mtmv_sql)
    job_name = getJobName(db, mv_name)
    waitingMTMVTaskFinished(job_name)

    // mv is range and sql equal and filter not in mv range
    query_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey 
        from lineitem_1 
        left join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        where l_shipdate = '2023-10-16'
        """
    explain {
        sql("${query_sql}")
        notContains "${mv_name}(${mv_name})"
    }

    // mv is range and sql equal and filter in mv range
    query_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey 
        from lineitem_1 
        left join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        where l_shipdate = '2023-10-17'
        """
    explain {
        sql("${query_sql}")
        notContains "${mv_name}(${mv_name})"
    }

    // mv is range and sql equal and filter in mv range
    query_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey
        from lineitem_1
        left join orders_1
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        where l_shipdate = '2023-10-19'
        """
    explain {
        sql("${query_sql}")
        contains "${mv_name}(${mv_name})"
    }
    compare_res(query_sql + " order by 1,2,3,4")

    // mv is range and sql is range and sql range is bigger than mv
    query_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey 
        from lineitem_1 
        left join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        where l_shipdate > '2023-10-16'
        """
    explain {
        sql("${query_sql}")
        notContains "${mv_name}(${mv_name})"
    }

    // mv is range and sql is range and sql range is not in mv range
    query_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey 
        from lineitem_1 
        left join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        where l_shipdate < '2023-10-16'
        """
    explain {
        sql("${query_sql}")
        notContains "${mv_name}(${mv_name})"
    }

    // mv is range and sql is range and sql range is bigger than mv
    query_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey 
        from lineitem_1 
        left join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        where l_shipdate > '2023-10-17'
        """
    explain {
        sql("${query_sql}")
        contains "${mv_name}(${mv_name})"
    }
    compare_res(query_sql + " order by 1,2,3,4")

    // mv is range and sql is range and sql range is not in mv range
    query_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey 
        from lineitem_1 
        left join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        where l_shipdate < '2023-10-17'
        """
    explain {
        sql("${query_sql}")
        notContains "${mv_name}(${mv_name})"
    }

    // mv is range and sql is range and sql range is bigger than mv
    query_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey
        from lineitem_1
        left join orders_1
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        where l_shipdate > '2023-10-18'
        """
    explain {
        sql("${query_sql}")
        contains "${mv_name}(${mv_name})"
    }
    compare_res(query_sql + " order by 1,2,3,4")

    // mv is range and sql is range and sql range is not in mv range
    query_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey 
        from lineitem_1 
        left join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        where l_shipdate < '2023-10-19'
        """
    explain {
        sql("${query_sql}")
        notContains "${mv_name}(${mv_name})"
    }

    // mv is range and sql is range and sql range is bigger than mv
    query_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey  
        from lineitem_1 
        left join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        where l_shipdate > '2023-10-16' and l_shipdate < '2023-10-17'
        """
    explain {
        sql("${query_sql}")
        notContains "${mv_name}(${mv_name})"
    }

    // mv is range and sql is range and sql range is not in mv range
    query_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey 
        from lineitem_1 
        left join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        where l_shipdate > '2023-10-17' and l_shipdate < '2023-10-19'
        """
    explain {
        sql("${query_sql}")
        contains "${mv_name}(${mv_name})"
    }
    compare_res(query_sql + " order by 1,2,3,4")

    // sql range is not in mv range
    query_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey 
        from lineitem_1 
        left join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        where l_shipdate in ('2023-10-17')
        """
    explain {
        sql("${query_sql}")
        notContains "${mv_name}(${mv_name})"
    }

    // sql range is in mv range
    // single value
    query_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey
        from lineitem_1
        left join orders_1
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        where l_shipdate in ('2023-10-18')
        """
    explain {
        sql("${query_sql}")
        contains "${mv_name}(${mv_name})"
    }
    compare_res(query_sql + " order by 1,2,3,4")

    // multi value
    query_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey
        from lineitem_1
        left join orders_1
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        where l_shipdate in ('2023-10-18', '2023-11-18')
        """
    explain {
        sql("${query_sql}")
        contains "${mv_name}(${mv_name})"
    }
    compare_res(query_sql + " order by 1,2,3,4")

    // sql range like mv range
    query_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey 
        from lineitem_1 
        left join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        where l_shipdate like "%2023-10-18%"
        """
    explain {
        sql("${query_sql}")
        notContains "${mv_name}(${mv_name})"
    }

    // sql range is null
    query_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey 
        from lineitem_1 
        left join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        where l_shipdate is null
        """
    explain {
        sql("${query_sql}")
        notContains "${mv_name}(${mv_name})"
    }

    // sql range is not null
    query_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey  
        from lineitem_1 
        left join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        where l_shipdate is not null
        """
    explain {
        sql("${query_sql}")
        notContains "${mv_name}(${mv_name})"
    }

    mtmv_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey, o_orderkey  
        from lineitem_1 
        left join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        where l_shipdate > '2023-10-16' and l_shipdate < '2023-10-19'
        """

    create_mv_lineitem(mv_name, mtmv_sql)
    job_name = getJobName(db, mv_name)
    waitingMTMVTaskFinished(job_name)

    // mv is range and sql is range and filter not in mv range
    query_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey
        from lineitem_1
        left join orders_1
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        where l_shipdate > '2023-10-16' and l_shipdate < '2023-10-19'
        """
    explain {
        sql("${query_sql}")
        contains "${mv_name}(${mv_name})"
    }
    compare_res(query_sql + " order by 1,2,3,4")


    query_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey
        from lineitem_1
        left join orders_1
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        where l_shipdate > '2023-10-17' and l_shipdate < '2023-10-19'
        """
    explain {
        sql("${query_sql}")
        contains "${mv_name}(${mv_name})"
    }
    compare_res(query_sql + " order by 1,2,3,4")

    //
    mtmv_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey, o_orderkey  
        from lineitem_1 
        left join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        where l_shipdate in ('2023-10-17', '2023-10-18', '2023-10-19')
        """

    create_mv_lineitem(mv_name, mtmv_sql)
    job_name = getJobName(db, mv_name)
    waitingMTMVTaskFinished(job_name)

    // mv is in range and sql is in range and filter is in mv range
    query_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey 
        from lineitem_1 
        left join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        where l_shipdate in ('2023-10-17', '2023-10-18', '2023-10-19')
        """
    explain {
        sql("${query_sql}")
        contains "${mv_name}(${mv_name})"
    }
    compare_res(query_sql + " order by 1,2,3,4")

    // mv is in range and sql is in range and filter is not in mv range
    query_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey 
        from lineitem_1 
        left join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        where l_shipdate not in ('2023-10-17', '2023-10-18', '2023-10-19')
        """
    explain {
        sql("${query_sql}")
        notContains "${mv_name}(${mv_name})"
    }

    mtmv_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey, o_orderkey  
        from lineitem_1 
        left join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        where l_shipdate like "%2023-10-17%"
        """

    create_mv_lineitem(mv_name, mtmv_sql)
    job_name = getJobName(db, mv_name)
    waitingMTMVTaskFinished(job_name)

    // mv is like filter and sql is like filter and filter number is equal
    query_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey 
        from lineitem_1 
        left join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        where l_shipdate like "%2023-10-17%"
        """
    explain {
        sql("${query_sql}")
        contains "${mv_name}(${mv_name})"
    }
    compare_res(query_sql + " order by 1,2,3,4")

    // mv is like filter and sql is not like filter
    query_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey 
        from lineitem_1 
        left join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        where l_shipdate like "%2023-10-17%"
        """
    explain {
        sql("${query_sql}")
        contains "${mv_name}(${mv_name})"
    }
    compare_res(query_sql + " order by 1,2,3,4")

    // Todo: It is not currently supported and is expected to be
    // between .. and ..
//    mtmv_sql = """
//        select l_shipdate, o_orderdate, l_partkey, l_suppkey, o_orderkey
//        from lineitem_1
//        left join orders_1
//        on lineitem_1.l_orderkey = orders_1.o_orderkey
//        where l_shipdate between '2023-10-16' and '2023-10-19'
//        """
//
//    create_mv_lineitem(mv_name, mtmv_sql)
//    job_name = getJobName(db, mv_name)
//    waitingMTMVTaskFinished(job_name)
//
//    query_sql = """
//        select l_shipdate, o_orderdate, l_partkey, l_suppkey
//        from lineitem_1
//        left join orders_1
//        on lineitem_1.l_orderkey = orders_1.o_orderkey
//        where l_shipdate between '2023-10-17' and '2023-10-19'
//        """
//    explain {
//        sql("${query_sql}")
//        contains "${mv_name}(${mv_name})"
//    }
//    compare_res(query_sql + " order by 1,2,3,4")

    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name};"""
}
