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
suite("negative_partition_mv_rewrite") {
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
    drop table if exists partsupp_1
    """

    sql """CREATE TABLE `partsupp_1` (
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

    sql"""
    insert into partsupp_1 values 
    (1, 1, 1, 99.5, 'yy'),
    (null, 2, 2, 109.2, 'mm'),
    (3, null, 1, 99.5, 'yy'); 
    """

    sql """analyze table orders_1 with sync;"""
    sql """analyze table lineitem_1 with sync;"""
    sql """analyze table partsupp_1 with sync;"""

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

    def create_mv = { mv_name, mv_sql ->
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

    def mv_name = "mv_1"
    def mtmv_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey 
        from lineitem_1 
        left join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        """

    create_mv_lineitem(mv_name, mtmv_sql)
    def job_name = getJobName(db, mv_name)
    waitingMTMVTaskFinished(job_name)

    // mtmv not exists query col
    def query_sql = """
        select o_orderkey 
        from lineitem_1 
        left join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        """
    explain {
        sql("${query_sql}")
        notContains "${mv_name}(${mv_name})"
    }

    // Swap tables on either side of the left join
    query_sql = """
        select l_shipdate 
        from  orders_1 
        left join lineitem_1 
        on orders_1.o_orderkey = lineitem_1.l_orderkey
        """
    explain {
        sql("${query_sql}")
        notContains "${mv_name}(${mv_name})"
    }

    // The filter condition of the query is not in the filter range of mtmv
    mtmv_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey, o_orderkey   
        from (select * from lineitem_1 where l_shipdate = '2023-10-17' ) t1 
        left join orders_1 
        on t1.l_orderkey = orders_1.o_orderkey
        """
    create_mv_lineitem(mv_name, mtmv_sql)
    job_name = getJobName(db, mv_name)
    waitingMTMVTaskFinished(job_name)
    query_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey, o_orderkey   
        from (select * from lineitem_1 where l_shipdate = '2023-10-18' ) t1 
        left join orders_1 
        on t1.l_orderkey = orders_1.o_orderkey
        """
    explain {
        sql("${query_sql}")
        notContains "${mv_name}(${mv_name})"
    }

    // The filter range of the query is larger than that of the mtmv
    mtmv_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey, o_orderkey 
        from lineitem_1 
        left join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey 
        where  orders_1.o_orderkey > 2
        """
    create_mv_lineitem(mv_name, mtmv_sql)
    job_name = getJobName(db, mv_name)
    waitingMTMVTaskFinished(job_name)
    query_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey, o_orderkey 
        from lineitem_1 
        left join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey 
        where  orders_1.o_orderkey > 1
        """
    explain {
        sql("${query_sql}")
        notContains "${mv_name}(${mv_name})"
    }

    query_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey, o_orderkey 
        from lineitem_1 
        left join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey 
        where  orders_1.o_orderkey > 2 or orders_1.o_orderkey < 0
        """
    explain {
        sql("${query_sql}")
        notContains "${mv_name}(${mv_name})"
    }

    // filter in
    mtmv_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey, o_orderkey 
        from lineitem_1 
        left join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey 
        where  orders_1.o_orderkey in (1, 2, 3)
        """
    create_mv_lineitem(mv_name, mtmv_sql)
    job_name = getJobName(db, mv_name)
    waitingMTMVTaskFinished(job_name)
    query_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey, o_orderkey 
        from lineitem_1 
        left join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey 
        where  orders_1.o_orderkey in (1, 2, 3, 4)
        """
    explain {
        sql("${query_sql}")
        notContains "${mv_name}(${mv_name})"
    }

    // agg not roll up
    mtmv_sql = """
        select o_orderdate, o_shippriority, o_comment 
            from orders_1 
            group by 
            o_orderdate, 
            o_shippriority, 
            o_comment 
        """
    create_mv_orders(mv_name, mtmv_sql)
    job_name = getJobName(db, mv_name)
    waitingMTMVTaskFinished(job_name)

    query_sql = """
        select o_orderdate 
            from orders_1
        """
    explain {
        sql("${query_sql}")
        notContains "${mv_name}(${mv_name})"
    }

    mtmv_sql = """
        select o_orderdate, o_shippriority, o_comment,
            sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1,
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2
            from orders_1
            group by
            o_orderdate,
            o_shippriority,
            o_comment
        """
    create_mv_orders(mv_name, mtmv_sql)
    job_name = getJobName(db, mv_name)
    waitingMTMVTaskFinished(job_name)

    query_sql = """
        select 
            sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1,
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2
            from orders_1
        """
    explain {
        sql("${query_sql}")
        contains "${mv_name}(${mv_name})"
    }

    // query partial rewriting
    mtmv_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey, count(*)
        from lineitem_1
        left join orders_1
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        group by l_shipdate, o_orderdate, l_partkey, l_suppkey
        """
    create_mv_lineitem(mv_name, mtmv_sql)
    job_name = getJobName(db, mv_name)
    waitingMTMVTaskFinished(job_name)

    query_sql = """
        select l_shipdate, l_partkey, count(*) from lineitem_1 
        group by l_shipdate, l_partkey
        """
    explain {
        sql("${query_sql}")
        notContains "${mv_name}(${mv_name})"
    }

    mtmv_sql = """
        select
        sum(o_totalprice) as sum_total,
        max(o_totalprice) as max_total,
        min(o_totalprice) as min_total,
        count(*) as count_all,
        bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1,
        bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2
        from orders_1
        left join lineitem_1 on lineitem_1.l_orderkey = orders_1.o_orderkey
        """
    create_mv(mv_name, mtmv_sql)
    job_name = getJobName(db, mv_name)
    waitingMTMVTaskFinished(job_name)

    query_sql = """
        select
        count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) as cnt_1,
        count(distinct case when O_SHIPPRIORITY > 2 and o_orderkey IN (2) then o_custkey else null end) as cnt_2,
        sum(o_totalprice),
        max(o_totalprice),
        min(o_totalprice),
        count(*)
        from orders_1
        """
    explain {
        sql("${query_sql}")
        notContains "${mv_name}(${mv_name})"
    }

    // view partial rewriting
    mtmv_sql = """
        select l_shipdate, l_partkey, l_orderkey, count(*) from lineitem_1 group by l_shipdate, l_partkey, l_orderkey
        """
    create_mv_lineitem(mv_name, mtmv_sql)
    job_name = getJobName(db, mv_name)
    waitingMTMVTaskFinished(job_name)

    query_sql = """
        select l_shipdate, l_partkey, l_orderkey, count(*) 
        from lineitem_1 left join orders_1 
        on lineitem_1.l_shipdate=orders_1.o_orderdate 
        group by l_shipdate, l_partkey, l_orderkey
        """
    explain {
        sql("${query_sql}")
        notContains "${mv_name}(${mv_name})"
    }

    mtmv_sql = """
        select o_orderdate, o_shippriority, o_comment, l_orderkey, o_orderkey, sum(o_orderkey)   
            from orders_1 
            left join lineitem_1 on lineitem_1.l_orderkey = orders_1.o_orderkey
            group by 
            o_orderdate, 
            o_shippriority, 
            o_comment,
            l_orderkey,
            o_orderkey
        """
    create_mv_orders(mv_name, mtmv_sql)
    job_name = getJobName(db, mv_name)
    waitingMTMVTaskFinished(job_name)

    query_sql = """
        select o_orderdate, o_shippriority, o_comment, l_orderkey, ps_partkey, sum(o_orderkey)
            from orders_1
            left join lineitem_1 on lineitem_1.l_orderkey = orders_1.o_orderkey
            left join partsupp_1 on partsupp_1.ps_partkey = lineitem_1.l_orderkey
            group by
            o_orderdate,
            o_shippriority,
            o_comment,
            l_orderkey, 
            ps_partkey
        """
    explain {
        sql("${query_sql}")
        notContains "${mv_name}(${mv_name})"
    }

    // union rewrite
    mtmv_sql = """
        select l_shipdate, o_orderdate, l_partkey, count(*)
        from lineitem_1
        left join orders_1
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        where l_shipdate >= "2023-10-17"
        group by l_shipdate, o_orderdate, l_partkey
        """
    create_mv_lineitem(mv_name, mtmv_sql)
    job_name = getJobName(db, mv_name)
    waitingMTMVTaskFinished(job_name)

    query_sql = """
        select l_shipdate, o_orderdate, l_partkey, count(*)
        from lineitem_1
        left join orders_1
        on lineitem_1.l_orderkey = orders_1.o_orderkey
        where l_shipdate >= "2023-10-15"
        group by l_shipdate, o_orderdate, l_partkey
        """
    explain {
        sql("${query_sql}")
        notContains "${mv_name}(${mv_name})"
    }

    mtmv_sql = """
        select l_shipdate, l_partkey, l_orderkey
        from lineitem_1
        where l_shipdate >= "2023-10-10"
        group by l_shipdate, l_partkey, l_orderkey
        """
    create_mv_lineitem(mv_name, mtmv_sql)
    job_name = getJobName(db, mv_name)
    waitingMTMVTaskFinished(job_name)

    query_sql = """
        select t.l_shipdate, o_orderdate, t.l_partkey
        from (select l_shipdate, l_partkey, l_orderkey from lineitem_1) t
        left join orders_1
        on t.l_orderkey = orders_1.o_orderkey
        where l_shipdate >= "2023-10-10"
        group by t.l_shipdate, o_orderdate, t.l_partkey
        """
    explain {
        sql("${query_sql}")
        notContains "${mv_name}(${mv_name})"
    }

    // project rewriting
    mtmv_sql = """
        select o_orderdate, o_shippriority, o_comment, o_orderkey, o_shippriority + o_custkey,
        case when o_shippriority > 0 and o_orderkey IN (1, 3) then o_custkey else null end cnt_1,
        case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end as cnt_2
        from orders_1;
        """
    create_mv_orders(mv_name, mtmv_sql)
    job_name = getJobName(db, mv_name)
    waitingMTMVTaskFinished(job_name)

    query_sql = """
        select o_shippriority, o_comment, o_shippriority + o_custkey  + o_orderkey,
        case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end cnt_1,
        case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end as cnt_2
        from orders_1;
        """
    explain {
        sql("${query_sql}")
        notContains "${mv_name}(${mv_name})"
    }

    // agg under join
    mtmv_sql = """
        select t1.o_orderdate, t1.o_shippriority, t1.o_orderkey 
        from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_shippriority from orders_1 group by o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_shippriority) as t1
        left join lineitem_1 on lineitem_1.l_orderkey = t1.o_orderkey
        group by
        t1.o_orderdate, t1.o_shippriority, t1.o_orderkey
        """
    create_mv_orders(mv_name, mtmv_sql)
    job_name = getJobName(db, mv_name)
    waitingMTMVTaskFinished(job_name)

    query_sql = """
        select t1.o_orderdate, t1.o_shippriority, t1.o_orderkey 
        from (select o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_shippriority from orders_1  where o_custkey > 1 group by o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_shippriority) as t1
        left join lineitem_1 on lineitem_1.l_orderkey = t1.o_orderkey
        group by
        t1.o_orderdate, t1.o_shippriority, t1.o_orderkey
        """
    explain {
        sql("${query_sql}")
        notContains "${mv_name}(${mv_name})"
    }

    // filter include and or
    mtmv_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey, o_orderkey 
        from lineitem_1 
        left join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey 
        where  orders_1.o_orderkey > 2 and orders_1.o_orderdate >= "2023-10-17" or l_partkey > 1
        """
    create_mv_lineitem(mv_name, mtmv_sql)
    job_name = getJobName(db, mv_name)
    waitingMTMVTaskFinished(job_name)
    query_sql = """
        select l_shipdate, o_orderdate, l_partkey, l_suppkey, o_orderkey 
        from lineitem_1 
        left join orders_1 
        on lineitem_1.l_orderkey = orders_1.o_orderkey 
        where  orders_1.o_orderkey > 2 and (orders_1.o_orderdate >= "2023-10-17" or l_partkey > 1)
        """
    explain {
        sql("${query_sql}")
        notContains "${mv_name}(${mv_name})"
    }

    // group by under group by
    mtmv_sql = """
        SELECT c_count, count(*) AS custdist 
        FROM (SELECT l_orderkey, count(o_orderkey) AS c_count FROM lineitem_1 
              LEFT OUTER JOIN orders_1 ON l_orderkey = o_custkey AND o_comment NOT LIKE '%special%requests%' 
              GROUP BY l_orderkey) AS c_orders 
        GROUP BY c_count ORDER BY custdist DESC, c_count DESC;
        """
    create_mv(mv_name, mtmv_sql)
    job_name = getJobName(db, mv_name)
    waitingMTMVTaskFinished(job_name)
    query_sql = """
        SELECT c_count, count(*) AS custdist 
        FROM (SELECT l_orderkey, count(o_orderkey) AS c_count FROM lineitem_1 
              LEFT OUTER JOIN orders_1 ON l_orderkey = o_custkey AND o_comment NOT LIKE '%special%requests%' 
              GROUP BY l_orderkey) AS c_orders 
        GROUP BY c_count ORDER BY custdist DESC, c_count DESC;"""
    explain {
        sql("${query_sql}")
        notContains "${mv_name}(${mv_name})"
    }

    // condition on not equal
    mtmv_sql = """
        select l_shipdate, o_orderdate, l_partkey, count(*)
        from lineitem_1
        left join orders_1
        on lineitem_1.l_orderkey > orders_1.o_orderkey 
        group by l_shipdate, o_orderdate, l_partkey;
        """
    create_mv_lineitem(mv_name, mtmv_sql)
    job_name = getJobName(db, mv_name)
    waitingMTMVTaskFinished(job_name)
    query_sql = """
        select l_shipdate, o_orderdate, l_partkey, count(*)
        from lineitem_1
        left join orders_1
        on lineitem_1.l_orderkey > orders_1.o_orderkey 
        group by l_shipdate, o_orderdate, l_partkey
    """
    explain {
        sql("${query_sql}")
        notContains "${mv_name}(${mv_name})"
    }

    // mtmv exists join but not exists agg, query exists agg
    mtmv_sql = """
        select l_shipdate, o_orderdate, l_partkey 
        from (select l_shipdate, l_partkey, l_orderkey from lineitem_1) as t
        left join orders_1
        on t.l_orderkey = orders_1.o_orderkey;
        """
    create_mv_lineitem(mv_name, mtmv_sql)
    job_name = getJobName(db, mv_name)
    waitingMTMVTaskFinished(job_name)
    query_sql = """    
        select l_shipdate, o_orderdate, l_partkey 
        from (select l_shipdate, l_partkey, l_orderkey from lineitem_1 group by l_shipdate, l_partkey, l_orderkey) as t
        left join orders_1
        on t.l_orderkey = orders_1.o_orderkey 
        group by l_shipdate, o_orderdate, l_partkey ;
        """
    explain {
        sql("${query_sql}")
        notContains "${mv_name}(${mv_name})"
    }
}
