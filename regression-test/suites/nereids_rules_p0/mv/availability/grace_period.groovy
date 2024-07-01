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

suite("grace_period") {

    // when mv refresh_time is in the grace_period(unit is second), materialized view will be use to
    // query rewrite regardless of the base table is update or not
    // when mv refresh_time is out of the grace_period(unit is second), will check the base table is update or not
    // if update will not be used to query rewrite
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set runtime_filter_mode=OFF"

    sql """
    drop table if exists orders_partition
    """
    sql """
    CREATE TABLE IF NOT EXISTS orders_partition (
      o_orderkey       integer not null,
      o_custkey        integer not null,
      o_orderstatus    char(1) not null,
      o_totalprice     decimalv3(15,2) not null,
      o_orderdate      date not null,
      o_orderpriority  char(15) not null,  
      o_clerk          char(15) not null, 
      o_shippriority   integer not null,
      o_comment        varchar(79) not null
    )
    DUPLICATE KEY(o_orderkey, o_custkey)
    PARTITION BY RANGE(o_orderdate)(
    FROM ('2023-10-17') TO ('2023-10-20') INTERVAL 1 DAY
    )
    DISTRIBUTED BY HASH(o_orderkey) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    );
    """

    sql """
    drop table if exists lineitem_partition
    """

    sql """
    CREATE TABLE IF NOT EXISTS lineitem_partition (
      l_orderkey    integer not null,
      l_partkey     integer not null,
      l_suppkey     integer not null,
      l_linenumber  integer not null,
      l_quantity    decimalv3(15,2) not null,
      l_extendedprice  decimalv3(15,2) not null,
      l_discount    decimalv3(15,2) not null,
      l_tax         decimalv3(15,2) not null,
      l_returnflag  char(1) not null,
      l_linestatus  char(1) not null,
      l_shipdate    date not null,
      l_commitdate  date not null,
      l_receiptdate date not null,
      l_shipinstruct char(25) not null,
      l_shipmode     char(10) not null,
      l_comment      varchar(44) not null
    )
    DUPLICATE KEY(l_orderkey, l_partkey, l_suppkey, l_linenumber)
    PARTITION BY RANGE(l_shipdate) 
    (FROM ('2023-10-17') TO ('2023-10-20') INTERVAL 1 DAY)
    DISTRIBUTED BY HASH(l_orderkey) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    );
    """

    sql """
    insert into orders_partition values 
    (1, 1, 'ok', 99.5, '2023-10-17', 'a', 'b', 1, 'yy'),
    (2, 2, 'ok', 109.2, '2023-10-18', 'c','d',2, 'mm'),
    (3, 3, 'ok', 99.5, '2023-10-19', 'a', 'b', 1, 'yy'); 
    """

    sql """
    insert into lineitem_partition values 
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (2, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy'),
    (3, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx');
    """


    def create_partition_mv = { mv_name, mv_sql, grace_period, partition_field ->
        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name}"""
        sql"""
        CREATE MATERIALIZED VIEW ${mv_name} 
        BUILD IMMEDIATE REFRESH AUTO ON MANUAL
        partition by(${partition_field})
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1', 'grace_period'= '${grace_period}') 
        AS ${mv_sql}
        """
        def job_name = getJobName(db, mv_name);
        waitingMTMVTaskFinished(job_name)
    }


    def create_un_partition_mv = { mv_name, mv_sql, grace_period ->
        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name}"""
        sql"""
        CREATE MATERIALIZED VIEW ${mv_name} 
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1', 'grace_period'= '${grace_period}') 
        AS ${mv_sql}
        """
        def job_name = getJobName(db, mv_name);
        waitingMTMVTaskFinished(job_name)
    }


    def mv_partition_consistent_name = "mv_partition_consistent"
    create_partition_mv(mv_partition_consistent_name,
            """
            select l_shipdate, o_orderdate, l_partkey,
            l_suppkey, sum(o_totalprice) as sum_total
            from lineitem_partition
            left join orders_partition on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            l_shipdate,
            o_orderdate,
            l_partkey,
            l_suppkey""",
            0,
            "l_shipdate")

    sql """
    insert into lineitem_partition values 
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy');
    """

    // force consistency when partition table, and query use the partition changed, should fail
    explain {
        sql("""
        select l_shipdate, o_orderdate, l_partkey,
        l_suppkey, sum(o_totalprice) as sum_total
        from lineitem_partition
        left join orders_partition on l_orderkey = o_orderkey and l_shipdate = o_orderdate
        where l_shipdate = '2023-10-17'
        group by
        l_shipdate,
        o_orderdate,
        l_partkey,
        l_suppkey;
        """)
        notContains("${mv_partition_consistent_name}(${mv_partition_consistent_name})")
    }
    // force consistency when partition table, and query doesn't use the partition changed, should success
    explain {
        sql("""
        select l_shipdate, o_orderdate, l_partkey,
        l_suppkey, sum(o_totalprice) as sum_total
        from lineitem_partition
        left join orders_partition on l_orderkey = o_orderkey and l_shipdate = o_orderdate
        where l_shipdate = '2023-10-18'
        group by
        l_shipdate,
        o_orderdate,
        l_partkey,
        l_suppkey;
        """)
        contains("${mv_partition_consistent_name}(${mv_partition_consistent_name})")
    }
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_partition_consistent_name}"""




    def mv_un_partition_consistent_name = "mv_un_partition_consistent"
    create_un_partition_mv(mv_un_partition_consistent_name,
            """
            select l_shipdate, o_orderdate, l_partkey,
            l_suppkey, sum(o_totalprice) as sum_total
            from lineitem_partition
            left join orders_partition on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            l_shipdate,
            o_orderdate,
            l_partkey,
            l_suppkey""",
            0)
    sql """
    insert into lineitem_partition values 
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy');
    """
    // force consistency when un partition table, and query use the partition changed, should fail
    explain {
        sql("""
        select l_shipdate, o_orderdate, l_partkey,
        l_suppkey, sum(o_totalprice) as sum_total
        from lineitem_partition
        left join orders_partition on l_orderkey = o_orderkey and l_shipdate = o_orderdate
        where l_shipdate = '2023-10-17'
        group by
        l_shipdate,
        o_orderdate,
        l_partkey,
        l_suppkey;
        """)
        notContains("${mv_un_partition_consistent_name}(${mv_un_partition_consistent_name})")
    }

    // force consistency when un partition table, and query use the partition changed, should fail
    explain {
        sql("""
        select l_shipdate, o_orderdate, l_partkey,
        l_suppkey, sum(o_totalprice) as sum_total
        from lineitem_partition
        left join orders_partition on l_orderkey = o_orderkey and l_shipdate = o_orderdate
        where l_shipdate = '2023-10-18'
        group by
        l_shipdate,
        o_orderdate,
        l_partkey,
        l_suppkey;
        """)
        notContains("${mv_un_partition_consistent_name}(${mv_un_partition_consistent_name})")
    }
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_un_partition_consistent_name}"""




    def mv_partition_allow_staleness_name = "mv_partition_allow_staleness"
    create_partition_mv(mv_partition_allow_staleness_name,
            """
            select l_shipdate, o_orderdate, l_partkey,
            l_suppkey, sum(o_totalprice) as sum_total
            from lineitem_partition
            left join orders_partition on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            l_shipdate,
            o_orderdate,
            l_partkey,
            l_suppkey""",
            15,
            "l_shipdate")

    sql """
    insert into lineitem_partition values 
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy');
    """

    // allow 10s staleness when partition table, and query use the partition changed, should success
    explain {
        sql("""
        select l_shipdate, o_orderdate, l_partkey,
        l_suppkey, sum(o_totalprice) as sum_total
        from lineitem_partition
        left join orders_partition on l_orderkey = o_orderkey and l_shipdate = o_orderdate
        where l_shipdate = '2023-10-17'
        group by
        l_shipdate,
        o_orderdate,
        l_partkey,
        l_suppkey;
        """)
        contains("${mv_partition_allow_staleness_name}(${mv_partition_allow_staleness_name})")
    }
    // allow 10s staleness when partition table, and query doesn't use the partition changed, should success
    explain {
        sql("""
        select l_shipdate, o_orderdate, l_partkey,
        l_suppkey, sum(o_totalprice) as sum_total
        from lineitem_partition
        left join orders_partition on l_orderkey = o_orderkey and l_shipdate = o_orderdate
        where l_shipdate = '2023-10-18'
        group by
        l_shipdate,
        o_orderdate,
        l_partkey,
        l_suppkey;
        """)
        contains("${mv_partition_allow_staleness_name}(${mv_partition_allow_staleness_name})")
    }
    sql "SET enable_materialized_view_rewrite=false"
    // allow 10s staleness when partition table, and query use the partition changed, should success,
    // but disable materialized view rewrite, should fail
    explain {
        sql("""
        select l_shipdate, o_orderdate, l_partkey,
        l_suppkey, sum(o_totalprice) as sum_total
        from lineitem_partition
        left join orders_partition on l_orderkey = o_orderkey and l_shipdate = o_orderdate
        where l_shipdate = '2023-10-17'
        group by
        l_shipdate,
        o_orderdate,
        l_partkey,
        l_suppkey;
        """)
        notContains("${mv_partition_allow_staleness_name}(${mv_partition_allow_staleness_name})")
    }
    // allow 10s staleness when partition table, and query doesn't use the partition changed,
    // but disable materialized view rewrite, should fail
    explain {
        sql("""
        select l_shipdate, o_orderdate, l_partkey,
        l_suppkey, sum(o_totalprice) as sum_total
        from lineitem_partition
        left join orders_partition on l_orderkey = o_orderkey and l_shipdate = o_orderdate
        where l_shipdate = '2023-10-18'
        group by
        l_shipdate,
        o_orderdate,
        l_partkey,
        l_suppkey;
        """)
        notContains("${mv_partition_allow_staleness_name}(${mv_partition_allow_staleness_name})")
    }
    sql "SET enable_materialized_view_rewrite=true"
    Thread.sleep(15000);
    // after 10s when partition table, and query use the partition changed, should fail
    explain {
        sql("""
        select l_shipdate, o_orderdate, l_partkey,
        l_suppkey, sum(o_totalprice) as sum_total
        from lineitem_partition
        left join orders_partition on l_orderkey = o_orderkey and l_shipdate = o_orderdate
        where l_shipdate = '2023-10-17'
        group by
        l_shipdate,
        o_orderdate,
        l_partkey,
        l_suppkey;
        """)
        notContains("${mv_partition_allow_staleness_name}(${mv_partition_allow_staleness_name})")
    }
    // after 10s when partition table, and query doesn't use the partition changed, should success
    explain {
        sql("""
        select l_shipdate, o_orderdate, l_partkey,
        l_suppkey, sum(o_totalprice) as sum_total
        from lineitem_partition
        left join orders_partition on l_orderkey = o_orderkey and l_shipdate = o_orderdate
        where l_shipdate = '2023-10-18'
        group by
        l_shipdate,
        o_orderdate,
        l_partkey,
        l_suppkey;
        """)
        contains("${mv_partition_allow_staleness_name}(${mv_partition_allow_staleness_name})")
    }
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_partition_allow_staleness_name}"""




    def mv_un_partition_allow_staleness_name = "mv_un_partition_allow_staleness"
    create_un_partition_mv(mv_un_partition_allow_staleness_name,
            """
            select l_shipdate, o_orderdate, l_partkey,
            l_suppkey, sum(o_totalprice) as sum_total
            from lineitem_partition
            left join orders_partition on l_orderkey = o_orderkey and l_shipdate = o_orderdate
            group by
            l_shipdate,
            o_orderdate,
            l_partkey,
            l_suppkey""",
            15)

    sql """
    insert into lineitem_partition values 
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy');
    """
    // allow 10s staleness when un partition table should success
    explain {
        sql("""
        select l_shipdate, o_orderdate, l_partkey,
        l_suppkey, sum(o_totalprice) as sum_total
        from lineitem_partition
        left join orders_partition on l_orderkey = o_orderkey and l_shipdate = o_orderdate
        where l_shipdate = '2023-10-17'
        group by
        l_shipdate,
        o_orderdate,
        l_partkey,
        l_suppkey;
        """)
        contains("${mv_un_partition_allow_staleness_name}(${mv_un_partition_allow_staleness_name})")
    }
    // allow 10s staleness when un partition table, should success
    explain {
        sql("""
        select l_shipdate, o_orderdate, l_partkey,
        l_suppkey, sum(o_totalprice) as sum_total
        from lineitem_partition
        left join orders_partition on l_orderkey = o_orderkey and l_shipdate = o_orderdate
        where l_shipdate = '2023-10-18'
        group by
        l_shipdate,
        o_orderdate,
        l_partkey,
        l_suppkey;
        """)
        contains("${mv_un_partition_allow_staleness_name}(${mv_un_partition_allow_staleness_name})")
    }
    sql "SET enable_materialized_view_rewrite=false"
    // allow 10s staleness when un partition table, but disable materialized view rewrite, should fail
    explain {
        sql("""
        select l_shipdate, o_orderdate, l_partkey,
        l_suppkey, sum(o_totalprice) as sum_total
        from lineitem_partition
        left join orders_partition on l_orderkey = o_orderkey and l_shipdate = o_orderdate
        where l_shipdate = '2023-10-17'
        group by
        l_shipdate,
        o_orderdate,
        l_partkey,
        l_suppkey;
        """)
        notContains("${mv_un_partition_allow_staleness_name}(${mv_un_partition_allow_staleness_name})")
    }
    // allow 10s staleness when un partition table, but disable materialized view rewrite, should fail
    explain {
        sql("""
        select l_shipdate, o_orderdate, l_partkey,
        l_suppkey, sum(o_totalprice) as sum_total
        from lineitem_partition
        left join orders_partition on l_orderkey = o_orderkey and l_shipdate = o_orderdate
        where l_shipdate = '2023-10-18'
        group by
        l_shipdate,
        o_orderdate,
        l_partkey,
        l_suppkey;
        """)
        notContains("${mv_un_partition_allow_staleness_name}(${mv_un_partition_allow_staleness_name})")
    }
    sql "SET enable_materialized_view_rewrite=true"
    Thread.sleep(15000);
    // after 10s when un partition table, and query use the partition changed, should fail
    explain {
        sql("""
        select l_shipdate, o_orderdate, l_partkey,
        l_suppkey, sum(o_totalprice) as sum_total
        from lineitem_partition
        left join orders_partition on l_orderkey = o_orderkey and l_shipdate = o_orderdate
        where l_shipdate = '2023-10-17'
        group by
        l_shipdate,
        o_orderdate,
        l_partkey,
        l_suppkey;
        """)
        notContains("${mv_un_partition_allow_staleness_name}(${mv_un_partition_allow_staleness_name})")
    }
    // after 10s when un partition table, and query doesn't use the partition changed, should fail
    explain {
        sql("""
        select l_shipdate, o_orderdate, l_partkey,
        l_suppkey, sum(o_totalprice) as sum_total
        from lineitem_partition
        left join orders_partition on l_orderkey = o_orderkey and l_shipdate = o_orderdate
        where l_shipdate = '2023-10-18'
        group by
        l_shipdate,
        o_orderdate,
        l_partkey,
        l_suppkey;
        """)
        notContains("${mv_un_partition_allow_staleness_name}(${mv_un_partition_allow_staleness_name})")
    }
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_un_partition_allow_staleness_name}"""
}
