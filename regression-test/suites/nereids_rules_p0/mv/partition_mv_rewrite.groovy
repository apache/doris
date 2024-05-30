import java.text.SimpleDateFormat

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

suite("partition_mv_rewrite") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "SET enable_nereids_planner=true"
    sql "set runtime_filter_mode=OFF"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET enable_materialized_view_rewrite=true"

    sql """
    drop table if exists orders
    """

    sql """
    CREATE TABLE IF NOT EXISTS orders  (
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
    FROM ('2023-10-16') TO ('2023-11-01') INTERVAL 1 DAY
    )
    DISTRIBUTED BY HASH(o_orderkey) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    );
    """

    sql """
    drop table if exists lineitem
    """

    // test pre init partition
    sql"""
    CREATE TABLE IF NOT EXISTS lineitem (
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
    (FROM ('2023-10-16') TO ('2023-11-01') INTERVAL 1 DAY)
    DISTRIBUTED BY HASH(l_orderkey) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    );
    """

    sql"""
    insert into orders values 
    (1, 1, 'ok', 99.5, '2023-10-17', 'a', 'b', 1, 'yy'),
    (2, 2, 'ok', 109.2, '2023-10-18', 'c','d',2, 'mm'),
    (3, 3, 'ok', 99.5, '2023-10-19', 'a', 'b', 1, 'yy'); 
    """

    sql """
    insert into lineitem values 
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (2, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy'),
    (3, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx');
    """


    def mv_def_sql = """
    select l_shipdate, o_orderdate, l_partkey,
    l_suppkey, sum(o_totalprice) as sum_total
    from lineitem
    left join orders on lineitem.l_orderkey = orders.o_orderkey and l_shipdate = o_orderdate
    group by
    l_shipdate,
    o_orderdate,
    l_partkey,
    l_suppkey;
    """

    def all_partition_sql = """
    select l_shipdate, o_orderdate, l_partkey, l_suppkey, sum(o_totalprice) as sum_total
    from lineitem
    left join orders on lineitem.l_orderkey = orders.o_orderkey and l_shipdate = o_orderdate
    group by
    l_shipdate,
    o_orderdate,
    l_partkey,
    l_suppkey;
   """


    def partition_sql = """
    select l_shipdate, o_orderdate, l_partkey, l_suppkey, sum(o_totalprice) as sum_total
    from lineitem
    left join orders on lineitem.l_orderkey = orders.o_orderkey and l_shipdate = o_orderdate
    where (l_shipdate>= '2023-10-18' and l_shipdate <= '2023-10-19')
    group by
    l_shipdate,
    o_orderdate,
    l_partkey,
    l_suppkey;
    """

    sql """DROP MATERIALIZED VIEW IF EXISTS mv_10086"""
    sql """DROP TABLE IF EXISTS mv_10086"""
    sql"""
        CREATE MATERIALIZED VIEW mv_10086
        BUILD IMMEDIATE REFRESH AUTO ON MANUAL
        partition by(l_shipdate)
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        ${mv_def_sql}
        """

    def mv_name = "mv_10086"

    def job_name = getJobName(db, mv_name);
    waitingMTMVTaskFinished(job_name)

    explain {
        sql("${all_partition_sql}")
        contains("${mv_name}(${mv_name})")
    }
    explain {
        sql("${partition_sql}")
        contains("${mv_name}(${mv_name})")
    }
    // base table partition data change
    sql """
    insert into lineitem values
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy');
    """
    waitingPartitionIsExpected("${mv_name}", "p_20231017_20231018", false)

    sql "SET enable_materialized_view_union_rewrite=false;"
    sql "SET enable_materialized_view_rewrite=false"
    order_qt_query_1_0_before "${all_partition_sql}"
    sql "SET enable_materialized_view_rewrite=true"
    explain {
        sql("${all_partition_sql}")
        // should rewrite fail when union rewrite disable if sub partition is invalid
        notContains("${mv_name}(${mv_name})")
    }
    order_qt_query_1_0_after "${all_partition_sql}"

    sql "SET enable_materialized_view_rewrite=false"
    order_qt_query_2_0_before "${partition_sql}"
    sql "SET enable_materialized_view_rewrite=true"
    explain {
        sql("${partition_sql}")
        // should rewrite successfully when union rewrite disable if doesn't query invalid partition
        contains("${mv_name}(${mv_name})")
    }
    order_qt_query_2_0_after "${partition_sql}"

    // enable union rewrite
    sql "SET enable_materialized_view_union_rewrite=true"
    sql "SET enable_materialized_view_rewrite=false"
    order_qt_query_3_0_before "${all_partition_sql}"
    sql "SET enable_materialized_view_rewrite=true"
    explain {
        sql("${all_partition_sql}")
        // should rewrite successful when union rewrite enalbe if sub partition is invalid
        contains("${mv_name}(${mv_name})")
    }
    order_qt_query_3_0_after "${all_partition_sql}"

    sql "SET enable_materialized_view_rewrite=false"
    order_qt_query_4_0_before "${partition_sql}"
    sql "SET enable_materialized_view_rewrite=true"
    explain {
        sql("${partition_sql}")
        // should rewrite successfully when union rewrite enable if doesn't query invalid partition
        contains("${mv_name}(${mv_name})")
    }
    order_qt_query_4_0_after "${partition_sql}"


    // base table add partition
    sql "REFRESH MATERIALIZED VIEW ${mv_name} AUTO"
    sql "SET enable_materialized_view_union_rewrite=false"
    waitingMTMVTaskFinished(getJobName(db, mv_name))
    sql """
    insert into lineitem values
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-21', '2023-10-21', '2023-10-21', 'a', 'b', 'yyyyyyyyy');
    """


    waitingPartitionIsExpected("${mv_name}", "p_20231021_20231022", false)
    sql "SET enable_materialized_view_rewrite=false"
    order_qt_query_5_0_before "${all_partition_sql}"
    sql "SET enable_materialized_view_rewrite=true"
    explain {
        sql("${all_partition_sql}")
        // should rewrite fail when union rewrite disable if base table add new partition
        notContains("${mv_name}(${mv_name})")
    }
    order_qt_query_5_0_after "${all_partition_sql}"

    sql "SET enable_materialized_view_rewrite=false"
    order_qt_query_6_0_before "${partition_sql}"
    sql "SET enable_materialized_view_rewrite=true"
    explain {
        sql("${partition_sql}")
        // should rewrite successfully when union rewrite disable if doesn't query new partition
        contains("${mv_name}(${mv_name})")
    }
    order_qt_query_6_0_after "${partition_sql}"

    // enable union rewrite
    sql "SET enable_materialized_view_union_rewrite=true"
    sql "SET enable_materialized_view_rewrite=false"
    order_qt_query_7_0_before "${all_partition_sql}"
    sql "SET enable_materialized_view_rewrite=true"
    explain {
        sql("${all_partition_sql}")
        // should rewrite successful when union rewrite enalbe if base table add new partition
        contains("${mv_name}(${mv_name})")
    }
    order_qt_query_7_0_after "${all_partition_sql}"

    sql "SET enable_materialized_view_rewrite=false"
    order_qt_query_8_0_before "${partition_sql}"
    sql "SET enable_materialized_view_rewrite=true"
    explain {
        sql("${partition_sql}")
        // should rewrite successfully when union rewrite enable if doesn't query new partition
        contains("${mv_name}(${mv_name})")
    }
    order_qt_query_8_0_after "${partition_sql}"


    // base table delete partition test
    sql "REFRESH MATERIALIZED VIEW ${mv_name} AUTO"
    sql "SET enable_materialized_view_union_rewrite=false"
    waitingMTMVTaskFinished(getJobName(db, mv_name))
    sql """ ALTER TABLE lineitem DROP PARTITION IF EXISTS p_20231017 FORCE;
    """
    // show partitions will cause error, tmp comment
//    waitingPartitionIsExpected("${mv_name}", "p_20231017_20231018", false)

    sql "SET enable_materialized_view_rewrite=false"
    order_qt_query_9_0_before "${all_partition_sql}"
    sql "SET enable_materialized_view_rewrite=true"
    explain {
        sql("${all_partition_sql}")
        // should rewrite fail when union rewrite disable if base table delete partition
        notContains("${mv_name}(${mv_name})")
    }
    order_qt_query_9_0_after "${all_partition_sql}"

    sql "SET enable_materialized_view_rewrite=false"
    order_qt_query_10_0_before "${partition_sql}"
    sql "SET enable_materialized_view_rewrite=true"
    explain {
        sql("${partition_sql}")
        // should rewrite successfully when union rewrite disable if doesn't query deleted partition
        contains("${mv_name}(${mv_name})")
    }
    order_qt_query_10_0_after "${partition_sql}"

    // enable union rewrite
    sql "SET enable_materialized_view_union_rewrite=true"
    sql "SET enable_materialized_view_rewrite=false"
    order_qt_query_11_0_before "${all_partition_sql}"
    sql "SET enable_materialized_view_rewrite=true"
    explain {
        sql("${all_partition_sql}")
        // should rewrite successful when union rewrite enalbe if base table delete partition
        contains("${mv_name}(${mv_name})")
    }
    order_qt_query_11_0_after "${all_partition_sql}"

    sql "SET enable_materialized_view_rewrite=false"
    order_qt_query_12_0_before "${partition_sql}"
    sql "SET enable_materialized_view_rewrite=true"
    explain {
        sql("${partition_sql}")
        // should rewrite successfully when union rewrite enable if doesn't query deleted partition
        contains("${mv_name}(${mv_name})")
    }
    order_qt_query_12_0_after "${partition_sql}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv_10086"""

    // test mv with ttl
    def today_str = new SimpleDateFormat("yyyy-MM-dd").format(new Date()).toString();

    sql """
    drop table if exists lineitem_static;
    """
    sql"""
    CREATE TABLE IF NOT EXISTS lineitem_static (
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
    (
    PARTITION `p1` VALUES LESS THAN ("2023-10-18"),
    PARTITION `p2` VALUES [("2023-10-18"), ("2023-10-20")),
    PARTITION `other` VALUES LESS THAN (MAXVALUE)
    )
    DISTRIBUTED BY HASH(l_orderkey) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    );
    """
    sql """
    insert into lineitem_static values 
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (2, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy'),
    (3, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx');
    """
    sql """
    insert into lineitem_static values 
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '${today_str}', '${today_str}', '${today_str}', 'a', 'b', 'yyyyyyyyy');
    """

    def ttl_mv_def_sql = """
    select l_shipdate, o_orderdate, l_partkey,
    l_suppkey, sum(o_totalprice) as sum_total
    from lineitem_static
    left join orders on l_orderkey = o_orderkey and l_shipdate = o_orderdate
    group by
    l_shipdate,
    o_orderdate,
    l_partkey,
    l_suppkey;
    """
    def ttl_all_partition_sql = """
    select l_shipdate, o_orderdate, l_partkey, l_suppkey, sum(o_totalprice) as sum_total
    from lineitem_static
    left join orders on l_orderkey = o_orderkey and l_shipdate = o_orderdate
    group by
    l_shipdate,
    o_orderdate,
    l_partkey,
    l_suppkey;
   """
    def ttl_partition_sql = """
    select l_shipdate, o_orderdate, l_partkey, l_suppkey, sum(o_totalprice) as sum_total
    from lineitem_static
    left join orders on l_orderkey = o_orderkey and l_shipdate = o_orderdate
    where (l_shipdate>= '2023-10-18' and l_shipdate <= '2023-10-19')
    group by
    l_shipdate,
    o_orderdate,
    l_partkey,
    l_suppkey;
    """
    def ttl_mv_name = "mv_10000"

    sql """analyze table lineitem_static with sync;"""

    def create_ttl_mtmv = { db_name, mv_inner_name, mv_inner_sql ->
        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_inner_name}"""
        sql"""
        CREATE MATERIALIZED VIEW ${mv_inner_name}
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        PARTITION BY(l_shipdate)
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1',
        'partition_sync_limit' = 2,
        'partition_sync_time_unit' = 'DAY',
        'partition_date_format' = 'yyyy-MM-dd')
        AS ${mv_inner_sql}
        """
        waitingMTMVTaskFinished(getJobName(db_name, mv_inner_name))
    }

    create_ttl_mtmv(db, ttl_mv_name, ttl_mv_def_sql)

    // test when mv is ttl
    sql "SET enable_materialized_view_union_rewrite=false"
    sql "SET enable_materialized_view_rewrite=true"
    explain {
        sql("${ttl_all_partition_sql}")
        // should rewrite fail when union rewrite disable and mv is ttl
        notContains("${ttl_mv_name}(${ttl_mv_name})")
    }

    sql "SET enable_materialized_view_rewrite=false"
    order_qt_query_14_0_before "${ttl_partition_sql}"
    sql "SET enable_materialized_view_rewrite=true"
    explain {
        sql("${ttl_partition_sql}")
        // should rewrite fail when union rewrite disable and query the partition which is not in mv
        notContains("${ttl_mv_name}(${ttl_mv_name})")
    }
    order_qt_query_14_0_after "${ttl_partition_sql}"

    // enable union rewrite
    sql "SET enable_materialized_view_union_rewrite=true"
    sql "SET enable_materialized_view_rewrite=true"
    explain {
        sql("${ttl_all_partition_sql}")
        // should rewrite successful when union rewrite enalbe and mv is ttl, query the partition which is in mv
        contains("${ttl_mv_name}(${ttl_mv_name})")
    }

    sql "SET enable_materialized_view_rewrite=false"
    order_qt_query_16_0_before "${ttl_partition_sql}"
    sql "SET enable_materialized_view_rewrite=true"
    explain {
        sql("${ttl_partition_sql}")
        // should rewrite fail when union rewrite enalbe and query the partition which is not in mv
        notContains("${ttl_mv_name}(${ttl_mv_name})")
    }
    order_qt_query_16_0_after "${ttl_partition_sql}"

    sql """ DROP MATERIALIZED VIEW IF EXISTS ${ttl_mv_name}"""
}
