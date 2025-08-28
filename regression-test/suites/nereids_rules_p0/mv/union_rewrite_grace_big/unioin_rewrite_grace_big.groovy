package mv.partition_union_rewrite

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

suite("union_rewrite_grace_big") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set runtime_filter_mode=OFF"

    sql """
    drop table if exists orders
    """

    sql """
    CREATE TABLE IF NOT EXISTS orders  (
      o_orderkey       integer not null,
      o_custkey        integer not null,
      o_orderstatus    char(9) not null,
      o_totalprice     decimalv3(15,2) not null,
      o_orderdate      date not null,
      o_orderpriority  char(15) not null,  
      o_clerk          char(15) not null, 
      o_shippriority   integer not null,
      o_comment        varchar(79) not null
    )
    DUPLICATE KEY(o_orderkey, o_custkey)
    PARTITION BY RANGE(o_orderdate)(
    FROM ('2023-09-16') TO ('2023-12-30') INTERVAL 1 DAY
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
    (FROM ('2023-09-16') TO ('2023-12-30') INTERVAL 1 DAY)
    DISTRIBUTED BY HASH(l_orderkey) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    );
    """

    sql"""
    insert into orders values 
    (1, 1, 'ok', 99.5, '2023-10-17', 'a', 'b', 1, 'yy'),
    (1, 1, 'ok', 99.5, '2023-10-17', 'a', 'b', 1, 'yy'),
    (1, 1, 'ok', 99.5, '2023-10-17', 'a', 'b', 1, 'yy'),
    (1, 1, 'ok', 99.5, '2023-10-17', 'a', 'b', 1, 'yy'),
    (2, 2, 'ok', 109.2, '2023-10-18', 'c','d',2, 'mm'),
    (2, 2, 'ok', 109.2, '2023-10-18', 'c','d',2, 'mm'),
    (2, 2, 'ok', 109.2, '2023-10-18', 'c','d',2, 'mm'),
    (2, 2, 'ok', 109.2, '2023-10-18', 'c','d',2, 'mm'),
    (3, 3, 'ok', 99.5, '2023-10-19', 'a', 'b', 1, 'yy'),
    (3, 3, 'ok', 99.5, '2023-10-19', 'a', 'b', 1, 'yy'),
    (3, 3, 'ok', 99.5, '2023-10-19', 'a', 'b', 1, 'yy'),
    (3, 3, 'ok', 99.5, '2023-10-19', 'a', 'b', 1, 'yy'),
    (3, 3, 'ok', 99.5, '2023-12-19', 'a', 'b', 1, 'yy'),
    (3, 3, 'ok', 99.5, '2023-12-19', 'a', 'b', 1, 'yy'),
    (3, 3, 'ok', 99.5, '2023-12-19', 'a', 'b', 1, 'yy'),
    (3, 3, 'ok', 99.5, '2023-12-19', 'a', 'b', 1, 'yy'); 
    """

    sql """
    insert into lineitem values 
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (2, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy'),
    (2, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy'),
    (2, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy'),
    (2, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy'),
    (3, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx'),
    (3, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx'),
    (3, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx'),
    (3, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx'),
    (3, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-12-19', '2023-12-19', '2023-12-19', 'c', 'd', 'xxxxxxxxx'),
    (3, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-12-19', '2023-12-19', '2023-12-19', 'c', 'd', 'xxxxxxxxx'),
    (3, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-12-19', '2023-12-19', '2023-12-19', 'c', 'd', 'xxxxxxxxx'),
    (3, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-12-19', '2023-12-19', '2023-12-19', 'c', 'd', 'xxxxxxxxx');
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

    def query_all_partition_sql = """
    select l_shipdate, o_orderdate, l_partkey, l_suppkey, sum(o_totalprice) as sum_total
    from lineitem
    left join orders on lineitem.l_orderkey = orders.o_orderkey and l_shipdate = o_orderdate
    group by
    l_shipdate,
    o_orderdate,
    l_partkey,
    l_suppkey;
   """


    def query_partition_sql = """
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


    multi_sql """
        analyze table lineitem with sync;
        analyze table orders with sync;
        """

    sql """alter table orders modify column o_comment set stats ('row_count'='20');"""
    sql """alter table lineitem modify column l_comment set stats ('row_count'='20');"""


    def mv_1_partition_name = "mv_10086"

    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_1_partition_name}"""
    sql """DROP TABLE IF EXISTS ${mv_1_partition_name}"""
    sql"""
        CREATE MATERIALIZED VIEW ${mv_1_partition_name}
        BUILD IMMEDIATE REFRESH AUTO ON MANUAL
        partition by(l_shipdate)
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1',
            'grace_period' = '31536000')
        AS
        ${mv_def_sql}
        """

    waitingMTMVTaskFinished(getJobName(db, mv_1_partition_name))

    multi_sql """
         analyze table ${mv_1_partition_name} with sync;
         """

    mv_rewrite_success(query_all_partition_sql, mv_1_partition_name, true,
            is_partition_statistics_ready(db, ["lineitem", "orders", mv_1_partition_name]))

    mv_rewrite_success(query_partition_sql, mv_1_partition_name, true,
            is_partition_statistics_ready(db, ["lineitem", "orders", mv_1_partition_name]))

    // test base table partition data change
    sql """
    insert into lineitem values
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy');
    """
    waitingPartitionIsExpected(mv_1_partition_name, "p_20231017_20231018", false)


    sql "SET enable_materialized_view_rewrite=false"
    order_qt_query_3_0_before "${query_all_partition_sql}"
    sql "SET enable_materialized_view_rewrite=true"


    // should rewrite successful when union rewrite enalbe if sub partition is invalid
    mv_rewrite_success(query_all_partition_sql, mv_1_partition_name, true,
            is_partition_statistics_ready(db, ["lineitem", "orders", mv_1_partition_name]))
    order_qt_query_3_0_after "${query_all_partition_sql}"

    sql "SET enable_materialized_view_rewrite=false"
    order_qt_query_4_0_before "${query_partition_sql}"
    sql "SET enable_materialized_view_rewrite=true"
    // should rewrite successfully when union rewrite enable if doesn't query invalid partition
    mv_rewrite_success(query_partition_sql, mv_1_partition_name, true,
            is_partition_statistics_ready(db, ["lineitem", "orders", mv_1_partition_name]))
    order_qt_query_4_0_after "${query_partition_sql}"

    // base table add partition
    sql "REFRESH MATERIALIZED VIEW ${mv_1_partition_name} AUTO"
    waitingMTMVTaskFinished(getJobName(db, mv_1_partition_name))
    sql """
    insert into lineitem values
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-21', '2023-10-21', '2023-10-21', 'a', 'b', 'yyyyyyyyy');
    """

    waitingPartitionIsExpected(mv_1_partition_name, "p_20231021_20231022", false)


    sql "SET enable_materialized_view_rewrite=false"
    order_qt_query_7_0_before "${query_all_partition_sql}"
    sql "SET enable_materialized_view_rewrite=true"
    // should rewrite successful when union rewrite enalbe if base table add new partition
    mv_rewrite_success(query_all_partition_sql, mv_1_partition_name, true,
            is_partition_statistics_ready(db, ["lineitem", "orders", mv_1_partition_name]))
    order_qt_query_7_0_after "${query_all_partition_sql}"

    sql "SET enable_materialized_view_rewrite=false"
    order_qt_query_8_0_before "${query_partition_sql}"
    sql "SET enable_materialized_view_rewrite=true"
    // should rewrite successfully when union rewrite enable if doesn't query new partition
    mv_rewrite_success(query_partition_sql, mv_1_partition_name, true,
            is_partition_statistics_ready(db, ["lineitem", "orders", mv_1_partition_name]))
    order_qt_query_8_0_after "${query_partition_sql}"

    // base table delete partition test
    sql "REFRESH MATERIALIZED VIEW ${mv_1_partition_name} AUTO"
    waitingMTMVTaskFinished(getJobName(db, mv_1_partition_name))
    sql """ ALTER TABLE lineitem DROP PARTITION IF EXISTS p_20231017 FORCE;
    """
    // show partitions will cause error, tmp comment
    waitingPartitionIsExpected(mv_1_partition_name, "p_20231017_20231018", false)


    sql "SET enable_materialized_view_rewrite=false"
    order_qt_query_11_0_before "${query_all_partition_sql}"
    sql "SET enable_materialized_view_rewrite=true"
    // should rewrite successful when union rewrite enalbe if base table delete partition
    mv_rewrite_success(query_all_partition_sql, mv_1_partition_name, true,
            is_partition_statistics_ready(db, ["lineitem", "orders", mv_1_partition_name]))
    order_qt_query_11_0_after "${query_all_partition_sql}"

    sql "SET enable_materialized_view_rewrite=false"
    order_qt_query_12_0_before "${query_partition_sql}"
    sql "SET enable_materialized_view_rewrite=true"
    // should rewrite successfully when union rewrite enable if doesn't query deleted partition
    mv_rewrite_success(query_partition_sql, mv_1_partition_name, true,
            is_partition_statistics_ready(db, ["lineitem", "orders", mv_1_partition_name]))
    order_qt_query_12_0_after "${query_partition_sql}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS ${mv_1_partition_name}"""

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
    PARTITION `p2` VALUES [("2023-10-18"), ("2023-12-20")),
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
    (3, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx'),
    (3, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-12-19', '2023-12-19', '2023-12-19', 'c', 'd', 'xxxxxxxxx');
    """
    sql """
    insert into lineitem_static values 
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '${today_str}', '${today_str}', '${today_str}', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '${today_str}', '${today_str}', '${today_str}', 'a', 'b', 'yyyyyyyyy'),
    (2, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '${today_str}', '${today_str}', '${today_str}', 'a', 'b', 'yyyyyyyyy'),
    (2, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '${today_str}', '${today_str}', '${today_str}', 'a', 'b', 'yyyyyyyyy'),
    (3, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '${today_str}', '${today_str}', '${today_str}', 'a', 'b', 'yyyyyyyyy'),
    (4, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '${today_str}', '${today_str}', '${today_str}', 'a', 'b', 'yyyyyyyyy');
    """

    multi_sql """
        analyze table lineitem_static with sync;
        """
    sql """alter table lineitem_static modify column l_comment set stats ('row_count'='6');"""


    def ttl_mv_def_sql = """
    select l_shipdate, o_orderdate, l_partkey,
    l_suppkey, sum(o_totalprice) as sum_total
    from lineitem_static
    left join orders on l_orderkey = o_orderkey
    group by
    l_shipdate,
    o_orderdate,
    l_partkey,
    l_suppkey;
    """
    def query_ttl_all_partition_sql = """
    select l_shipdate, o_orderdate, l_partkey, l_suppkey, sum(o_totalprice) as sum_total
    from lineitem_static
    left join orders on l_orderkey = o_orderkey
    group by
    l_shipdate,
    o_orderdate,
    l_partkey,
    l_suppkey;
   """
    def query_ttl_partition_sql = """
    select l_shipdate, o_orderdate, l_partkey, l_suppkey, sum(o_totalprice) as sum_total
    from lineitem_static
    left join orders on l_orderkey = o_orderkey
    where (l_shipdate>= '2023-10-18' and l_shipdate <= '2023-10-19')
    group by
    l_shipdate,
    o_orderdate,
    l_partkey,
    l_suppkey;
    """
    def ttl_mv_name = "mv_10000"

    def create_ttl_mtmv = { db_name, mv_inner_name, mv_inner_sql ->
        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_inner_name}"""
        sql"""
        CREATE MATERIALIZED VIEW ${mv_inner_name}
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        PARTITION BY(l_shipdate)
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'grace_period' = '31536000',
        'replication_num' = '1',
        'partition_sync_limit' = 2,
        'partition_sync_time_unit' = 'DAY',
        'partition_date_format' = 'yyyy-MM-dd')
        AS ${mv_inner_sql}
        """
        waitingMTMVTaskFinished(getJobName(db_name, mv_inner_name))
    }

    create_ttl_mtmv(db, ttl_mv_name, ttl_mv_def_sql)


    // test when mv is partition roll up
    sql "SET enable_materialized_view_rewrite=true"
    // should rewrite successful when union rewrite enalbe and mv is ttl, query the partition which is in mv
    mv_rewrite_success(query_ttl_all_partition_sql, ttl_mv_name, true,
            is_partition_statistics_ready(db, ["lineitem_static", "orders", ttl_mv_name]))

    sql "SET enable_materialized_view_rewrite=false"
    order_qt_query_16_0_before "${query_ttl_partition_sql}"
    sql "SET enable_materialized_view_rewrite=true"
    // should rewrite fail when union rewrite enalbe and query the partition which is not in mv
    mv_rewrite_fail(query_ttl_partition_sql, ttl_mv_name)
    order_qt_query_16_0_after "${query_ttl_partition_sql}"

    sql """ DROP MATERIALIZED VIEW IF EXISTS ${ttl_mv_name}"""


    // test date roll up mv partition rewrite
    def roll_up_mv_def_sql = """
    select date_trunc(`l_shipdate`, 'day') as col1, l_shipdate, o_orderdate, l_partkey,
    l_suppkey, sum(o_totalprice) as sum_total
    from lineitem
    left join orders on lineitem.l_orderkey = orders.o_orderkey and l_shipdate = o_orderdate
    group by
    col1,
    l_shipdate,
    o_orderdate,
    l_partkey,
    l_suppkey;
    """

    def query_roll_up_all_partition_sql = """
    select date_trunc(`l_shipdate`, 'day') as col1, l_shipdate, o_orderdate, l_partkey,
    l_suppkey, sum(o_totalprice) as sum_total
    from lineitem
    left join orders on lineitem.l_orderkey = orders.o_orderkey and l_shipdate = o_orderdate
    group by
    col1,
    l_shipdate,
    o_orderdate,
    l_partkey,
    l_suppkey;
   """

    def query_roll_up_partition_sql = """
    select date_trunc(`l_shipdate`, 'day') as col1, l_shipdate, o_orderdate, l_partkey,
    l_suppkey, sum(o_totalprice) as sum_total
    from lineitem
    left join orders on lineitem.l_orderkey = orders.o_orderkey and l_shipdate = o_orderdate
    where (l_shipdate>= '2023-10-18' and l_shipdate <= '2023-10-19')
    group by
    col1,
    l_shipdate,
    o_orderdate,
    l_partkey,
    l_suppkey;
    """

    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_1_partition_name}"""
    sql """DROP TABLE IF EXISTS ${mv_1_partition_name}"""
    sql"""
        CREATE MATERIALIZED VIEW ${mv_1_partition_name}
        BUILD IMMEDIATE REFRESH AUTO ON MANUAL
        partition by (date_trunc(`col1`, 'month'))
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'grace_period' = '31536000',
        'replication_num' = '1'
        )
        AS
        ${roll_up_mv_def_sql}
        """
    waitingMTMVTaskFinished(getJobName(db, mv_1_partition_name))


    mv_rewrite_success(query_roll_up_all_partition_sql, mv_1_partition_name, true,
            is_partition_statistics_ready(db, ["lineitem", "orders", mv_1_partition_name]))
    mv_rewrite_success(query_roll_up_partition_sql, mv_1_partition_name, true,
            is_partition_statistics_ready(db, ["lineitem", "orders", mv_1_partition_name]))

    // base table add partition
    sql """
    insert into lineitem values
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-09-17', '2023-09-17', '2023-09-17', 'a', 'b', 'yyyyyyyyy');
    """


    sql "SET enable_materialized_view_rewrite=false"
    order_qt_query_17_0_before "${query_roll_up_all_partition_sql}"
    sql "SET enable_materialized_view_rewrite=true"


    // should rewrite successful when union rewrite enalbe if base table add new partition
    mv_rewrite_success(query_roll_up_all_partition_sql, mv_1_partition_name, true,
            is_partition_statistics_ready(db, ["lineitem", "orders", mv_1_partition_name]))
    order_qt_query_17_0_after "${query_roll_up_all_partition_sql}"

    sql "SET enable_materialized_view_rewrite=false"
    order_qt_query_18_0_before "${query_roll_up_partition_sql}"
    sql "SET enable_materialized_view_rewrite=true"
    // should rewrite successfully when union rewrite enable if doesn't query new partition
    mv_rewrite_success(query_roll_up_partition_sql, mv_1_partition_name, true,
            is_partition_statistics_ready(db, ["lineitem", "orders", mv_1_partition_name]))
    order_qt_query_18_0_after "${query_roll_up_partition_sql}"


    // base table partition modify data
    sql "REFRESH MATERIALIZED VIEW ${mv_1_partition_name} AUTO"
    waitingMTMVTaskFinished(getJobName(db, mv_1_partition_name))

    sql """
    insert into lineitem values 
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-11-21', '2023-11-21', '2023-11-21', 'd', 'd', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-11-22', '2023-11-22', '2023-11-22', 'd', 'd', 'yyyyyyyyy');
    """


    sql "SET enable_materialized_view_rewrite=false"
    order_qt_query_19_0_before "${query_roll_up_all_partition_sql}"
    sql "SET enable_materialized_view_rewrite=true"


    // should rewrite successful when union rewrite enalbe if base table add new partition
    mv_rewrite_success(query_roll_up_all_partition_sql, mv_1_partition_name, true,
            is_partition_statistics_ready(db, ["lineitem", "orders", mv_1_partition_name]))
    order_qt_query_19_0_after "${query_roll_up_all_partition_sql}"

    sql "SET enable_materialized_view_rewrite=false"
    order_qt_query_20_0_before "${query_roll_up_partition_sql}"
    sql "SET enable_materialized_view_rewrite=true"
    // should rewrite successfully when union rewrite enable if doesn't query new partition
    mv_rewrite_success(query_roll_up_partition_sql, mv_1_partition_name, true,
            is_partition_statistics_ready(db, ["lineitem", "orders", mv_1_partition_name]))

    order_qt_query_20_0_after "${query_roll_up_partition_sql}"


    // test base table delete partition
    sql "REFRESH MATERIALIZED VIEW ${mv_1_partition_name} AUTO"
    waitingMTMVTaskFinished(getJobName(db, mv_1_partition_name))
    sql """ ALTER TABLE lineitem DROP PARTITION IF EXISTS p_20231121 FORCE;
    """

    sql "SET enable_materialized_view_rewrite=false"
    order_qt_query_21_0_before "${query_roll_up_all_partition_sql}"
    sql "SET enable_materialized_view_rewrite=true"
    mv_rewrite_success(query_roll_up_all_partition_sql, mv_1_partition_name, true,
            is_partition_statistics_ready(db, ["lineitem", "orders", mv_1_partition_name]))
    order_qt_query_21_0_after "${query_roll_up_all_partition_sql}"

    sql "SET enable_materialized_view_rewrite=false"
    order_qt_query_22_0_before "${query_roll_up_partition_sql}"
    sql "SET enable_materialized_view_rewrite=true"
    mv_rewrite_success(query_roll_up_partition_sql, mv_1_partition_name, true,
            is_partition_statistics_ready(db, ["lineitem", "orders", mv_1_partition_name]))

    order_qt_query_22_0_after "${query_roll_up_partition_sql}"
}



