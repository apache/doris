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


suite("multi_trace_partition_mv_rewrite") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set runtime_filter_mode=OFF"

 def initTable =  {
  sql """
    drop table if exists orders_p
    """

  sql """
    CREATE TABLE IF NOT EXISTS orders_p  (
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
    FROM ('2023-09-16') TO ('2023-10-30') INTERVAL 1 DAY
    )
    DISTRIBUTED BY HASH(o_orderkey) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    );
    """

  sql """
    drop table if exists lineitem_p
    """

  // test pre init partition
  sql"""
    CREATE TABLE IF NOT EXISTS lineitem_p (
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
    (FROM ('2023-09-16') TO ('2023-10-30') INTERVAL 1 DAY)
    DISTRIBUTED BY HASH(l_orderkey) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    );
    """

     sql """
    drop table if exists partsupp
    """

  sql """
    CREATE TABLE IF NOT EXISTS partsupp (
      ps_partkey     INTEGER NOT NULL,
      ps_suppkey     INTEGER NOT NULL,
      ps_availqty    INTEGER NOT NULL,
      ps_supplycost  DECIMALV3(15,2)  NOT NULL,
      ps_comment     VARCHAR(199) NOT NULL 
    )
    DUPLICATE KEY(ps_partkey, ps_suppkey)
    DISTRIBUTED BY HASH(ps_partkey) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    )
    """

  sql"""
    insert into orders_p values 
    (1, 1, 'ok', 1, '2023-10-17', 'a', 'b', 1, 'yy'),
    (1, 1, 'ok', 1, '2023-10-17', 'a', 'b', 1, 'yy'),
    (2, 2, 'ok', 1, '2023-10-18', 'c','d',2, 'mm'),
    (2, 2, 'ok', 1, '2023-10-18', 'c','d',2, 'mm'),
    (2, 2, 'ok', 1, '2023-10-18', 'c','d',2, 'mm'),
    (3, 3, 'ok', 1, '2023-10-19', 'a', 'b', 1, 'yy'),
    (3, 3, 'ok', 1, '2023-10-19', 'a', 'b', 1, 'yy'),
    (3, 3, 'ok', 1, '2023-10-19', 'a', 'b', 1, 'yy'),
    (3, 3, 'ok', 1, '2023-10-19', 'a', 'b', 1, 'yy'),
    (3, 3, 'ok', 1, '2023-10-22', 'a', 'b', 1, 'yy'),
    (3, 3, 'ok', 1, '2023-10-22', 'a', 'b', 1, 'yy'),
    (3, 3, 'ok', 1, '2023-10-22', 'a', 'b', 1, 'yy'),
    (3, 3, 'ok', 1, '2023-10-22', 'a', 'b', 1, 'yy'); 
    """

  sql """
    insert into lineitem_p values 
    (1, 2, 3, 4, 5.5, 6.5, 1.5, 1.5, 'o', 'k', '2023-10-17', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 2.5, 2.5, 'o', 'k', '2023-10-17', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 3.5, 3.5, 'o', 'k', '2023-10-17', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (2, 2, 3, 4, 5.5, 6.5, 4.5, 4.5, 'o', 'k', '2023-10-18', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy'),
    (2, 2, 3, 4, 5.5, 6.5, 5.5, 5.5, 'o', 'k', '2023-10-18', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy'),
    (2, 2, 3, 4, 5.5, 6.5, 6.5, 6.5, 'o', 'k', '2023-10-18', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy'),
    (3, 2, 3, 6, 7.5, 8.5, 7.5, 7.5, 'k', 'o', '2023-10-19', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx'),
    (3, 2, 3, 6, 7.5, 8.5, 9.5, 8.5, 'k', 'o', '2023-10-19', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx'),
    (3, 2, 3, 6, 7.5, 8.5, 9.5, 9.5, 'k', 'o', '2023-10-19', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx'),
    (3, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-22', '2023-10-22', '2023-10-22', 'c', 'd', 'xxxxxxxxx'),
    (3, 2, 3, 6, 7.5, 8.5, 9.5, 11.5, 'k', 'o', '2023-10-22', '2023-10-22', '2023-10-22', 'c', 'd', 'xxxxxxxxx'),
    (3, 2, 3, 6, 7.5, 8.5, 9.5, 12.5, 'k', 'o', '2023-10-22', '2023-10-22', '2023-10-22', 'c', 'd', 'xxxxxxxxx');
    """

  sql """
    insert into partsupp values
    (2, 3, 9, 10.01, 'supply1'),
    (2, 3, 10, 11.01, 'supply2');
    """

  multi_sql """
        analyze table lineitem_p with sync;
        analyze table orders_p with sync;
        analyze table partsupp with sync;
        """

  sql """alter table orders_p modify column o_comment set stats ('row_count'='13');"""
  sql """alter table lineitem_p modify column l_comment set stats ('row_count'='12');"""
  sql """alter table partsupp modify column ps_partkey set stats ('row_count'='2');"""
 }


    def mv_name = "mv_10099"

    def mv_def_sql = """
    select l_shipdate, o_orderdate, ps_partkey,
    l_suppkey, sum(o_totalprice) as sum_total
    from lineitem_p
    inner join orders_p on l_shipdate = o_orderdate
    left join partsupp on l_partkey = ps_partkey and l_suppkey = ps_suppkey
    group by
    l_shipdate,
    o_orderdate,
    ps_partkey,
    l_suppkey;
    """

    def query_all_partition_sql = """
    select l_shipdate, o_orderdate, ps_partkey,
    l_suppkey, sum(o_totalprice) as sum_total
    from lineitem_p
    inner join orders_p on l_shipdate = o_orderdate
    left join partsupp on l_partkey = ps_partkey and l_suppkey = ps_suppkey
    group by
    l_shipdate,
    o_orderdate,
    ps_partkey,
    l_suppkey;
   """

    // partition intersection and union
    // lineitem_p add partition, orders_p add partition, partsupp
    initTable()
    create_async_partition_mv(db, mv_name, mv_def_sql, "(l_shipdate)")
    sql"""
    insert into lineitem_p values
    (1, 2, 3, 4, 5.5, 6.5, 1.5, 1.5, 'o', 'k', '2023-10-20', '2023-10-20', '2023-10-20', 'a', 'b', 'yyyyyyyyy')
    """
    sql"""
    insert into orders_p values
    (1, 1, 'ok', 1.5, '2023-10-20', 'a', 'b', 1, 'yy'),
    (1, 1, 'ok', 1.5, '2023-10-21', 'a', 'b', 1, 'yy');
    """
    waitingPartitionIsExpected(mv_name, "p_20231020_20231021", false)
    waitingPartitionIsExpected(mv_name, "p_20231021_20231022", false)

    sql "SET enable_materialized_view_rewrite=false"
    order_qt_query_1_0_before "${query_all_partition_sql}"
    sql "SET enable_materialized_view_rewrite=true"


    // lineitem_p add partition, orders_p add partition, partsupp
    mv_rewrite_success(query_all_partition_sql, mv_name,
            is_partition_statistics_ready(db, ["lineitem_p", "orders_p", mv_name]))
    order_qt_query_1_0_after "${query_all_partition_sql}"


    // lineitem_p add partition, orders_p delete partition, partsupp
    initTable()
    create_async_partition_mv(db, mv_name, mv_def_sql, "(l_shipdate)")
    sql"""
    insert into lineitem_p values
    (1, 2, 3, 4, 5.5, 6.5, 1.5, 1.5, 'o', 'k', '2023-10-20', '2023-10-20', '2023-10-20', 'a', 'b', 'yyyyyyyyy')
    """
    sql"""
    insert into orders_p values
    (1, 1, 'ok', 9.5, '2023-10-20', 'a', 'b', 1, 'yy');
    """
    sql """ ALTER TABLE orders_p DROP PARTITION IF EXISTS p_20231017 FORCE;
    """
    waitingPartitionIsExpected(mv_name, "p_20231020_20231021", false)
    waitingPartitionIsExpected(mv_name, "p_20231017_20231018", false)

    sql "SET enable_materialized_view_rewrite=false"
    order_qt_query_2_0_before "${query_all_partition_sql}"
    sql "SET enable_materialized_view_rewrite=true"


    // lineitem_p add partition, orders_p add partition, partsupp
    mv_rewrite_success(query_all_partition_sql, mv_name,
            is_partition_statistics_ready(db, ["lineitem_p", "orders_p", mv_name]))
    order_qt_query_2_0_after "${query_all_partition_sql}"


    // lineitem_p add partition, orders_p modify partition, partsupp
    initTable()
    create_async_partition_mv(db, mv_name, mv_def_sql, "(l_shipdate)")
    sql"""
    insert into lineitem_p values
    (1, 2, 3, 4, 5.5, 6.5, 1.5, 1.5, 'o', 'k', '2023-10-20', '2023-10-20', '2023-10-20', 'a', 'b', 'yyyyyyyyy')
    """
    sql"""
    insert into orders_p values
    (1, 1, 'ok', 1.5, '2023-10-20', 'a', 'b', 1, 'yy'),
    (1, 1, 'ok', 1.5, '2023-10-17', 'a', 'b', 1, 'yy');
    """
    waitingPartitionIsExpected(mv_name, "p_20231020_20231021", false)
    waitingPartitionIsExpected(mv_name, "p_20231017_20231018", false)

    sql "SET enable_materialized_view_rewrite=false"
    order_qt_query_3_0_before "${query_all_partition_sql}"
    sql "SET enable_materialized_view_rewrite=true"


    // lineitem_p add partition, orders_p add partition, partsupp
    mv_rewrite_success(query_all_partition_sql, mv_name,
            is_partition_statistics_ready(db, ["lineitem_p", "orders_p", mv_name]))
    order_qt_query_3_0_after "${query_all_partition_sql}"
//
//
//    // lineitem_p delete partition, orders_p add partition, partsupp
//    initTable()
//    create_async_partition_mv(db, mv_name, mv_def_sql, "(l_shipdate)")
//    sql """
//    ALTER TABLE lineitem_p DROP PARTITION IF EXISTS p_20231017 FORCE;
//    """
//    sql"""
//    insert into lineitem_p values
//    (1, 2, 3, 4, 5.5, 6.5, 1.5, 1.5, 'o', 'k', '2023-10-20', '2023-10-20', '2023-10-20', 'a', 'b', 'yyyyyyyyy')
//    """
//    sql"""
//    insert into orders_p values
//    (1, 1, 'ok', 1.5, '2023-10-20', 'a', 'b', 1, 'yy');
//    """
//    waitingPartitionIsExpected(mv_name, "p_20231017_20231018", false)
//    waitingPartitionIsExpected(mv_name, "p_20231020_20231021", false)
//
//    sql "SET enable_materialized_view_rewrite=false"
//    order_qt_query_4_0_before "${query_all_partition_sql}"
//    sql "SET enable_materialized_view_rewrite=true"
//
//
//    // lineitem_p add partition, orders_p add partition, partsupp
//    mv_rewrite_success(query_all_partition_sql, mv_name,
//            is_partition_statistics_ready(db, ["lineitem_p", "orders_p", mv_name]))
//    order_qt_query_4_0_after "${query_all_partition_sql}"
//
//
//    // lineitem_p delete partition, orders_p delete partition, partsupp
//    initTable()
//    create_async_partition_mv(db, mv_name, mv_def_sql, "(l_shipdate)")
//    sql """
//    ALTER TABLE lineitem_p DROP PARTITION IF EXISTS p_20231017 FORCE;
//    """
//    sql """
//    ALTER TABLE orders_p DROP PARTITION IF EXISTS p_20231018 FORCE;
//    """
//    waitingPartitionIsExpected(mv_name, "p_20231017_20231018", false)
//    waitingPartitionIsExpected(mv_name, "p_20231018_20231019", false)
//
//    sql "SET enable_materialized_view_rewrite=false"
//    order_qt_query_5_0_before "${query_all_partition_sql}"
//    sql "SET enable_materialized_view_rewrite=true"
//
//
//    // lineitem_p add partition, orders_p add partition, partsupp
//    mv_rewrite_success(query_all_partition_sql, mv_name,
//            is_partition_statistics_ready(db, ["lineitem_p", "orders_p", mv_name]))
//    order_qt_query_5_0_after "${query_all_partition_sql}"
//
//
//    // lineitem_p delete partition, orders_p modify partition, partsupp
//    initTable()
//    create_async_partition_mv(db, mv_name, mv_def_sql, "(l_shipdate)")
//    sql """
//    ALTER TABLE lineitem_p DROP PARTITION IF EXISTS p_20231017 FORCE;
//    """
//    sql"""
//    insert into lineitem_p values
//    (1, 2, 3, 4, 5.5, 6.5, 1.5, 1.5, 'o', 'k', '2023-10-19', '2023-10-19', '2023-10-19', 'a', 'b', 'yyyyyyyyy')
//    """
//    sql"""
//    insert into orders_p values
//    (1, 1, 'ok', 1.5, '2023-10-19', 'a', 'b', 1, 'yy');
//    """
//    waitingPartitionIsExpected(mv_name, "p_20231017_20231018", false)
//    waitingPartitionIsExpected(mv_name, "p_20231019_20231020", false)
//
//    sql "SET enable_materialized_view_rewrite=false"
//    order_qt_query_6_0_before "${query_all_partition_sql}"
//    sql "SET enable_materialized_view_rewrite=true"
//
//
//    // lineitem_p add partition, orders_p add partition, partsupp
//    mv_rewrite_success(query_all_partition_sql, mv_name,
//            is_partition_statistics_ready(db, ["lineitem_p", "orders_p", mv_name]))
//    order_qt_query_6_0_after "${query_all_partition_sql}"
//
//
//
//    // lineitem_p modify partition, orders_p add partition, partsupp
//    initTable()
//    create_async_partition_mv(db, mv_name, mv_def_sql, "(l_shipdate)")
//    sql"""
//    insert into lineitem_p values
//    (1, 2, 3, 4, 5.5, 6.5, 1.5, 1.5, 'o', 'k', '2023-10-17', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
//    (1, 2, 3, 4, 5.5, 6.5, 1.5, 1.5, 'o', 'k', '2023-10-20', '2023-10-20', '2023-10-20', 'a', 'b', 'yyyyyyyyy');
//    """
//    sql"""
//    insert into orders_p values
//    (1, 1, 'ok', 1.5, '2023-10-17', 'a', 'b', 1, 'yy'),
//    (1, 1, 'ok', 1.5, '2023-10-20', 'a', 'b', 1, 'yy');
//    """
//    waitingPartitionIsExpected(mv_name, "p_20231020_20231021", false)
//    waitingPartitionIsExpected(mv_name, "p_20231017_20231018", false)
//
//    sql "SET enable_materialized_view_rewrite=false"
//    order_qt_query_7_0_before "${query_all_partition_sql}"
//    sql "SET enable_materialized_view_rewrite=true"
//
//
//    // lineitem_p add partition, orders_p add partition, partsupp
//    mv_rewrite_success(query_all_partition_sql, mv_name,
//            is_partition_statistics_ready(db, ["lineitem_p", "orders_p", mv_name]))
//    order_qt_query_7_0_after "${query_all_partition_sql}"
//
//
//    // lineitem_p modify partition, orders_p delete partition, partsupp
//    initTable()
//    create_async_partition_mv(db, mv_name, mv_def_sql, "(l_shipdate)")
//    sql"""
//    insert into lineitem_p values
//    (1, 2, 3, 4, 5.5, 6.5, 1.5, 1.5, 'o', 'k', '2023-10-17', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy');
//    """
//    sql"""
//    insert into orders_p values
//    (1, 1, 'ok', 1.5, '2023-10-17', 'a', 'b', 1, 'yy');
//    """
//    sql """
//    ALTER TABLE orders_p DROP PARTITION IF EXISTS p_20231018 FORCE;
//    """
//
//    waitingPartitionIsExpected(mv_name, "p_20231020_20231021", false)
//    waitingPartitionIsExpected(mv_name, "p_20231017_20231018", false)
//
//    sql "SET enable_materialized_view_rewrite=false"
//    order_qt_query_8_0_before "${query_all_partition_sql}"
//    sql "SET enable_materialized_view_rewrite=true"
//
//
//    // lineitem_p add partition, orders_p add partition, partsupp
//    mv_rewrite_success(query_all_partition_sql, mv_name,
//            is_partition_statistics_ready(db, ["lineitem_p", "orders_p", mv_name]))
//    order_qt_query_8_0_after "${query_all_partition_sql}"
//
//
//    // lineitem_p modify partition, orders_p modify partition, partsupp
//    initTable()
//    create_async_partition_mv(db, mv_name, mv_def_sql, "(l_shipdate)")
//    sql"""
//    insert into lineitem_p values
//    (1, 2, 3, 4, 5.5, 6.5, 1.5, 1.5, 'o', 'k', '2023-10-17', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
//    (1, 2, 3, 4, 5.5, 6.5, 1.5, 1.5, 'o', 'k', '2023-10-18', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy');
//    """
//    sql"""
//    insert into orders_p values
//    (1, 1, 'ok', 1.5, '2023-10-17', 'a', 'b', 1, 'yy'),
//    (1, 1, 'ok', 1.5, '2023-10-18', 'a', 'b', 1, 'yy');
//    """
//
//    waitingPartitionIsExpected(mv_name, "p_20231017_20231018", false)
//    waitingPartitionIsExpected(mv_name, "p_20231018_20231019", false)
//
//    sql "SET enable_materialized_view_rewrite=false"
//    order_qt_query_9_0_before "${query_all_partition_sql}"
//    sql "SET enable_materialized_view_rewrite=true"
//
//
//    // lineitem_p add partition, orders_p add partition, partsupp
//    mv_rewrite_success(query_all_partition_sql, mv_name,
//            is_partition_statistics_ready(db, ["lineitem_p", "orders_p", mv_name]))
//    order_qt_query_9_0_after "${query_all_partition_sql}"

}

