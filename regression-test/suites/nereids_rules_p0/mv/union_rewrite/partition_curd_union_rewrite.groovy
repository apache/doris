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

suite ("partition_curd_union_rewrite") {
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
      o_orderstatus    char(20) not null,
      o_totalprice     decimalv3(15,2) not null,
      o_orderdate      date not null,
      o_orderpriority  char(15) not null,  
      o_clerk          char(15) not null, 
      o_shippriority   integer not null,
      o_comment        varchar(79) not null
    )
    DUPLICATE KEY(o_orderkey, o_custkey)
    PARTITION BY RANGE(o_orderdate)(
    FROM ('2023-10-17') TO ('2023-11-01') INTERVAL 1 DAY
    )
    DISTRIBUTED BY HASH(o_orderkey) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    );
    """

    sql """
    drop table if exists lineitem
    """

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
    (FROM ('2023-10-17') TO ('2023-11-01') INTERVAL 1 DAY)
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
    (2, 2, 'ok', 109.2, '2023-10-18', 'c','d',2, 'mm'),
    (2, 2, 'ok', 109.2, '2023-10-18', 'c','d',2, 'mm'),
    (2, 2, 'ok', 109.2, '2023-10-18', 'c','d',2, 'mm'),
    (3, 3, 'ok', 99.5, '2023-10-19', 'a', 'b', 1, 'yy'),
    (3, 3, 'ok', 99.5, '2023-10-19', 'a', 'b', 1, 'yy'),
    (3, 3, 'ok', 99.5, '2023-10-19', 'a', 'b', 1, 'yy'); 
    """

    sql """
    insert into lineitem values 
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (2, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy'),
    (2, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy'),
    (2, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy'),
    (3, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx'),
    (3, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx'),
    (3, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx');
    """

    sql """alter table orders modify column o_comment set stats ('row_count'='9');"""
    sql """alter table lineitem modify column l_comment set stats ('row_count'='9');"""

    sql """analyze table orders with sync;"""
    sql """analyze table lineitem with sync;"""


    def mv_def_sql = """
    select l_shipdate, o_orderdate, l_partkey,
    l_suppkey, sum(o_totalprice) as sum_total
    from lineitem
    left join orders on lineitem.l_orderkey = orders.o_orderkey and l_shipdate = o_orderdate
    group by
    l_shipdate,
    o_orderdate,
    l_partkey,
    l_suppkey
    """

    def all_partition_sql = """
    select l_shipdate, o_orderdate, l_partkey, l_suppkey, sum(o_totalprice) as sum_total
    from lineitem
    left join orders on lineitem.l_orderkey = orders.o_orderkey and l_shipdate = o_orderdate
    group by
    l_shipdate,
    o_orderdate,
    l_partkey,
    l_suppkey
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
    l_suppkey
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

    def mv_name = "mv_10086"
    def order_by_stmt = " order by 1,2,3,4,5"
    waitingMTMVTaskFinished(getJobName(db, mv_name))

    // All partition is valid, test query and rewrite by materialized view
    mv_rewrite_success(all_partition_sql, mv_name,
            is_partition_statistics_ready(db, ["lineitem", "orders", mv_name]))
    compare_res(all_partition_sql + order_by_stmt)

    mv_rewrite_success(partition_sql, mv_name,
            is_partition_statistics_ready(db, ["lineitem", "orders", mv_name]))
    compare_res(partition_sql + order_by_stmt)

    // Part partition is invalid, test can not use partition 2023-10-17 to rewrite
    sql """
    insert into lineitem values 
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy');
    """
    // wait partition is invalid
    sleep(5000)
    mv_rewrite_success(all_partition_sql, mv_name,
            is_partition_statistics_ready(db, ["lineitem", "orders", mv_name]))
    compare_res(all_partition_sql + order_by_stmt)
    mv_rewrite_success(partition_sql, mv_name,
            is_partition_statistics_ready(db, ["lineitem", "orders", mv_name]))
    compare_res(partition_sql + order_by_stmt)

    sql "REFRESH MATERIALIZED VIEW ${mv_name} AUTO"
    waitingMTMVTaskFinished(getJobName(db, mv_name))
    // Test when base table create partition
    sql """
    insert into lineitem values 
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-21', '2023-10-21', '2023-10-21', 'a', 'b', 'yyyyyyyyy');
    """
    // Wait partition is invalid
    sleep(5000)
    mv_rewrite_success(all_partition_sql, mv_name,
            is_partition_statistics_ready(db, ["lineitem", "orders", mv_name]))
    compare_res(all_partition_sql + order_by_stmt)
    mv_rewrite_success(partition_sql, mv_name,
            is_partition_statistics_ready(db, ["lineitem", "orders", mv_name]))
    compare_res(partition_sql + order_by_stmt)

    // Test when base table delete partition test
    sql "REFRESH MATERIALIZED VIEW ${mv_name} AUTO"
    waitingMTMVTaskFinished(getJobName(db, mv_name))
    sql """ ALTER TABLE lineitem DROP PARTITION IF EXISTS p_20231021 FORCE;
    """
    // Wait partition is invalid
    sleep(3000)
    mv_rewrite_success(all_partition_sql, mv_name,
            is_partition_statistics_ready(db, ["lineitem", "orders", mv_name]))
    compare_res(all_partition_sql + order_by_stmt)
    mv_rewrite_success(partition_sql, mv_name,
            is_partition_statistics_ready(db, ["lineitem", "orders", mv_name]))
    compare_res(partition_sql + order_by_stmt)

    // Use exactly two base partitions. After p2 is dropped, p2 is stale only in the MV: no
    // surviving base partition needs UNION ALL compensation. The query omits the MV partition column d,
    // so canUnionRewrite() must be false while the rewrite still succeeds by removing stale MV p2.
    sql "DROP MATERIALIZED VIEW IF EXISTS partition_compensation_mv"
    sql "DROP TABLE IF EXISTS partition_compensation_mv"
    sql "DROP TABLE IF EXISTS partition_compensation_base"
    sql """
    CREATE TABLE partition_compensation_base (
        id int not null,
        k int not null,
        d date not null,
        v int not null
    )
    DUPLICATE KEY(id, k, d)
    PARTITION BY RANGE(d) (
        PARTITION p1 VALUES [('2024-01-01'), ('2024-01-02')),
        PARTITION p2 VALUES [('2024-01-02'), ('2024-01-03'))
    )
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES ('replication_num' = '1')
    """
    sql """
    INSERT INTO partition_compensation_base VALUES
        (1, 1, '2024-01-01', 1),
        (2, 1, '2024-01-01', 1),
        (3, 1, '2024-01-01', 1),
        (4, 1, '2024-01-01', 1),
        (5, 1, '2024-01-01', 1),
        (6, 1, '2024-01-01', 1),
        (7, 1, '2024-01-01', 1),
        (8, 1, '2024-01-01', 1),
        (9, 1, '2024-01-01', 1),
        (10, 1, '2024-01-01', 1),
        (11, 1, '2024-01-01', 1),
        (12, 1, '2024-01-01', 1),
        (13, 1, '2024-01-01', 1),
        (14, 1, '2024-01-01', 1),
        (15, 1, '2024-01-01', 1),
        (16, 1, '2024-01-01', 1),
        (17, 1, '2024-01-01', 1),
        (18, 1, '2024-01-01', 1),
        (19, 1, '2024-01-01', 1),
        (20, 1, '2024-01-01', 1),
        (21, 1, '2024-01-02', 1)
    """

    def partition_compensation_mv_name = "partition_compensation_mv"
    def aggregate_without_partition_column_sql = """
    select k, sum(v) as sum_v
    from partition_compensation_base
    group by k
    """
    sql """
    CREATE MATERIALIZED VIEW ${partition_compensation_mv_name}
    BUILD IMMEDIATE REFRESH AUTO ON MANUAL
    PARTITION BY(d)
    DISTRIBUTED BY RANDOM BUCKETS 1
    PROPERTIES ('replication_num' = '1')
    AS
    select d, k, sum(v) as sum_v
    from partition_compensation_base
    group by d, k
    """
    waitingMTMVTaskFinished(getJobName(db, partition_compensation_mv_name))
    sql """
    analyze table partition_compensation_base with sync;
    analyze table ${partition_compensation_mv_name} with sync;
    """

    sql "ALTER TABLE partition_compensation_base DROP PARTITION p2 FORCE"
    waitingPartitionIsExpected(partition_compensation_mv_name, "p_20240102_20240103", false)
    mv_rewrite_success(aggregate_without_partition_column_sql, partition_compensation_mv_name,
            is_partition_statistics_ready(db, ["partition_compensation_base", partition_compensation_mv_name]))
    explain {
        sql "memo plan ${aggregate_without_partition_column_sql}"
        notContains "PhysicalUnion"
    }
    order_qt_aggregate_without_partition_column_after_partition_delete """
    ${aggregate_without_partition_column_sql}
    order by k
    """

    // Aggregate-on-detail rewrite has the same removal-only partition state. It should remove the stale
    // detail MV partition and must not require a base-table UNION ALL branch.
    sql "DROP MATERIALIZED VIEW IF EXISTS partition_compensation_detail_mv"
    sql "DROP TABLE IF EXISTS partition_compensation_detail_mv"
    sql "DROP TABLE IF EXISTS partition_compensation_detail_base"
    sql """
    CREATE TABLE partition_compensation_detail_base (
        id int not null,
        k int not null,
        d date not null,
        v int not null
    )
    DUPLICATE KEY(id, k, d)
    PARTITION BY RANGE(d) (
        PARTITION p1 VALUES [('2024-02-01'), ('2024-02-02')),
        PARTITION p2 VALUES [('2024-02-02'), ('2024-02-03'))
    )
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES ('replication_num' = '1')
    """
    sql """
    INSERT INTO partition_compensation_detail_base VALUES
        (1, 1, '2024-02-01', 1),
        (2, 1, '2024-02-02', 1)
    """

    def aggregate_on_detail_mv_sql = """
    select k, sum(v) as sum_v
    from partition_compensation_detail_base
    group by k
    """
    sql """
    CREATE MATERIALIZED VIEW partition_compensation_detail_mv
    BUILD IMMEDIATE REFRESH AUTO ON MANUAL
    PARTITION BY(d)
    DISTRIBUTED BY RANDOM BUCKETS 1
    PROPERTIES ('replication_num' = '1')
    AS
    select id, k, d, v
    from partition_compensation_detail_base
    """
    waitingMTMVTaskFinished(getJobName(db, "partition_compensation_detail_mv"))
    sql """
    analyze table partition_compensation_detail_base with sync;
    analyze table partition_compensation_detail_mv with sync;
    """

    sql "ALTER TABLE partition_compensation_detail_base DROP PARTITION p2 FORCE"
    waitingPartitionIsExpected("partition_compensation_detail_mv", "p_20240202_20240203", false)
    mv_rewrite_success(aggregate_on_detail_mv_sql, "partition_compensation_detail_mv",
            is_partition_statistics_ready(
                    db, ["partition_compensation_detail_base", "partition_compensation_detail_mv"]))
    explain {
        sql "memo plan ${aggregate_on_detail_mv_sql}"
        notContains "PhysicalUnion"
    }
    order_qt_aggregate_on_detail_mv_after_partition_delete """
    ${aggregate_on_detail_mv_sql}
    order by k
    """
}
