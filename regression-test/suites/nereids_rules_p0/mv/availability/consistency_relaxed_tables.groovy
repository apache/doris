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


suite("consistency_relaxed_tables") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set runtime_filter_mode=OFF"

    def initTable = {
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
    drop table if exists partsupp_p
    """

        sql """
    CREATE TABLE IF NOT EXISTS partsupp_p (
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
    insert into partsupp_p values
    (2, 3, 9, 10.01, 'supply1'),
    (2, 3, 10, 11.01, 'supply2');
    """

        multi_sql """
        analyze table lineitem_p with sync;
        analyze table orders_p with sync;
        analyze table partsupp_p with sync;
        """

        sql """alter table orders_p modify column o_comment set stats ('row_count'='13');"""
        sql """alter table lineitem_p modify column l_comment set stats ('row_count'='12');"""
        sql """alter table partsupp_p modify column ps_partkey set stats ('row_count'='2');"""
    }


    def mv_name = "consistency_relaxed_tables_mv_1"

    def mv_def_sql = """
    select l_shipdate, o_orderdate, ps_partkey,
    l_suppkey, sum(o_totalprice) as sum_total
    from lineitem_p
    left join orders_p on l_shipdate = o_orderdate
    left join partsupp_p on l_partkey = ps_partkey and l_suppkey = ps_suppkey
    group by
    l_shipdate,
    o_orderdate,
    ps_partkey,
    l_suppkey;
    """

    initTable()
    create_async_partition_mv(db, mv_name, mv_def_sql, "(l_shipdate)")

    sql """ALTER MATERIALIZED VIEW ${mv_name} set('async_mv.query_rewrite.consistency_relaxed_tables'='orders_p');"""
    sql"""
    insert into orders_p values 
    (1, 1, 'ok', 2, '2023-10-17', 'a', 'b', 1, 'yy')
    """
    // set consistency_relaxed_tables and dimension table has new data, mv can be used
    mv_rewrite_success(mv_def_sql, mv_name)
    sql """refresh materialized view ${mv_name} auto;"""
    waitingMTMVTaskFinishedByMvName(mv_name)

    def refresh_info = sql """select 
            JobName, Status, RefreshMode
            from tasks("type"="mv") where JobName="${getJobName(db, mv_name)}" order by CreateTime desc limit 1;"""
    logger.info("refresh_info: " + refresh_info.toString())
    assert (refresh_info[0][1] == "SUCCESS")
    assert (refresh_info[0][2] == "COMPLETE")


    //  set consistency_relaxed_tables and dimension table has new data, mv couldn't be used
    sql """ALTER MATERIALIZED VIEW ${mv_name} set('async_mv.query_rewrite.consistency_relaxed_tables'='');"""
    sql"""
    insert into orders_p values 
    (1, 1, 'ok', 2, '2023-10-17', 'a', 'b', 1, 'yy')
    """
    mv_not_part_in(mv_def_sql, mv_name)
    sql """refresh materialized view ${mv_name} auto;"""
    waitingMTMVTaskFinishedByMvName(mv_name)

    def refresh_info1 = sql """select 
            JobName, Status, RefreshMode
            from tasks("type"="mv") where JobName="${getJobName(db, mv_name)}" order by CreateTime desc limit 1;"""
    logger.info("refresh_info1: " + refresh_info1.toString())
    assert (refresh_info1[0][1] == "SUCCESS")
    assert (refresh_info1[0][2] == "COMPLETE")
    order_qt_mv_1_before_insert "select * from ${mv_name}"


    //  set consistency_relaxed_tables and dimension table has new data, rewrite in dml should not use the mv
    def another_mv_name = "consistency_relaxed_tables_mv2"
    create_async_partition_mv(db, another_mv_name, mv_def_sql, "(l_shipdate)")

    sql """ALTER MATERIALIZED VIEW ${another_mv_name} set('async_mv.query_rewrite.consistency_relaxed_tables'='orders_p');"""
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
    sql """refresh materialized view ${another_mv_name} auto;"""
    waitingMTMVTaskFinishedByMvName(another_mv_name, db)
    sql """set enable_materialized_view_rewrite = false;"""
    // should contain new data but not use another_mv_name to rewrite when insert
    order_qt_mv_2_after_insert "select * from ${another_mv_name}"



    //  set consistency_relaxed_tables and dimension table has new data, rewrite in dml should not use the mv
    def another_mv_name2 = "consistency_relaxed_tables_mv3"
    create_async_partition_mv(db, another_mv_name2, mv_def_sql, "(l_shipdate)")

    sql """ALTER MATERIALIZED VIEW ${another_mv_name2} set('async_mv.query_rewrite.consistency_relaxed_tables'='orders_p');"""
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
    sql """refresh materialized view ${another_mv_name2} auto;"""
    waitingMTMVTaskFinishedByMvName(another_mv_name2, db)
    sql """set enable_materialized_view_rewrite = false;"""
    // should not use another_mv_name to rewrite when refresh
    order_qt_mv_3_after_insert "select * from ${another_mv_name2}"

}
