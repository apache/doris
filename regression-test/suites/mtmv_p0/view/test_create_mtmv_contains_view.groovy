package view
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

suite("test_create_mtmv_contains_view","mtmv") {

    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set runtime_filter_mode=OFF";
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"

    sql """
    drop table if exists orders
    """

    sql """
    CREATE TABLE IF NOT EXISTS orders  (
      o_orderkey       INTEGER NOT NULL,
      o_custkey        INTEGER NOT NULL,
      o_orderstatus    CHAR(1) NOT NULL,
      o_totalprice     DECIMALV3(15,2) NOT NULL,
      o_orderdate      DATE NOT NULL,
      o_orderpriority  CHAR(15) NOT NULL,  
      o_clerk          CHAR(15) NOT NULL, 
      o_shippriority   INTEGER NOT NULL,
      O_COMMENT        VARCHAR(79) NOT NULL
    )
    DUPLICATE KEY(o_orderkey, o_custkey)
    PARTITION BY RANGE(o_orderdate) (
    PARTITION `day_2` VALUES LESS THAN ('2023-12-9'),
    PARTITION `day_3` VALUES LESS THAN ("2023-12-11"),
    PARTITION `day_4` VALUES LESS THAN ("2023-12-30")
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
      l_orderkey    INTEGER NOT NULL,
      l_partkey     INTEGER NOT NULL,
      l_suppkey     INTEGER NOT NULL,
      l_linenumber  INTEGER NOT NULL,
      l_quantity    DECIMALV3(15,2) NOT NULL,
      l_extendedprice  DECIMALV3(15,2) NOT NULL,
      l_discount    DECIMALV3(15,2) NOT NULL,
      l_tax         DECIMALV3(15,2) NOT NULL,
      l_returnflag  CHAR(1) NOT NULL,
      l_linestatus  CHAR(1) NOT NULL,
      l_shipdate    DATE NOT NULL,
      l_commitdate  DATE NOT NULL,
      l_receiptdate DATE NOT NULL,
      l_shipinstruct CHAR(25) NOT NULL,
      l_shipmode     CHAR(10) NOT NULL,
      l_comment      VARCHAR(44) NOT NULL
    )
    DUPLICATE KEY(l_orderkey, l_partkey, l_suppkey, l_linenumber)
    PARTITION BY RANGE(l_shipdate) (
    PARTITION `day_1` VALUES LESS THAN ('2023-12-9'),
    PARTITION `day_2` VALUES LESS THAN ("2023-12-11"),
    PARTITION `day_3` VALUES LESS THAN ("2023-12-30"))
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
    );
    """

    sql """ insert into lineitem values
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-08', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy'),
    (2, 4, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-09', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy'),
    (3, 2, 4, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-10', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy'),
    (4, 3, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-11', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy'),
    (5, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-12-12', '2023-12-12', '2023-12-13', 'c', 'd', 'xxxxxxxxx');
    """

    sql """
    insert into orders values
    (1, 1, 'o', 9.5, '2023-12-08', 'a', 'b', 1, 'yy'),
    (1, 1, 'o', 10.5, '2023-12-08', 'a', 'b', 1, 'yy'),
    (2, 1, 'o', 11.5, '2023-12-09', 'a', 'b', 1, 'yy'),
    (3, 1, 'o', 12.5, '2023-12-10', 'a', 'b', 1, 'yy'),
    (3, 1, 'o', 33.5, '2023-12-10', 'a', 'b', 1, 'yy'),
    (4, 2, 'o', 43.2, '2023-12-11', 'c','d',2, 'mm'),
    (5, 2, 'o', 56.2, '2023-12-12', 'c','d',2, 'mi'),
    (5, 2, 'o', 1.2, '2023-12-12', 'c','d',2, 'mi');  
    """

    sql """
    insert into partsupp values
    (2, 3, 9, 10.01, 'supply1'),
    (2, 3, 10, 11.01, 'supply2');
    """


    // test view which contains simple scan
    def mv1_name = 'mv1';
    def view1_name = 'view1';
    sql """drop view if exists `${view1_name}`"""
    sql """drop materialized view if exists ${mv1_name};"""
    sql"""
        create view ${view1_name} as
            select o_shippriority, o_comment,
            count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) as cnt_1,
            count(distinct case when O_SHIPPRIORITY > 2 and o_orderkey IN (2) then o_custkey else null end) as cnt_2,
            sum(o_totalprice) as sum_1,
            max(o_totalprice) as max_1,
            min(o_totalprice) as min_1,
            count(*)
            from orders
            where o_orderdate = '2023-12-09'
            group by
            o_shippriority,
            o_comment;
        """

    sql """
        CREATE MATERIALIZED VIEW ${mv1_name}
        BUILD IMMEDIATE REFRESH AUTO ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        SELECT cnt_1, cnt_2, sum_1, min_1 from ${view1_name};
        """
    waitingMTMVTaskFinishedByMvName(mv1_name)
    order_qt_mv1 "SELECT * FROM ${mv1_name}"
    sql """drop view if exists `${view1_name}`"""
    sql """drop materialized view if exists ${mv1_name};"""


    // test view which contains join
    def mv2_name = 'mv2';
    def view2_name = 'view2';
    sql """drop view if exists `${view2_name}`"""
    sql """drop materialized view if exists ${mv2_name};"""
    sql"""
        create view ${view2_name} as
        select  t1.L_LINENUMBER, orders.O_CUSTKEY, l_suppkey
        from (select * from lineitem where L_LINENUMBER > 1) t1
        left join orders on t1.L_ORDERKEY = orders.O_ORDERKEY;
        """

    sql """
        CREATE MATERIALIZED VIEW ${mv2_name}
        BUILD IMMEDIATE REFRESH AUTO ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        SELECT l_suppkey, O_CUSTKEY from ${view2_name};
        """
    waitingMTMVTaskFinishedByMvName(mv2_name)
    order_qt_mv2 "SELECT * FROM ${mv2_name}"
    sql """drop view if exists `${view2_name}`"""
    sql """drop materialized view if exists ${mv2_name};"""


    // test view which contains join and aggregate
    def mv3_name = 'mv3';
    def view3_name = 'view3';
    sql """drop view if exists `${view3_name}`"""
    sql """drop materialized view if exists ${mv3_name};"""
    sql"""
        create view ${view3_name} as
        select l_shipdate, o_orderdate, l_partkey, l_suppkey,
        sum(o_totalprice) as sum_1,
        max(o_totalprice) as max_1,
        min(o_totalprice),
        count(*),
        bitmap_union_count(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) as union_1,
        bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)),
        count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) as discintct_1
        from lineitem
        left join (select * from orders where o_orderstatus = 'o') t2
        on lineitem.l_orderkey = o_orderkey and l_shipdate = o_orderdate
        group by
        l_shipdate,
        o_orderdate,
        l_partkey,
        l_suppkey;
        """
    sql """
        CREATE MATERIALIZED VIEW ${mv3_name}
        BUILD IMMEDIATE REFRESH AUTO ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        SELECT l_suppkey, sum_1, max_1, union_1, discintct_1 from ${view3_name};
        """
    waitingMTMVTaskFinishedByMvName(mv3_name)
    order_qt_mv3 "SELECT * FROM ${mv3_name}"
    sql """drop view if exists `${view3_name}`"""
    sql """drop materialized view if exists ${mv3_name};"""


    def mv4_name = 'mv1';
    def view4_name = 'view1';
    sql """drop view if exists `${view4_name}`"""
    // basic test for mv contain view for rewrite
    sql"""
        create view ${view4_name} as
            select * 
            from
            orders left
            join lineitem on l_orderkey = o_orderkey;
        """

    def mv4 = """
    SELECT * from ${view4_name};
    """
    def query4 = """
            select
              o_orderdate,
              o_shippriority,
              o_comment,
              l_orderkey,
              ps_partkey,
              count(*)
            from
              orders left
              join lineitem on l_orderkey = o_orderkey
              left join partsupp on ps_partkey = l_orderkey
              group by
              o_orderdate,
              o_shippriority,
              o_comment,
              l_orderkey,
              ps_partkey;
    """
    order_qt_query4_before "${query4}"
    async_mv_rewrite_success(db, mv4, query4, mv4_name)
    order_qt_query4_after "${query4}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS ${mv4_name}"""

    sql """ drop table if exists orders;"""
    sql """ drop table if exists lineitem;"""
    sql """ drop table if exists partsupp;"""
}
