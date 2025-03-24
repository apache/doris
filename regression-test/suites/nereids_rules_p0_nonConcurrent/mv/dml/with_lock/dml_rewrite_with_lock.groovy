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

suite("dml_rewrite_with_lock", "zfr_mtmv_test") {

    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "SET enable_materialized_view_rewrite=true"
    sql "SET enable_materialized_view_nest_rewrite=true"
    sql "SET enable_materialized_view_union_rewrite=true"

    sql """
    drop table if exists lineitem_range_date_union
    """

    sql """CREATE TABLE `lineitem_range_date_union` (
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
    partition by range (`l_shipdate`) (
        partition p1 values [("2023-10-29"), ("2023-10-30")), 
        partition p2 values [("2023-10-30"), ("2023-10-31")), 
        partition p3 values [("2023-10-31"), ("2023-11-01")))
    DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    drop table if exists orders_range_date_union
    """

    sql """CREATE TABLE `orders_range_date_union` (
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
    partition by range (`o_orderdate`) (
        partition p1 values [("2023-10-29"), ("2023-10-30")), 
        partition p2 values [("2023-10-30"), ("2023-10-31")), 
        partition p3 values [("2023-10-31"), ("2023-11-01")),
        partition p4 values [("2023-11-01"), ("2023-11-02")), 
        partition p5 values [("2023-11-02"), ("2023-11-03")))
    DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    insert into lineitem_range_date_union values 
    (null, 1, 2, 3, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-29'),
    (1, null, 3, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-29'),
    (3, 3, null, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx', '2023-10-31'),
    (1, 2, 3, null, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-29'),
    (2, 3, 2, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-30'),
    (3, 1, 1, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', null, 'c', 'd', 'xxxxxxxxx', '2023-10-31'),
    (1, 3, 2, 2, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-29');
    """

    sql """
    insert into orders_range_date_union values 
    (null, 1, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-29'),
    (1, null, 'o', 109.2, 'c','d',2, 'mm', '2023-10-29'),
    (3, 3, null, 99.5, 'a', 'b', 1, 'yy', '2023-10-30'),
    (1, 2, 'o', null, 'a', 'b', 1, 'yy', '2023-11-01'),
    (2, 3, 'k', 109.2, null,'d',2, 'mm', '2023-11-02'),
    (3, 1, 'k', 99.5, 'a', null, 1, 'yy', '2023-11-02'),
    (1, 3, 'o', 99.5, 'a', 'b', null, 'yy', '2023-10-31'),
    (2, 1, 'o', 109.2, 'c','d',2, null, '2023-10-30'),
    (3, 2, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-29'),
    (4, 5, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-31'); 
    """

    sql """DROP MATERIALIZED VIEW if exists day_mv;"""
    create_async_mv(db, "day_mv",
            """select date_trunc(`l_shipdate`, 'day') as col1, l_shipdate, l_orderkey 
               from lineitem_range_date_union as t1 left join orders_range_date_union as t2 
               on t1.l_orderkey = t2.o_orderkey group by col1, l_shipdate, l_orderkey;
            """
    )

    def query1 = """
    select date_trunc(`l_shipdate`, 'day') as col1, l_shipdate, l_orderkey 
    from lineitem_range_date_union as t1 left join orders_range_date_union as t2 
    on t1.l_orderkey = t2.o_orderkey 
    group by col1, l_shipdate, l_orderkey
    """

    mv_rewrite_success(query1, "day_mv")

    def query2 = """
    select date_trunc(`l_shipdate`, 'hour') as col1, l_shipdate, l_orderkey from 
    lineitem_range_date_union as t1 left join orders_range_date_union as t2 
    on t1.l_orderkey = t2.o_orderkey 
    group by col1, l_shipdate, l_orderkey
    """

    sql """DROP MATERIALIZED VIEW if exists hour_mv;"""
    create_async_mv(db, "hour_mv",
            """
    select date_trunc(`l_shipdate`, 'hour') as col1, l_shipdate, l_orderkey from 
    lineitem_range_date_union as t1 left join orders_range_date_union as t2 
    on t1.l_orderkey = t2.o_orderkey group by col1, l_shipdate, l_orderkey;
    """)
    mv_rewrite_success(query2, "hour_mv")


    sql """alter table lineitem_range_date_union add partition p4 values [("2023-11-01"), ("2023-11-02"));"""
    sql """insert into lineitem_range_date_union values
        (1, null, 3, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-11-01')"""

    sql """refresh MATERIALIZED VIEW hour_mv auto;"""
    waitingMTMVTaskFinishedByMvName("hour_mv")

    sql """refresh MATERIALIZED VIEW day_mv auto;"""
    waitingMTMVTaskFinishedByMvName("day_mv")

    mv_rewrite_success(query1, "day_mv")
    mv_rewrite_success(query2, "hour_mv")
}



