package mv.date_trunc
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

suite("mv_with_date_trunc") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set runtime_filter_mode=OFF";
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"

    sql """
    drop table if exists lineitem
    """

    sql """
    drop table if exists lineitem;
    """

    sql """
    CREATE TABLE IF NOT EXISTS lineitem (
            l_orderkey integer not null,
            l_partkey integer not null,
            l_suppkey integer not null,
            l_linenumber integer not null,
            l_quantity decimalv3(15, 2) not null,
            l_extendedprice decimalv3(15, 2) not null,
            l_discount decimalv3(15, 2) not null,
            l_tax decimalv3(15, 2) not null,
            l_returnflag char(1) not null,
            l_linestatus char(1) not null,
            l_shipdate date not null,
            l_commitdate DATETIME(6) not null,
            l_commitdate_var varchar(44) not null,
            l_receiptdate date not null,
            l_shipinstruct char(25) not null,
            l_shipmode char(10) not null,
            l_comment varchar(44) not null
    ) DUPLICATE KEY(
            l_orderkey, l_partkey, l_suppkey,
            l_linenumber
    ) PARTITION BY RANGE(l_shipdate) (
            FROM
            ('2022-12-31') TO ('2023-12-31') INTERVAL 1 DAY
    ) DISTRIBUTED BY HASH(l_orderkey) BUCKETS 3 PROPERTIES ("replication_num" = "1");
    """


    sql """
    insert into lineitem
    values
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-01-01', '2023-01-01 00:00:00.999999', '2023-01-01 00:00:00.999999', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-01-16', '2023-01-01 00:00:00.999999', '2023-01-01 00:00:00.999999', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-01-01', '2023-01-01 02:01:45', '2023-01-01 02:01:45', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-01-01', '2023-01-01 03:02:43', '2023-01-01 03:02:43', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-04-01', '2023-04-01 05:03:42', '2023-04-01 05:03:42', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-04-01', '2023-04-01 07:04:40', '2023-04-01 07:04:40', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-04-01', '2023-04-01 09:05:36', '2023-04-01 09:05:36', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-07-01', '2023-07-01 11:06:35', '2023-07-01 11:06:35', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-07-01', '2023-07-01 13:07:34', '2023-07-01 13:07:34', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-07-01', '2023-07-01 15:08:30', '2023-07-01 15:08:30', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-01', '2023-10-01 17:09:25', '2023-10-01 17:09:25', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-01', '2023-10-01 19:10:24', '2023-10-01 19:10:24', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-01', '2023-10-01 21:11:23', '2023-10-01 21:11:23', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-15', '2023-10-15 00:00:00.000000', '2023-10-15 00:00:00.000000', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-15', '2023-10-15 00:00:00.000001', '2023-10-15 00:00:00.000001', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-15', '2023-10-15 00:00:00.000001', '2023-10-abcd', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-15', '2023-10-15 23:59:59.999999', '2023-10-15 23:59:59.999999', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-16', '2023-10-16 00:00:00', '2023-10-16 00:00:00', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-16', '2023-10-16 00:00:00.000001', '2023-10-16 00:00:00.000001', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-16', '2023-10-16 00:00:00.000001', '2023-10-abcd', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-16', '2023-10-16 23:59:59.999999', '2023-10-16 23:59:59.999999', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17 00:00:00.000000', '2023-10-17 00:00:00.000000', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17 00:00:00.000001', '2023-10-17 00:00:00.000001', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17 00:00:00.000001', '2023-10-17 00:abcd', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17 23:59:59.999999', '2023-10-17 23:59:59.999999', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (2, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k','2023-10-18', '2023-10-18 00:00:00.000000', '2023-10-18 00:00:00.000000', '2023-10-18', 'a', 'b', 'yyyyyyyyy'),
    (2, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k','2023-10-18', '2023-10-18 00:00:00.000001','2023-10-18 00:00:00.000001', '2023-10-18', 'a', 'b', 'yyyyyyyyy'),
    (2, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k','2023-10-18', '2023-10-18 00:00:00.000001','2023-10-18 00:00:abcd', '2023-10-18', 'a', 'b', 'yyyyyyyyy'),
    (2, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k','2023-10-18', '2023-10-18 23:59:59.999999', '2023-10-18 23:59:59.999999', '2023-10-18', 'a', 'b', 'yyyyyyyyy'),
    (3, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o','2023-10-19', '2023-10-19 00:00:00','2023-10-19 00:00:00', '2023-10-19', 'c', 'd', 'xxxxxxxxx'),
    (3, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o','2023-10-19', '2023-10-19 00:00:00.000001','2023-10-19 00:00:00.000001','2023-10-19', 'c', 'd', 'xxxxxxxxx'),
    (3, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o','2023-10-19', '2023-10-19 00:00:00.000001','2023-10-19 00:00:00.abcd','2023-10-19', 'c', 'd', 'xxxxxxxxx'),
    (3, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o','2023-10-19', '2023-10-19 23:59:59.999999','2023-10-19 23:59:59.999999', '2023-10-19', 'c', 'd', 'xxxxxxxxx');
    """

    sql """analyze table lineitem with sync;"""
    sql """alter table lineitem modify column l_comment set stats ('row_count'='27');"""
    def result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)

    // second, minute ,hour, day, week, month, quarter, year
    // 1. expr in date_trunc is simple col
    // 1.1 positive case, use field data type is datetime
    // day
    def mv1_0 = """
    select
    l_shipmode,
    date_trunc(l_commitdate, 'day') as day_trunc,
    count(*)
    from
    lineitem
    group by
    l_shipmode,
    date_trunc(l_commitdate, 'day');
    """
    def query1_0 = """
    select
    l_shipmode,
    count(*)
    from
    lineitem
    where
    l_commitdate >= '2023-10-17' and  l_commitdate < '2023-10-18'
    group by
    l_shipmode,
    date_trunc(l_commitdate, 'day');
    """
    order_qt_query1_0_before "${query1_0}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    async_mv_rewrite_success(db, mv1_0, query1_0, "mv1_0")
    order_qt_query1_0_after "${query1_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_0"""

    def mv1_1 = """
            select
              l_shipmode,
              date_trunc(l_commitdate, 'day') as day_trunc,
              count(*)
            from
              lineitem
            group by
              l_shipmode,
              date_trunc(l_commitdate, 'day');
    """
    def query1_1 = """
        select
          l_shipmode,
          count(*)
        from
          lineitem
        where
          '2023-10-17' <= l_commitdate and '2023-10-18' > l_commitdate
        group by
          l_shipmode;
    """
    order_qt_query1_1_before "${query1_1}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    async_mv_rewrite_success(db, mv1_1, query1_1, "mv1_1")
    order_qt_query1_1_after "${query1_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_1"""


    def mv1_1_0 = """
            select
              l_shipmode,
              date_trunc(l_commitdate, 'day') as day_trunc
            from
              lineitem;
    """
    def query1_1_0 = """
        select
          l_shipmode
        from
          lineitem
        where
          '2023-10-17' <= l_commitdate and '2023-10-18' > l_commitdate;
    """
    order_qt_query1_1_0_before "${query1_1_0}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    // without group, should success
    async_mv_rewrite_success(db, mv1_1_0, query1_1_0, "mv1_1_0")
    order_qt_query1_1_0_after "${query1_1_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_1_0"""


    def mv1_1_1 = """
            select
              l_shipmode,
              date_trunc(l_commitdate, 'day') as day_trunc
            from
              lineitem;
    """
    def query1_1_1 = """
        select
          l_shipmode
        from
          lineitem
        where
          '2023-10-17' <= date_trunc(l_commitdate, 'day') and '2023-10-18' > date_trunc(l_commitdate, 'day');
    """
    order_qt_query1_1_1_before "${query1_1_1}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    async_mv_rewrite_success(db, mv1_1_1, query1_1_1, "mv1_1_1", [TRY_IN_RBO, FORCE_IN_RBO])
    async_mv_rewrite_fail(db, mv1_1_1, query1_1_1, "mv1_1_1", [NOT_IN_RBO])
    order_qt_query1_1_1_after "${query1_1_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_1_1"""


    // second
    def mv1_2 = """
    select
    l_shipmode,
    date_trunc(l_commitdate, 'second') as day_trunc,
    count(*)
    from
    lineitem
    group by
    l_shipmode,
    date_trunc(l_commitdate, 'second');
    """
    def query1_2 = """
    select
    l_shipmode,
    count(*)
    from
    lineitem
    where
    l_commitdate >= '2023-10-17 00:05:08' and  l_commitdate < '2023-10-18'
    group by
    l_shipmode;
    """
    order_qt_query1_2_before "${query1_2}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    async_mv_rewrite_success(db, mv1_2, query1_2, "mv1_2")
    order_qt_query1_2_after "${query1_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_2"""


    def mv1_3 = """
    select
    l_shipmode,
    date_trunc(l_shipdate, 'second') as day_trunc,
    count(*)
    from
    lineitem
    group by
    l_shipmode,
    date_trunc(l_shipdate, 'second');
    """
    def query1_3 = """
    select
    l_shipmode,
    count(*)
    from
    lineitem
    where
    '2023-01-17 00:05:08' <= l_shipdate and '2023-10-18' > l_shipdate
    group by
    l_shipmode;
    """
    order_qt_query1_3_before "${query1_3}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    async_mv_rewrite_success(db, mv1_3, query1_3, "mv1_3")
    order_qt_query1_3_after "${query1_3}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_3"""


    def mv1_3_0 = """
    select
    l_shipmode,
    date_trunc(l_shipdate, 'second') as day_trunc
    from
    lineitem;
    """
    def query1_3_0 = """
    select
    l_shipmode
    from
    lineitem
    where
    '2023-10-16 00:05:08' <= l_shipdate and '2023-10-18' > l_shipdate;
    """
    order_qt_query1_3_0_before "${query1_3_0}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    async_mv_rewrite_success_without_check_chosen(db, mv1_3_0, query1_3_0, "mv1_3_0")
    order_qt_query1_3_0_after "${query1_3_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_3_0"""


    def mv1_3_1 = """
    select
    l_shipmode,
    date_trunc(l_shipdate, 'second') as day_trunc
    from
    lineitem;
    """
    def query1_3_1 = """
    select
    l_shipmode
    from
    lineitem
    where
    '2023-10-16 00:05:08' <= date_trunc(l_shipdate, 'second') and '2023-10-18' > date_trunc(l_shipdate, 'second');
    """
    order_qt_query1_3_1_before "${query1_3_1}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    async_mv_rewrite_success_without_check_chosen(db, mv1_3_1, query1_3_1, "mv1_3_1", [TRY_IN_RBO, FORCE_IN_RBO])
    async_mv_rewrite_fail(db, mv1_3_1, query1_3_1, "mv1_3_1", [NOT_IN_RBO])
    order_qt_query1_3_1_after "${query1_3_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_3_1"""


    def mv1_3_2 = """
    select
    l_shipmode,
    date_trunc(l_commitdate, 'second') as day_trunc
    from
    lineitem;
    """
    def query1_3_2 = """
    select
    l_shipmode
    from
    lineitem
    where
    '2023-01-17 00:05:08.999999' <= date_trunc(l_commitdate, 'second') and '2023-10-18' > date_trunc(l_commitdate, 'second');
    """
    order_qt_query1_3_2_before "${query1_3_2}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    async_mv_rewrite_success_without_check_chosen(db, mv1_3_2, query1_3_2, "mv1_3_2", [TRY_IN_RBO, FORCE_IN_RBO])
    async_mv_rewrite_fail(db, mv1_3_2, query1_3_2, "mv1_3_2", [NOT_IN_RBO])
    order_qt_query1_3_2_after "${query1_3_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_3_2"""


    def mv1_3_3 = """
    select
    l_shipmode,
    date_trunc(l_commitdate, 'second') as day_trunc
    from
    lineitem;
    """
    def query1_3_3 = """
    select
    l_shipmode
    from
    lineitem
    where
    '2023-01-17 00:05:08.999999' <= l_commitdate and '2023-10-18' > l_commitdate;
    """
    order_qt_query1_3_3_before "${query1_3_3}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    // use micro second,should fail
    async_mv_rewrite_fail(db, mv1_3_3, query1_3_3, "mv1_3_3")
    order_qt_query1_3_3_after "${query1_3_3}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_3_3"""


    // minute
    def mv1_4 = """
    select
    l_shipmode,
    date_trunc(l_shipdate, 'minute') as day_trunc,
    count(*)
    from
    lineitem
    group by
    l_shipmode,
    date_trunc(l_shipdate, 'minute');
    """
    def query1_4 = """
    select
    l_shipmode,
    count(*)
    from
    lineitem
    where
    l_shipdate >= '2023-01-17 00:05:00' and  l_shipdate < '2023-10-18'
    group by
    l_shipmode;
    """
    order_qt_query1_4_before "${query1_4}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    async_mv_rewrite_success(db, mv1_4, query1_4, "mv1_4")
    order_qt_query1_4_after "${query1_4}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_4"""


    def mv1_5 = """
    select
    l_shipmode,
    date_trunc(l_commitdate, 'minute') as day_trunc,
    count(*)
    from
    lineitem
    group by
    l_shipmode,
    date_trunc(l_commitdate, 'minute');
    """
    def query1_5 = """
    select
    l_shipmode,
    count(*)
    from
    lineitem
    where
    '2023-10-18' > l_commitdate and '2023-01-17 00:05:00' <= l_commitdate
    group by
    l_shipmode;
    """
    order_qt_query1_5_before "${query1_5}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    async_mv_rewrite_success(db, mv1_5, query1_5, "mv1_5")
    order_qt_query1_5_after "${query1_5}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_5"""


    def mv1_5_0 = """
    select
    l_shipmode,
    date_trunc(l_commitdate, 'minute') as day_trunc
    from
    lineitem;
    """
    def query1_5_0 = """
    select
    l_shipmode
    from
    lineitem
    where
    '2023-10-18' > l_commitdate and '2023-10-16 00:05:00' <= l_commitdate;
    """
    order_qt_query1_5_0_before "${query1_5_0}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    async_mv_rewrite_success_without_check_chosen(db, mv1_5_0, query1_5_0, "mv1_5_0")
    order_qt_query1_5_0_after "${query1_5_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_5_0"""

    def mv1_5_1 = """
    select
    l_shipmode,
    date_trunc(l_commitdate, 'minute') as day_trunc
    from
    lineitem;
    """
    def query1_5_1 = """
    select
    l_shipmode
    from
    lineitem
    where
    '2023-10-18' > date_trunc(l_commitdate, 'minute') and '2023-10-16 00:05:00' <= date_trunc(l_commitdate, 'minute');
    """
    order_qt_query1_5_1_before "${query1_5_1}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    async_mv_rewrite_success_without_check_chosen(db, mv1_5_1, query1_5_1, "mv1_5_1", [TRY_IN_RBO, FORCE_IN_RBO])
    async_mv_rewrite_fail(db, mv1_5_1, query1_5_1, "mv1_5_1", [NOT_IN_RBO])
    order_qt_query1_5_1_after "${query1_5_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_5_1"""


    // hour
    def mv1_6 = """
    select
    l_shipmode,
    date_trunc(l_commitdate, 'hour') as day_trunc,
    count(*)
    from
    lineitem
    group by
    l_shipmode,
    date_trunc(l_commitdate, 'hour');
    """
    def query1_6 = """
    select
    l_shipmode,
    count(*)
    from
    lineitem
    where
    l_commitdate < '2023-10-18' and l_commitdate >= '2023-05-17 01:00:00'
    group by
    l_shipmode;
    """
    order_qt_query1_6_before "${query1_6}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    async_mv_rewrite_success(db, mv1_6, query1_6, "mv1_6")
    order_qt_query1_6_after "${query1_6}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_6"""


    def mv1_7 = """
    select
    l_shipmode,
    date_trunc(l_commitdate, 'hour') as day_trunc,
    count(*)
    from
    lineitem
    group by
    l_shipmode,
    date_trunc(l_commitdate, 'hour');
    """
    def query1_7 = """
    select
    l_shipmode,
    count(*)
    from
    lineitem
    where
    '2023-05-17 01:00:00' <= l_commitdate and '2023-10-18' > l_commitdate
    group by
    l_shipmode;
    """
    order_qt_query1_7_before "${query1_7}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    async_mv_rewrite_success(db, mv1_7, query1_7, "mv1_7")
    order_qt_query1_7_after "${query1_7}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_7"""

    def mv1_7_0 = """
    select
    l_shipmode,
    date_trunc(l_commitdate, 'hour') as day_trunc
    from
    lineitem;
    """
    def query1_7_0 = """
    select
    l_shipmode
    from
    lineitem
    where
    '2023-10-16 01:00:00' <= l_commitdate and '2023-10-18' > l_commitdate;
    """
    order_qt_query1_7_0_before "${query1_7_0}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    async_mv_rewrite_success_without_check_chosen(db, mv1_7_0, query1_7_0, "mv1_7_0")
    order_qt_query1_7_0_after "${query1_7_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_7_0"""


    def mv1_7_1 = """
    select
    l_shipmode,
    date_trunc(l_commitdate, 'hour') as day_trunc
    from
    lineitem;
    """
    def query1_7_1 = """
    select
    l_shipmode
    from
    lineitem
    where
    '2023-10-16 01:00:00' <= date_trunc(l_commitdate, 'hour') and '2023-10-18' > date_trunc(l_commitdate, 'hour');
    """
    order_qt_query1_7_1_before "${query1_7_1}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    async_mv_rewrite_success_without_check_chosen(db, mv1_7_1, query1_7_1, "mv1_7_1", [TRY_IN_RBO, FORCE_IN_RBO])
    async_mv_rewrite_fail(db, mv1_7_1, query1_7_1, "mv1_7_1", [NOT_IN_RBO])
    order_qt_query1_7_1_after "${query1_7_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_7_1"""


    // week
    def mv1_8 = """
    select
    l_shipmode,
    date_trunc(l_shipdate, 'week') as day_trunc,
    count(*)
    from
    lineitem
    group by
    l_shipmode,
    date_trunc(l_shipdate, 'week');
    """
    def query1_8 = """
    select
    l_shipmode,
    count(*)
    from
    lineitem
    where
    l_shipdate < '2023-09-04' and '2023-01-16' <=  l_shipdate
    group by
    l_shipmode;
    """
    order_qt_query1_8_before "${query1_8}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    async_mv_rewrite_success_without_check_chosen(db, mv1_8, query1_8, "mv1_8")
    order_qt_query1_8_after "${query1_8}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_8"""


    def mv1_9 = """
    select
    l_shipmode,
    date_trunc(l_commitdate, 'week') as day_trunc,
    count(*)
    from
    lineitem
    group by
    l_shipmode,
    date_trunc(l_commitdate, 'week');
    """
    def query1_9 = """
    select
    l_shipmode,
    count(*)
    from
    lineitem
    where
    '2023-01-16' <= l_commitdate and '2023-09-04' > l_commitdate
    group by
    l_shipmode;
    """
    order_qt_query1_9_before "${query1_9}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    async_mv_rewrite_success(db, mv1_9, query1_9, "mv1_9")
    order_qt_query1_9_after "${query1_9}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_9"""


    def mv1_9_0 = """
    select
    l_shipmode,
    date_trunc(l_commitdate, 'week') as day_trunc
    from
    lineitem;
    """
    def query1_9_0 = """
    select
    l_shipmode
    from
    lineitem
    where
    '2023-01-16' <= l_commitdate and '2023-09-04' > l_commitdate;
    """
    order_qt_query1_9_0_before "${query1_9_0}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    async_mv_rewrite_success_without_check_chosen(db, mv1_9_0, query1_9_0, "mv1_9_0")
    order_qt_query1_9_0_after "${query1_9_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_9_0"""



    def mv1_9_1 = """
    select
    l_shipmode,
    date_trunc(l_commitdate, 'week') as day_trunc
    from
    lineitem;
    """
    def query1_9_1 = """
    select
    l_shipmode
    from
    lineitem
    where
    '2023-01-16' <= date_trunc(l_commitdate, 'week') and '2023-09-04' > date_trunc(l_commitdate, 'week');
    """
    order_qt_query1_9_1_before "${query1_9_1}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    async_mv_rewrite_success_without_check_chosen(db, mv1_9_1, query1_9_1, "mv1_9_1", [TRY_IN_RBO, FORCE_IN_RBO])
    async_mv_rewrite_fail(db, mv1_9_1, query1_9_1, "mv1_9_1", [NOT_IN_RBO])
    order_qt_query1_9_1_after "${query1_9_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_9_1"""

    // month
    def mv1_10 = """
    select
    l_shipmode,
    date_trunc(l_shipdate, 'month') as day_trunc,
    count(*)
    from
    lineitem
    group by
    l_shipmode,
    date_trunc(l_shipdate, 'month');
    """
    def query1_10 = """
    select
    l_shipmode,
    count(*)
    from
    lineitem
    where
    l_shipdate >= '2023-02-01' and  l_shipdate < '2023-12-01'
    group by
    l_shipmode;
    """
    order_qt_query1_10_before "${query1_10}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    async_mv_rewrite_success(db, mv1_10, query1_10, "mv1_10")
    order_qt_query1_10_after "${query1_10}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_10"""


    def mv1_11 = """
    select
    l_shipmode,
    date_trunc(l_commitdate, 'month') as day_trunc,
    count(*)
    from
    lineitem
    group by
    l_shipmode,
    date_trunc(l_commitdate, 'month');
    """
    def query1_11 = """
    select
    l_shipmode,
    count(*)
    from
    lineitem
    where
    '2023-02-01' <= l_commitdate and '2023-12-01' > l_commitdate
    group by
    l_shipmode;
    """
    order_qt_query1_11_before "${query1_11}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    async_mv_rewrite_success(db, mv1_11, query1_11, "mv1_11")
    order_qt_query1_11_after "${query1_11}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_11"""


    def mv1_11_0 = """
    select
    l_shipmode,
    date_trunc(l_commitdate, 'month') as day_trunc
    from
    lineitem;
    """
    def query1_11_0 = """
    select
    l_shipmode
    from
    lineitem
    where
    '2023-02-01' <= l_commitdate and '2023-12-01' > l_commitdate;
    """
    order_qt_query1_11_0_before "${query1_11_0}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    async_mv_rewrite_success_without_check_chosen(db, mv1_11_0, query1_11_0, "mv1_11_0")
    order_qt_query1_11_0_after "${query1_11_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_11_0"""


    def mv1_11_1 = """
    select
    l_shipmode,
    date_trunc(l_commitdate, 'month') as day_trunc
    from
    lineitem;
    """
    def query1_11_1 = """
    select
    l_shipmode
    from
    lineitem
    where
    '2023-02-01' <= date_trunc(l_commitdate, 'month') and '2023-12-01' > date_trunc(l_commitdate, 'month');
    """
    order_qt_query1_11_1_before "${query1_11_1}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    async_mv_rewrite_success_without_check_chosen(db, mv1_11_1, query1_11_1, "mv1_11_1", [TRY_IN_RBO, FORCE_IN_RBO])
    async_mv_rewrite_fail(db, mv1_11_1, query1_11_1, "mv1_11_1", [NOT_IN_RBO])
    order_qt_query1_11_1_after "${query1_11_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_11_1"""


    // quarter
    def mv1_12 = """
    select
    l_shipmode,
    date_trunc(l_commitdate, 'quarter') as day_trunc,
    count(*)
    from
    lineitem
    group by
    l_shipmode,
    date_trunc(l_commitdate, 'quarter');
    """
    def query1_12 = """
    select
    l_shipmode,
    count(*)
    from
    lineitem
    where
    l_commitdate >= '2023-04-01' and  l_commitdate < '2023-10-01'
    group by
    l_shipmode;
    """
    order_qt_query1_12_before "${query1_12}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    async_mv_rewrite_success(db, mv1_12, query1_12, "mv1_12")
    order_qt_query1_12_after "${query1_12}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_12"""


    def mv1_13 = """
    select
    l_shipmode,
    date_trunc(l_shipdate, 'quarter') as day_trunc,
    count(*)
    from
    lineitem
    group by
    l_shipmode,
    date_trunc(l_shipdate, 'quarter');
    """
    def query1_13 = """
    select
    l_shipmode,
    count(*)
    from
    lineitem
    where
    '2023-04-01' <= l_shipdate and '2023-10-01' > l_shipdate
    group by
    l_shipmode;
    """
    order_qt_query1_13_before "${query1_13}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    async_mv_rewrite_success(db, mv1_13, query1_13, "mv1_13")
    order_qt_query1_13_after "${query1_13}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_13"""


    def mv1_13_0 = """
    select
    l_shipmode,
    date_trunc(l_shipdate, 'quarter') as day_trunc
    from
    lineitem;
    """
    def query1_13_0 = """
    select
    l_shipmode
    from
    lineitem
    where
    '2023-04-01' <= l_shipdate and '2023-10-01' > l_shipdate;
    """
    order_qt_query1_13_0_before "${query1_13_0}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    async_mv_rewrite_success_without_check_chosen(db, mv1_13_0, query1_13_0, "mv1_13_0")
    order_qt_query1_13_0_after "${query1_13_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_13_0"""


    def mv1_13_1 = """
    select
    l_shipmode,
    date_trunc(l_shipdate, 'quarter') as day_trunc
    from
    lineitem;
    """
    def query1_13_1 = """
    select
    l_shipmode
    from
    lineitem
    where
    '2023-04-01' <= date_trunc(l_shipdate, 'quarter') and '2023-10-01' > date_trunc(l_shipdate, 'quarter');
    """
    order_qt_query1_13_1_before "${query1_13_1}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    async_mv_rewrite_success_without_check_chosen(db, mv1_13_1, query1_13_1, "mv1_13_1", [TRY_IN_RBO, FORCE_IN_RBO])
    async_mv_rewrite_fail(db, mv1_13_1, query1_13_1, "mv1_13_1", [NOT_IN_RBO])
    order_qt_query1_13_1_after "${query1_13_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_13_1"""


    // year
    def mv1_14 = """
    select
    l_shipmode,
    date_trunc(l_commitdate, 'year') as day_trunc,
    count(*)
    from
    lineitem
    group by
    l_shipmode,
    date_trunc(l_commitdate, 'year');
    """
    def query1_14 = """
    select
    l_shipmode,
    count(*)
    from
    lineitem
    where
    l_commitdate >= '2023-01-01' and  l_commitdate < '2024-01-01'
    group by
    l_shipmode;
    """
    order_qt_query1_14_before "${query1_14}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    async_mv_rewrite_success(db, mv1_14, query1_14, "mv1_14")
    order_qt_query1_14_after "${query1_14}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_14"""


    def mv1_15 = """
    select
    l_shipmode,
    date_trunc(l_commitdate, 'year') as day_trunc,
    count(*)
    from
    lineitem
    group by
    l_shipmode,
    date_trunc(l_commitdate, 'year');
    """
    def query1_15 = """
    select
    l_shipmode,
    count(*)
    from
    lineitem
    where
    '2023-01-01' <= l_commitdate and '2024-01-01' > l_commitdate
    group by
    l_shipmode;
    """
    order_qt_query1_15_before "${query1_15}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    async_mv_rewrite_success(db, mv1_15, query1_15, "mv1_15")
    order_qt_query1_15_after "${query1_15}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_15"""


    def mv1_15_0 = """
    select
    l_shipmode,
    date_trunc(l_commitdate, 'year') as day_trunc
    from
    lineitem;
    """
    def query1_15_0 = """
    select
    l_shipmode
    from
    lineitem
    where
    '2023-01-01' <= l_commitdate and '2024-01-01' > l_commitdate;
    """
    order_qt_query1_15_0_before "${query1_15_0}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    async_mv_rewrite_success_without_check_chosen(db, mv1_15_0, query1_15_0, "mv1_15_0")
    order_qt_query1_15_0_after "${query1_15_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_15_0"""


    def mv1_15_1 = """
    select
    l_shipmode,
    date_trunc(l_commitdate, 'year') as day_trunc
    from
    lineitem;
    """
    def query1_15_1 = """
    select
    l_shipmode
    from
    lineitem
    where
    '2023-01-01' <= date_trunc(l_commitdate, 'year') and '2024-01-01' > date_trunc(l_commitdate, 'year');
    """
    order_qt_query1_15_1_before "${query1_15_1}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    async_mv_rewrite_success_without_check_chosen(db, mv1_15_1, query1_15_1, "mv1_15_1", [TRY_IN_RBO, FORCE_IN_RBO])
    async_mv_rewrite_fail(db, mv1_15_1, query1_15_1, "mv1_15_1", [NOT_IN_RBO])
    order_qt_query1_15_1_after "${query1_15_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_15_1"""


    // use ComparisonPredicate except >= and <， should fail
    def mv2_0 = """
    select
    l_shipmode,
    date_trunc(l_commitdate, 'day') as day_trunc,
    count(*)
    from
    lineitem
    group by
    l_shipmode,
    date_trunc(l_commitdate, 'day');
    """
    def query2_0 = """
            select
              l_shipmode,
              count(*)
            from
              lineitem
            where
               l_commitdate <= '2023-09-04' and '2023-01-16' <=  l_commitdate
            group by
              l_shipmode;
            """
    order_qt_query2_0_before "${query2_0}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    async_mv_rewrite_fail(db, mv2_0, query2_0, "mv2_0")
    order_qt_query2_0_after "${query2_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_0"""


    def mv2_1 = """
            select
              l_shipmode,
              date_trunc(l_shipdate, 'week') as day_trunc,
              count(*)
            from
              lineitem
            group by
              l_shipmode,
              date_trunc(l_shipdate, 'week');
            """
    def query2_1 = """
            select
              l_shipmode,
              count(*)
            from
              lineitem
            where
              '2023-01-16' = l_shipdate
            group by
              l_shipmode;
            """
    order_qt_query2_1_before "${query2_1}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    async_mv_rewrite_success_without_check_chosen(db, mv2_1, query2_1, "mv2_1")
    order_qt_query2_1_after "${query2_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_1"""

    // use un expect constant should fail
    def mv2_2 = """
            select
              l_shipmode,
              date_trunc(l_shipdate, 'minute') as day_trunc,
              count(*)
            from
              lineitem
            group by
              l_shipmode,
              date_trunc(l_shipdate, 'minute');
            """
    def query2_2 = """
            select
              l_shipmode,
              count(*)
            from
              lineitem
            where
              l_shipdate >= '2023-01-17 00:05:01' and  l_shipdate < '2023-10-18'
            group by
              l_shipmode;
            """
    order_qt_query2_2_before "${query2_2}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    // l_shipdate >= '2023-01-17 00:05:01' because l_shipdate data type is date, so
    // simply to l_shipdate >= '2023-01-18 00:00:00'
    async_mv_rewrite_success(db, mv2_2, query2_2, "mv2_2")
    order_qt_query2_2_after "${query2_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_2"""


    def mv2_3 = """
            select
              l_shipmode,
              date_trunc(l_commitdate, 'minute') as day_trunc,
              count(*)
            from
              lineitem
            group by
              l_shipmode,
              date_trunc(l_commitdate, 'minute');
            """
    def query2_3 = """
            select
              l_shipmode,
              count(*)
            from
              lineitem
            where
              l_commitdate >= '2023-01-17 00:05:01' and  l_commitdate < '2023-10-18'
            group by
              l_shipmode;
            """
    order_qt_query2_3_before "${query2_3}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    async_mv_rewrite_fail(db, mv2_3, query2_3, "mv2_3")
    order_qt_query2_3_after "${query2_3}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_3"""


    // 2. expr in date_trunc is cmplex expr
    def mv3_0 = """
    select
    l_shipmode,
    date_add(l_commitdate, INTERVAL 2 DAY) as day_trunc,
    count(*)
    from
    lineitem
    group by
    l_shipmode,
    date_add(l_commitdate, INTERVAL 2 DAY);
    """
    def query3_0 = """
            select
              l_shipmode,
              count(*)
            from
              lineitem
            where
              '2023-10-01' <=  date_add(l_commitdate, INTERVAL 2 DAY) and '2023-10-30 00:10:00' > date_add(l_commitdate, INTERVAL 2 DAY)
            group by
              l_shipmode;
            """
    order_qt_query3_0_before "${query3_0}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    async_mv_rewrite_fail(db, mv3_0, query3_0, "mv3_0")
    order_qt_query3_0_after "${query3_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_0"""


    def mv3_1 = """
            select
              l_shipmode,
              date_trunc(date_add(l_commitdate, INTERVAL 2 DAY), 'day') as day_trunc,
              count(*)
            from
              lineitem
            group by
              l_shipmode,
              date_trunc(date_add(l_commitdate, INTERVAL 2 DAY), 'day');
            """
    def query3_1 = """
            select
              l_shipmode,
              count(*)
            from
              lineitem
            where
              '2023-10-01' <=  date_add(l_commitdate, INTERVAL 2 DAY) and '2023-10-30 00:10:00' >  date_add(l_commitdate, INTERVAL 2 DAY)
            group by
              l_shipmode;
            """
    order_qt_query3_1_before "${query3_1}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    async_mv_rewrite_fail(db, mv3_1, query3_1, "mv3_1")
    order_qt_query3_1_after "${query3_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_1"""


    def mv3_2 = """
            select
              l_shipmode,
              date_trunc(l_commitdate, 'day') as day_trunc,
              count(*)
            from
              lineitem
            group by
              l_shipmode,
              date_trunc(l_commitdate, 'day');
            """
    def query3_2 = """
            select
              l_shipmode,
              count(*)
            from
              lineitem
            where
              date_add(l_commitdate, INTERVAL 2 DAY) >= '2023-10-01' and  date_add(l_commitdate, INTERVAL 2 DAY) < '2023-10-25'
            group by
              l_shipmode;
            """
    order_qt_query3_2_before "${query3_2}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    async_mv_rewrite_success(db, mv3_2, query3_2, "mv3_2")
    order_qt_query3_2_after "${query3_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_2"""


    def mv3_3 = """
            select
              l_shipmode,
              date_trunc(l_commitdate, 'day') as day_trunc,
              count(*)
            from
              lineitem
            group by
              l_shipmode,
              date_trunc(l_commitdate, 'day');
            """
    def query3_3 = """
            select
              l_shipmode,
              count(*)
            from
              lineitem
            where
              '2023-10-01' <=  date_add(l_commitdate, INTERVAL 2 DAY) and '2023-10-30' >  date_add(l_commitdate, INTERVAL 2 DAY)
            group by
              l_shipmode;
            """
    order_qt_query3_3_before "${query3_3}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    async_mv_rewrite_success(db, mv3_3, query3_3, "mv3_3")
    order_qt_query3_3_after "${query3_3}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_3"""


    // use ComparisonPredicate except >= and <， should fail
    def mv3_4 = """
            select
              l_shipmode,
              date_trunc(l_commitdate, 'day') as day_trunc,
              count(*)
            from
              lineitem
            group by
              l_shipmode,
              date_trunc(l_commitdate, 'day');
            """
    def query3_4 = """
            select
              l_shipmode,
              count(*)
            from
              lineitem
            where
              date_add(l_commitdate, INTERVAL 2 DAY) >= '2023-10-01' and  date_add(l_commitdate, INTERVAL 2 DAY) <= '2023-10-25'
            group by
              l_shipmode;
            """
    order_qt_query3_4_before "${query3_4}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    async_mv_rewrite_fail(db, mv3_4, query3_4, "mv3_4")
    order_qt_query3_4_after "${query3_4}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_4"""


    // use un expect constant should fail
    def mv3_5 = """
            select
              l_shipmode,
              date_trunc(l_commitdate, 'day') as day_trunc,
              count(*)
            from
              lineitem
            group by
              l_shipmode,
              date_trunc(l_commitdate, 'day');
            """
    def query3_5 = """
            select
              l_shipmode,
              count(*)
            from
              lineitem
            where
              '2023-10-01' <=  date_add(l_commitdate, INTERVAL 2 DAY) and '2023-10-30 00:10:00' >  date_add(l_commitdate, INTERVAL 2 DAY)
            group by
              l_shipmode;
            """
    order_qt_query3_5_before "${query3_5}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    async_mv_rewrite_fail(db, mv3_5, query3_5, "mv3_5")
    order_qt_query3_5_after "${query3_5}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_5"""

    // some partition is invalid, should compensate partition
    create_async_partition_mv(db, "mv4_0", """
    select
    l_shipmode,
    date_trunc(l_shipdate, 'day') as day_trunc,
    count(*)
    from
    lineitem
    group by
    l_shipmode,
    date_trunc(l_shipdate, 'day');
    """, "(day_trunc)")


    create_async_partition_mv(db, "mv4_1", """
    select
    l_shipmode,
    date_trunc(l_shipdate, 'day') as day_trunc
    from
    lineitem;
    """, "(day_trunc)")

    sql """
    insert into lineitem
    values
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-01-01', '2023-01-01 00:00:00.999999', '2023-01-01 00:00:00.999999', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-01-01', '2023-01-01 02:01:45', '2023-01-01 02:01:45', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-01-01', '2023-01-01 03:02:43', '2023-01-01 03:02:43', '2023-10-17', 'a', 'b', 'yyyyyyyyy');
    """

    def query4_0 = """
    select
    l_shipmode,
    date_trunc(l_shipdate, 'day') as day_trunc,
    count(*)
    from
    lineitem
    where l_shipdate >= '2023-01-01' and l_shipdate < '2023-05-01'
    group by
    l_shipmode,
    date_trunc(l_shipdate, 'day');
    """
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    mv_rewrite_success(query4_0, "mv4_0", is_partition_statistics_ready(db, ["lineitem", "mv4_0"]))
    order_qt_query4_0_after "${query4_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv4_0"""


    def query4_1 = """
    select
    l_shipmode,
    date_trunc(l_shipdate, 'day') as day_trunc
    from
    lineitem
    where l_shipdate >= '2023-01-01' and l_shipdate < '2023-05-01';
    """
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    mv_rewrite_success_without_check_chosen(query4_1, "mv4_1")
    order_qt_query4_1_after "${query4_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv4_1"""




    // mv def
    def mv5_0 = """
    select
    l_shipmode,
    date_trunc(l_commitdate_var, 'day') as day_trunc,
    count(*)
    from
    lineitem
    group by
    l_shipmode,
    date_trunc(l_commitdate_var, 'day');
    """
    def query5_0 = """
    select
    l_shipmode,
    date_trunc(l_commitdate_var, 'day'),
    count(*)
    from
    lineitem
    where
    l_commitdate_var >= cast('2023-10-17' as datetime) and l_commitdate_var < cast('2023-10-18' as datetime)
    group by
    l_shipmode,
    date_trunc(l_commitdate_var, 'day');
    """
    order_qt_query5_0_before "${query5_0}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    // should success because as datetime would be datetime(6)
    async_mv_rewrite_success(db, mv5_0, query5_0, "mv5_0")
    order_qt_query5_0_after "${query5_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv5_0"""


    def mv5_1 = """
    select
    l_shipmode,
    date_trunc(l_commitdate_var, 'day') as day_trunc,
    count(*)
    from
    lineitem
    group by
    l_shipmode,
    date_trunc(l_commitdate_var, 'day');
    """
    def query5_1 = """
    select
    l_shipmode,
    date_trunc(l_commitdate_var, 'day'),
    count(*)
    from
    lineitem
    where
    l_commitdate_var >= cast('2023-10-17' as date) and l_commitdate_var < cast('2023-10-18' as date)
    group by
    l_shipmode,
    date_trunc(l_commitdate_var, 'day');
    """
    order_qt_query5_1_before "${query5_1}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    // fail because as datetime would be datetime(0), but mv is datetime(6)
    async_mv_rewrite_fail(db, mv5_1, query5_1, "mv5_1")
    order_qt_query5_1_after "${query5_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv5_1"""


    def mv5_3 = """
    select
    l_shipmode,
    date_trunc(l_commitdate_var, 'minute') as day_trunc,
    count(*)
    from
    lineitem
    group by
    l_shipmode,
    date_trunc(l_commitdate_var, 'minute');
    """
    def query5_3 = """
    select
    l_shipmode,
    date_trunc(l_commitdate_var, 'minute'),
    count(*)
    from
    lineitem
    where
    l_commitdate_var >= cast('2023-10-01' as datetime) and l_commitdate_var < cast('2023-10-19' as datetime)
    group by
    l_shipmode,
    date_trunc(l_commitdate_var, 'minute');
    """
    order_qt_query5_3_before "${query5_3}"
    result = sql """show table stats lineitem"""
    logger.info("lineitem table stats: " + result)
    result = sql """show index stats lineitem lineitem"""
    logger.info("lineitem index stats: " + result)
    // data is not valid
    async_mv_rewrite_success(db, mv5_3, query5_3, "mv5_3")
    order_qt_query5_3_after "${query5_3}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv5_3"""
}

