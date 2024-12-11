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
            l_commitdate DATETIME not null,
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
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-01-01', '2023-01-01 00:00:01', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-01-01', '2023-01-01 02:01:45', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-01-01', '2023-01-01 03:02:43', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-04-01', '2023-04-01 05:03:42', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-04-01', '2023-04-01 07:04:40', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-04-01', '2023-04-01 09:05:36', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-07-01', '2023-07-01 11:06:35', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-07-01', '2023-07-01 13:07:34', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-07-01', '2023-07-01 15:08:30', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-01', '2023-10-01 17:09:25', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-01', '2023-10-01 19:10:24', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-01', '2023-10-01 21:11:23', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-15', '2023-10-15 16:12:22', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-15', '2023-10-15 00:05:12', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-15', '2023-10-15 23:59:59', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-16', '2023-10-16 00:00:00', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-16', '2023-10-16 00:05:03', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-16', '2023-10-16 23:59:59', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17 00:00:00', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17 00:05:05', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17 23:59:59', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (2, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k','2023-10-18', '2023-10-18 00:00:00','2023-10-18', 'a', 'b', 'yyyyyyyyy'),
    (2, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k','2023-10-18', '2023-10-18 00:05:12','2023-10-18', 'a', 'b', 'yyyyyyyyy'),
    (2, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k','2023-10-18', '2023-10-18 23:59:59','2023-10-18', 'a', 'b', 'yyyyyyyyy'),
    (3, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o','2023-10-19', '2023-10-19 00:00:00','2023-10-19', 'c', 'd', 'xxxxxxxxx'),
    (3, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o','2023-10-19', '2023-10-19 00:05:15','2023-10-19', 'c', 'd', 'xxxxxxxxx'),
    (3, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o','2023-10-19', '2023-10-19 23:59:59','2023-10-19', 'c', 'd', 'xxxxxxxxx');
    """

    sql """analyze table lineitem with sync;"""
    sql """alter table lineitem modify column l_comment set stats ('row_count'='9');"""

    // second, minute ,hour, day, week, month, quarter, year
    // 1. expr in date_trunc is simple col
    // 1.1 positive case, use field data type is datetime
    // day
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
    async_mv_rewrite_fail(db, mv3_5, query3_5, "mv3_5")
    order_qt_query3_5_after "${query3_5}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_5"""

}
