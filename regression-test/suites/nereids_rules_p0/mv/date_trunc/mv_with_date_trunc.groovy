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
            ('2023-10-16') TO ('2023-11-30') INTERVAL 1 DAY
    ) DISTRIBUTED BY HASH(l_orderkey) BUCKETS 3 PROPERTIES ("replication_num" = "1");
    """


    sql """
    insert into lineitem
    values
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k',
    '2023-10-17', '2023-10-17 00:10:00',
    '2023-10-17', 'a', 'b', 'yyyyyyyyy'
    ),
    (2, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k',
    '2023-10-18', '2023-10-18 08:10:00',
    '2023-10-18', 'a', 'b', 'yyyyyyyyy'
    ),
    (3, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o',
    '2023-10-19', '2023-10-19 12:10:00',
    '2023-10-19', 'c', 'd', 'xxxxxxxxx'
    );
    """

    def mv1_0 = """
            select 
              l_shipmode, 
              date_trunc(l_shipdate, 'day') as day_trunc, 
              count(*) 
            from 
              lineitem 
            group by 
              l_shipmode, 
              date_trunc(l_shipdate, 'day');
            """
    def query1_0 = """
            select 
              l_shipmode, 
              count(*) 
            from 
              lineitem 
            where 
              l_shipdate >= '2023-10-17' and  l_shipdate < '2023-10-18'
            group by 
              l_shipmode;
            """
    order_qt_query1_0_before "${query1_0}"
    async_mv_rewrite_success(db, mv1_0, query1_0, "mv1_0")
    order_qt_query1_0_after "${query1_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_0"""



//    def mv1_0 = """
//            select
//              l_shipmode,
//              date_trunc(l_shipdate, 'day') as day_trunc,
//              count(*)
//            from
//              lineitem
//            group by
//              l_shipmode,
//              date_trunc(l_shipdate, 'day');
//            """
//    def query1_0 = """
//            select
//              l_shipmode,
//              count(*)
//            from
//              lineitem
//            where
//              l_shipdate >= '2023-10-17' and  l_shipdate < '2023-10-18'
//            group by
//              l_shipmode;
//            """
//    order_qt_query1_0_before "${query1_0}"
//    async_mv_rewrite_success(db, mv1_0, query1_0, "mv1_0")
//    order_qt_query1_0_after "${query1_0}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_0"""

}
