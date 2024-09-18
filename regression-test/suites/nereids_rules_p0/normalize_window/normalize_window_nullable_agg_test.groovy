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
suite("normalize_window_nullable_agg") {
    sql "drop table if exists normalize_window_nullable_agg"
    sql """create table normalize_window_nullable_agg (a int, b int,c int,d array<int>) distributed by hash(a)
        properties("replication_num"="1");
         """
    sql """insert into normalize_window_nullable_agg values(1,2,1,[1,2]),(1,3,2,[3,2]),(2,3,3,[1,5]),(2,2,4,[3,2]),(2,5,5,[5,2])
    ,(2,3,6,[1,2]),(2,5,7,[1,2]),(null,3,8,[1,23]),(null,6,9,[3,2]);"""
    qt_max "select max(b) over(order by c rows between 2 preceding and 1 preceding) from normalize_window_nullable_agg"
    qt_min "select min(b) over(order by c rows between 2 preceding and 1 preceding) from normalize_window_nullable_agg"
    qt_sum "select sum(b) over(order by c rows between 2 preceding and 1 preceding) from normalize_window_nullable_agg"
    qt_avg "select avg(b) over(order by c rows between 2 preceding and 1 preceding) from normalize_window_nullable_agg"
    qt_topn "select topn(c,3) over(order by c rows between 2 preceding and 1 preceding) from normalize_window_nullable_agg"
    qt_topn_array "select topn_array(c,3) over(order by c rows between 2 preceding and 1 preceding) from normalize_window_nullable_agg"
    qt_topn_weighted "select topn_weighted(c,c,3) over(order by c rows between 2 preceding and 1 preceding) from normalize_window_nullable_agg"
    qt_max_by "select max_by(b,c) over(order by c rows between 2 preceding and 1 preceding) from normalize_window_nullable_agg"
    qt_min_by "select min_by(b,c) over(order by c rows between 2 preceding and 1 preceding) from normalize_window_nullable_agg"
    qt_avg_weighted "select avg_weighted(b,a) over(order by c rows between 2 preceding and 1 preceding) from normalize_window_nullable_agg"
    qt_variance "select variance(b) over(order by c rows between 2 preceding and 1 preceding) from normalize_window_nullable_agg"
    qt_variance_samp "select variance_samp(b) over(order by c rows between 2 preceding and 1 preceding) from normalize_window_nullable_agg"
    qt_percentile "select PERCENTILE(b,0.5) over(order by c rows between 2 preceding and 1 preceding) from normalize_window_nullable_agg"
    qt_percentile_approx "select PERCENTILE_approx(b,0.99) over(order by c rows between 2 preceding and 1 preceding) from normalize_window_nullable_agg"
    qt_stddev "select stddev(b) over(order by c rows between 2 preceding and 1 preceding) from normalize_window_nullable_agg"
    qt_stddev_samp "select stddev_samp(b) over(order by c rows between 2 preceding and 1 preceding) from normalize_window_nullable_agg"
    qt_corr "select corr(a,b) over(order by c rows between 2 preceding and 1 preceding) from normalize_window_nullable_agg"
    qt_covar "select covar(a,b) over(order by c rows between 2 preceding and 1 preceding) from normalize_window_nullable_agg"
    qt_covar_samp "select covar_samp(a,b) over(order by c rows between 2 preceding and 1 preceding) from normalize_window_nullable_agg"
    qt_group_concat "select group_concat(cast(a as varchar(10)),',') over(order by c rows between 2 preceding and 1 preceding) from normalize_window_nullable_agg"
    qt_retention "select retention(a=1,b>2) over(order by c rows between 2 preceding and 1 preceding) from normalize_window_nullable_agg"
    qt_group_bit_and "select group_bit_and(a) over(order by c rows between 2 preceding and 1 preceding) from normalize_window_nullable_agg"
    qt_group_bit_or "select group_bit_or(a) over(order by c rows between 2 preceding and 1 preceding) from normalize_window_nullable_agg"
    qt_group_bit_xor "select group_bit_xor(a) over(order by c rows between 2 preceding and 1 preceding) from normalize_window_nullable_agg"
    qt_group_bitmap_xor "select group_bitmap_xor(to_bitmap(a)) over(order by c rows between 2 preceding and 1 preceding) from normalize_window_nullable_agg"
    qt_sum_foreach "select sum_foreach(d) over(order by c rows between 2 preceding and 1 preceding) from normalize_window_nullable_agg"

    sql "drop table if exists windowfunnel_test_normalize_window"
    sql """CREATE TABLE windowfunnel_test_normalize_window (
            `xwho` varchar(50) NULL COMMENT 'xwho',
            `xwhen` datetime COMMENT 'xwhen',
            `xwhat` int NULL COMMENT 'xwhat'
    )
    DUPLICATE KEY(xwho)
    DISTRIBUTED BY HASH(xwho) BUCKETS 3
    PROPERTIES (
            "replication_num" = "1"
    );"""

    sql """INSERT into windowfunnel_test_normalize_window (xwho, xwhen, xwhat) values ('1', '2022-03-12 10:41:00', 1),
    ('1', '2022-03-12 13:28:02', 2),
    ('1', '2022-03-12 16:15:01', 3),
    ('1', '2022-03-12 19:05:04', 4);"""
    //这个目前会core
//    qt_window_funnel """select window_funnel(3600 * 3, 'default', t.xwhen, t.xwhat = 1, t.xwhat = 2 ) over (order by xwhat rows
//    between 2 preceding and 1 preceding) AS level from windowfunnel_test_normalize_window t;"""
    qt_sequence_match "SELECT sequence_match('(?1)(?2)', xwhen, xwhat = 1, xwhat = 3) over (order by xwhat rows between 2 preceding and 1 preceding) FROM windowfunnel_test_normalize_window;"
}