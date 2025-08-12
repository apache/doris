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

suite("agg_strategy") {
    sql "SET ignore_shape_nodes='PhysicalProject'"
    sql "set enable_parallel_result_sink=false"
    sql "set global enable_auto_analyze=false"

    // ndv is high
    sql "drop table if exists t_gbykey_10_dstkey_10_1000_id"
    sql """create table t_gbykey_10_dstkey_10_1000_id(id int, gby_key int, dst_key1 int, dst_key2 int) duplicate key(id) distributed by hash(id)
    buckets 32 properties('replication_num' = '1');"""
    sql """INSERT INTO t_gbykey_10_dstkey_10_1000_id VALUES
        (1, 3, 7, 42),
        (2, 5, 9, 18),
        (3, 2, 4, 76),
        (4, 8, 1, 33),
        (5, 6, 3, 91),
        (6, 1, 5, 27),
        (7, 4, 8, 64),
        (8, 9, 2, 55),
        (9, 7, 6, 13),
        (10, 10, 10, 100);"""
    for (int i = 0; i < 2; i++) {
        if (i == 0) {
            // 无统计信息
            sql """drop stats t_gbykey_10_dstkey_10_1000_id"""
        } else {
            // 有统计信息
            sql """analyze table t_gbykey_10_dstkey_10_1000_id with sync;"""
        }
        qt_non_agg_func "select gby_key from t_gbykey_10_dstkey_10_1000_id group by gby_key order by 1"
        qt_agg_func "select count(dst_key1), gby_key from t_gbykey_10_dstkey_10_1000_id group by gby_key order by 1,2"
        qt_agg_distinct_with_gby_key "select count(distinct dst_key1), gby_key from t_gbykey_10_dstkey_10_1000_id group by gby_key order by 1,2"
        qt_agg_distinct_satisfy_gby_key "select count(distinct dst_key1), id from t_gbykey_10_dstkey_10_1000_id group by id order by 1,2"
        qt_agg_distinct_satisfy_dst_key "select count(distinct id) from t_gbykey_10_dstkey_10_1000_id group by gby_key order by 1"
        qt_agg_distinct_with_gby_key_with_other_func "select count(distinct dst_key1), gby_key, sum(dst_key2), avg(dst_key2) from t_gbykey_10_dstkey_10_1000_id group by gby_key order by 1,2,3,4"
        qt_agg_distinct_satisfy_gby_key_with_other_func "select count(distinct dst_key1), id, sum(dst_key2), avg(dst_key2) from t_gbykey_10_dstkey_10_1000_id group by id order by 1,2,3,4"
        qt_agg_distinct_satisfy_dst_key_with_other_func "select count(distinct id), sum(dst_key2), avg(dst_key2) from t_gbykey_10_dstkey_10_1000_id group by gby_key order by 1,2,3"

        qt_agg_distinct_without_gby_key "select count(distinct dst_key1) from t_gbykey_10_dstkey_10_1000_id"
        qt_agg_distinct_without_gby_key_satisfy_dst_key "select count(distinct id) from t_gbykey_10_dstkey_10_1000_id"
        qt_agg_distinct_without_gby_key_with_other_func "select count(distinct dst_key1),sum(dst_key1) from t_gbykey_10_dstkey_10_1000_id"
        qt_agg_distinct_without_gby_key_satisfy_dst_key_with_other_func "select count(distinct id),avg(dst_key1) from t_gbykey_10_dstkey_10_1000_id"

        //shape
        qt_non_agg_func "explain shape plan select gby_key from t_gbykey_10_dstkey_10_1000_id group by gby_key order by 1"
        qt_agg_func "explain shape plan select count(dst_key1), gby_key from t_gbykey_10_dstkey_10_1000_id group by gby_key order by 1,2"
        qt_agg_distinct_with_gby_key "explain shape plan select count(distinct dst_key1), gby_key from t_gbykey_10_dstkey_10_1000_id group by gby_key order by 1,2"
        qt_agg_distinct_satisfy_gby_key "explain shape plan select count(distinct dst_key1), id from t_gbykey_10_dstkey_10_1000_id group by id order by 1,2"
        qt_agg_distinct_satisfy_dst_key "explain shape plan select count(distinct id) from t_gbykey_10_dstkey_10_1000_id group by gby_key order by 1"
        qt_agg_distinct_with_gby_key_with_other_func "explain shape plan select count(distinct dst_key1), gby_key, sum(dst_key2), avg(dst_key2) from t_gbykey_10_dstkey_10_1000_id group by gby_key order by 1,2,3,4"
        qt_agg_distinct_satisfy_gby_key_with_other_func "explain shape plan select count(distinct dst_key1), id, sum(dst_key2), avg(dst_key2) from t_gbykey_10_dstkey_10_1000_id group by id order by 1,2,3,4"
        qt_agg_distinct_satisfy_dst_key_with_other_func "explain shape plan select count(distinct id), sum(dst_key2), avg(dst_key2) from t_gbykey_10_dstkey_10_1000_id group by gby_key order by 1,2,3"

        qt_agg_distinct_without_gby_key "explain shape plan select count(distinct dst_key1) from t_gbykey_10_dstkey_10_1000_id"
        //这个使用的是final multi_distinct + sum0
        qt_agg_distinct_without_gby_key_satisfy_dst_key "explain shape plan select count(distinct id) from t_gbykey_10_dstkey_10_1000_id"
        qt_agg_distinct_without_gby_key_with_other_func "explain shape plan select count(distinct dst_key1),sum(dst_key1) from t_gbykey_10_dstkey_10_1000_id"
        qt_agg_distinct_without_gby_key_satisfy_dst_key_with_other_func "explain shape plan select count(distinct id),avg(dst_key1) from t_gbykey_10_dstkey_10_1000_id"
    }

    // count(distinct a,b)
    qt_count_multi_expr "select count(distinct id, dst_key1) from t_gbykey_10_dstkey_10_1000_id group by dst_key2 order by 1;"
    qt_count_multi_expr_multi_count "select count(distinct id, dst_key1), count(distinct id, dst_key2) from t_gbykey_10_dstkey_10_1000_id group by dst_key2 order by 1,2;"

    // ndv is low
    sql "drop table if exists t_gbykey_2_dstkey_10_30_id"
    sql """create table t_gbykey_2_dstkey_10_30_id(id int, gby_key int, dst_key1 int, dst_key2 int) duplicate key(id) distributed by hash(id)
        buckets 32 properties('replication_num' = '1');"""
    sql """
        INSERT INTO t_gbykey_2_dstkey_10_30_id (id, gby_key, dst_key1, dst_key2) VALUES 
        (0, 0, 0, 0),(1, 1, 1, 1),(0, 0, 0, 2),(1, 1, 1, 3),(0, 0, 0, 4),(1, 1, 1, 5),(0, 0, 0, 6),
        (1, 1, 1, 7),(0, 0, 0, 8),(1, 1, 1, 9),(0, 0, 1, 0),(1, 1, 0, 1),(0, 0, 1, 2),(0, 1, 0, 3),
        (1, 0, 1, 4),(0, 1, 0, 5),(1, 0, 1, 6),(0, 1, 0, 7),(1, 0, 1, 8),(0, 1, 0, 9),(1, 0, 0, 0),
        (0, 1, 1, 1),(1, 0, 0, 2),(0, 1, 1, 3),(1, 0, 0, 4),(0, 1, 1, 5),(0, 0, 0, 6),(1, 1, 1, 7),
        (0, 0, 0, 8),(1, 1, 1, 9);"""

    for (int i = 0; i < 2; i++) {
        if (i == 0) {
            // 无统计信息
            sql """drop stats t_gbykey_2_dstkey_10_30_id"""
        } else {
            // 有统计信息
            sql """analyze table t_gbykey_2_dstkey_10_30_id with sync;"""
        }
        qt_non_agg_func_low_ndv "select gby_key from t_gbykey_2_dstkey_10_30_id group by gby_key order by 1"
        qt_agg_func_low_ndv "select count(dst_key1), gby_key from t_gbykey_2_dstkey_10_30_id group by gby_key order by 1,2"
        qt_agg_distinct_with_gby_key_low_ndv "select count(distinct dst_key1), gby_key from t_gbykey_2_dstkey_10_30_id group by gby_key order by 1,2"
        qt_agg_distinct_satisfy_gby_key_low_ndv "select count(distinct dst_key1), id from t_gbykey_2_dstkey_10_30_id group by id order by 1,2"
        qt_agg_distinct_satisfy_dst_key_low_ndv "select count(distinct id) from t_gbykey_2_dstkey_10_30_id group by gby_key order by 1"
        qt_agg_distinct_with_gby_key_with_other_func_low_ndv "select count(distinct dst_key1), gby_key, sum(dst_key2), avg(dst_key2) from t_gbykey_2_dstkey_10_30_id group by gby_key order by 1,2,3,4"
        qt_agg_distinct_satisfy_gby_key_with_other_func_low_ndv "select count(distinct dst_key1), id, sum(dst_key2), avg(dst_key2) from t_gbykey_2_dstkey_10_30_id group by id order by 1,2,3,4"
        qt_agg_distinct_satisfy_dst_key_with_other_func_low_ndv "select count(distinct id), sum(dst_key2), avg(dst_key2) from t_gbykey_2_dstkey_10_30_id group by gby_key order by 1,2,3"

        qt_agg_distinct_without_gby_key_low_ndv "select count(distinct dst_key1) from t_gbykey_2_dstkey_10_30_id"
        qt_agg_distinct_without_gby_key_satisfy_dst_key_low_ndv "select count(distinct id) from t_gbykey_2_dstkey_10_30_id"
        qt_agg_distinct_without_gby_key_with_other_func_low_ndv "select count(distinct dst_key1),sum(dst_key1) from t_gbykey_2_dstkey_10_30_id"
        qt_agg_distinct_without_gby_key_satisfy_dst_key_with_other_func_low_ndv "select count(distinct id),avg(dst_key1) from t_gbykey_2_dstkey_10_30_id"

        //shape
        qt_non_agg_func_low_ndv "explain shape plan select gby_key from t_gbykey_2_dstkey_10_30_id group by gby_key order by 1"
        qt_agg_func_low_ndv "explain shape plan select count(dst_key1), gby_key from t_gbykey_2_dstkey_10_30_id group by gby_key order by 1,2"
        qt_agg_distinct_with_gby_key_low_ndv "explain shape plan select count(distinct dst_key1), gby_key from t_gbykey_2_dstkey_10_30_id group by gby_key order by 1,2"
        qt_agg_distinct_satisfy_gby_key_low_ndv "explain shape plan select count(distinct dst_key1), id from t_gbykey_2_dstkey_10_30_id group by id order by 1,2"
        qt_agg_distinct_satisfy_dst_key_low_ndv "explain shape plan select count(distinct id) from t_gbykey_2_dstkey_10_30_id group by gby_key order by 1"
        qt_agg_distinct_with_gby_key_with_other_func_low_ndv "explain shape plan select count(distinct dst_key1), gby_key, sum(dst_key2), avg(dst_key2) from t_gbykey_2_dstkey_10_30_id group by gby_key order by 1,2,3,4"
        qt_agg_distinct_satisfy_gby_key_with_other_func_low_ndv "explain shape plan select count(distinct dst_key1), id, sum(dst_key2), avg(dst_key2) from t_gbykey_2_dstkey_10_30_id group by id order by 1,2,3,4"
        qt_agg_distinct_satisfy_dst_key_with_other_func_low_ndv"explain shape plan select count(distinct id), sum(dst_key2), avg(dst_key2) from t_gbykey_2_dstkey_10_30_id group by gby_key order by 1,2,3"
        qt_agg_distinct_without_gby_key_low_ndv "explain shape plan select count(distinct dst_key1) from t_gbykey_2_dstkey_10_30_id"
        //这个使用的是final multi_distinct + sum0
        qt_agg_distinct_without_gby_key_satisfy_dst_key_low_ndv "explain shape plan select count(distinct id) from t_gbykey_2_dstkey_10_30_id"
        qt_agg_distinct_without_gby_key_with_other_func_low_ndv "explain shape plan select count(distinct dst_key1),sum(dst_key1) from t_gbykey_2_dstkey_10_30_id"
        qt_agg_distinct_without_gby_key_satisfy_dst_key_with_other_func_low_ndv "explain shape plan select count(distinct id),avg(dst_key1) from t_gbykey_2_dstkey_10_30_id"
    }

    // test count has multi children count(a,b)
    qt_with_gby_split_in_rewrite "select count(distinct dst_key1,dst_key2) from t_gbykey_10_dstkey_10_1000_id group by gby_key order by 1;"
    qt_with_gby_split_in_cascades "select count(distinct dst_key1,dst_key2),avg(dst_key2) from t_gbykey_10_dstkey_10_1000_id group by gby_key order by 1,2;"
    qt_without_gby "select count(distinct dst_key1,dst_key2) from t_gbykey_10_dstkey_10_1000_id;"
    qt_without_gby_satisfy "select count(distinct id,dst_key1) from t_gbykey_10_dstkey_10_1000_id;"
}