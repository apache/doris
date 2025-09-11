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
    sql "set runtime_filter_mode=OFF"
    sql "set be_number_for_test=1;"

    for (int i = 0; i < 2; i++) {
        if (i == 0) {
            // not have statistic
            sql """drop stats t_gbykey_10_dstkey_10_1000_id"""
        } else {
            // have statistic
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
        //final multi_distinct + sum0
        qt_agg_distinct_without_gby_key_satisfy_dst_key "explain shape plan select count(distinct id) from t_gbykey_10_dstkey_10_1000_id"
        qt_agg_distinct_without_gby_key_with_other_func "explain shape plan select count(distinct dst_key1),sum(dst_key1) from t_gbykey_10_dstkey_10_1000_id"
        qt_agg_distinct_without_gby_key_satisfy_dst_key_with_other_func "explain shape plan select count(distinct id),avg(dst_key1) from t_gbykey_10_dstkey_10_1000_id"
    }

    // count(distinct a,b)
    qt_count_multi_expr "select count(distinct id, dst_key1) from t_gbykey_10_dstkey_10_1000_id group by dst_key2 order by 1;"
    qt_count_multi_expr_multi_count "select count(distinct id, dst_key1), count(distinct id, dst_key2) from t_gbykey_10_dstkey_10_1000_id group by dst_key2 order by 1,2;"

    for (int i = 0; i < 2; i++) {
        if (i == 0) {
            // not have statistic
            sql """drop stats t_gbykey_2_dstkey_10_30_id"""
        } else {
            // have statistic
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
        //use final multi_distinct + sum0
        qt_agg_distinct_without_gby_key_satisfy_dst_key_low_ndv "explain shape plan select count(distinct id) from t_gbykey_2_dstkey_10_30_id"
        qt_agg_distinct_without_gby_key_with_other_func_low_ndv "explain shape plan select count(distinct dst_key1),sum(dst_key1) from t_gbykey_2_dstkey_10_30_id"
        qt_agg_distinct_without_gby_key_satisfy_dst_key_with_other_func_low_ndv "explain shape plan select count(distinct id),avg(dst_key1) from t_gbykey_2_dstkey_10_30_id"
    }

    // test count has multi children count(a,b)
    qt_with_gby_split_in_rewrite "select count(distinct dst_key1,dst_key2) from t_gbykey_10_dstkey_10_1000_id group by gby_key order by 1;"
    qt_with_gby_split_in_cascades "select count(distinct dst_key1,dst_key2),avg(dst_key2) from t_gbykey_10_dstkey_10_1000_id group by gby_key order by 1,2;"
    qt_without_gby "select count(distinct dst_key1,dst_key2) from t_gbykey_10_dstkey_10_1000_id;"
    qt_without_gby_satisfy "select count(distinct id,dst_key1) from t_gbykey_10_dstkey_10_1000_id;"

    qt_group_concat_with_order_by_without_gby_with_distinct "select group_concat(distinct dst_key1 order by id separator ',') FROM t_gbykey_10_dstkey_10_1000_id;"
    qt_group_concat_with_order_by_with_gby_with_distinct "select group_concat(distinct dst_key1 order by id separator ',') FROM t_gbykey_10_dstkey_10_1000_id group by gby_key order by 1;"
    qt_group_concat_with_order_by_without_gby "select group_concat(dst_key1 order by id separator ',') FROM t_gbykey_10_dstkey_10_1000_id;"
    qt_group_concat_with_order_by_with_gby "select group_concat(dst_key1 order by id separator ',') FROM t_gbykey_10_dstkey_10_1000_id group by gby_key order by 1;"
    qt_group_concat_with_multi_order_by_without_gby_with_distinct "select group_concat(distinct dst_key1 order by id,dst_key2 separator ',') FROM t_gbykey_10_dstkey_10_1000_id;"
    qt_group_concat_with_multi_order_by_with_gby_with_distinct "select group_concat(distinct dst_key1 order by id,dst_key2 separator ',') FROM t_gbykey_10_dstkey_10_1000_id group by gby_key order by 1;"

    // final multi_distinct + sum0
    qt_final_multi_distinct_sum0_count "select count(distinct id), max(gby_key),min(gby_key),sum(gby_key),sum0(gby_key),count(gby_key) from t_gbykey_10_dstkey_10_1000_id;"
    qt_final_multi_distinct_sum0_sum "select sum(distinct id), max(gby_key),min(gby_key),sum(gby_key),sum0(gby_key),count(gby_key) from t_gbykey_10_dstkey_10_1000_id;"
    qt_final_multi_distinct_sum0_sum0 "select sum0(distinct id), max(gby_key),min(gby_key),sum(gby_key),sum0(gby_key),count(gby_key) from t_gbykey_10_dstkey_10_1000_id;"

    qt_agg_recieve_hash_request_shape """
    explain shape plan 
    select l_shipdate, l_orderkey, O_ORDERDATE,
            count(orders_left.O_ORDERDATE) over (partition by lineitem_left.L_SHIPDATE order by lineitem_left.L_ORDERKEY) as window_count
    from lineitem_left
    left join orders_left
    on lineitem_left.l_orderkey = orders_left.o_orderkey
    group by l_shipdate, l_orderkey, O_ORDERDATE;"""

    qt_agg_recieve_hash_request """select l_shipdate, l_orderkey, O_ORDERDATE,
            count(orders_left.O_ORDERDATE) over (partition by lineitem_left.L_SHIPDATE order by lineitem_left.L_ORDERKEY) as window_count
    from lineitem_left
    left join orders_left
    on lineitem_left.l_orderkey = orders_left.o_orderkey
    group by l_shipdate, l_orderkey, O_ORDERDATE;"""

    qt_group_concat_distinct_key_is_varchar_and_distribute_key """explain shape plan
    select group_concat(distinct dst_key1 ,' ') from t_gbykey_10_dstkey_10_1000_dst_key1;"""
}