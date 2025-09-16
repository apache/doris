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

suite("agg_strategy_variable") {
    sql "SET ignore_shape_nodes='PhysicalProject'"
    sql "set enable_parallel_result_sink=false"
    sql "set global enable_auto_analyze=false"
    sql "set runtime_filter_mode=OFF"
    sql "set be_number_for_test=1;"

    sql "drop table if exists t_gbykey_10_dstkey_10_1000_id_2"
    sql """create table t_gbykey_10_dstkey_10_1000_id_2(id int, gby_key int, dst_key1 int, dst_key2 int) duplicate key(id) distributed by hash(id)
    buckets 32 properties('replication_num' = '1');"""
    sql """INSERT INTO t_gbykey_10_dstkey_10_1000_id_2 VALUES
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

    // test variable agg_phase
    sql "set agg_phase=1;"
    qt_agg_phase_1 """explain shape plan
    select max(gby_key),min(gby_key),sum(gby_key),sum0(gby_key),count(gby_key) from t_gbykey_10_dstkey_10_1000_id_2 group by gby_key"""
    sql "set agg_phase=2;"
    qt_agg_phase_2 """explain shape plan
    select max(gby_key),min(gby_key),sum(gby_key),sum0(gby_key),count(gby_key) from t_gbykey_10_dstkey_10_1000_id_2 group by gby_key"""

    sql "set agg_phase=3;"
    qt_agg_phase_3 """explain shape plan
    select count(distinct dst_key2), max(gby_key),min(gby_key),sum(gby_key),sum0(gby_key),count(gby_key) from t_gbykey_10_dstkey_10_1000_id_2 group by gby_key;"""
    sql "set agg_phase=4;"
    qt_agg_phase_4 """explain shape plan
    select count(distinct dst_key2), max(gby_key),min(gby_key),sum(gby_key),sum0(gby_key),count(gby_key) from t_gbykey_10_dstkey_10_1000_id_2 group by gby_key;"""

    sql "set agg_phase=1;"
    qt_distinct_phase_1_shape """explain shape plan
    select count(distinct dst_key1) from t_gbykey_10_dstkey_10_1000_id_2 group by gby_key;"""
    qt_distinct_phase_1 "select count(distinct dst_key1) from t_gbykey_10_dstkey_10_1000_id_2 group by gby_key order by 1;"

    sql "set agg_phase=2;"
    qt_distinct_phase_2_shape """explain shape plan
    select count(distinct dst_key1) from t_gbykey_10_dstkey_10_1000_id_2 group by gby_key;"""
    qt_distinct_phase_2 "select count(distinct dst_key1) from t_gbykey_10_dstkey_10_1000_id_2 group by gby_key order by 1;"

    sql "set agg_phase=1;"
    qt_distinct_phase_1_satisfy_gby_key_shape """explain shape plan
    select count(distinct dst_key1) from t_gbykey_10_dstkey_10_1000_id_2 group by id;"""
    qt_distinct_phase_1_satisfy_gby_key"""select count(distinct dst_key1) from t_gbykey_10_dstkey_10_1000_id_2 group by id order by 1;"""

    sql "set agg_phase=2;"
    qt_distinct_phase_2_satisfy_gby_key_shape """explain shape plan
    select count(distinct dst_key1) from t_gbykey_10_dstkey_10_1000_id_2 group by id;"""
    qt_distinct_phase_2_satisfy_gby_key "select count(distinct dst_key1) from t_gbykey_10_dstkey_10_1000_id_2 group by id order by 1;"

    sql "set agg_phase=1;"
    qt_distinct_and_other_phase_1_shape """explain shape plan
    select count(distinct dst_key1), max(dst_key2) from t_gbykey_10_dstkey_10_1000_id_2 group by gby_key;"""
    qt_distinct_and_other_phase_1 "select count(distinct dst_key1), max(dst_key2) from t_gbykey_10_dstkey_10_1000_id_2 group by gby_key order by 1,2;"

    sql "set agg_phase=2;"
    qt_distinct_and_other_phase_2_shape """explain shape plan
    select count(distinct dst_key1), max(dst_key2) from t_gbykey_10_dstkey_10_1000_id_2 group by gby_key;"""
    qt_distinct_and_other_phase_2 "select count(distinct dst_key1), max(dst_key2) from t_gbykey_10_dstkey_10_1000_id_2 group by gby_key order by 1,2;"

    sql "set agg_phase=1;"
    qt_distinct_and_other_phase_1_satisfy_gby_key_shape """explain shape plan
    select count(distinct dst_key1), max(dst_key2) from t_gbykey_10_dstkey_10_1000_id_2 group by id;"""
    qt_distinct_and_other_phase_1_satisfy_gby_key "select count(distinct dst_key1), max(dst_key2) from t_gbykey_10_dstkey_10_1000_id_2 group by id order by 1,2;"

    sql "set agg_phase=2;"
    qt_distinct_and_other_phase_2_satisfy_gby_key_shape """explain shape plan
    select count(distinct dst_key1), max(dst_key2) from t_gbykey_10_dstkey_10_1000_id_2 group by id;"""
    qt_distinct_and_other_phase_2_satisfy_gby_key "select count(distinct dst_key1), max(dst_key2) from t_gbykey_10_dstkey_10_1000_id_2 group by id order by 1,2;"

    // test without group by key
    sql "set agg_phase=3;"
    qt_use_multi_phase_distinct_key_satisfy """explain shape plan
    select count(distinct id), max(dst_key2) from t_gbykey_10_dstkey_10_1000_id_2;"""

    sql "set agg_phase=1;"
    qt_distinct_key_not_satisfy_use_final_multi """explain shape plan
    select count(distinct dst_key1), max(dst_key2) from t_gbykey_10_dstkey_10_1000_id_2;"""
    sql "set agg_phase=0;"

    // test variable multi_distinct_strategy
    sql "set multi_distinct_strategy=1;"
    qt_multi_distinct_strategy_1 """explain shape plan
    select count(distinct dst_key2), count(distinct dst_key1), max(gby_key),min(gby_key),sum(gby_key),sum0(gby_key),count(gby_key) from t_gbykey_10_dstkey_10_1000_id_2 group by gby_key;"""
    sql "set multi_distinct_strategy=2;"
    qt_multi_distinct_strategy_2 """explain shape plan
    select count(distinct dst_key2), count(distinct dst_key1), max(gby_key),min(gby_key),sum(gby_key),sum0(gby_key),count(gby_key) from t_gbykey_10_dstkey_10_1000_id_2 group by gby_key;"""
    sql "set multi_distinct_strategy=0;"

    sql "set agg_phase=4;"
    sql "set multi_distinct_strategy=2;"
    qt_agg_phase4_and_multi_distinct_strategy2 """explain shape plan
    select count(distinct dst_key2), count(distinct dst_key1), max(gby_key),min(gby_key),sum(gby_key),sum0(gby_key),count(gby_key) from t_gbykey_10_dstkey_10_1000_id_2 group by gby_key;"""
    sql "set agg_phase=0;"
    sql "set multi_distinct_strategy=0;"

    //test exception
    test {
        sql "set agg_phase=5;"
        exception "agg_phase should be between 0 and 4"
    }
    test {
        sql "set multi_distinct_strategy=-1;"
        exception "multi_distinct_strategy should be between 0 and 2"
    }
}