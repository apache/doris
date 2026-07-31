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

suite("hash_shuffle_key_prune") {
    multi_sql """
        drop table if exists t1;
        create table t1(a int, b int, c int, d int, e int, f int, g int, h int, i int, j int) properties("replication_num"="1");
        alter table t1 modify column a set stats ('row_count'='204000', 'ndv'='8', 'min_value'='0', 'max_value'='1000', 'avg_size'='880961', 'max_size'='880961','hot_values'='' );
        alter table t1 modify column b set stats ('row_count'='204000', 'ndv'='30', 'min_value'='0', 'max_value'='1000', 'avg_size'='880961', 'max_size'='880961','hot_values'='' );
        alter table t1 modify column c set stats ('row_count'='204000', 'ndv'='100', 'min_value'='0', 'max_value'='1000', 'avg_size'='880961', 'max_size'='880961','hot_values'='' );
        alter table t1 modify column d set stats ('row_count'='204000', 'ndv'='15000', 'min_value'='0', 'max_value'='1000', 'avg_size'='880961', 'max_size'='880961','hot_values'='' );
        alter table t1 modify column e set stats ('row_count'='204000', 'ndv'='20000', 'min_value'='0', 'max_value'='1000', 'avg_size'='880961', 'max_size'='880961','hot_values'='' );
        alter table t1 modify column f set stats ('row_count'='204000', 'ndv'='8', 'min_value'='0', 'max_value'='1000', 'avg_size'='880961', 'max_size'='880961','hot_values'='' );
        alter table t1 modify column g set stats ('row_count'='204000', 'ndv'='30', 'min_value'='0', 'max_value'='1000', 'avg_size'='880961', 'max_size'='880961','hot_values'='' );
        alter table t1 modify column h set stats ('row_count'='204000', 'ndv'='100', 'min_value'='0', 'max_value'='1000', 'avg_size'='880961', 'max_size'='880961','hot_values'='' );
        alter table t1 modify column i set stats ('row_count'='204000', 'ndv'='15000', 'min_value'='0', 'max_value'='1000', 'avg_size'='880961', 'max_size'='880961','hot_values'='' );
        alter table t1 modify column j set stats ('row_count'='204000', 'ndv'='20000', 'min_value'='0', 'max_value'='1000', 'avg_size'='880961', 'max_size'='880961','hot_values'='' );
        
        drop table if exists t2;
        create table t2(a int, b int, c int, d int, e int, f int, g int, h int, i int, j int) distributed by hash(a) properties("replication_num"="1");
        alter table t2 modify column a set stats ('row_count'='204000', 'ndv'='8', 'min_value'='0', 'max_value'='1000', 'avg_size'='880961', 'max_size'='880961','hot_values'='' );
        alter table t2 modify column b set stats ('row_count'='204000', 'ndv'='30', 'min_value'='0', 'max_value'='1000', 'avg_size'='880961', 'max_size'='880961','hot_values'='' );
        alter table t2 modify column c set stats ('row_count'='204000', 'ndv'='100', 'min_value'='0', 'max_value'='1000', 'avg_size'='880961', 'max_size'='880961','hot_values'='' );
        alter table t2 modify column d set stats ('row_count'='204000', 'ndv'='15000', 'min_value'='0', 'max_value'='1000', 'avg_size'='880961', 'max_size'='880961','hot_values'='' );
        alter table t2 modify column e set stats ('row_count'='204000', 'ndv'='20000', 'min_value'='0', 'max_value'='1000', 'avg_size'='880961', 'max_size'='880961','hot_values'='' );
        alter table t2 modify column f set stats ('row_count'='204000', 'ndv'='8', 'min_value'='0', 'max_value'='1000', 'avg_size'='880961', 'max_size'='880961','hot_values'='' );
        alter table t2 modify column g set stats ('row_count'='204000', 'ndv'='30', 'min_value'='0', 'max_value'='1000', 'avg_size'='880961', 'max_size'='880961','hot_values'='' );
        alter table t2 modify column h set stats ('row_count'='204000', 'ndv'='100', 'min_value'='0', 'max_value'='1000', 'avg_size'='880961', 'max_size'='880961','hot_values'='' );
        alter table t2 modify column i set stats ('row_count'='204000', 'ndv'='15000', 'min_value'='0', 'max_value'='1000', 'avg_size'='880961', 'max_size'='880961','hot_values'='' );
        alter table t2 modify column j set stats ('row_count'='204000', 'ndv'='20000', 'min_value'='0', 'max_value'='1000', 'avg_size'='880961', 'max_size'='880961','hot_values'='' );
        
        drop table if exists t3;
        create table t3(a int, b int, c int, d int, e int, f int, g int, h int, i int, j int) distributed by hash(e) properties("replication_num"="1");
        alter table t3 modify column a set stats ('row_count'='204000', 'ndv'='2000', 'min_value'='0', 'max_value'='1000', 'avg_size'='880961', 'max_size'='880961','hot_values'='' );
        alter table t3 modify column b set stats ('row_count'='204000', 'ndv'='30', 'min_value'='0', 'max_value'='1000', 'avg_size'='880961', 'max_size'='880961','hot_values'='' );
        alter table t3 modify column c set stats ('row_count'='204000', 'ndv'='100', 'min_value'='0', 'max_value'='1000', 'avg_size'='880961', 'max_size'='880961','hot_values'='' );
        alter table t3 modify column d set stats ('row_count'='204000', 'ndv'='15000', 'min_value'='0', 'max_value'='1000', 'avg_size'='880961', 'max_size'='880961','hot_values'='' );
        alter table t3 modify column e set stats ('row_count'='204000', 'ndv'='20000', 'min_value'='0', 'max_value'='1000', 'avg_size'='880961', 'max_size'='880961','hot_values'='' );
        alter table t3 modify column f set stats ('row_count'='204000', 'ndv'='8', 'min_value'='0', 'max_value'='1000', 'avg_size'='880961', 'max_size'='880961','hot_values'='' );
        alter table t3 modify column g set stats ('row_count'='204000', 'ndv'='30', 'min_value'='0', 'max_value'='1000', 'avg_size'='880961', 'max_size'='880961','hot_values'='' );
        alter table t3 modify column h set stats ('row_count'='204000', 'ndv'='100', 'min_value'='0', 'max_value'='1000', 'avg_size'='880961', 'max_size'='880961','hot_values'='' );
        alter table t3 modify column i set stats ('row_count'='204000', 'ndv'='15000', 'min_value'='0', 'max_value'='1000', 'avg_size'='880961', 'max_size'='880961','hot_values'='' );
        alter table t3 modify column j set stats ('row_count'='204000', 'ndv'='20000', 'min_value'='0', 'max_value'='1000', 'avg_size'='880961', 'max_size'='880961','hot_values'='' );

        set disable_nereids_rules='prune_empty_partition';
        set enable_shuffle_key_prune=true;
        set detail_shape_nodes='PhysicalDistribute';
        set runtime_filter_mode=OFF;
        set global enable_auto_analyze=false;
        set parallel_pipeline_task_num=4;
    """
    sql """
        insert into t1 values
        (1,1,1,1,1,1,1,1,1,10),
        (1,1,1,1,1,1,1,1,1,20),
        (1,1,1,2,2,2,2,2,2,30),
        (1,2,2,2,2,2,2,2,2,40),
        (2,2,2,2,2,2,2,2,2,50),
        (2,2,3,3,3,3,3,3,3,60),
        (3,3,3,3,3,3,3,3,3,70);
    """
    sql """
        insert into t2 values
        (1,1,1,1,1,1,1,1,1,10),
        (1,1,1,1,1,1,1,1,1,20),
        (1,1,1,2,2,2,2,2,2,30),
        (1,2,2,2,2,2,2,2,2,40),
        (2,2,2,2,2,2,2,2,2,50),
        (2,2,3,3,3,3,3,3,3,60),
        (3,3,3,3,3,3,3,3,3,70);
    """
    sql """
        insert into t3 values
        (1,1,1,1,1,1,1,1,1,10),
        (1,1,1,1,1,1,1,1,1,20),
        (1,1,1,2,2,2,2,2,2,30),
        (1,2,2,2,2,2,2,2,2,40),
        (2,2,2,2,2,2,2,2,2,50),
        (2,2,3,3,3,3,3,3,3,60),
        (3,3,3,3,3,3,3,3,3,70);
    """
    def checkShuffleKey = { String sqlString, String... expectedHashCols ->
        def result = sql """ explain shape plan ${sqlString}"""
        for (def expected : expectedHashCols) {
            assertTrue(result.toString().contains("Hash Columns:[${expected}]"), "\n${result.collect { it[0] }.join('\n')} not contains expected hash cols: ${expected}")
        }
    }
    // 1. simple agg
    explainAndOrderResult "simple_agg", "select a from t1 group by a,b,c,d,e,f order by a,b,c,d,e,f;"
    // 2. agg receive a hash request from upper node
    explainAndOrderResult "agg_receive_shuffle_request", "select sum(a) over (partition by a,b,c,d,e,f order by a,b,c) from (select a,b,c,d,e,f,g,h from t1 group by a,b,c,d,e,f,g,h) t order by 1;"
    // 3. agg with distinct agg function
    // 3.1 random distribute (not satisfy group by key)
    // 2+1
    explainAndOrderResult "distinct_agg_func_with_random_distribute_gby_key_greater_than_threshold", "select d,e,f,g,h,i,j, count(distinct a,b,c) from t1 group by d,e,f,g,h,i,j order by d,e,f,g,h,i,j;"
    explainAndOrderResult "distinct_agg_func_with_random_distribute_distinct_key_greater_than_threshold", "select d,e,f,g,h,i,j, count(distinct a,b,c,d,e,f) from t1 group by d,e,f,g,h,i,j order by d,e,f,g,h,i,j;"
    // 2+2
    sql "set agg_phase=4"
    explainAndOrderResult "distinct_agg_func_with_random_distribute_gby_key_greater_than_threshold_2_plus_2", "select d,e,f,g,h,i,j, count(distinct a,b,c) from t1 group by d,e,f,g,h,i,j order by d,e,f,g,h,i,j;"
    explainAndOrderResult "distinct_agg_func_with_random_distribute_distinct_key_greater_than_threshold_2_plus_2", "select d,e,f,g,h,i,j, count(distinct a,b,c,d,e,f) from t1 group by d,e,f,g,h,i,j order by d,e,f,g,h,i,j;"
    explainAndOrderResult "distinct_agg_func_with_random_distribute_without_redundant_agg", "select d,f,g,h,i,a, count(distinct e,b,c) from t1 group by d,f,g,h,i,a order by d,f,g,h,i,a;"
    explainAndOrderResult "distinct_agg_func_with_random_distribute_with_redundant_agg", "select d,f,e,h,i,a, count(distinct a,b,c) from t1 group by d,f,e,h,i,a order by d,f,e,h,i,a;"
    sql "set agg_phase=0"

    // 3.2 hash distribute by a
    // 1+1
    explainAndOrderResult "distinct_agg_func_with_a_hash_distribute_gby_key_has_a", "select a,e,f,g,h,i,j, count(distinct e,b,c) from t2 group by a,e,f,g,h,i,j order by a,e,f,g,h,i,j;"
    explainAndOrderResult "distinct_agg_func_with_e_hash_distribute_gby_key_has_a", "select a,e,f,g,h,i,j, count(distinct e,b,c) from t2 group by a,e,f,g,h,i,j order by a,e,f,g,h,i,j;"
    // 1+2 satisfy distinct key
    explainAndOrderResult "distinct_agg_func_with_a_hash_distribute_distinct_key_has_a", "select d,e,f,g,h,i,j, count(distinct a,b,c) from t2 group by d,e,f,g,h,i,j order by d,e,f,g,h,i,j;"
    // 1+2 with redundant agg
    sql "set agg_phase=4"
    explainAndOrderResult "distinct_agg_func_with_a_hash_distribute_gby_key_has_a_1_plus_2_redundant_agg", "select a,e,f,g,h,i,j, count(distinct e,b,c) from t2 group by a,e,f,g,h,i,j order by a,e,f,g,h,i,j;"
    explainAndOrderResult "distinct_agg_func_with_e_hash_distribute_gby_key_has_a_1_plus_2_redundant_agg", "select a,e,f,g,h,i,j, count(distinct e,b,c) from t3 group by a,e,f,g,h,i,j order by a,e,f,g,h,i,j;"
    sql "set agg_phase=0"

    // 4. agg with distinct agg function and a avg
    // 4.1 random distribute
    // 2+1
    explainAndOrderResult "distinct_agg_func_with_random_distribute_gby_key_greater_than_threshold_avg", "select d,e,f,g,h,i,j, count(distinct a,b,c), round(avg(a),4) from t1 group by d,e,f,g,h,i,j order by d,e,f,g,h,i,j;"
    explainAndOrderResult "distinct_agg_func_with_random_distribute_distinct_key_greater_than_threshold_avg", "select d,e,f,g,h,i,j, count(distinct a,b,c,d,e,f), round(avg(a),4) from t1 group by d,e,f,g,h,i,j order by d,e,f,g,h,i,j;"
    // 2+2
    sql "set agg_phase=4"
    explainAndOrderResult "distinct_agg_func_with_random_distribute_gby_key_greater_than_threshold_2_plus_2_avg", "select d,e,f,g,h,i,j, count(distinct a,b,c), round(avg(a),4) from t1 group by d,e,f,g,h,i,j order by d,e,f,g,h,i,j;"
    explainAndOrderResult "distinct_agg_func_with_random_distribute_distinct_key_greater_than_threshold_2_plus_2_avg", "select d,e,f,g,h,i,j, count(distinct a,b,c,d,e,f), round(avg(a),4) from t1 group by d,e,f,g,h,i,j order by d,e,f,g,h,i,j;"
    explainAndOrderResult "distinct_agg_func_with_random_distribute_without_redundant_agg_avg", "select d,f,g,h,i,a, count(distinct e,b,c), round(avg(a),4) from t1 group by d,f,g,h,i,a order by d,f,g,h,i,a;"
    explainAndOrderResult "distinct_agg_func_with_random_distribute_with_redundant_agg_avg", "select d,f,e,h,i,a, count(distinct a,b,c), round(avg(a),4) from t1 group by d,f,e,h,i,a order by d,f,e,h,i,a;"
    sql "set agg_phase=0"

    // 4.2 hash distribute by a
    // 1+1
    explainAndOrderResult "distinct_agg_func_with_a_hash_distribute_gby_key_has_a_avg", "select a,e,f,g,h,i,j, count(distinct e,b,c), round(avg(a),4) from t2 group by a,e,f,g,h,i,j order by a,e,f,g,h,i,j;"
    explainAndOrderResult "distinct_agg_func_with_e_hash_distribute_gby_key_has_a_avg", "select a,e,f,g,h,i,j, count(distinct e,b,c), round(avg(a),4) from t2 group by a,e,f,g,h,i,j order by a,e,f,g,h,i,j;"
    // 1+2 satisfy distinct key
    explainAndOrderResult "distinct_agg_func_with_a_hash_distribute_distinct_key_has_a_avg", "select d,e,f,g,h,i,j, count(distinct a,b,c), round(avg(a),4) from t2 group by d,e,f,g,h,i,j order by d,e,f,g,h,i,j;"
    // 1+2 with redundant agg
    sql "set agg_phase=4"
    explainAndOrderResult "distinct_agg_func_with_a_hash_distribute_gby_key_has_a_1_plus_2_redundant_agg_avg", "select a,e,f,g,h,i,j, count(distinct e,b,c), round(avg(a),4) from t2 group by a,e,f,g,h,i,j order by a,e,f,g,h,i,j;"
    explainAndOrderResult "distinct_agg_func_with_e_hash_distribute_gby_key_has_a_1_plus_2_redundant_agg_avg", "select a,e,f,g,h,i,j, count(distinct e,b,c), round(avg(a),4) from t3 group by a,e,f,g,h,i,j order by a,e,f,g,h,i,j;"
    sql "set agg_phase=0"

    // 5. agg and join (visitPhysicalHashJoin single shuffle key selection)
    // Both sides Global AGG, join 7 columns (>5): should choose single column shuffle (e.g. e)
    explainAndOrderResult "agg_join", """select t1.a,t1.b,t1.c,t1.d,t1.e,t1.f,t1.g from (select a,b,c,d,e,f,g from t1 group by a,b,c,d,e,f,g) t1 inner join (select a,b,c,d,e,f,g from t1 group by a,b,c,d,e,f,g) t2 on t1.a=t2.a and t1.b=t2.b and t1.c=t2.c and t1.d=t2.d and t1.e=t2.e and t1.f=t2.f and t1.g=t2.g order by t1.a,t1.b,t1.c,t1.d,t1.e,t1.f,t1.g;"""
    // Both sides Global AGG, join 5 columns, agg gby key also 5 columns (=threshold): size>5 not satisfied, use full 5 column shuffle, no single key
    explainAndOrderResult "agg_join_5_keys", """select t1.a,t1.b,t1.c,t1.d,t1.e from (select a,b,c,d,e from t1 group by a,b,c,d,e) t1 inner join (select a,b,c,d,e from t1 group by a,b,c,d,e) t2 on t1.a=t2.a and t1.b=t2.b and t1.c=t2.c and t1.d=t2.d and t1.e=t2.e order by t1.a,t1.b,t1.c,t1.d,t1.e;"""
    // join 5 columns, agg gby key > 5 columns
    // Why does this use 1 shuffle key? Because after agg with 1 shuffle key e, join does not need to re-hash.
    explainAndOrderResult "agg_7_join_5_keys", """select t1.a,t1.b,t1.c,t1.d,t1.e from (select a,b,c,d,e from t1 group by a,b,c,d,e,f,g) t1 inner join (select a,b,c,d,e from t1 group by a,b,c,d,e,f,g) t2 on t1.a=t2.a and t1.b=t2.b and t1.c=t2.c and t1.d=t2.d and t1.e=t2.e order by t1.a,t1.b,t1.c,t1.d,t1.e;"""

    // Both sides Global AGG, join 3 columns (<threshold): full 3 column shuffle
    // Why does this use 3 shuffle keys? If we used one shuffle key e, join would still need to do hash shuffle again.
    explainAndOrderResult "agg_join_3_keys_not_choose_1_key", """select t1.a,t1.b,t1.c from (select a,b,c from t1 group by a,b,c,d,e,f,g) t1 inner join (select a,b,c from t1 group by a,b,c,d,e,f,g) t2 on t1.a=t2.a and t1.b=t2.b and t1.c=t2.c order by t1.a,t1.b,t1.c;"""
    // If join key contains e, agg shuffle uses single shuffle key e, join does not need to re-hash.
    explainAndOrderResult "agg_7_join_3_keys_choose_1_key", """select t1.a,t1.b,t1.c,t1.e from (select a,b,c,e from t1 group by a,b,c,d,e,f,g) t1 inner join (select a,b,c,e from t1 group by a,b,c,d,e,f,g) t2 on t1.a=t2.a and t1.b=t2.b and t1.e=t2.e order by t1.a,t1.b,t1.c,t1.e;"""

    // Both sides from different tables, both Global AGG, join 7 columns: single key optimization
    explainAndOrderResult "agg_join_t1_t2", """select t1.a,t1.b,t1.c,t1.d,t1.e,t1.f,t1.g from (select a,b,c,d,e,f,g from t1 group by a,b,c,d,e,f,g) t1 inner join (select a,b,c,d,e,f,g from t2 group by a,b,c,d,e,f,g) t2 on t1.a=t2.a and t1.b=t2.b and t1.c=t2.c and t1.d=t2.d and t1.e=t2.e and t1.f=t2.f and t1.g=t2.g order by t1.a,t1.b,t1.c,t1.d,t1.e,t1.f,t1.g;"""
    // one-side is scan, the other side is agg, not satisfy "both sides are Global AGG", use normal shuffle (full join key)
    explainAndOrderResult "agg_join_one_side_agg", """select t1.a,t1.b,t1.c from t1 inner join (select a,b,c from t2 group by a,b,c) t2 on t1.a=t2.a and t1.b=t2.b and t1.c=t2.c order by t1.a,t1.b,t1.c;"""

    // e=f f=e non-one-to-one mapping scenario
    explainAndOrderResult "join_e_equal_f", """select t1.a,t1.b,t1.c,t1.d,t1.e,t1.f,t1.g from (select a,b,c,d,e,f,g from t1 group by a,b,c,d,e,f,g) t1 inner join (select a,b,c,d,e,f,g from t1 group by a,b,c,d,e,f,g) t2 on t1.a=t2.a and t1.b=t2.b and t1.c=t2.c and t1.d=t2.d and t1.e=t2.f and t1.f=t2.e and t1.g=t2.g order by t1.a,t1.b,t1.c,t1.d,t1.e,t1.f,t1.g;"""

    // When both sides satisfy agg distribution
    explainAndOrderResult "join_both_side_satisfy_agg_distribution", """select t1.a,t1.b,t1.c,t1.d,t1.e,t1.f,t1.g from (select a,b,c,d,e,f,g from t2 group by a,b,c,d,e,f,g) t1 inner join (select a,b,c,d,e,f,g from t2 group by a,b,c,d,e,f,g) t2 on t1.a=t2.a and t1.b=t2.b and t1.c=t2.c and t1.d=t2.d and t1.e=t2.e and t1.f=t2.f and t1.g=t2.g order by t1.a,t1.b,t1.c,t1.d,t1.e,t1.f,t1.g;"""
    // One side of join satisfies agg distribution
    explainAndOrderResult "join_one_side_satisfy_agg_distribution", """select t1.a,t1.b,t1.c,t1.d,t1.e,t1.f,t1.g from (select a,b,c,d,e,f,g from t1 group by a,b,c,d,e,f,g) t1 inner join (select a,b,c,d,e,f,g from t2 group by a,b,c,d,e,f,g) t2 on t1.a=t2.a and t1.b=t2.b and t1.c=t2.c and t1.d=t2.d and t1.e=t2.e and t1.f=t2.f and t1.g=t2.g order by t1.a,t1.b,t1.c,t1.d,t1.e,t1.f,t1.g;"""

    explainAndOrderResult "join_scan_scan", "select t1.a,t2.b from t1 join t2 on t1.b=t2.b and t1.d=t2.d order by 1,2"

    // 5.7 Disable single key optimization: should use full join column shuffle
    sql "set enable_shuffle_key_prune=false"
    explainAndOrderResult "agg_join_no_opt", """select t1.a,t1.b,t1.c,t1.d,t1.e,t1.f,t1.g from (select a,b,c,d,e,f,g from t1 group by a,b,c,d,e,f,g) t1 inner join (select a,b,c,d,e,f,g from t1 group by a,b,c,d,e,f,g) t2 on t1.a=t2.a and t1.b=t2.b and t1.c=t2.c and t1.d=t2.d and t1.e=t2.e and t1.f=t2.f and t1.g=t2.g order by t1.a,t1.b,t1.c,t1.d,t1.e,t1.f,t1.g;"""
    sql "set enable_shuffle_key_prune=true"

    // No optimization, because under join is project, not agg
    explainAndOrderResult "join_project_agg", """select * from (select a,b,c,e,c+1 col1 ,b+2 col2 from t1 group by a,b,c,e) t1 inner join (select a,b,c,e,e+1 col1,c+2 col2 from t1 group by a,b,c,e) t2 on t1.a=t2.a and t1.b=t2.b and t1.c=t2.c and t1.e=t2.e and t1.col1=t2.col1 and t1.col2=t2.col2 order by t1.a,t1.b,t1.c,t1.e;"""

    // Not optimize
    explainAndOrderResult "distinct_agg_func_with_a_hash_distribute_no_optimize", "select d,e,f, count(distinct a,b,c) from t2 group by d,e,f order by d,e,f;"
}