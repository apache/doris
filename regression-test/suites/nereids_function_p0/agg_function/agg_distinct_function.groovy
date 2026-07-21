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

suite("agg_distinct_function") {
    sql "drop table if exists agg_distinct_function"
    sql """
        create table agg_distinct_function (
            k int,
            a int,
            b int,
            s varchar(10),
            w int
        ) engine=olap
        duplicate key(k)
        distributed by hash(k) buckets 1
        properties ("replication_num" = "1")
    """

    // The duplicate rows make DISTINCT observable. Every query is run both as a
    // scalar aggregation and as a grouped aggregation.
    sql """
        insert into agg_distinct_function values
            (1, 1, 10, 'a', 1), (1, 1, 10, 'a', 1),
            (1, 2, 20, 'b', 2), (1, 3, 30, 'c', 3),
            (2, 1, 10, 'a', 1), (2, 4, 40, 'd', 4),
            (2, 4, 40, 'd', 4), (2, 5, 50, 'e', 5)
    """

    qt_array_agg "select array_sort(array_agg(distinct a)) from agg_distinct_function"
    order_qt_array_agg_group "select k, array_sort(array_agg(distinct a)) from agg_distinct_function group by k order by k"
    qt_avg "select avg(distinct a) from agg_distinct_function"
    order_qt_avg_group "select k, avg(distinct a) from agg_distinct_function group by k order by k"
    qt_avg_weighted "select avg_weighted(distinct a, w) from agg_distinct_function"
    order_qt_avg_weighted_group "select k, avg_weighted(distinct a, w) from agg_distinct_function group by k order by k"
    qt_collect_list "select array_sort(collect_list(distinct a)) from agg_distinct_function"
    order_qt_collect_list_group "select k, array_sort(collect_list(distinct a)) from agg_distinct_function group by k order by k"
    qt_collect_set "select array_sort(collect_set(distinct a)) from agg_distinct_function"
    order_qt_collect_set_group "select k, array_sort(collect_set(distinct a)) from agg_distinct_function group by k order by k"
    qt_corr_welford "select corr_welford(distinct a, b) from agg_distinct_function"
    order_qt_corr_welford_group "select k, corr_welford(distinct a, b) from agg_distinct_function group by k order by k"
    qt_corr "select corr(distinct a, b) from agg_distinct_function"
    order_qt_corr_group "select k, corr(distinct a, b) from agg_distinct_function group by k order by k"
    qt_covar "select covar(distinct a, b) from agg_distinct_function"
    order_qt_covar_group "select k, covar(distinct a, b) from agg_distinct_function group by k order by k"
    qt_covar_samp "select covar_samp(distinct a, b) from agg_distinct_function"
    order_qt_covar_samp_group "select k, covar_samp(distinct a, b) from agg_distinct_function group by k order by k"
    qt_group_bit_and "select group_bit_and(distinct a) from agg_distinct_function"
    order_qt_group_bit_and_group "select k, group_bit_and(distinct a) from agg_distinct_function group by k order by k"
    qt_group_bit_or "select group_bit_or(distinct a) from agg_distinct_function"
    order_qt_group_bit_or_group "select k, group_bit_or(distinct a) from agg_distinct_function group by k order by k"
    qt_group_bit_xor "select group_bit_xor(distinct a) from agg_distinct_function"
    order_qt_group_bit_xor_group "select k, group_bit_xor(distinct a) from agg_distinct_function group by k order by k"
//    qt_group_bitmap_xor "select bitmap_to_string(group_bitmap_xor(distinct bitmap_hash(a))) from agg_distinct_function"
//    order_qt_group_bitmap_xor_group "select k, bitmap_to_string(group_bitmap_xor(distinct bitmap_hash(a))) from agg_distinct_function group by k order by k"
    qt_group_concat "select group_concat(distinct s order by s) from agg_distinct_function"
    order_qt_group_concat_group "select k, group_concat(distinct s order by s) from agg_distinct_function group by k order by k"
    qt_histogram "select histogram(distinct a) from agg_distinct_function"
    order_qt_histogram_group "select k, histogram(distinct a) from agg_distinct_function group by k order by k"
    qt_hll_raw_agg "select hll_cardinality(hll_raw_agg(distinct hll_hash(a))) from agg_distinct_function"
    order_qt_hll_raw_agg_group "select k, hll_cardinality(hll_raw_agg(distinct hll_hash(a))) from agg_distinct_function group by k order by k"
    qt_hll_union_agg "select hll_union_agg(distinct hll_hash(a)) from agg_distinct_function"
    order_qt_hll_union_agg_group "select k, hll_union_agg(distinct hll_hash(a)) from agg_distinct_function group by k order by k"
    qt_kurt "select kurt(distinct a) from agg_distinct_function"
    order_qt_kurt_group "select k, kurt(distinct a) from agg_distinct_function group by k order by k"
    order_qt_map_agg_group """
        SELECT
            array_sort(map_keys(m)) AS sorted_keys,
            array_sortby(map_values(m), map_keys(m)) AS values_by_sorted_key
        FROM (
            SELECT map_agg(DISTINCT a, s) AS m
            FROM agg_distinct_function
            group by k
        ) t;
    """
    order_qt_map_agg """
       SELECT
            array_sort(map_keys(m)) AS sorted_keys,
            array_sortby(map_values(m), map_keys(m)) AS values_by_sorted_key
        FROM (
            SELECT map_agg(DISTINCT a, s) AS m
            FROM agg_distinct_function
        ) t;
    """
    qt_max_by "select max_by(distinct s, a) from agg_distinct_function"
    order_qt_max_by_group "select k, max_by(distinct s, a) from agg_distinct_function group by k order by k"
    qt_min_by "select min_by(distinct s, a) from agg_distinct_function"
    order_qt_min_by_group "select k, min_by(distinct s, a) from agg_distinct_function group by k order by k"
    qt_percentile "select percentile(distinct a, 0.5) from agg_distinct_function"
    order_qt_percentile_group "select k, percentile(distinct a, 0.5) from agg_distinct_function group by k order by k"
    qt_percentile_approx "select percentile_approx(distinct a, 0.5) from agg_distinct_function"
    order_qt_percentile_approx_group "select k, percentile_approx(distinct a, 0.5) from agg_distinct_function group by k order by k"
    qt_percentile_array "select percentile_array(distinct a, [0.25, 0.5, 0.75]) from agg_distinct_function"
    order_qt_percentile_array_group "select k, percentile_array(distinct a, [0.25, 0.5, 0.75]) from agg_distinct_function group by k order by k"
    qt_percentile_approx_weighted "select percentile_approx_weighted(distinct a, w, 0.5) from agg_distinct_function"
    order_qt_percentile_approx_weighted_group "select k, percentile_approx_weighted(distinct a, w, 0.5) from agg_distinct_function group by k order by k"
    qt_topn "select topn(distinct s, 3) from agg_distinct_function"
    order_qt_topn_group "select k, topn(distinct s, 3) from agg_distinct_function group by k order by k"
    qt_topn_array "select topn_array(distinct s, 3) from agg_distinct_function"
    order_qt_topn_array_group "select k, topn_array(distinct s, 3) from agg_distinct_function group by k order by k"
    qt_topn_weighted "select topn_weighted(distinct s, w, 3) from agg_distinct_function"
    order_qt_topn_weighted_group "select k, topn_weighted(distinct s, w, 3) from agg_distinct_function group by k order by k"
    sql "select count_by_enum(distinct s) from agg_distinct_function"
    sql "select k, count_by_enum(distinct s) from agg_distinct_function group by k order by k"

    // Each pair uses different DISTINCT argument groups and exercises the CTE split path.
    qt_two_distinct_functions "select array_sort(array_agg(distinct a)), array_sort(array_agg(distinct b)) from agg_distinct_function"
    order_qt_two_distinct_functions_group "select k, array_sort(array_agg(distinct a)), array_sort(array_agg(distinct b)) from agg_distinct_function group by k order by k"
    qt_two_avg "select avg(distinct a), avg(distinct b) from agg_distinct_function"
    order_qt_two_avg_group "select k, avg(distinct a), avg(distinct b) from agg_distinct_function group by k order by k"
    qt_two_avg_weighted "select avg_weighted(distinct a, w), avg_weighted(distinct b, w) from agg_distinct_function"
    order_qt_two_avg_weighted_group "select k, avg_weighted(distinct a, w), avg_weighted(distinct b, w) from agg_distinct_function group by k order by k"
    qt_two_collect_list "select array_sort(collect_list(distinct a)), array_sort(collect_list(distinct b)) from agg_distinct_function"
    order_qt_two_collect_list_group "select k, array_sort(collect_list(distinct a)), array_sort(collect_list(distinct b)) from agg_distinct_function group by k order by k"
    qt_two_collect_set "select array_sort(collect_set(distinct a)), array_sort(collect_set(distinct b)) from agg_distinct_function"
    order_qt_two_collect_set_group "select k, array_sort(collect_set(distinct a)), array_sort(collect_set(distinct b)) from agg_distinct_function group by k order by k"
    qt_two_corr_welford "select corr_welford(distinct a, b), corr_welford(distinct a, w) from agg_distinct_function"
    order_qt_two_corr_welford_group "select k, corr_welford(distinct a, b), corr_welford(distinct a, w) from agg_distinct_function group by k order by k"
    qt_two_corr "select corr(distinct a, b), corr(distinct a, w) from agg_distinct_function"
    order_qt_two_corr_group "select k, corr(distinct a, b), corr(distinct a, w) from agg_distinct_function group by k order by k"
    qt_two_covar "select covar(distinct a, b), covar(distinct a, w) from agg_distinct_function"
    order_qt_two_covar_group "select k, covar(distinct a, b), covar(distinct a, w) from agg_distinct_function group by k order by k"
    qt_two_covar_samp "select covar_samp(distinct a, b), covar_samp(distinct a, w) from agg_distinct_function"
    order_qt_two_covar_samp_group "select k, covar_samp(distinct a, b), covar_samp(distinct a, w) from agg_distinct_function group by k order by k"
    qt_two_group_bit_and "select group_bit_and(distinct a), group_bit_and(distinct b) from agg_distinct_function"
    order_qt_two_group_bit_and_group "select k, group_bit_and(distinct a), group_bit_and(distinct b) from agg_distinct_function group by k order by k"
    qt_two_group_bit_or "select group_bit_or(distinct a), group_bit_or(distinct b) from agg_distinct_function"
    order_qt_two_group_bit_or_group "select k, group_bit_or(distinct a), group_bit_or(distinct b) from agg_distinct_function group by k order by k"
    qt_two_group_bit_xor "select group_bit_xor(distinct a), group_bit_xor(distinct b) from agg_distinct_function"
    order_qt_two_group_bit_xor_group "select k, group_bit_xor(distinct a), group_bit_xor(distinct b) from agg_distinct_function group by k order by k"
//    qt_two_group_bitmap_xor "select bitmap_to_string(group_bitmap_xor(distinct bitmap_hash(a))), bitmap_to_string(group_bitmap_xor(distinct bitmap_hash(b))) from agg_distinct_function"
//    order_qt_two_group_bitmap_xor_group "select k, bitmap_to_string(group_bitmap_xor(distinct bitmap_hash(a))), bitmap_to_string(group_bitmap_xor(distinct bitmap_hash(b))) from agg_distinct_function group by k order by k"
    qt_two_group_concat "select group_concat(distinct s order by s), group_concat(distinct cast(a as string) order by cast(a as string)) from agg_distinct_function"
    order_qt_two_group_concat_group "select k, group_concat(distinct s order by s), group_concat(distinct cast(a as string) order by cast(a as string)) from agg_distinct_function group by k order by k"
    qt_two_histogram "select histogram(distinct a), histogram(distinct b) from agg_distinct_function"
    order_qt_two_histogram_group "select k, histogram(distinct a), histogram(distinct b) from agg_distinct_function group by k order by k"
    qt_two_hll_raw_agg "select hll_cardinality(hll_raw_agg(distinct hll_hash(a))), hll_cardinality(hll_raw_agg(distinct hll_hash(b))) from agg_distinct_function"
    order_qt_two_hll_raw_agg_group "select k, hll_cardinality(hll_raw_agg(distinct hll_hash(a))), hll_cardinality(hll_raw_agg(distinct hll_hash(b))) from agg_distinct_function group by k order by k"
    qt_two_hll_union_agg "select hll_union_agg(distinct hll_hash(a)), hll_union_agg(distinct hll_hash(b)) from agg_distinct_function"
    order_qt_two_hll_union_agg_group "select k, hll_union_agg(distinct hll_hash(a)), hll_union_agg(distinct hll_hash(b)) from agg_distinct_function group by k order by k"
    qt_two_kurt "select kurt(distinct a), kurt(distinct b) from agg_distinct_function"
    order_qt_two_kurt_group "select k, kurt(distinct a), kurt(distinct b) from agg_distinct_function group by k order by k"
    qt_two_map_agg """
        select array_sort(map_keys(a_map)), array_sortby(map_values(a_map), map_keys(a_map)),
                array_sort(map_keys(b_map)), array_sortby(map_values(b_map), map_keys(b_map))
        from (
            select map_agg(distinct a, s) as a_map, map_agg(distinct b, s) as b_map
            from agg_distinct_function
        ) t
    """
    order_qt_two_map_agg_group """
        select k, array_sort(map_keys(a_map)), array_sortby(map_values(a_map), map_keys(a_map)),
                array_sort(map_keys(b_map)), array_sortby(map_values(b_map), map_keys(b_map))
        from (
            select k, map_agg(distinct a, s) as a_map, map_agg(distinct b, s) as b_map
            from agg_distinct_function
            group by k
        ) t
        order by k
    """
    qt_two_max_by "select max_by(distinct s, a), max_by(distinct s, b) from agg_distinct_function"
    order_qt_two_max_by_group "select k, max_by(distinct s, a), max_by(distinct s, b) from agg_distinct_function group by k order by k"
    qt_two_min_by "select min_by(distinct s, a), min_by(distinct s, b) from agg_distinct_function"
    order_qt_two_min_by_group "select k, min_by(distinct s, a), min_by(distinct s, b) from agg_distinct_function group by k order by k"
    qt_two_percentile "select percentile(distinct a, 0.5), percentile(distinct b, 0.5) from agg_distinct_function"
    order_qt_two_percentile_group "select k, percentile(distinct a, 0.5), percentile(distinct b, 0.5) from agg_distinct_function group by k order by k"
    qt_two_percentile_approx "select percentile_approx(distinct a, 0.5), percentile_approx(distinct b, 0.5) from agg_distinct_function"
    order_qt_two_percentile_approx_group "select k, percentile_approx(distinct a, 0.5), percentile_approx(distinct b, 0.5) from agg_distinct_function group by k order by k"
    qt_two_percentile_array "select percentile_array(distinct a, [0.25, 0.5, 0.75]), percentile_array(distinct b, [0.25, 0.5, 0.75]) from agg_distinct_function"
    order_qt_two_percentile_array_group "select k, percentile_array(distinct a, [0.25, 0.5, 0.75]), percentile_array(distinct b, [0.25, 0.5, 0.75]) from agg_distinct_function group by k order by k"
    qt_two_percentile_approx_weighted "select percentile_approx_weighted(distinct a, w, 0.5), percentile_approx_weighted(distinct b, w, 0.5) from agg_distinct_function"
    order_qt_two_percentile_approx_weighted_group "select k, percentile_approx_weighted(distinct a, w, 0.5), percentile_approx_weighted(distinct b, w, 0.5) from agg_distinct_function group by k order by k"
    qt_two_topn "select topn(distinct s, 3), topn(distinct cast(a as string), 3) from agg_distinct_function"
    order_qt_two_topn_group "select k, topn(distinct s, 3), topn(distinct cast(a as string), 3) from agg_distinct_function group by k order by k"
    qt_two_topn_array "select topn_array(distinct s, 3), topn_array(distinct cast(a as string), 3) from agg_distinct_function"
    order_qt_two_topn_array_group "select k, topn_array(distinct s, 3), topn_array(distinct cast(a as string), 3) from agg_distinct_function group by k order by k"
    qt_two_topn_weighted "select topn_weighted(distinct s, w, 3), topn_weighted(distinct cast(a as string), w, 3) from agg_distinct_function"
    order_qt_two_topn_weighted_group "select k, topn_weighted(distinct s, w, 3), topn_weighted(distinct cast(a as string), w, 3) from agg_distinct_function group by k order by k"
    sql "select count_by_enum(distinct s), count_by_enum(distinct cast(a as string)) from agg_distinct_function"
    sql "select k, count_by_enum(distinct s), count_by_enum(distinct cast(a as string)) from agg_distinct_function group by k order by k"
    order_qt_percentile_res """select percentile_reservoir(distinct a, 0.25), percentile_reservoir(distinct b, 0.75) from agg_distinct_function"""
    order_qt_percentile_res_group """select percentile_reservoir(distinct a, 0.25), percentile_reservoir(distinct b, 0.75) from agg_distinct_function group by k"""
    order_qt_ema """select exponential_moving_average(distinct 1.0, w, b),exponential_moving_average(distinct 5.0, a, b) from agg_distinct_function"""
    order_qt_ema_group """select exponential_moving_average(distinct 1.0, w, b),exponential_moving_average(distinct 5.0, a, b) from agg_distinct_function group by k"""

    sql """
        select percentile_reservoir(distinct a, 0.25), percentile_reservoir(distinct a, 0.75)
        from agg_distinct_function group by rollup(k)
    """
    sql """
        select exponential_moving_average(distinct 1.0, a, b),exponential_moving_average(distinct 5.0, a, b)
        from agg_distinct_function group by rollup(k)
    """

    explain {
        sql "select bitmap_to_string(group_bitmap_xor(distinct bitmap_hash(a))) from agg_distinct_function"
        exception "group_bitmap_xor does not support DISTINCT"
    }
}
