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

suite("agg_sync_mv") {
	sql """ use regression_test_nereids_syntax_p0_mv """
    sql """ SET enable_nereids_planner=true """
    sql """ SET enable_fallback_to_original_planner=false """
    sql """ analyze table agg_mv_test with sync"""
    sql """ set enable_stats=false"""

    qt_select_any_value """select id, any_value(kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, any_value(kint) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, any_value(kint) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_any_value_mv """select id, any_value(kint) from agg_mv_test group by id order by id;"""

    // sum_foreach is not supported in old planner
    // qt_select_sum_foreach """select id, sum_foreach(kaint) from agg_mv_test group by id order by id;"""
    // sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    // createMV("""create materialized view mv_sync as select id, sum_foreach(kaint) from agg_mv_test group by id order by id;""")
    // explain {
    //     sql("select id, sum_foreach(kaint) from agg_mv_test group by id order by id;")
    //     contains "(mv_sync)"
    // }
    // qt_select_sum_foreach_mv """select id, sum_foreach(kaint) from agg_mv_test group by id order by id;"""

    qt_select_approx_count_distinct """select id, approx_count_distinct(kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, approx_count_distinct(kint) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, approx_count_distinct(kint) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_approx_count_distinct_mv """select id, approx_count_distinct(kint) from agg_mv_test group by id order by id;"""

    qt_select_collect_set """select id, collect_set(kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, collect_set(kint) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, collect_set(kint) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_collect_set_mv """select id, collect_set(kint) from agg_mv_test group by id order by id;"""

    qt_select_collect_list """select id, collect_list(kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, collect_list(kint) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, collect_list(kint) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_collect_list_mv """select id, collect_list(kint) from agg_mv_test group by id order by id;"""

    qt_select_corr """select id, corr(kint, kbint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, corr(kint, kbint) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, corr(kint, kbint) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_corr_mv """select id, corr(kint, kbint) from agg_mv_test group by id order by id;"""

    qt_select_percentile_array """select id, percentile_array(kint, [0.5,0.55,0.805]) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, percentile_array(kint, [0.5,0.55,0.805]) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, percentile_array(kint, [0.5,0.55,0.805]) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_percentile_array_mv """select id, percentile_array(kint, [0.5,0.55,0.805]) from agg_mv_test group by id order by id;"""

    qt_select_quantile_union """select id, quantile_union(to_quantile_state(kbint, 2048)) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, quantile_union(to_quantile_state(kbint, 2048)) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, quantile_union(to_quantile_state(kbint, 2048)) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_quantile_union_mv """select id, quantile_union(to_quantile_state(kbint, 2048)) from agg_mv_test group by id order by id;"""

    qt_select_count_by_enum """select id, count_by_enum(kstr) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, count_by_enum(kstr) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, count_by_enum(kstr) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_count_by_enum_mv """select id, count_by_enum(kstr) from agg_mv_test group by id order by id;"""

    qt_select_avg_weighted """select id, avg_weighted(ktint, kdbl) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, avg_weighted(ktint, kdbl) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, avg_weighted(ktint, kdbl) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_avg_weighted_mv """select id, avg_weighted(ktint, kdbl) from agg_mv_test group by id order by id;"""

    qt_select_bitmap_intersect """select id, bitmap_intersect(bitmap_hash(kbint)) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, bitmap_intersect(bitmap_hash(kbint)) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, bitmap_intersect(bitmap_hash(kbint)) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_bitmap_intersect_mv """select id, bitmap_intersect(bitmap_hash(kbint)) from agg_mv_test group by id order by id;"""

    qt_select_bitmap_agg """select id, bitmap_agg(kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, bitmap_agg(kint) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, bitmap_agg(kint) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_bitmap_agg_mv """select id, bitmap_agg(kint) from agg_mv_test group by id order by id;"""

    qt_select_bitmap_union """select id, bitmap_union(bitmap_hash(kbint)) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, bitmap_union(bitmap_hash(kbint)) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, bitmap_union(bitmap_hash(kbint)) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_bitmap_union_mv """select id, bitmap_union(bitmap_hash(kbint)) from agg_mv_test group by id order by id;"""

    qt_select_bitmap_union_count """select id, bitmap_union_count(bitmap_hash(kbint)) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, bitmap_union_count(bitmap_hash(kbint)) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, bitmap_union_count(bitmap_hash(kbint)) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_bitmap_union_count_mv """select id, bitmap_union_count(bitmap_hash(kbint)) from agg_mv_test group by id order by id;"""

    qt_select_bitmap_union_int """select id, bitmap_union_int(kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, bitmap_union_int(kint) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, bitmap_union_int(kint) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_bitmap_union_int_mv """select id, bitmap_union_int(kint) from agg_mv_test group by id order by id;"""

    qt_select_group_array_intersect """select id, group_array_intersect(kaint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, group_array_intersect(kaint) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, group_array_intersect(kaint) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_group_array_intersect_mv """select id, group_array_intersect(kaint) from agg_mv_test group by id order by id;"""

    qt_select_group_bit_and """select id, group_bit_and(kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, group_bit_and(kint) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, group_bit_and(kint) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_group_bit_and_mv """select id, group_bit_and(kint) from agg_mv_test group by id order by id;"""

    qt_select_group_bit_or """select id, group_bit_or(kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, group_bit_or(kint) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, group_bit_or(kint) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_group_bit_or_mv """select id, group_bit_or(kint) from agg_mv_test group by id order by id;"""

    qt_select_group_bit_xor """select id, group_bit_xor(kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, group_bit_xor(kint) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, group_bit_xor(kint) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_group_bit_xor_mv """select id, group_bit_xor(kint) from agg_mv_test group by id order by id;"""

    qt_select_group_bitmap_xor """select id, group_bitmap_xor(bitmap_hash(kbint)) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, group_bitmap_xor(bitmap_hash(kbint)) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, group_bitmap_xor(bitmap_hash(kbint)) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_group_bitmap_xor_mv """select id, group_bitmap_xor(bitmap_hash(kbint)) from agg_mv_test group by id order by id;"""

    qt_select_hll_union_agg """select id, hll_union_agg(hll_hash(kbint)) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, hll_union_agg(hll_hash(kbint)) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, hll_union_agg(hll_hash(kbint)) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_hll_union_agg_mv """select id, hll_union_agg(hll_hash(kbint)) from agg_mv_test group by id order by id;"""

    qt_select_hll_union """select id, hll_union(hll_hash(kbint)) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, hll_union(hll_hash(kbint)) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, hll_union(hll_hash(kbint)) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_hll_union_mv """select id, hll_union(hll_hash(kbint)) from agg_mv_test group by id order by id;"""

    qt_select_intersect_count """select id, intersect_count(bitmap_hash(kbint), kint, 3, 4) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, intersect_count(bitmap_hash(kbint), kint, 3, 4) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, intersect_count(bitmap_hash(kbint), kint, 3, 4) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_intersect_count_mv """select id, intersect_count(bitmap_hash(kbint), kint, 3, 4) from agg_mv_test group by id order by id;"""

    qt_select_group_concat """select id, group_concat(cast(abs(kint) as varchar)) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, group_concat(cast(abs(kint) as varchar)) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, group_concat(cast(abs(kint) as varchar)) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_group_concat_mv """select id, group_concat(cast(abs(kint) as varchar)) from agg_mv_test group by id order by id;"""

    qt_select_multi_distinct_group_concat """select id, multi_distinct_group_concat(cast(abs(kint) as varchar)) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, multi_distinct_group_concat(cast(abs(kint) as varchar)) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, multi_distinct_group_concat(cast(abs(kint) as varchar)) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_multi_distinct_group_concat_mv """select id, multi_distinct_group_concat(cast(abs(kint) as varchar)) from agg_mv_test group by id order by id;"""

    qt_select_multi_distinct_sum0 """select id, multi_distinct_sum0(kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, multi_distinct_sum0(kint) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, multi_distinct_sum0(kint) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_multi_distinct_sum0_mv """select id, multi_distinct_sum0(kint) from agg_mv_test group by id order by id;"""

    qt_select_multi_distinct_sum """select id, multi_distinct_sum(kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, multi_distinct_sum(kint) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, multi_distinct_sum(kint) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_multi_distinct_sum_mv """select id, multi_distinct_sum(kint) from agg_mv_test group by id order by id;"""


    qt_select_histogram """select id, histogram(kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, histogram(kint) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, histogram(kint) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_histogram_mv """select id, histogram(kint) from agg_mv_test group by id order by id;"""

    qt_select_max_by """select id, max_by(kint, kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, max_by(kint, kint) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, max_by(kint, kint) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_max_by_mv """select id, max_by(kint, kint) from agg_mv_test group by id order by id;"""

    qt_select_min_by """select id, min_by(kint, kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, min_by(kint, kint) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, min_by(kint, kint) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_min_by_mv """select id, min_by(kint, kint) from agg_mv_test group by id order by id;"""

    qt_select_multi_distinct_count """select id, multi_distinct_count(kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, multi_distinct_count(kint) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, multi_distinct_count(kint) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_multi_distinct_count_mv """select id, multi_distinct_count(kint) from agg_mv_test group by id order by id;"""

    qt_select_ndv """select id, ndv(kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, ndv(kint) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, ndv(kint) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_ndv_mv """select id, ndv(kint) from agg_mv_test group by id order by id;"""

    qt_select_covar """select id, covar(kint, kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, covar(kint, kint) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, covar(kint, kint) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_covar_mv """select id, covar(kint, kint) from agg_mv_test group by id order by id;"""

    qt_select_covar_samp """select id, covar_samp(kint, kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, covar_samp(kint, kint) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, covar_samp(kint, kint) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_covar_samp_mv """select id, covar_samp(kint, kint) from agg_mv_test group by id order by id;"""

    qt_select_percentile """select id, percentile(kbint, 0.6) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, percentile(kbint, 0.6) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, percentile(kbint, 0.6) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_percentile_mv """select id, percentile(kbint, 0.6) from agg_mv_test group by id order by id;"""

    qt_select_percentile_approx """select id, percentile_approx(kbint, 0.6) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, percentile_approx(kbint, 0.6) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, percentile_approx(kbint, 0.6) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_percentile_approx_mv """select id, percentile_approx(kbint, 0.6) from agg_mv_test group by id order by id;"""

    // percentile_approx_weighted is not supported in old planner
    // qt_select_percentile_approx_weighted """select id, percentile_approx_weighted(kint, kbint, 0.6) from agg_mv_test group by id order by id;"""
    // sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    // createMV("""create materialized view mv_sync as select id, percentile_approx_weighted(kint, kbint, 0.6) from agg_mv_test group by id order by id;""")
    // explain {
    //     sql("select id, percentile_approx_weighted(kint, kbint, 0.6) from agg_mv_test group by id order by id;")
    //     contains "(mv_sync)"
    // }
    // qt_select_percentile_approx_weighted_mv """select id, percentile_approx_weighted(kint, kbint, 0.6) from agg_mv_test group by id order by id;"""

    qt_select_sequence_count """select id, sequence_count('(?1)(?2)', kdtv2, kint = 1, kint = 2) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, sequence_count('(?1)(?2)', kdtv2, kint = 1, kint = 2) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, sequence_count('(?1)(?2)', kdtv2, kint = 1, kint = 2) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_sequence_count_mv """select id, sequence_count('(?1)(?2)', kdtv2, kint = 1, kint = 2) from agg_mv_test group by id order by id;"""

    qt_select_sequence_match """select id, sequence_match('(?1)(?2)', kdtv2, kint = 1, kint = 2) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, sequence_match('(?1)(?2)', kdtv2, kint = 1, kint = 2) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, sequence_match('(?1)(?2)', kdtv2, kint = 1, kint = 2) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_sequence_match_mv """select id, sequence_match('(?1)(?2)', kdtv2, kint = 1, kint = 2) from agg_mv_test group by id order by id;"""

    qt_select_stddev """select id, stddev(kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, stddev(kint) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, stddev(kint) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_stddev_mv """select id, stddev(kint) from agg_mv_test group by id order by id;"""

    qt_select_stddev_pop """select id, stddev_pop(kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, stddev_pop(kint) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, stddev_pop(kint) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_stddev_pop_mv """select id, stddev_pop(kint) from agg_mv_test group by id order by id;"""

    qt_select_stddev_samp """select id, stddev_samp(kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, stddev_samp(kint) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, stddev_samp(kint) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_stddev_samp_mv """select id, stddev_samp(kint) from agg_mv_test group by id order by id;"""

    qt_select_sum0 """select id, sum0(kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, sum0(kint) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, sum0(kint) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_sum0_mv """select id, sum0(kint) from agg_mv_test group by id order by id;"""

    qt_select_topn """select id, topn(kvchrs1, 3) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, topn(kvchrs1, 3) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, topn(kvchrs1, 3) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_topn_mv """select id, topn(kvchrs1, 3) from agg_mv_test group by id order by id;"""

    qt_select_topn_array """select id, topn_array(kvchrs1, 3) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, topn_array(kvchrs1, 3) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, topn_array(kvchrs1, 3) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_topn_array_mv """select id, topn_array(kvchrs1, 3) from agg_mv_test group by id order by id;"""

    qt_select_topn_weighted """select id, topn_weighted(kvchrs1, ktint, 3) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, topn_weighted(kvchrs1, ktint, 3) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, topn_weighted(kvchrs1, ktint, 3) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_topn_weighted_mv """select id, topn_weighted(kvchrs1, ktint, 3) from agg_mv_test group by id order by id;"""

    qt_select_variance """select id, variance(kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, variance(kint) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, variance(kint) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_variance_mv """select id, variance(kint) from agg_mv_test group by id order by id;"""

    qt_select_var_pop """select id, var_pop(kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, var_pop(kint) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, var_pop(kint) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_var_pop_mv """select id, var_pop(kint) from agg_mv_test group by id order by id;"""

    qt_select_variance_samp """select id, variance_samp(kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, variance_samp(kint) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, variance_samp(kint) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_variance_samp_mv """select id, variance_samp(kint) from agg_mv_test group by id order by id;"""

    qt_select_var_samp """select id, var_samp(kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, var_samp(kint) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, var_samp(kint) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_var_samp_mv """select id, var_samp(kint) from agg_mv_test group by id order by id;"""

    qt_select_window_funnel """select id, window_funnel(3600 * 3, 'default', kdtm, kint = 1, kint = 2) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, window_funnel(3600 * 3, 'default', kdtm, kint = 1, kint = 2) from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, window_funnel(3600 * 3, 'default', kdtm, kint = 1, kint = 2) from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_window_funnel_mv """select id, window_funnel(3600 * 3, 'default', kdtm, kint = 1, kint = 2) from agg_mv_test group by id order by id;"""

    // map_agg is not supported yet
    // qt_select_map_agg """select id, map_agg(kint, kstr) from agg_mv_test group by id order by id;"""
    // sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    // createMV("""create materialized view mv_sync as select id, map_agg(kint, kstr) from agg_mv_test group by id order by id;""")
    // explain {
    //     sql("select id, map_agg(kint, kstr) from agg_mv_test group by id order by id;")
    //     contains "(mv_sync)"
    // }
    // qt_select_map_agg_mv """select id, map_agg(kint, kstr) from agg_mv_test group by id order by id;"""

    // array_agg is not supported yet
    // qt_select_array_agg """select id, array_agg(kstr) from agg_mv_test group by id order by id;"""
    // sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    // createMV("""create materialized view mv_sync as select id, array_agg(kstr) from agg_mv_test group by id order by id;""")
    // explain {
    //     sql("select id, array_agg(kstr) from agg_mv_test group by id order by id;")
    //     contains "(mv_sync)"
    // }
    // qt_select_array_agg_mv """select id, array_agg(kstr) from agg_mv_test group by id order by id;"""

    qt_select_retention """select id, retention(kdtm = '2012-03-11', kdtm = '2012-03-12') from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync on agg_mv_test;"""
    createMV("""create materialized view mv_sync as select id, retention(kdtm = '2012-03-11', kdtm = '2012-03-12') from agg_mv_test group by id order by id;""")
    explain {
        sql("select id, retention(kdtm = '2012-03-11', kdtm = '2012-03-12') from agg_mv_test group by id order by id;")
        contains "(mv_sync)"
    }
    qt_select_retention_mv """select id, retention(kdtm = '2012-03-11', kdtm = '2012-03-12') from agg_mv_test group by id order by id;"""

}
