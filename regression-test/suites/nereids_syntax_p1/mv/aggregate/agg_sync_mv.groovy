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
    sql "drop table if exists agg_mv_test"
    sql """
        CREATE TABLE IF NOT EXISTS `agg_mv_test` (
            `id` int null,
            `kbool` boolean null,
            `ktint` tinyint(4) null,
            `ksint` smallint(6) null,
            `kint` int(11) null,
            `kbint` bigint(20) null,
            `klint` largeint(40) null,
            `kfloat` float null,
            `kdbl` double null,
            `kdcmls1` decimal(9, 3) null,
            `kdcmls2` decimal(15, 5) null,
            `kdcmls3` decimal(27, 9) null,
            `kdcmlv3s1` decimalv3(9, 3) null,
            `kdcmlv3s2` decimalv3(15, 5) null,
            `kdcmlv3s3` decimalv3(27, 9) null,
            `kchrs1` char(10) null,
            `kchrs2` char(20) null,
            `kchrs3` char(50) null,
            `kvchrs1` varchar(10) null,
            `kvchrs2` varchar(20) null,
            `kvchrs3` varchar(50) null,
            `kstr` string null,
            `kdt` date null,
            `kdtv2` datev2 null,
            `kdtm` datetime null,
            `kdtmv2s1` datetimev2(0) null,
            `kdtmv2s2` datetimev2(4) null,
            `kdtmv2s3` datetimev2(6) null,
            `kabool` array<boolean> null,
            `katint` array<tinyint(4)> null,
            `kasint` array<smallint(6)> null,
            `kaint` array<int> null,
            `kabint` array<bigint(20)> null,
            `kalint` array<largeint(40)> null,
            `kafloat` array<float> null,
            `kadbl` array<double> null,
            `kadt` array<date> null,
            `kadtm` array<datetime> null,
            `kadtv2` array<datev2> null,
            `kadtmv2` array<datetimev2(6)> null,
            `kachr` array<char(50)> null,
            `kavchr` array<varchar(50)> null,
            `kastr` array<string> null,
            `kadcml` array<decimal(27, 9)> null,
            `st_point_str` string null,
            `st_point_vc` varchar(50) null,
            `x_lng` double null,
            `x_lat` double null,
            `y_lng` double null,
            `y_lat` double null,
            `z_lng` double null,
            `z_lat` double null,
            `radius` double null,
            `linestring_wkt` varchar(50) null,
            `polygon_wkt` varchar(50) null,
            `km_bool_tint` map<boolean, tinyint> null,
            `km_tint_tint` map<tinyint, tinyint> null,
            `km_sint_tint` map<smallint, tinyint> null,
            `km_int_tint` map<int, tinyint> null,
            `km_bint_tint` map<bigint, tinyint> null,
            `km_lint_tint` map<largeint, tinyint> null,
            `km_float_tint` map<float, tinyint> null,
            `km_dbl_tint` map<double, tinyint> null,
            `km_dcml_tint` map<decimal(22,9), tinyint> null,
            `km_chr_tint` map<char(5), tinyint> null,
            `km_vchr_tint` map<varchar(50), tinyint> null,
            `km_str_tint` map<string, tinyint> null,
            `km_date_tint` map<date, tinyint> null,
            `km_dtm_tint` map<datetime, tinyint> null,
            `km_tint_bool` map<tinyint, boolean> null,
            `km_int_int` map<int, int> null,
            `km_tint_sint` map<tinyint, smallint> null,
            `km_tint_int` map<tinyint, int> null,
            `km_tint_bint` map<tinyint, bigint> null,
            `km_tint_lint` map<tinyint, largeint> null,
            `km_tint_float` map<tinyint, float> null,
            `km_tint_dbl` map<tinyint, double> null,
            `km_tint_dcml` map<tinyint, decimal(22,9)> null,
            `km_tint_chr` map<tinyint, char(5)> null,
            `km_tint_vchr` map<tinyint, varchar(50)> null,
            `km_tint_str` map<tinyint, string> null,
            `km_tint_date` map<tinyint, date> null,
            `km_tint_dtm` map<tinyint, datetime> null,
            `kjson` JSON null,
            `kstruct` STRUCT<id: int> null
        ) engine=olap
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        properties("replication_num" = "1")
    """

    streamLoad {
        table "agg_mv_test"
        db "regression_test_nereids_syntax_p1_mv_aggregate"
        set 'column_separator', ';'
        set 'columns', '''
            id, kbool, ktint, ksint, kint, kbint, klint, kfloat, kdbl, kdcmls1, kdcmls2, kdcmls3,
            kdcmlv3s1, kdcmlv3s2, kdcmlv3s3, kchrs1, kchrs2, kchrs3, kvchrs1, kvchrs2, kvchrs3, kstr,
            kdt, kdtv2, kdtm, kdtmv2s1, kdtmv2s2, kdtmv2s3, kabool, katint, kasint, kaint,
            kabint, kalint, kafloat, kadbl, kadt, kadtm, kadtv2, kadtmv2, kachr, kavchr, kastr, kadcml,
            st_point_str, st_point_vc, x_lng, x_lat, y_lng, y_lat, z_lng, z_lat, radius, linestring_wkt, polygon_wkt,
            km_bool_tint, km_tint_tint, km_sint_tint, km_int_tint, km_bint_tint, km_lint_tint, km_float_tint,
            km_dbl_tint, km_dcml_tint, km_chr_tint, km_vchr_tint, km_str_tint, km_date_tint, km_dtm_tint,
            km_tint_bool, km_int_int, km_tint_sint, km_tint_int, km_tint_bint, km_tint_lint, km_tint_float,
            km_tint_dbl, km_tint_dcml, km_tint_chr, km_tint_vchr, km_tint_str, km_tint_date, km_tint_dtm, kjson, kstruct
            '''
        file "agg_mv_test.dat"
    }


	sql """ use regression_test_nereids_syntax_p1_mv_aggregate """
    sql """ SET enable_nereids_planner=true """
    sql """ SET enable_fallback_to_original_planner=false """
    sql """ analyze table agg_mv_test with sync"""
    sql """ set enable_stats=false"""

    qt_select_any_value """select id, any_value(kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync1 on agg_mv_test;"""
    createMV("""create materialized view mv_sync1 as select id as a1, any_value(kint) from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, any_value(kint) from agg_mv_test group by id order by id;", "mv_sync1")
    qt_select_any_value_mv """select id, any_value(kint) from agg_mv_test group by id order by id;"""

    // sum_foreach is not supported in old planner
    // qt_select_sum_foreach """select id, sum_foreach(kaint) from agg_mv_test group by id order by id;"""
    // sql """drop materialized view if exists mv_sync2 on agg_mv_test;"""
    // createMV("""create materialized view mv_sync2 as select id, sum_foreach(kaint) from agg_mv_test group by id order by id;""")
    // explain {
    //     sql("select id, sum_foreach(kaint) from agg_mv_test group by id order by id;")
    //     contains "(mv_sync2)"
    // }
    // qt_select_sum_foreach_mv """select id, sum_foreach(kaint) from agg_mv_test group by id order by id;"""

    qt_select_approx_count_distinct """select id, approx_count_distinct(kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync3 on agg_mv_test;"""
    createMV("""create materialized view mv_sync3 as select id as a2, approx_count_distinct(kint) from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, approx_count_distinct(kint) from agg_mv_test group by id order by id;", "mv_sync3")
    qt_select_approx_count_distinct_mv """select id, approx_count_distinct(kint) from agg_mv_test group by id order by id;"""

    qt_select_collect_set """select id, collect_set(kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync4 on agg_mv_test;"""
    createMV("""create materialized view mv_sync4 as select id as a3, collect_set(kint) from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, collect_set(kint) from agg_mv_test group by id order by id;", "mv_sync4")
    qt_select_collect_set_mv """select id, collect_set(kint) from agg_mv_test group by id order by id;"""

    qt_select_collect_list """select id, collect_list(kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync5 on agg_mv_test;"""
    createMV("""create materialized view mv_sync5 as select id as a4, collect_list(kint) from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, collect_list(kint) from agg_mv_test group by id order by id;", "mv_sync5")
    qt_select_collect_list_mv """select id, collect_list(kint) from agg_mv_test group by id order by id;"""

    qt_select_corr """select id, corr(kint, kbint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync6 on agg_mv_test;"""
    createMV("""create materialized view mv_sync6 as select id as a5, corr(kint, kbint) from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, corr(kint, kbint) from agg_mv_test group by id order by id;", "mv_sync6")
    qt_select_corr_mv """select id, corr(kint, kbint) from agg_mv_test group by id order by id;"""

    qt_select_percentile_array """select id, percentile_array(kint, [0.5,0.55,0.805]) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync7 on agg_mv_test;"""
    createMV("""create materialized view mv_sync7 as select id as a6, percentile_array(kint, [0.5,0.55,0.805]) from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, percentile_array(kint, [0.5,0.55,0.805]) from agg_mv_test group by id order by id;", "mv_sync7")
    qt_select_percentile_array_mv """select id, percentile_array(kint, [0.5,0.55,0.805]) from agg_mv_test group by id order by id;"""

    qt_select_quantile_union """select id, quantile_union(to_quantile_state(kbint, 2048)) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync8 on agg_mv_test;"""
    createMV("""create materialized view mv_sync8 as select id as b1, quantile_union(to_quantile_state(kbint, 2048)) from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, quantile_union(to_quantile_state(kbint, 2048)) from agg_mv_test group by id order by id;",
            "mv_sync8")
    qt_select_quantile_union_mv """select id, quantile_union(to_quantile_state(kbint, 2048)) from agg_mv_test group by id order by id;"""

    qt_select_count_by_enum """select id, count_by_enum(kstr) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync9 on agg_mv_test;"""
    createMV("""create materialized view mv_sync9 as select id as b2, count_by_enum(kstr) from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, count_by_enum(kstr) from agg_mv_test group by id order by id;", "mv_sync9")
    qt_select_count_by_enum_mv """select id, count_by_enum(kstr) from agg_mv_test group by id order by id;"""

    qt_select_avg_weighted """select id, avg_weighted(ktint, kdbl) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync10 on agg_mv_test;"""
    createMV("""create materialized view mv_sync10 as select id as b3, avg_weighted(ktint, kdbl) from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, avg_weighted(ktint, kdbl) from agg_mv_test group by id order by id;", "mv_sync10")
    qt_select_avg_weighted_mv """select id, avg_weighted(ktint, kdbl) from agg_mv_test group by id order by id;"""

    qt_select_bitmap_intersect """select id, bitmap_intersect(bitmap_hash(kbint)) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync11 on agg_mv_test;"""
    createMV("""create materialized view mv_sync11 as select id as b4, bitmap_intersect(bitmap_hash(kbint)) from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, bitmap_intersect(bitmap_hash(kbint)) from agg_mv_test group by id order by id;", "mv_sync11")
    qt_select_bitmap_intersect_mv """select id, bitmap_intersect(bitmap_hash(kbint)) from agg_mv_test group by id order by id;"""

    qt_select_bitmap_agg """select id, bitmap_agg(kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync12 on agg_mv_test;"""
    createMV("""create materialized view mv_sync12 as select id as b5, bitmap_agg(kint) from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, bitmap_agg(kint) from agg_mv_test group by id order by id;", "mv_sync12")
    qt_select_bitmap_agg_mv """select id, bitmap_agg(kint) from agg_mv_test group by id order by id;"""

    qt_select_bitmap_union """select id, bitmap_union(bitmap_hash(kbint)) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync13 on agg_mv_test;"""
    createMV("""create materialized view mv_sync13 as select id as b6, bitmap_union(bitmap_hash(kbint)) from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, bitmap_union(bitmap_hash(kbint)) from agg_mv_test group by id order by id;", "mv_sync13")
    qt_select_bitmap_union_mv """select id, bitmap_union(bitmap_hash(kbint)) from agg_mv_test group by id order by id;"""

    qt_select_bitmap_union_count """select id, bitmap_union_count(bitmap_hash(kbint)) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync14 on agg_mv_test;"""
    createMV("""create materialized view mv_sync14 as select id as c1, bitmap_union_count(bitmap_hash(kbint)) from agg_mv_test group by id order by id;""")
    mv_rewrite_any_success("select id, bitmap_union_count(bitmap_hash(kbint)) from agg_mv_test group by id order by id;", ["mv_sync13", "mv_sync14"])
    qt_select_bitmap_union_count_mv """select id, bitmap_union_count(bitmap_hash(kbint)) from agg_mv_test group by id order by id;"""

    qt_select_bitmap_union_int """select id, bitmap_union_int(kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync15 on agg_mv_test;"""
    createMV("""create materialized view mv_sync15 as select id as c2, bitmap_union_int(kint) from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, bitmap_union_int(kint) from agg_mv_test group by id order by id;", "mv_sync15")
    qt_select_bitmap_union_int_mv """select id, bitmap_union_int(kint) from agg_mv_test group by id order by id;"""

    qt_select_group_array_intersect """select id, group_array_intersect(kaint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync16 on agg_mv_test;"""
    createMV("""create materialized view mv_sync16 as select id as c3, group_array_intersect(kaint) from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, group_array_intersect(kaint) from agg_mv_test group by id order by id;", "mv_sync16")
    qt_select_group_array_intersect_mv """select id, group_array_intersect(kaint) from agg_mv_test group by id order by id;"""

    qt_select_group_bit_and """select id, group_bit_and(kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync17 on agg_mv_test;"""
    createMV("""create materialized view mv_sync17 as select id as c4, group_bit_and(kint) from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, group_bit_and(kint) from agg_mv_test group by id order by id;", "mv_sync17")
    qt_select_group_bit_and_mv """select id, group_bit_and(kint) from agg_mv_test group by id order by id;"""

    qt_select_group_bit_or """select id, group_bit_or(kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync18 on agg_mv_test;"""
    createMV("""create materialized view mv_sync18 as select id as c5, group_bit_or(kint) from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, group_bit_or(kint) from agg_mv_test group by id order by id;", "mv_sync18")
    qt_select_group_bit_or_mv """select id, group_bit_or(kint) from agg_mv_test group by id order by id;"""

    qt_select_group_bit_xor """select id, group_bit_xor(kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync19 on agg_mv_test;"""
    createMV("""create materialized view mv_sync19 as select id as c6, group_bit_xor(kint) from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, group_bit_xor(kint) from agg_mv_test group by id order by id;", "mv_sync19")
    qt_select_group_bit_xor_mv """select id, group_bit_xor(kint) from agg_mv_test group by id order by id;"""

    qt_select_group_bitmap_xor """select id, group_bitmap_xor(bitmap_hash(kbint)) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync20 on agg_mv_test;"""
    createMV("""create materialized view mv_sync20 as select id as d1, group_bitmap_xor(bitmap_hash(kbint)) from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, group_bitmap_xor(bitmap_hash(kbint)) from agg_mv_test group by id order by id;", "mv_sync20")
    qt_select_group_bitmap_xor_mv """select id, group_bitmap_xor(bitmap_hash(kbint)) from agg_mv_test group by id order by id;"""

    qt_select_hll_union_agg """select id, hll_union_agg(hll_hash(kbint)) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync21 on agg_mv_test;"""
    createMV("""create materialized view mv_sync21 as select id as d2, hll_union_agg(hll_hash(kbint)) from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, hll_union_agg(hll_hash(kbint)) from agg_mv_test group by id order by id;", "mv_sync21")
    qt_select_hll_union_agg_mv """select id, hll_union_agg(hll_hash(kbint)) from agg_mv_test group by id order by id;"""

    qt_select_hll_union """select id, hll_union(hll_hash(kbint)) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync22 on agg_mv_test;"""
    createMV("""create materialized view mv_sync22 as select id as d3, hll_union(hll_hash(kbint)) from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, hll_union(hll_hash(kbint)) from agg_mv_test group by id order by id;", "mv_sync22")
    qt_select_hll_union_mv """select id, hll_union(hll_hash(kbint)) from agg_mv_test group by id order by id;"""

    qt_select_intersect_count """select id, intersect_count(bitmap_hash(kbint), kint, 3, 4) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync23 on agg_mv_test;"""
    createMV("""create materialized view mv_sync23 as select id as d4, intersect_count(bitmap_hash(kbint), kint, 3, 4) from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, intersect_count(bitmap_hash(kbint), kint, 3, 4) from agg_mv_test group by id order by id;", "mv_sync23")
    qt_select_intersect_count_mv """select id, intersect_count(bitmap_hash(kbint), kint, 3, 4) from agg_mv_test group by id order by id;"""

    qt_select_group_concat """select id, group_concat(cast(abs(kint) as varchar)) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync24 on agg_mv_test;"""
    createMV("""create materialized view mv_sync24 as select id as d5, group_concat(cast(abs(kint) as varchar)) from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, group_concat(cast(abs(kint) as varchar)) from agg_mv_test group by id order by id;", "mv_sync24")
    qt_select_group_concat_mv """select id, group_concat(cast(abs(kint) as varchar)) from agg_mv_test group by id order by id;"""

    qt_select_multi_distinct_group_concat """select id, multi_distinct_group_concat(cast(abs(kint) as varchar)) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync25 on agg_mv_test;"""
    createMV("""create materialized view mv_sync25 as select id as d6, multi_distinct_group_concat(cast(abs(kint) as varchar)) from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, multi_distinct_group_concat(cast(abs(kint) as varchar)) from agg_mv_test group by id order by id;", "mv_sync25")
    qt_select_multi_distinct_group_concat_mv """select id, multi_distinct_group_concat(cast(abs(kint) as varchar)) from agg_mv_test group by id order by id;"""

    qt_select_multi_distinct_sum0 """select id, multi_distinct_sum0(kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync26 on agg_mv_test;"""
    createMV("""create materialized view mv_sync26 as select id as e1, multi_distinct_sum0(kint) from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, multi_distinct_sum0(kint) from agg_mv_test group by id order by id;", "mv_sync26")
    qt_select_multi_distinct_sum0_mv """select id, multi_distinct_sum0(kint) from agg_mv_test group by id order by id;"""

    qt_select_multi_distinct_sum """select id, multi_distinct_sum(kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync27 on agg_mv_test;"""
    createMV("""create materialized view mv_sync27 as select id as e2, multi_distinct_sum(kint) from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, multi_distinct_sum(kint) from agg_mv_test group by id order by id;", "mv_sync27")
    qt_select_multi_distinct_sum_mv """select id, multi_distinct_sum(kint) from agg_mv_test group by id order by id;"""


    qt_select_histogram """select id, histogram(kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync28 on agg_mv_test;"""
    createMV("""create materialized view mv_sync28 as select id as e3, histogram(kint) from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, histogram(kint) from agg_mv_test group by id order by id;", "mv_sync28")
    qt_select_histogram_mv """select id, histogram(kint) from agg_mv_test group by id order by id;"""

    qt_select_max_by """select id, max_by(kint, kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync29 on agg_mv_test;"""
    createMV("""create materialized view mv_sync29 as select id as e4, max_by(kint, kint) from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, max_by(kint, kint) from agg_mv_test group by id order by id;", "mv_sync29")
    qt_select_max_by_mv """select id, max_by(kint, kint) from agg_mv_test group by id order by id;"""

    qt_select_min_by """select id, min_by(kint, kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync30 on agg_mv_test;"""
    createMV("""create materialized view mv_sync30 as select id as e5, min_by(kint, kint) from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, min_by(kint, kint) from agg_mv_test group by id order by id;", "mv_sync30")
    qt_select_min_by_mv """select id, min_by(kint, kint) from agg_mv_test group by id order by id;"""

    qt_select_multi_distinct_count """select id, multi_distinct_count(kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync31 on agg_mv_test;"""
    createMV("""create materialized view mv_sync31 as select id as e6, multi_distinct_count(kint) from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, multi_distinct_count(kint) from agg_mv_test group by id order by id;", "mv_sync31")
    qt_select_multi_distinct_count_mv """select id, multi_distinct_count(kint) from agg_mv_test group by id order by id;"""

    qt_select_ndv """select id, ndv(kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync32 on agg_mv_test;"""
    createMV("""create materialized view mv_sync32 as select id as f1, ndv(kint) as xx1 from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, ndv(kint) from agg_mv_test group by id order by id;", "mv_sync32")
    qt_select_ndv_mv """select id, ndv(kint) from agg_mv_test group by id order by id;"""

    qt_select_covar """select id, covar(kint, kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync33 on agg_mv_test;"""
    createMV("""create materialized view mv_sync33 as select id as f2, covar(kint, kint) as xx2 from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, covar(kint, kint) from agg_mv_test group by id order by id;", "mv_sync33")
    qt_select_covar_mv """select id, covar(kint, kint) from agg_mv_test group by id order by id;"""

    qt_select_covar_samp """select id, covar_samp(kint, kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync34 on agg_mv_test;"""
    createMV("""create materialized view mv_sync34 as select id as f3, covar_samp(kint, kint) from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, covar_samp(kint, kint) from agg_mv_test group by id order by id;", "mv_sync34")
    qt_select_covar_samp_mv """select id, covar_samp(kint, kint) from agg_mv_test group by id order by id;"""

    qt_select_percentile """select id, percentile(kbint, 0.6) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync35 on agg_mv_test;"""
    createMV("""create materialized view mv_sync35 as select id as f4, percentile(kbint, 0.6) as xx3 from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, percentile(kbint, 0.6) from agg_mv_test group by id order by id;", "mv_sync35")
    qt_select_percentile_mv """select id, percentile(kbint, 0.6) from agg_mv_test group by id order by id;"""

    qt_select_percentile_approx """select id, percentile_approx(kbint, 0.6) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync36 on agg_mv_test;"""
    createMV("""create materialized view mv_sync36 as select id as f5, percentile_approx(kbint, 0.6) from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, percentile_approx(kbint, 0.6) from agg_mv_test group by id order by id;", "mv_sync36")
    qt_select_percentile_approx_mv """select id, percentile_approx(kbint, 0.6) from agg_mv_test group by id order by id;"""

    // percentile_approx_weighted is not supported in old planner
    // qt_select_percentile_approx_weighted """select id, percentile_approx_weighted(kint, kbint, 0.6) from agg_mv_test group by id order by id;"""
    // sql """drop materialized view if exists mv_sync37 on agg_mv_test;"""
    // createMV("""create materialized view mv_sync37 as select id, percentile_approx_weighted(kint, kbint, 0.6) from agg_mv_test group by id order by id;""")
    // explain {
    //     sql("select id, percentile_approx_weighted(kint, kbint, 0.6) from agg_mv_test group by id order by id;")
    //     contains "(mv_sync37)"
    // }
    // qt_select_percentile_approx_weighted_mv """select id, percentile_approx_weighted(kint, kbint, 0.6) from agg_mv_test group by id order by id;"""

    qt_select_sequence_count """select id, sequence_count('(?1)(?2)', kdtv2, kint = 1, kint = 2) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync38 on agg_mv_test;"""
    createMV("""create materialized view mv_sync38 as select id as f6, sequence_count('(?1)(?2)', kdtv2, kint = 1, kint = 2) from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, sequence_count('(?1)(?2)', kdtv2, kint = 1, kint = 2) from agg_mv_test group by id order by id;", "mv_sync38")
    qt_select_sequence_count_mv """select id, sequence_count('(?1)(?2)', kdtv2, kint = 1, kint = 2) from agg_mv_test group by id order by id;"""

    qt_select_sequence_match """select id, sequence_match('(?1)(?2)', kdtv2, kint = 1, kint = 2) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync39 on agg_mv_test;"""
    createMV("""create materialized view mv_sync39 as select id as g1, sequence_match('(?1)(?2)', kdtv2, kint = 1, kint = 2) from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, sequence_match('(?1)(?2)', kdtv2, kint = 1, kint = 2) from agg_mv_test group by id order by id;", "mv_sync39")
    qt_select_sequence_match_mv """select id, sequence_match('(?1)(?2)', kdtv2, kint = 1, kint = 2) from agg_mv_test group by id order by id;"""

    qt_select_stddev """select id, stddev(kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync40 on agg_mv_test;"""
    createMV("""create materialized view mv_sync40 as select id as g2, stddev(kint) from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, stddev(kint) from agg_mv_test group by id order by id;", "mv_sync40")
    qt_select_stddev_mv """select id, stddev(kint) from agg_mv_test group by id order by id;"""

    qt_select_stddev_pop """select id, stddev_pop(kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync41 on agg_mv_test;"""
    createMV("""create materialized view mv_sync41 as select id as g3, stddev_pop(kint) from agg_mv_test group by id order by id;""")
    mv_rewrite_any_success("select id, stddev_pop(kint) from agg_mv_test group by id order by id;", ["mv_sync40", "mv_sync41"])
    qt_select_stddev_pop_mv """select id, stddev_pop(kint) from agg_mv_test group by id order by id;"""

    qt_select_stddev_samp """select id, stddev_samp(kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync42 on agg_mv_test;"""
    createMV("""create materialized view mv_sync42 as select id as g4, stddev_samp(kint) from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, stddev_samp(kint) from agg_mv_test group by id order by id;", "mv_sync42")
    qt_select_stddev_samp_mv """select id, stddev_samp(kint) from agg_mv_test group by id order by id;"""

    qt_select_sum0 """select id, sum0(kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync43 on agg_mv_test;"""
    createMV("""create materialized view mv_sync43 as select id as g5, sum0(kint) from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, sum0(kint) from agg_mv_test group by id order by id;", "mv_sync43")
    qt_select_sum0_mv """select id, sum0(kint) from agg_mv_test group by id order by id;"""

    qt_select_topn """select id, topn(kvchrs1, 3) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync44 on agg_mv_test;"""
    createMV("""create materialized view mv_sync44 as select id as g6, topn(kvchrs1, 3) from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, topn(kvchrs1, 3) from agg_mv_test group by id order by id;", "mv_sync44")
    qt_select_topn_mv """select id, topn(kvchrs1, 3) from agg_mv_test group by id order by id;"""

    qt_select_topn_array """select id, topn_array(kvchrs1, 3) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync45 on agg_mv_test;"""
    createMV("""create materialized view mv_sync45 as select id as s1, topn_array(kvchrs1, 3) from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, topn_array(kvchrs1, 3) from agg_mv_test group by id order by id;", "mv_sync45")
    qt_select_topn_array_mv """select id, topn_array(kvchrs1, 3) from agg_mv_test group by id order by id;"""

    qt_select_topn_weighted """select id, topn_weighted(kvchrs1, ktint, 3) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync46 on agg_mv_test;"""
    createMV("""create materialized view mv_sync46 as select id as s2, topn_weighted(kvchrs1, ktint, 3) from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, topn_weighted(kvchrs1, ktint, 3) from agg_mv_test group by id order by id;", "mv_sync46")
    qt_select_topn_weighted_mv """select id, topn_weighted(kvchrs1, ktint, 3) from agg_mv_test group by id order by id;"""

    qt_select_variance """select id, variance(kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync47 on agg_mv_test;"""
    createMV("""create materialized view mv_sync47 as select id as s3, variance(kint) from agg_mv_test group by id order by id;""")
    mv_rewrite_any_success("select id, variance(kint) from agg_mv_test group by id order by id;", ["mv_sync47", "mv_sync48"])
    qt_select_variance_mv """select id, variance(kint) from agg_mv_test group by id order by id;"""

    qt_select_var_pop """select id, var_pop(kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync48 on agg_mv_test;"""
    createMV("""create materialized view mv_sync48 as select id as s4, var_pop(kint) from agg_mv_test group by id order by id;""")
    mv_rewrite_any_success("select id, var_pop(kint) from agg_mv_test group by id order by id;", ["mv_sync47", "mv_sync48"])
    qt_select_var_pop_mv """select id, var_pop(kint) from agg_mv_test group by id order by id;"""

    qt_select_variance_samp """select id, variance_samp(kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync49 on agg_mv_test;"""
    createMV("""create materialized view mv_sync49 as select id as s5, variance_samp(kint) from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, variance_samp(kint) from agg_mv_test group by id order by id;", "mv_sync49")
    qt_select_variance_samp_mv """select id, variance_samp(kint) from agg_mv_test group by id order by id;"""

    qt_select_var_samp """select id, var_samp(kint) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync50 on agg_mv_test;"""
    createMV("""create materialized view mv_sync50 as select id as s6, var_samp(kint) from agg_mv_test group by id order by id;""")
    mv_rewrite_any_success("select id, var_samp(kint) from agg_mv_test group by id order by id;", ["mv_sync49", "mv_sync50"])
    qt_select_var_samp_mv """select id, var_samp(kint) from agg_mv_test group by id order by id;"""

    qt_select_window_funnel """select id, window_funnel(3600 * 3, 'default', kdtm, kint = 1, kint = 2) from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync51 on agg_mv_test;"""
    createMV("""create materialized view mv_sync51 as select id as q1, window_funnel(3600 * 3, 'default', kdtm, kint = 1, kint = 2) from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, window_funnel(3600 * 3, 'default', kdtm, kint = 1, kint = 2) from agg_mv_test group by id order by id;", "mv_sync51")
    qt_select_window_funnel_mv """select id, window_funnel(3600 * 3, 'default', kdtm, kint = 1, kint = 2) from agg_mv_test group by id order by id;"""

    // map_agg is not supported yet
    // qt_select_map_agg """select id, map_agg(kint, kstr) from agg_mv_test group by id order by id;"""
    // sql """drop materialized view if exists mv_sync52 on agg_mv_test;"""
    // createMV("""create materialized view mv_sync52 as select id, map_agg(kint, kstr) from agg_mv_test group by id order by id;""")
    // explain {
    //     sql("select id, map_agg(kint, kstr) from agg_mv_test group by id order by id;")
    //     contains "(mv_sync52)"
    // }
    // qt_select_map_agg_mv """select id, map_agg(kint, kstr) from agg_mv_test group by id order by id;"""

    // array_agg is not supported yet
    // qt_select_array_agg """select id, array_agg(kstr) from agg_mv_test group by id order by id;"""
    // sql """drop materialized view if exists mv_sync53 on agg_mv_test;"""
    // createMV("""create materialized view mv_sync53 as select id, array_agg(kstr) from agg_mv_test group by id order by id;""")
    // explain {
    //     sql("select id, array_agg(kstr) from agg_mv_test group by id order by id;")
    //     contains "(mv_sync53)"
    // }
    // qt_select_array_agg_mv """select id, array_agg(kstr) from agg_mv_test group by id order by id;"""

    qt_select_retention """select id, retention(kdtm = '2012-03-11', kdtm = '2012-03-12') from agg_mv_test group by id order by id;"""
    sql """drop materialized view if exists mv_sync54 on agg_mv_test;"""
    createMV("""create materialized view mv_sync54 as select id as q2, retention(kdtm = '2012-03-11', kdtm = '2012-03-12') from agg_mv_test group by id order by id;""")
    mv_rewrite_success("select id, retention(kdtm = '2012-03-11', kdtm = '2012-03-12') from agg_mv_test group by id order by id;", "mv_sync54")
    qt_select_retention_mv """select id, retention(kdtm = '2012-03-11', kdtm = '2012-03-12') from agg_mv_test group by id order by id;"""


    streamLoad {
        table "agg_mv_test"
        db "regression_test_nereids_syntax_p1_mv"
        set 'column_separator', ';'
        set 'columns', '''
            id, kbool, ktint, ksint, kint, kbint, klint, kfloat, kdbl, kdcmls1, kdcmls2, kdcmls3,
            kdcmlv3s1, kdcmlv3s2, kdcmlv3s3, kchrs1, kchrs2, kchrs3, kvchrs1, kvchrs2, kvchrs3, kstr,
            kdt, kdtv2, kdtm, kdtmv2s1, kdtmv2s2, kdtmv2s3, kabool, katint, kasint, kaint,
            kabint, kalint, kafloat, kadbl, kadt, kadtm, kadtv2, kadtmv2, kachr, kavchr, kastr, kadcml,
            st_point_str, st_point_vc, x_lng, x_lat, y_lng, y_lat, z_lng, z_lat, radius, linestring_wkt, polygon_wkt,
            km_bool_tint, km_tint_tint, km_sint_tint, km_int_tint, km_bint_tint, km_lint_tint, km_float_tint,
            km_dbl_tint, km_dcml_tint, km_chr_tint, km_vchr_tint, km_str_tint, km_date_tint, km_dtm_tint,
            km_tint_bool, km_int_int, km_tint_sint, km_tint_int, km_tint_bint, km_tint_lint, km_tint_float,
            km_tint_dbl, km_tint_dcml, km_tint_chr, km_tint_vchr, km_tint_str, km_tint_date, km_tint_dtm, kjson, kstruct
            '''
        file "../agg_mv_test.dat"
    }


    sql "insert into agg_mv_test select * from agg_mv_test;"
    sql "insert into agg_mv_test select * from agg_mv_test;"
    sql "insert into agg_mv_test select * from agg_mv_test;"
    sql "insert into agg_mv_test select * from agg_mv_test;"
    sql "insert into agg_mv_test select * from agg_mv_test;"
    sql "insert into agg_mv_test select * from agg_mv_test;"
    sql "insert into agg_mv_test select * from agg_mv_test;"
    sql "insert into agg_mv_test select * from agg_mv_test;"
    sql "insert into agg_mv_test select * from agg_mv_test;"
    sql "insert into agg_mv_test select * from agg_mv_test;"
    sql "insert into agg_mv_test select * from agg_mv_test;"
    sql "insert into agg_mv_test select * from agg_mv_test;"
    sql "insert into agg_mv_test select * from agg_mv_test;"

    sql "set parallel_pipeline_task_num=1"
    qt_test "select kbint, map_agg(id, kstr) from agg_mv_test group by kbint order by kbint;"
}
