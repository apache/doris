/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

suite("agg_nullable_2") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"
    sql "drop table if exists agg_nullable_test_2"
    sql """
        CREATE TABLE IF NOT EXISTS `agg_nullable_test_2` (
            `id` int null,
            `kntint` tinyint(4) null,
            `knint` int(11) null,
            `knbint` bigint(20) null,
            `kndbl` double null,
            `knvchrs1` varchar(10) null,
            `knstr` string null,
            `kndtv2` datev2 null,
            `kndtm` datetime null,
            `knaint` array<int> null,
            `knabint` array<bigint(20)> null,
            `ktint` tinyint(4) not null,
            `kint` int(11) not null,
            `kbint` bigint(20) not null,
            `kdbl` double not null,
            `kvchrs1` varchar(10) not null,
            `kstr` string not null,
            `kdtv2` datev2 not null,
            `kdtm` datetime not null,
            `kaint` array<int> not null,
            `kabint` array<bigint(20)> not null
        ) engine=olap
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        properties("replication_num" = "1");
    """
    sql """
        INSERT INTO `agg_nullable_test_2` (
            `id`, `kntint`, `knint`, `knbint`, `kndbl`, `knvchrs1`, `knstr`, 
            `kndtv2`, `kndtm`, `knaint`, `knabint`, `ktint`, `kint`, `kbint`, 
            `kdbl`, `kvchrs1`, `kstr`, `kdtv2`, `kdtm`, `kaint`, `kabint`
        ) VALUES (
            1, 10, 100, 1000, 10.5, 'text', 'string', 
            '2023-01-01', '2023-01-01 12:00:00', [1, 2, 3], [1000, 2000], 5, 50, 5000, 
            15.5, 'non-null', 'non-null string', '2023-02-01', '2023-02-01 14:00:00', [4, 5, 6], [3000, 4000]
        );
    """
    sql """ select * from agg_nullable_test_2 order by 1;"""
    qt_select_any_value """select any_value(kint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select any_value(kint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=int, nullable=true"
    }

    qt_select_any_value2 """select any_value(kint) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select any_value(kint) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=int, nullable=false"
    }

    qt_select_any_value_n """select any_value(knint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select any_value(knint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=int, nullable=true"
    }

    qt_select_sum_foreach """select sum_foreach(kaint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select sum_foreach(kaint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=array<bigint>, nullable=true"
    }

    qt_select_sum_foreach2 """select sum_foreach(kaint) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select sum_foreach(kaint) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=array<bigint>, nullable=false"
    }

    qt_select_sum_foreach_n """select sum_foreach(knaint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select sum_foreach(knaint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=array<bigint>, nullable=true"
    }

    qt_select_approx_count_distinct """select approx_count_distinct(kint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select approx_count_distinct(kint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=bigint, nullable=false"
    }

    qt_select_approx_count_distinct2 """select approx_count_distinct(kint) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select approx_count_distinct(kint) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=bigint, nullable=false"
    }

    qt_select_approx_count_distinct_n """select approx_count_distinct(knint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select approx_count_distinct(knint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=bigint, nullable=false"
    }

    qt_select_collect_set """select collect_set(kint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select collect_set(kint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=array<int>, nullable=false"
    }

    qt_select_collect_set2 """select collect_set(kint) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select collect_set(kint) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=array<int>, nullable=false"
    }

    qt_select_collect_set_n """select collect_set(knint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select collect_set(knint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=array<int>, nullable=false"
    }

    qt_select_collect_list """select collect_list(kint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select collect_list(kint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=array<int>, nullable=false"
    }

    qt_select_collect_list2 """select collect_list(kint) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select collect_list(kint) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=array<int>, nullable=false"
    }

    qt_select_collect_list_n """select collect_list(knint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select collect_list(knint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=array<int>, nullable=false"
    }
    
    qt_select_corr """select corr(kint, kbint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select corr(kint, kbint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=double, nullable=true"
    }

    qt_select_corr2 """select corr(kint, kbint) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select corr(kint, kbint) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=double, nullable=false"
    }

    qt_select_corr_n """select corr(knint, knbint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select corr(knint, knbint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=double, nullable=true"
    }

    qt_select_percentile_array """select percentile_array(kint, [0.5,0.55,0.805]) from agg_nullable_test_2;"""
    explain {
        sql("verbose select percentile_array(kint, [0.5,0.55,0.805]) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=array<double>, nullable=false"
    }

    qt_select_percentile_array2 """select percentile_array(kint, [0.5,0.55,0.805]) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select percentile_array(kint, [0.5,0.55,0.805]) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=array<double>, nullable=false"
    }

    qt_select_percentile_array_n """select percentile_array(knint, [0.5,0.55,0.805]) from agg_nullable_test_2;"""
    explain {
        sql("verbose select percentile_array(knint, [0.5,0.55,0.805]) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=array<double>, nullable=false"
    }

    qt_select_quantile_union """select quantile_union(to_quantile_state(kbint, 2048)) from agg_nullable_test_2;"""
    explain {
        sql("verbose select quantile_union(to_quantile_state(kbint, 2048)) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=quantile_state, nullable=false"
    }

    qt_select_quantile_union2 """select quantile_union(to_quantile_state(kbint, 2048)) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select quantile_union(to_quantile_state(kbint, 2048)) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=quantile_state, nullable=false"
    }

    qt_select_quantile_union_n """select quantile_union(to_quantile_state(knbint, 2048)) from agg_nullable_test_2;"""
    explain {
        sql("verbose select quantile_union(to_quantile_state(knbint, 2048)) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=quantile_state, nullable=false"
    }
    
    qt_select_count_by_enum """select count_by_enum(kstr) from agg_nullable_test_2;"""
    explain {
        sql("verbose select count_by_enum(kstr) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=text, nullable=false"
    }

    qt_select_count_by_enum2 """select count_by_enum(kstr) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select count_by_enum(kstr) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=text, nullable=false"
    }

    qt_select_count_by_enum_n """select count_by_enum(knstr) from agg_nullable_test_2;"""
    explain {
        sql("verbose select count_by_enum(knstr) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=text, nullable=false"
    }

    qt_select_avg_weighted """select avg_weighted(ktint, kdbl) from agg_nullable_test_2;"""
    explain {
        sql("verbose select avg_weighted(ktint, kdbl) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=double, nullable=true"
    }

    qt_select_avg_weighted2 """select avg_weighted(ktint, kdbl) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select avg_weighted(ktint, kdbl) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=double, nullable=false"
    }

    qt_select_avg_weighted_n """select avg_weighted(kntint, kndbl) from agg_nullable_test_2;"""
    explain {
        sql("verbose select avg_weighted(kntint, kndbl) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=double, nullable=true"
    }

    qt_select_bitmap_intersect """select bitmap_intersect(bitmap_hash(kbint)) from agg_nullable_test_2;"""
    explain {
        sql("verbose select bitmap_intersect(bitmap_hash(kbint)) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=bitmap, nullable=false"
    }

    qt_select_bitmap_intersect2 """select bitmap_intersect(bitmap_hash(kbint)) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select bitmap_intersect(bitmap_hash(kbint)) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=bitmap, nullable=false"
    }

    qt_select_bitmap_intersect_n """select bitmap_intersect(bitmap_hash(knbint)) from agg_nullable_test_2;"""
    explain {
        sql("verbose select bitmap_intersect(bitmap_hash(knbint)) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=bitmap, nullable=false"
    }

    qt_select_bitmap_agg """select bitmap_agg(kint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select bitmap_agg(kint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=bitmap, nullable=false"
    }

    qt_select_bitmap_agg2 """select bitmap_agg(kint) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select bitmap_agg(kint) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=bitmap, nullable=false"
    }

    qt_select_bitmap_agg_n """select bitmap_agg(kbint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select bitmap_agg(kbint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=bitmap, nullable=false"
    }

    qt_select_bitmap_union """select bitmap_union(bitmap_hash(kbint)) from agg_nullable_test_2;"""
    explain {
        sql("verbose select bitmap_union(bitmap_hash(kbint)) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=bitmap, nullable=false"
    }

    qt_select_bitmap_union2 """select bitmap_union(bitmap_hash(kbint)) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select bitmap_union(bitmap_hash(kbint)) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=bitmap, nullable=false"
    }

    qt_select_bitmap_union_n """select bitmap_union(bitmap_hash(knbint)) from agg_nullable_test_2;"""
    explain {
        sql("verbose select bitmap_union(bitmap_hash(knbint)) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=bitmap, nullable=false"
    }
    
    qt_select_bitmap_union_count """select bitmap_union_count(bitmap_hash(kbint)) from agg_nullable_test_2;"""
    explain {
        sql("verbose select bitmap_union_count(bitmap_hash(kbint)) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=bigint, nullable=false"
    }

    qt_select_bitmap_union_count2 """select bitmap_union_count(bitmap_hash(kbint)) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select bitmap_union_count(bitmap_hash(kbint)) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=bigint, nullable=false"
    }

    qt_select_bitmap_union_count_n """select bitmap_union_count(bitmap_hash(knbint)) from agg_nullable_test_2;"""
    explain {
        sql("verbose select bitmap_union_count(bitmap_hash(knbint)) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=bigint, nullable=false"
    }

    qt_select_bitmap_union_int """select bitmap_union_int(kint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select bitmap_union_int(kint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=bigint, nullable=false"
    }

    qt_select_bitmap_union_int2 """select bitmap_union_int(kint) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select bitmap_union_int(kint) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=bigint, nullable=false"
    }

    qt_select_bitmap_union_int_n """select bitmap_union_int(knint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select bitmap_union_int(knint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=bigint, nullable=false"
    }
    
    qt_select_group_array_intersect """select array_sort(group_array_intersect(kaint)) from agg_nullable_test_2;"""
    explain {
        sql("verbose select group_array_intersect(kaint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=array<int>, nullable=false"
    }

    qt_select_group_array_intersect2 """select array_sort(group_array_intersect(kaint)) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select group_array_intersect(kaint) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=array<int>, nullable=false"
    }

    qt_select_group_array_intersect_n """select array_sort(group_array_intersect(knaint)) from agg_nullable_test_2;"""
    explain {
        sql("verbose select group_array_intersect(knaint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=array<int>, nullable=false"
    }

    qt_select_group_bit_and """select group_bit_and(kint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select group_bit_and(kint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=int, nullable=true"
    }

    qt_select_group_bit_and2 """select group_bit_and(kint) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select group_bit_and(kint) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=int, nullable=false"
    }

    qt_select_group_bit_and_n """select group_bit_and(knint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select group_bit_and(knint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=int, nullable=true"
    }

    qt_select_group_bit_or """select group_bit_or(kint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select group_bit_or(kint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=int, nullable=true"
    }

    qt_select_group_bit_or2 """select group_bit_or(kint) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select group_bit_or(kint) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=int, nullable=false"
    }

    qt_select_group_bit_or_n """select group_bit_or(knint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select group_bit_or(knint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=int, nullable=true"
    }

    qt_select_group_bit_xor """select group_bit_xor(kint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select group_bit_xor(kint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=int, nullable=true"
    }

    qt_select_group_bit_xor2 """select group_bit_xor(kint) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select group_bit_xor(kint) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=int, nullable=false"
    }

    qt_select_group_bit_xor_n """select group_bit_xor(knint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select group_bit_xor(knint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=int, nullable=true"
    }
    
    qt_select_group_bitmap_xor """select group_bitmap_xor(bitmap_hash(kbint)) from agg_nullable_test_2;"""
    explain {
        sql("verbose select group_bitmap_xor(bitmap_hash(kbint)) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=bitmap, nullable=true"
    }

    qt_select_group_bitmap_xor2 """select group_bitmap_xor(bitmap_hash(kbint)) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select group_bitmap_xor(bitmap_hash(kbint)) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=bitmap, nullable=false"
    }

    qt_select_group_bitmap_xor_n """select group_bitmap_xor(bitmap_hash(knbint)) from agg_nullable_test_2;"""
    explain {
        sql("verbose select group_bitmap_xor(bitmap_hash(knbint)) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=bitmap, nullable=true"
    }

    qt_select_hll_union_agg """select hll_union_agg(hll_hash(kbint)) from agg_nullable_test_2;"""
    explain {
        sql("verbose select hll_union_agg(hll_hash(kbint)) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=bigint, nullable=false"
    }

    qt_select_hll_union_agg2 """select hll_union_agg(hll_hash(kbint)) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select hll_union_agg(hll_hash(kbint)) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=bigint, nullable=false"
    }

    qt_select_hll_union_agg_n """select hll_union_agg(hll_hash(knbint)) from agg_nullable_test_2;"""
    explain {
        sql("verbose select hll_union_agg(hll_hash(knbint)) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=bigint, nullable=false"
    }

    qt_select_hll_union """select hll_union(hll_hash(kbint)) from agg_nullable_test_2;"""
    explain {
        sql("verbose select hll_union(hll_hash(kbint)) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=hll, nullable=false"
    }

    qt_select_hll_union2 """select hll_union(hll_hash(kbint)) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select hll_union(hll_hash(kbint)) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=hll, nullable=false"
    }

    qt_select_hll_union_n """select hll_union(hll_hash(knbint)) from agg_nullable_test_2;"""
    explain {
        sql("verbose select hll_union(hll_hash(knbint)) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=hll, nullable=false"
    }

    qt_select_intersect_count """select intersect_count(bitmap_hash(kbint), kint, 3, 4) from agg_nullable_test_2;"""
    explain {
        sql("verbose select intersect_count(bitmap_hash(kbint), kint, 3, 4) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=bigint, nullable=false"
    }

    qt_select_intersect_count2 """select intersect_count(bitmap_hash(kbint), kint, 3, 4) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select intersect_count(bitmap_hash(kbint), kint, 3, 4) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=bigint, nullable=false"
    }

    qt_select_intersect_count_n """select intersect_count(bitmap_hash(knbint), knint, 3, 4) from agg_nullable_test_2;"""
    explain {
        sql("verbose select intersect_count(bitmap_hash(knbint), knint, 3, 4) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=bigint, nullable=false"
    }

    qt_select_group_concat """select group_concat(kvchrs1) from agg_nullable_test_2;"""
    explain {
        sql("verbose select group_concat(kvchrs1) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=varchar(65533), nullable=true"
    }

    qt_select_group_concat2 """select group_concat(kvchrs1) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select group_concat(kvchrs1) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=varchar(65533), nullable=false"
    }

    qt_select_group_concat_n """select group_concat(knvchrs1) from agg_nullable_test_2;"""
    explain {
        sql("verbose select group_concat(knvchrs1) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=varchar(65533), nullable=true"
    }

    qt_select_multi_distinct_group_concat """select multi_distinct_group_concat(kvchrs1) from agg_nullable_test_2;"""
    explain {
        sql("verbose select multi_distinct_group_concat(kvchrs1) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=varchar(65533), nullable=true"
    }

    qt_select_multi_distinct_group_concat2 """select multi_distinct_group_concat(kvchrs1) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select multi_distinct_group_concat(kvchrs1) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=varchar(65533), nullable=false"
    }

    qt_select_multi_distinct_group_concat_n """select multi_distinct_group_concat(knvchrs1) from agg_nullable_test_2;"""
    explain {
        sql("verbose select multi_distinct_group_concat(knvchrs1) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=varchar(65533), nullable=true"
    }

    qt_select_multi_distinct_sum0 """select multi_distinct_sum0(kint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select multi_distinct_sum0(kint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=bigint, nullable=false"
    }

    qt_select_multi_distinct_sum02 """select multi_distinct_sum0(kint) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select multi_distinct_sum0(kint) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=bigint, nullable=false"
    }

    qt_select_multi_distinct_sum0_n """select multi_distinct_sum0(knint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select multi_distinct_sum0(knint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=bigint, nullable=false"
    }

    qt_select_multi_distinct_sum """select multi_distinct_sum(kint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select multi_distinct_sum(kint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=bigint, nullable=true"
    }

    qt_select_multi_distinct_sum2 """select multi_distinct_sum(kint) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select multi_distinct_sum(kint) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=bigint, nullable=false"
    }

    qt_select_multi_distinct_sum_n """select multi_distinct_sum(knint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select multi_distinct_sum(knint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=bigint, nullable=true"
    }
    
    qt_select_histogram """select histogram(kint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select histogram(kint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=varchar(65533), nullable=false"
    }

    qt_select_histogram2 """select histogram(kint) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select histogram(kint) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=varchar(65533), nullable=false"
    }

    qt_select_histogram_n """select histogram(knint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select histogram(knint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=varchar(65533), nullable=false"
    }

    qt_select_max_by """select max_by(kint, kint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select max_by(kint, kint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=int, nullable=true"
    }

    qt_select_max_by2 """select max_by(kint, kint) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select max_by(kint, kint) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=int, nullable=false"
    }

    qt_select_max_by_n """select max_by(knint, knint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select max_by(knint, knint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=int, nullable=true"
    }

    qt_select_min_by """select min_by(kint, kint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select min_by(kint, kint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=int, nullable=true"
    }

    qt_select_min_by2 """select min_by(kint, kint) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select min_by(kint, kint) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=int, nullable=false"
    }

    qt_select_min_by_n """select min_by(knint, knint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select min_by(knint, knint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=int, nullable=true"
    }

    qt_select_multi_distinct_count """select multi_distinct_count(kint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select multi_distinct_count(kint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=bigint, nullable=false"
    }

    qt_select_multi_distinct_count2 """select multi_distinct_count(kint) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select multi_distinct_count(kint) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=bigint, nullable=false"
    }

    qt_select_multi_distinct_count_n """select multi_distinct_count(knint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select multi_distinct_count(knint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=bigint, nullable=false"
    }

    qt_select_ndv """select ndv(kint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select ndv(kint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=bigint, nullable=false"
    }

    qt_select_ndv2 """select ndv(kint) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select ndv(kint) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=bigint, nullable=false"
    }

    qt_select_ndv_n """select ndv(knint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select ndv(knint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=bigint, nullable=false"
    }

    qt_select_covar """select covar(kint, kint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select covar(kint, kint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=double, nullable=true"
    }

    qt_select_covar2 """select covar(kint, kint) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select covar(kint, kint) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=double, nullable=false"
    }

    qt_select_covar_n """select covar(knint, knint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select covar(knint, knint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=double, nullable=true"
    }

    qt_select_covar_samp """select covar_samp(kint, kint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select covar_samp(kint, kint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=double, nullable=true"
    }

    qt_select_covar_samp2 """select covar_samp(kint, kint) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select covar_samp(kint, kint) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=double, nullable=false"
    }

    qt_select_covar_samp_n """select covar_samp(knint, knint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select covar_samp(knint, knint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=double, nullable=true"
    }

    qt_select_percentile """select percentile(kbint, 0.6) from agg_nullable_test_2;"""
    explain {
        sql("verbose select percentile(kbint, 0.6) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=double, nullable=true"
    }

    qt_select_percentile2 """select percentile(kbint, 0.6) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select percentile(kbint, 0.6) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=double, nullable=false"
    }

    qt_select_percentile_n """select percentile(knbint, 0.6) from agg_nullable_test_2;"""
    explain {
        sql("verbose select percentile(knbint, 0.6) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=double, nullable=true"
    }

    qt_select_percentile_approx """select percentile_approx(kbint, 0.6) from agg_nullable_test_2;"""
    explain {
        sql("verbose select percentile_approx(kbint, 0.6) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=double, nullable=true"
    }

    qt_select_percentile_approx2 """select percentile_approx(kbint, 0.6) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select percentile_approx(kbint, 0.6) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=double, nullable=false"
    }

    qt_select_percentile_approx_n """select percentile_approx(knbint, 0.6) from agg_nullable_test_2;"""
    explain {
        sql("verbose select percentile_approx(knbint, 0.6) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=double, nullable=true"
    }

    qt_select_percentile_approx_weighted """select percentile_approx_weighted(kint, kbint, 0.6) from agg_nullable_test_2;"""
    explain {
        sql("verbose select percentile_approx_weighted(kint, kbint, 0.6) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=double, nullable=true"
    }

    qt_select_percentile_approx_weighted2 """select percentile_approx_weighted(kint, kbint, 0.6) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select percentile_approx_weighted(kint, kbint, 0.6) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=double, nullable=false"
    }

    qt_select_percentile_approx_weighted_n """select percentile_approx_weighted(knint, knbint, 0.6) from agg_nullable_test_2;"""
    explain {
        sql("verbose select percentile_approx_weighted(knint, knbint, 0.6) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=double, nullable=true"
    }

    qt_select_sequence_count """select sequence_count('(?1)(?2)', kdtv2, kint = 1, kint = 2) from agg_nullable_test_2;"""
    explain {
        sql("verbose select sequence_count('(?1)(?2)', kdtv2, kint = 1, kint = 2) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=bigint, nullable=false"
    }

    qt_select_sequence_count2 """select sequence_count('(?1)(?2)', kdtv2, kint = 1, kint = 2) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select sequence_count('(?1)(?2)', kdtv2, kint = 1, kint = 2) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=bigint, nullable=false"
    }

    qt_select_sequence_count_n """select sequence_count('(?1)(?2)', kndtv2, knint = 1, knint = 2) from agg_nullable_test_2;"""
    explain {
        sql("verbose select sequence_count('(?1)(?2)', kndtv2, knint = 1, knint = 2) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=bigint, nullable=false"
    }

    qt_select_sequence_match """select sequence_match('(?1)(?2)', kdtv2, kint = 1, kint = 2) from agg_nullable_test_2;"""
    explain {
        sql("verbose select sequence_match('(?1)(?2)', kdtv2, kint = 1, kint = 2) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=boolean, nullable=true"
    }

    qt_select_sequence_match2 """select sequence_match('(?1)(?2)', kdtv2, kint = 1, kint = 2) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select sequence_match('(?1)(?2)', kdtv2, kint = 1, kint = 2) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=boolean, nullable=false"
    }

    qt_select_sequence_match_n """select sequence_match('(?1)(?2)', kndtv2, knint = 1, knint = 2) from agg_nullable_test_2;"""
    explain {
        sql("verbose select sequence_match('(?1)(?2)', kndtv2, knint = 1, knint = 2) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=boolean, nullable=true"
    }

    qt_select_stddev """select stddev(kint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select stddev(kint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=double, nullable=true"
    }

    qt_select_stddev2 """select stddev(kint) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select stddev(kint) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=double, nullable=false"
    }

    qt_select_stddev_n """select stddev(knint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select stddev(knint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=double, nullable=true"
    }

    qt_select_stddev_pop """select stddev_pop(kint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select stddev_pop(kint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=double, nullable=true"
    }

    qt_select_stddev_pop2 """select stddev_pop(kint) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select stddev_pop(kint) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=double, nullable=false"
    }

    qt_select_stddev_pop_n """select stddev_pop(knint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select stddev_pop(knint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=double, nullable=true"
    }

    qt_select_stddev_samp """select stddev_samp(kint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select stddev_samp(kint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=double, nullable=true"
    }

    qt_select_stddev_samp2 """select stddev_samp(kint) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select stddev_samp(kint) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=double, nullable=false"
    }

    qt_select_stddev_samp_n """select stddev_samp(knint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select stddev_samp(knint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=double, nullable=true"
    }

    qt_select_sum0 """select sum0(kint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select sum0(kint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=bigint, nullable=false"
    }

    qt_select_sum02 """select sum0(kint) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select sum0(kint) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=bigint, nullable=false"
    }

    qt_select_sum0_n """select sum0(knint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select sum0(knint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=bigint, nullable=false"
    }

    qt_select_topn """select topn(kvchrs1, 3) from agg_nullable_test_2;"""
    explain {
        sql("verbose select topn(kvchrs1, 3) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=varchar(65533), nullable=true"
    }

    qt_select_topn2 """select topn(kvchrs1, 3) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select topn(kvchrs1, 3) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=varchar(65533), nullable=false"
    }

    qt_select_topn_n """select topn(knvchrs1, 3) from agg_nullable_test_2;"""
    explain {
        sql("verbose select topn(knvchrs1, 3) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=varchar(65533), nullable=true"
    }

    qt_select_topn_array """select topn_array(kvchrs1, 3) from agg_nullable_test_2;"""
    explain {
        sql("verbose select topn_array(kvchrs1, 3) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=array<varchar(10)>, nullable=true"
    }

    qt_select_topn_array2 """select topn_array(kvchrs1, 3) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select topn_array(kvchrs1, 3) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=array<varchar(10)>, nullable=false"
    }

    qt_select_topn_array_n """select topn_array(knvchrs1, 3) from agg_nullable_test_2;"""
    explain {
        sql("verbose select topn_array(knvchrs1, 3) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=array<varchar(10)>, nullable=true"
    }

    qt_select_topn_weighted """select topn_weighted(kvchrs1, ktint, 3) from agg_nullable_test_2;"""
    explain {
        sql("verbose select topn_weighted(kvchrs1, ktint, 3) from agg_nullable_test_2;")
        contains ">, nullable=true"
    }

    qt_select_topn_weighted2 """select topn_weighted(kvchrs1, ktint, 3) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select topn_weighted(kvchrs1, ktint, 3) from agg_nullable_test_2 group by id;")
        contains ">, nullable=false"
    }

    qt_select_topn_weighted_n """select topn_weighted(knvchrs1, kntint, 3) from agg_nullable_test_2;"""
    explain {
        sql("verbose select topn_weighted(knvchrs1, kntint, 3) from agg_nullable_test_2;")
        contains ">, nullable=true"
    }

    qt_select_variance """select variance(kint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select variance(kint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=double, nullable=true"
    }

    qt_select_variance2 """select variance(kint) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select variance(kint) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=double, nullable=false"
    }

    qt_select_variance_n """select variance(knint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select variance(knint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=double, nullable=true"
    }

    qt_select_var_pop """select var_pop(kint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select var_pop(kint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=double, nullable=true"
    }

    qt_select_var_pop2 """select var_pop(kint) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select var_pop(kint) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=double, nullable=false"
    }

    qt_select_var_pop_n """select var_pop(knint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select var_pop(knint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=double, nullable=true"
    }

    qt_select_variance_samp """select variance_samp(kint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select variance_samp(kint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=double, nullable=true"
    }

    qt_select_variance_samp2 """select variance_samp(kint) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select variance_samp(kint) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=double, nullable=false"
    }

    qt_select_variance_samp_n """select variance_samp(knint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select variance_samp(knint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=double, nullable=true"
    }

    qt_select_var_samp """select var_samp(kint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select var_samp(kint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=double, nullable=true"
    }

    qt_select_var_samp2 """select var_samp(kint) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select var_samp(kint) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=double, nullable=false"
    }

    qt_select_var_samp_n """select var_samp(knint) from agg_nullable_test_2;"""
    explain {
        sql("verbose select var_samp(knint) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=double, nullable=true"
    }

    qt_select_window_funnel """select window_funnel(3600 * 3, 'default', kdtm, kint = 1, kint = 2) from agg_nullable_test_2;"""
    explain {
        sql("verbose select window_funnel(3600 * 3, 'default', kdtm, kint = 1, kint = 2) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=int, nullable=true"
    }

    qt_select_window_funnel2 """select window_funnel(3600 * 3, 'default', kdtm, kint = 1, kint = 2) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select window_funnel(3600 * 3, 'default', kdtm, kint = 1, kint = 2) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=int, nullable=false"
    }

    qt_select_window_funnel_n """select window_funnel(3600 * 3, 'default', kndtm, knint = 1, knint = 2) from agg_nullable_test_2;"""
    explain {
        sql("verbose select window_funnel(3600 * 3, 'default', kndtm, knint = 1, knint = 2) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=int, nullable=true"
    }

    qt_select_map_agg """select map_agg(kint, kstr) from agg_nullable_test_2;"""
    explain {
        sql("verbose select map_agg(kint, kstr) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=map<int,text>, nullable=false"
    }

    qt_select_map_agg2 """select map_agg(kint, kstr) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select map_agg(kint, kstr) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=map<int,text>, nullable=false"
    }

    qt_select_map_agg_n """select map_agg(knint, knstr) from agg_nullable_test_2;"""
    explain {
        sql("verbose select map_agg(knint, knstr) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=map<int,text>, nullable=false"
    }

    qt_select_array_agg """select array_agg(kstr) from agg_nullable_test_2;"""
    explain {
        sql("verbose select array_agg(kstr) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=array<text>, nullable=false"
    }

    qt_select_array_agg2 """select array_agg(kstr) from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select array_agg(kstr) from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=array<text>, nullable=false"
    }

    qt_select_array_agg_n """select array_agg(knstr) from agg_nullable_test_2;"""
    explain {
        sql("verbose select array_agg(knstr) from agg_nullable_test_2;")
        contains "colUniqueId=null, type=array<text>, nullable=false"
    }

    qt_select_retention """select retention(kdtm = '2012-03-11', kdtm = '2012-03-12') from agg_nullable_test_2;"""
    explain {
        sql("verbose select retention(kdtm = '2012-03-11', kdtm = '2012-03-12') from agg_nullable_test_2;")
        contains "colUniqueId=null, type=array<boolean>, nullable=true"
    }

    qt_select_retention2 """select retention(kdtm = '2012-03-11', kdtm = '2012-03-12') from agg_nullable_test_2 group by id;"""
    explain {
        sql("verbose select retention(kdtm = '2012-03-11', kdtm = '2012-03-12') from agg_nullable_test_2 group by id;")
        contains "colUniqueId=null, type=array<boolean>, nullable=false"
    }

    qt_select_retention_n """select retention(kndtm = '2012-03-11', kndtm = '2012-03-12') from agg_nullable_test_2;"""
    explain {
        sql("verbose select retention(kndtm = '2012-03-11', kndtm = '2012-03-12') from agg_nullable_test_2;")
        contains "colUniqueId=null, type=array<boolean>, nullable=true"
    }
}
