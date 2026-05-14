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

suite("infer_predicate_cast_scan_common_expr") {
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"
    sql "set runtime_filter_mode=OFF"
    sql "set enable_decimal256=true"

    sql "drop table if exists infer_predicate_cast_scan_common_expr_t1"
    sql "drop table if exists infer_predicate_cast_scan_common_expr_t2"

    sql """
        create table infer_predicate_cast_scan_common_expr_t1 (
            pk int,
            col_int_undef_signed int,
            col_bigint_undef_signed bigint,
            col_date_undef_signed date,
            col_datetime_undef_signed datetime,
            col_varchar_10__undef_signed varchar(10),
            col_decimal_10_2__undef_signed decimal(10,2),
            col_decimal_38_9__undef_signed decimal(38,9),
            col_decimal_76_40__undef_signed decimal(76,40),
            col_int_undef_signed2 int MIN,
            col_bigint_undef_signed2 bigint MIN,
            col_int_undef_signed3 int MAX,
            col_bigint_undef_signed3 bigint MAX,
            col_int_undef_signed4 int SUM,
            col_bigint_undef_signed4 bigint SUM,
            col_date_undef_signed2 date MIN,
            col_datetime_undef_signed2 datetime MIN,
            col_date_undef_signed3 date MAX,
            col_datetime_undef_signed3 datetime MAX,
            col_varchar_20__undef_signed varchar(20) MIN,
            col_varchar_20__undef_signed2 varchar(20) MAX,
            col_decimal_10_2__undef_signed2 decimal(10,2) MIN,
            col_decimal_38_9__undef_signed2 decimal(38,9) MIN,
            col_decimal_10_2__undef_signed3 decimal(10,2) MAX,
            col_decimal_38_9__undef_signed3 decimal(38,9) MAX,
            col_decimal_10_2__undef_signed4 decimal(10,2) SUM,
            col_decimal_38_9__undef_signed4 decimal(38,9) SUM,
            col_bitmap__undef_signed bitmap bitmap_union,
            col_hll__undef_signed hll hll_union
        ) engine=olap
        aggregate key(
            pk, col_int_undef_signed, col_bigint_undef_signed, col_date_undef_signed,
            col_datetime_undef_signed, col_varchar_10__undef_signed,
            col_decimal_10_2__undef_signed, col_decimal_38_9__undef_signed,
            col_decimal_76_40__undef_signed
        )
        distributed by hash(pk) buckets 1
        properties("replication_num" = "1", "disable_auto_compaction" = "true");
    """

    sql """
        create table infer_predicate_cast_scan_common_expr_t2
        like infer_predicate_cast_scan_common_expr_t1
    """

    sql """
        insert into infer_predicate_cast_scan_common_expr_t1 values
        (1, 1, 1, '2025-01-01', '2025-01-01 12:00:00', 'a',
            1.10, 1.100000000, 1.1000000000000000000000000000000000000000,
            1, 1, 1, 1, 1, 1, '2025-01-01', '2025-01-01 12:00:00',
            '2025-01-01', '2025-01-01 12:00:00', 'aa', 'aa',
            1.10, 1.100000000, 1.10, 1.100000000, 1.10, 1.100000000,
            bitmap_from_array([1,2]), hll_hash(1)),
        (2, 2, 2, '2025-01-02', '2025-01-02 12:00:00', 'b',
            2.20, 2.200000000, 2.2000000000000000000000000000000000000000,
            2, 2, 2, 2, 2, 2, '2025-01-02', '2025-01-02 12:00:00',
            '2025-01-02', '2025-01-02 12:00:00', 'bb', 'bb',
            2.20, 2.200000000, 2.20, 2.200000000, 2.20, 2.200000000,
            bitmap_from_array([2,3]), hll_hash(2)),
        (3, 3, 30, '2025-01-03', '2025-01-03 12:00:00', 'c',
            3.30, 3.300000000, 3.3000000000000000000000000000000000000000,
            3, 3, 3, 3, 3, 3, '2025-01-03', '2025-01-03 12:00:00',
            '2025-01-03', '2025-01-03 12:00:00', 'cc', 'cc',
            3.30, 3.300000000, 3.30, 3.300000000, 3.30, 3.300000000,
            bitmap_from_array([3,4]), hll_hash(3))
    """

    sql """
        insert into infer_predicate_cast_scan_common_expr_t2
        select * from infer_predicate_cast_scan_common_expr_t1
    """

    order_qt_cast_scan_common_expr """
        select
            t1.col_decimal_76_40__undef_signed,
            t1.pk,
            t1.col_bigint_undef_signed,
            t1.col_datetime_undef_signed,
            t1.col_decimal_38_9__undef_signed,
            bitmap_count(bitmap_union(t1.col_bitmap__undef_signed)),
            bitmap_union_count(t1.col_bitmap__undef_signed),
            count(distinct t2.col_datetime_undef_signed3)
        from infer_predicate_cast_scan_common_expr_t2 t1
        inner join infer_predicate_cast_scan_common_expr_t1 t2
            on t1.col_date_undef_signed = t2.col_date_undef_signed
            and t1.pk = t2.pk
            and t1.col_bigint_undef_signed = t2.col_bigint_undef_signed
            and t1.col_bigint_undef_signed = t2.col_bigint_undef_signed
            and t1.col_bigint_undef_signed = t2.pk
        group by
            t1.col_decimal_76_40__undef_signed,
            t1.pk,
            t1.col_bigint_undef_signed,
            t1.col_datetime_undef_signed,
            t1.col_decimal_38_9__undef_signed
        order by 1, 2, 3, 4, 5
    """
}
