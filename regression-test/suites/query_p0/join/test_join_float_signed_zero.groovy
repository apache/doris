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

// -0.0 and +0.0 are equal floating point values (and Doris treats every NaN as one value),
// but their bit patterns differ. Hash joins, set operations, partition TopN, shuffle
// partitioning and bloom runtime filters must treat them as one key instead of silently
// dropping rows, while the stored values (a stored -0.0 still prints as -0) must survive
// unchanged in the output.
suite("test_join_float_signed_zero") {
    sql "drop table if exists join_float_signed_zero_outer"
    sql "drop table if exists join_float_signed_zero_inner"
    sql """
        create table join_float_signed_zero_outer (
            id int,
            x double,
            f float
        ) duplicate key(id)
        distributed by hash(id) buckets 4
        properties ("replication_num" = "1")
    """
    sql """
        create table join_float_signed_zero_inner (
            id int,
            a int,
            b double,
            g float
        ) duplicate key(id)
        distributed by hash(id) buckets 4
        properties ("replication_num" = "1")
    """
    sql """
        insert into join_float_signed_zero_outer values
            (1, cast('-0.0' as double), cast('-0.0' as float)),
            (2, 0.0, 0.0),
            (3, 1.0, 1.0),
            (4, null, null),
            (5, cast('nan' as double), cast('nan' as float))
    """
    sql """
        insert into join_float_signed_zero_inner values
            (1, 0, 0.0, 0.0),
            (2, 1, 1.0, 1.0),
            (3, 2, cast('-0.0' as double), cast('-0.0' as float)),
            (4, null, null, null),
            (5, 3, cast('-nan' as double), cast('-nan' as float))
    """

    order_qt_scan_values """
        select id, x, f, cast(x as string) as x_str, x = 0.0 as x_eq_zero, f = 0.0 as f_eq_zero
        from join_float_signed_zero_outer
    """

    order_qt_inner_join_broadcast """
        select o.id, o.x, i.id, i.b
        from join_float_signed_zero_outer o join [broadcast] join_float_signed_zero_inner i on o.x = i.b
    """
    order_qt_inner_join_shuffle """
        select o.id, o.x, i.id, i.b
        from join_float_signed_zero_outer o join [shuffle] join_float_signed_zero_inner i on o.x = i.b
    """
    order_qt_inner_join_float """
        select o.id, o.f, i.id, i.g
        from join_float_signed_zero_outer o join [shuffle] join_float_signed_zero_inner i on o.f = i.g
    """
    order_qt_inner_join_int_double """
        select o.id, o.x, i.id, i.a
        from join_float_signed_zero_outer o join join_float_signed_zero_inner i on i.a = o.x
    """
    order_qt_inner_join_multi_keys """
        select o.id, o.x, i.id, i.b
        from join_float_signed_zero_outer o join [shuffle] join_float_signed_zero_inner i
        on o.x = i.b and o.id = i.id
    """
    // Other join conjuncts still see the stored sign of both key columns: only the pairs
    // whose zeros differ in sign survive.
    order_qt_other_conjunct_on_key_sign """
        select o.id, o.x, i.id, i.b
        from join_float_signed_zero_outer o join [shuffle] join_float_signed_zero_inner i
        on o.x = i.b and cast(o.x as string) <> cast(i.b as string)
    """
    order_qt_left_join """
        select o.id, o.x, i.id, i.b
        from join_float_signed_zero_outer o left join join_float_signed_zero_inner i on o.x = i.b
    """
    // Empty build side: the outer-join shortcut appends the build-side NULL columns to the
    // probe block, so the float key must not change the probe block's shape.
    order_qt_left_join_empty_build """
        select o.id, o.x, i.id, i.b
        from join_float_signed_zero_outer o
        left join (select * from join_float_signed_zero_inner where id > 100) i on o.x = i.b
    """
    order_qt_full_join_empty_build """
        select o.id, o.x, i.id, i.b
        from join_float_signed_zero_outer o
        full join (select * from join_float_signed_zero_inner where id > 100) i on o.x = i.b
    """
    // The key slot is not part of the output.
    order_qt_probe_only_output """
        select o.id
        from join_float_signed_zero_outer o join [shuffle] join_float_signed_zero_inner i on o.x = i.b
    """
    order_qt_null_safe_join """
        select o.id, o.x, i.id, i.b
        from join_float_signed_zero_outer o join join_float_signed_zero_inner i on o.x <=> i.b
    """
    order_qt_array_key_join """
        select o.id, o.x, i.id
        from join_float_signed_zero_outer o join join_float_signed_zero_inner i on array(o.x) = array(i.b)
    """
    order_qt_exists_subquery """
        select o.id, o.x
        from join_float_signed_zero_outer o
        where exists (select 1 from join_float_signed_zero_inner i where i.a = o.x and i.a = i.b)
    """
    order_qt_in_subquery """
        select o.id, o.x
        from join_float_signed_zero_outer o
        where o.x in (select b from join_float_signed_zero_inner where id = 1)
    """
    // Null-aware anti join (nullable right side without NULL rows): +0.0 must be rejected by
    // the stored -0.0, NaN is the only surviving non-NULL row.
    order_qt_not_in_subquery """
        select o.id, o.x
        from join_float_signed_zero_outer o
        where o.x not in (select b from join_float_signed_zero_inner where id in (2, 3))
    """
    // Null-aware anti join with an other conjunct that reads both float columns.
    order_qt_not_in_other_conjunct """
        select o.id, o.x
        from join_float_signed_zero_outer o
        where o.x not in (select i.b from join_float_signed_zero_inner i where i.g > o.f)
    """
    // Only the stored -0.0 is kept on the first child, so the representative row of the
    // zero group is deterministic.
    order_qt_intersect """
        select x from join_float_signed_zero_outer where id <> 2 intersect select b from join_float_signed_zero_inner
    """
    order_qt_except """
        select x from join_float_signed_zero_outer except select b from join_float_signed_zero_inner where id = 1
    """
    // The surviving row keeps the stored -0.0.
    order_qt_except_value_preserved """
        select x, cast(x as string) from join_float_signed_zero_outer where id = 1
        except select b, cast(b as string) from join_float_signed_zero_inner where id = 2
    """
    order_qt_intersect_value_preserved """
        select x from join_float_signed_zero_outer where id = 1
        intersect select b from join_float_signed_zero_inner where id = 1
    """

    // Window partitions (sort based after a hash shuffle) and the partition TopN pushdown
    // must agree.
    order_qt_window_partition_count """
        select id, x, count(*) over (partition by x) as cnt from join_float_signed_zero_outer
    """
    order_qt_partition_topn """
        select id, x from (
            select id, x, row_number() over (partition by x order by id) as rn
            from join_float_signed_zero_outer
        ) v where rn <= 1
    """
    order_qt_window_row_number """
        select id, x, row_number() over (partition by x order by id) as rn
        from join_float_signed_zero_outer
    """

    // Legacy shuffle hash (zlib crc32) with scalar and nested keys.
    sql "set enable_new_shuffle_hash_method = false"
    order_qt_legacy_shuffle_double """
        select o.id, o.x, i.id, i.b
        from join_float_signed_zero_outer o join [shuffle] join_float_signed_zero_inner i on o.x = i.b
    """
    order_qt_legacy_shuffle_array """
        select o.id, o.x, i.id
        from join_float_signed_zero_outer o join [shuffle] join_float_signed_zero_inner i on array(o.x) = array(i.b)
    """
    sql "set enable_new_shuffle_hash_method = true"

    // Force a bloom runtime filter (the pruner would drop it for these tiny tables) and wait
    // for it, so the probe side is filtered by the hash of the build keys - both through the
    // expression on the scan and through the storage-level bloom predicate - before it
    // reaches the join.
    sql "set enable_runtime_filter_prune = false"
    sql "set runtime_filter_type = 2"
    sql "set runtime_filter_wait_infinitely = true"
    order_qt_bloom_runtime_filter """
        select o.id, o.x, i.id, i.b
        from join_float_signed_zero_outer o join [shuffle] join_float_signed_zero_inner i on o.x = i.b
    """
    order_qt_bloom_runtime_filter_broadcast """
        select o.id, o.x, i.id, i.b
        from join_float_signed_zero_outer o join [broadcast] join_float_signed_zero_inner i on o.x = i.b
    """
    order_qt_bloom_runtime_filter_float """
        select o.id, o.f, i.id, i.g
        from join_float_signed_zero_outer o join [shuffle] join_float_signed_zero_inner i on o.f = i.g
    """
    order_qt_bloom_runtime_filter_float_broadcast """
        select o.id, o.f, i.id, i.g
        from join_float_signed_zero_outer o join [broadcast] join_float_signed_zero_inner i on o.f = i.g
    """
    sql "set runtime_filter_type = 12"
    sql "set runtime_filter_wait_infinitely = false"
    sql "set enable_runtime_filter_prune = true"

    // Spilled (partitioned) hash join repartitions both sides with the shuffle hash.
    sql "set enable_spill = true"
    sql "set enable_force_spill = true"
    order_qt_spill_join_double """
        select o.id, o.x, i.id, i.b
        from join_float_signed_zero_outer o join [shuffle] join_float_signed_zero_inner i on o.x = i.b
    """
    order_qt_spill_join_array """
        select o.id, o.x, i.id
        from join_float_signed_zero_outer o join [shuffle] join_float_signed_zero_inner i on array(o.x) = array(i.b)
    """
    sql "set enable_spill = false"
    sql "set enable_force_spill = false"
}
