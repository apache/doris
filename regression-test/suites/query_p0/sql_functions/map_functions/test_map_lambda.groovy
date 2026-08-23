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

suite("test_map_lambda", "p0") {
    sql "set enable_nereids_planner = true"
    sql "set enable_fallback_to_original_planner = false"
    sql "drop table if exists test_map_lambda"
    sql """
        create table test_map_lambda (
            id int,
            bias int,
            mii map<int, int>,
            mss map<string, string>,
            mia map<int, array<int>>,
            mim map<int, map<int, int>>
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_map_lambda values
            (1, 10,
             map(1, 10, 2, 20),
             map('a', 'x', 'b', 'y'),
             map(1, [10], 2, [20, 21]),
             map(1, map(2, 20), 3, map(4, 40))),
            (2, 0,
             cast(map() as map<int, int>),
             cast(map() as map<string, string>),
             cast(map() as map<int, array<int>>),
             cast(map() as map<int, map<int, int>>)),
            (3, 0, null, null, null, null)
    """

    qt_map_apply """
        select map_size(r), r[2], r[3]
        from (
            select map_apply((k, v) -> struct(k + 1, v * 2), mii) r
            from test_map_lambda where id = 1
        ) t
    """

    qt_map_apply_tuple_lambda """
        select map_size(tuple_result), tuple_result[3], tuple_result[6],
               map_size(struct_result), struct_result[3], struct_result[6]
        from (
            select map_apply((k, v) -> (k * 3, v + 1), mii) tuple_result,
                   map_apply((k, v) -> struct(k * 3, v + 1), mii) struct_result
            from test_map_lambda where id = 1
        ) t
    """

    qt_map_from_arrays """
        select map_size(r), r[1], r[2]
        from (
            select map_from_arrays([1, 2], [10, 20]) r
        ) t
    """

    qt_map_from_arrays_last_win """
        select map_size(r), r[1]
        from (
            select map_from_arrays([1, 1], [10, 20]) r
        ) t
    """

    qt_map_from_entries """
        select map_size(r), r[1], r[2]
        from (
            select map_from_entries(array(struct(1, 10), struct(2, 20))) r
        ) t
    """

    qt_map_from_entries_last_win """
        select map_size(r), r[1]
        from (
            select map_from_entries(array(struct(1, 10), struct(1, 20))) r
        ) t
    """

    order_qt_map_from_entries_column """
        select id, map_size(r), r[1], r[2]
        from (
            select id, map_from_entries(map_entries(mii)) r
            from test_map_lambda
        ) t
        order by id
    """

    qt_map_from_entries_null """
        select map_from_entries(cast(null as array<struct<k:int,v:int>>))
    """

    testFoldConst("select map_from_entries(array(struct(1, 'a'), struct(2, 'b')))")

    qt_map_filter_null_is_false """
        select map_size(r), r[1], r[2]
        from (
            select map_filter(
                (k, v) -> if(k = 1, cast(null as boolean), true), mii) r
            from test_map_lambda where id = 1
        ) t
    """

    order_qt_map_filter_two_argument_nullable_map """
        select id, map_filter(
            mii, if(id = 1, [true, true], if(id = 2, [], [true])))
        from test_map_lambda
        order by id
    """

    order_qt_map_filter_constant_map_captures_column """
        select id, map_size(r), r[1], r[2]
        from (
            select id, map_filter((k, v) -> v > id, map(1, 10, 2, 20)) r
            from test_map_lambda
        ) t
        order by id
    """

    // Both transformations intentionally produce duplicate keys. ColumnMap uses last-win.
    qt_transform_keys_last_win """
        select map_size(r), r[1]
        from (
            select transform_keys((k, v) -> 1, mii) r
            from test_map_lambda where id = 1
        ) t
    """
    qt_map_apply_last_win """
        select map_size(r), r[1]
        from (
            select map_apply((k, v) -> struct(1, v * 2), mii) r
            from test_map_lambda where id = 1
        ) t
    """

    qt_transform_values """
        select r[1], r[2]
        from (
            select transform_values((k, v) -> v + 1, mii) r
            from test_map_lambda where id = 1
        ) t
    """
    qt_transform_values_null_type """
        select map_size(r), r[1], r[2]
        from (
            select transform_values((k, v) -> null, mii) r
            from test_map_lambda where id = 1
        ) t
    """
    qt_transform_keys_null_type """
        select map_size(r), map_values(r)[1]
        from (
            select transform_keys((k, v) -> null, mii) r
            from test_map_lambda where id = 1
        ) t
    """
    qt_map_apply_null_value_type """
        select map_size(r), r[1], r[2]
        from (
            select map_apply((k, v) -> struct(k, null), mii) r
            from test_map_lambda where id = 1
        ) t
    """
    qt_transform_values_nested_array_null_type """
        select r[1]
        from (
            select transform_values((k, v) -> [], map(1, [10])) r
        ) t
    """
    qt_map_apply_nested_array_null_type """
        select r[1]
        from (
            select map_apply((k, v) -> struct(k, []), map(1, [10])) r
        ) t
    """
    qt_transform_values_nested_map_null_type """
        select map_size(r[1])
        from (
            select transform_values((k, v) -> map(), map(1, map(2, 20))) r
        ) t
    """
    qt_transform_values_nested_struct_null_type """
        select r[1]
        from (
            select transform_values(
                (k, v) -> struct(null, []), map(1, struct(10, [20]))) r
        ) t
    """
    qt_transform_values_string """
        select r['a'], r['b']
        from (
            select transform_values((k, v) -> concat(v, ':', k), mss) r
            from test_map_lambda where id = 1
        ) t
    """
    qt_transform_values_array """
        select r[1], r[2]
        from (
            select transform_values((k, v) -> array_pushback(v, k), mia) r
            from test_map_lambda where id = 1
        ) t
    """
    qt_nested_array_lambda """
        select map_apply(
            (k, vals) -> struct(
                k, array_map(x -> x + k + 10, vals)),
            map(1, [10], 2, [20, 21]))
    """

    qt_map_quantifiers """
        select cast(map_exists((k, v) -> v = 20, mii) as int),
               cast(map_all((k, v) -> v > 10, mii) as int)
        from test_map_lambda where id = 1
    """
    qt_empty_map_quantifiers """
        select cast(map_exists((k, v) -> true, mii) as int),
               cast(map_all((k, v) -> false, mii) as int)
        from test_map_lambda where id = 2
    """
    qt_null_predicate_quantifiers """
        select map_exists((k, v) -> cast(null as boolean), mii),
               map_all((k, v) -> cast(null as boolean), mii)
        from test_map_lambda where id = 1
    """

    qt_empty_map_all_functions """
        select map_size(map_apply((k, v) -> struct(k, v), mii)),
               map_size(map_filter((k, v) -> true, mii)),
               map_size(transform_keys((k, v) -> k + 1, mii)),
               map_size(transform_values((k, v) -> v + 1, mii)),
               cast(map_exists((k, v) -> true, mii) as int),
               cast(map_all((k, v) -> false, mii) as int)
        from test_map_lambda where id = 2
    """

    qt_null_map_all_functions """
        select cast(map_apply((k, v) -> struct(k, v), mii) is null as int),
               cast(map_filter((k, v) -> true, mii) is null as int),
               cast(transform_keys((k, v) -> k + 1, mii) is null as int),
               cast(transform_values((k, v) -> v + 1, mii) is null as int),
               cast(map_exists((k, v) -> true, mii) is null as int),
               cast(map_all((k, v) -> false, mii) is null as int)
        from test_map_lambda where id = 3
    """

    // A deterministic constant Map is an explicitly supported stable source.
    qt_constant_map """
        select r[1], r[2]
        from (
            select transform_values(
                (k, v) -> v + 1, map(1, 10, 2, 20)) r
        ) t
    """

    qt_materialized_computed_map """
        select r[1], r[3]
        from (
            select transform_values((k, v) -> v + 1, computed_map) r
            from (
                select map(1, mii[1], 3, 30) computed_map
                from test_map_lambda where id = 1
            ) producer
        ) consumer
    """

    // Deterministic computed Maps compose directly with all Map Lambda functions.
    qt_direct_computed_map """
        select r[1], r[3]
        from (
            select transform_values(
                (k, v) -> v + 1, map(1, mii[1], 3, 30)) r
            from test_map_lambda where id = 1
        ) t
    """

    qt_all_functions_direct_computed_map """
        select map_apply(
                   (k, v) -> struct(k, v + 1),
                   map(1, mii[1], 3, 30))[1],
               map_filter(
                   (k, v) -> k = 3,
                   map(1, mii[1], 3, 30))[3],
               transform_keys(
                   (k, v) -> k + 1,
                   map(1, mii[1], 3, 30))[2],
               transform_values(
                   (k, v) -> v + 1,
                   map(1, mii[1], 3, 30))[3],
               cast(map_exists(
                   (k, v) -> v = 30,
                   map(1, mii[1], 3, 30)) as int),
               cast(map_all(
                   (k, v) -> v >= 10,
                   map(1, mii[1], 3, 30)) as int)
        from test_map_lambda where id = 1
    """

    order_qt_map_lambda_aggregate_output """
        select id, count(*) as row_count,
               transform_values(
                   (k, v) -> v + 1,
                   map(1, id, 2, id + 10))[2] as transformed_value
        from test_map_lambda
        group by id
        order by id
    """

    order_qt_map_lambda_having """
        select id, count(*) as row_count
        from test_map_lambda
        group by id
        having map_exists(
            (k, v) -> true,
            map(1, id, 2, id + 10))
        order by id
    """

    order_qt_map_lambda_join_on """
        select l.id, r.id
        from test_map_lambda l
        join test_map_lambda r
          on transform_values(
              (k, v) -> v,
              map(1, l.id, 2, l.id + 1))[2] = r.id
        order by l.id, r.id
    """

    qt_nondeterministic_map_input """
        select map_size(r), map_values(r)[1]
        from (
            select transform_values(
                (k, v) -> v, map(cast(random() as int), 10)) r
        ) t
    """

    qt_nondeterministic_map_single_evaluation """
        select cast(map_keys(r)[1] = map_values(r)[1] as int)
        from (
            select transform_values(
                (k, v) -> k, map(random(1, 1000000000), 10)) r
        ) t
    """

    qt_map_lambda_generate_volatile """
        select cast(k = v as int)
        from (select 1 as seed) t
        lateral view explode_map(
            transform_values(
                (mk, mv) -> mk,
                map(random(1, 1000000000), 0))) tmp as k, v
    """

    qt_nested_nondeterministic_map_single_evaluation """
        select map_size(r), cast(map_keys(r)[1] = map_values(r)[1] as int)
        from (
            select transform_values(
                (k, v) -> v,
                transform_values(
                    (ik, iv) -> ik,
                    map(random(1, 1000000000), 0))) r
        ) t
    """

    qt_lambda_nested_lambda_map_single_evaluation """
        select cast(map_keys(r[1])[1] = map_values(r[1])[1] as int)
        from (
            select transform_values(
                (ok, ov) -> transform_values(
                    (ik, iv) -> ik,
                    map(ok + random(1, 1000000000), ov)),
                map(1, 10)) r
        ) t
    """

    qt_array_map_nested_map_lambda_single_evaluation """
        select cast(
            map_keys(r[1])[1] = map_values(r[1])[1]
            and map_keys(r[2])[1] = map_values(r[2])[1]
            and map_keys(r[1])[1] != map_keys(r[2])[1]
            as int)
        from (
            select array_map(
                x -> transform_values((k, v) -> k, map(uuid(), x)),
                [1, 2]) r
        ) t
    """

    qt_array_map_nested_map_apply_single_evaluation """
        select cast(
            map_keys(r[1])[1] = map_values(r[1])[1]
            and map_keys(r[2])[1] = map_values(r[2])[1]
            and map_keys(r[1])[1] != map_keys(r[2])[1]
            as int)
        from (
            select array_map(
                x -> map_apply((k, v) -> struct(k, k), map(uuid(), x)),
                [1, 2]) r
        ) t
    """

    test {
        sql "select map_filter(k -> k > 0, map(1, 10))"
        exception "requires exactly two arguments"
    }
    test {
        sql "select map_from_arrays([[1]], [10])"
        exception "MAP key type must be a primitive type"
    }
    qt_map_from_arrays_nested_empty_array "select map_from_arrays([1], [[]])"
    test {
        sql "select map_from_entries(1)"
        exception "requires an array of structs with exactly two fields"
    }
    test {
        sql "select map_from_entries(array(struct(1)))"
        exception "requires an array of structs with exactly two fields"
    }
    test {
        sql "select map_from_entries(array(cast(null as struct<k:int,v:int>)))"
        exception "Map entry of function map_from_entries cannot be null"
    }
    test {
        sql """
            select map_apply(
                (k, v) -> if(k > 0, struct(k, v), cast(null as struct<k:int,v:int>)),
                map(1, 10))
        """
        exception "must return a non-nullable struct with exactly two fields"
    }
}
