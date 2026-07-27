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

suite("test_datetimev2_nano_cast_complex") {
    sql "set debug_skip_fold_constant = false"
    order_qt_cast_folded """
        select
            cast('1677-09-21 00:12:43.145224192' as datetimev2(9)),
            cast('1970-01-01 00:00:00.000000000' as datetimev2(9)),
            cast('2262-04-11 23:47:16.854775807' as datetimev2(9)),
            cast(cast('1970-01-01 00:00:00.123456789' as datetimev2(9))
                 as datetimev2(7)),
            cast(cast('1970-01-01 00:00:00.123456789' as datetimev2(9))
                 as datetimev2(8))
    """
    sql "set debug_skip_fold_constant = true"
    order_qt_cast_runtime """
        select
            cast('1677-09-21 00:12:43.145224192' as datetimev2(9)),
            cast('1970-01-01 00:00:00.000000000' as datetimev2(9)),
            cast('2262-04-11 23:47:16.854775807' as datetimev2(9)),
            cast(cast('1970-01-01 00:00:00.123456789' as datetimev2(9))
                 as datetimev2(7)),
            cast(cast('1970-01-01 00:00:00.123456789' as datetimev2(9))
                 as datetimev2(8))
    """
    sql "set debug_skip_fold_constant = false"

    sql "drop table if exists test_datetimev2_nano_cast_strings"
    sql """
        create table test_datetimev2_nano_cast_strings (
            id int,
            value string
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_datetimev2_nano_cast_strings values
        (1, '1677-09-21 00:12:43.145224192'),
        (2, '1970-01-01 00:00:00.000000000'),
        (3, '2262-04-11 23:47:16.854775807'),
        (4, '1677-09-21 00:12:43.145224191'),
        (5, 'not-a-datetime')
    """
    order_qt_cast_column """
        select id, value, cast(value as datetimev2(9))
        from test_datetimev2_nano_cast_strings
        order by id
    """

    order_qt_complex_constructors """
        select
            array(
                cast('1677-09-21 00:12:43.145224192' as datetimev2(9)),
                cast('1970-01-01 00:00:00.000000000' as datetimev2(9)),
                cast('2262-04-11 23:47:16.854775807' as datetimev2(9))),
            map(
                1, cast('1677-09-21 00:12:43.145224192' as datetimev2(9)),
                2, cast('1970-01-01 00:00:00.000000000' as datetimev2(9)),
                3, cast('2262-04-11 23:47:16.854775807' as datetimev2(9))),
            named_struct(
                'minimum', cast('1677-09-21 00:12:43.145224192' as datetimev2(9)),
                'epoch', cast('1970-01-01 00:00:00.000000000' as datetimev2(9)),
                'maximum', cast('2262-04-11 23:47:16.854775807' as datetimev2(9)))
    """
    order_qt_complex_parse """
        select
            cast(
                '["1677-09-21 00:12:43.145224192","1970-01-01 00:00:00.000000000","2262-04-11 23:47:16.854775807"]'
                as array<datetimev2(9)>),
            cast(
                '{"1":"1677-09-21 00:12:43.145224192","2":"1970-01-01 00:00:00.000000000","3":"2262-04-11 23:47:16.854775807"}'
                as map<int, datetimev2(9)>),
            cast(
                '{"minimum":"1677-09-21 00:12:43.145224192","epoch":"1970-01-01 00:00:00.000000000","maximum":"2262-04-11 23:47:16.854775807"}'
                as struct<minimum:datetimev2(9),
                          epoch:datetimev2(9),
                          maximum:datetimev2(9)>)
    """

    sql "drop table if exists test_datetimev2_nano_array_functions"
    sql """
        create table test_datetimev2_nano_array_functions (
            id int,
            values_array array<datetimev2(9)>,
            excluded_array array<datetimev2(9)>
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_datetimev2_nano_array_functions values
        (1,
         array(
             cast('2262-04-11 23:47:16.854775807' as datetimev2(9)),
             cast('1970-01-01 00:00:00.000000000' as datetimev2(9)),
             cast('1677-09-21 00:12:43.145224192' as datetimev2(9))),
         array(cast('1970-01-01 00:00:00.000000000' as datetimev2(9)))),
        (2, null, null)
    """
    order_qt_array_min_max """
        select id, array_min(values_array), array_max(values_array)
        from test_datetimev2_nano_array_functions
        order by id
    """
    order_qt_array_join """
        select id, array_join(array_sort(values_array), '|')
        from test_datetimev2_nano_array_functions
        order by id
    """
    order_qt_array_search """
        select id,
               array_position(
                   values_array,
                   cast('1970-01-01 00:00:00.000000000' as datetimev2(9))),
               array_contains(
                   values_array,
                   cast('1677-09-21 00:12:43.145224192' as datetimev2(9))),
               countequal(
                   values_array,
                   cast('2262-04-11 23:47:16.854775807' as datetimev2(9)))
        from test_datetimev2_nano_array_functions
        order by id
    """
    order_qt_map_contains_entry """
        select
            map_contains_entry(
                map(
                    'minimum', cast('1677-09-21 00:12:43.145224192' as datetimev2(9)),
                    'epoch', cast('1970-01-01 00:00:00.000000000' as datetimev2(9)),
                    'maximum', cast('2262-04-11 23:47:16.854775807' as datetimev2(9))),
                'epoch',
                cast('1970-01-01 00:00:00.000000000' as datetimev2(9))),
            map_contains_entry(
                map(
                    cast('1677-09-21 00:12:43.145224192' as datetimev2(9)), 'minimum',
                    cast('1970-01-01 00:00:00.000000000' as datetimev2(9)), 'epoch',
                    cast('2262-04-11 23:47:16.854775807' as datetimev2(9)), 'maximum'),
                cast('2262-04-11 23:47:16.854775807' as datetimev2(9)),
                'maximum')
    """

    sql "drop table if exists test_datetimev2_nano_min_max_by"
    sql """
        create table test_datetimev2_nano_min_max_by (
            id int,
            label string,
            dt datetimev2(9)
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_datetimev2_nano_min_max_by values
        (1, 'minimum', '1677-09-21 00:12:43.145224192'),
        (2, 'epoch', '1970-01-01 00:00:00.000000000'),
        (3, 'maximum', '2262-04-11 23:47:16.854775807')
    """
    order_qt_min_max_by """
        select max_by(dt, id), min_by(dt, id),
               max_by(label, dt), min_by(label, dt)
        from test_datetimev2_nano_min_max_by
    """

    test {
        sql """
            select id,
                   array_apply(values_array, '=',
                               cast('1970-01-01 00:00:00.000000000' as datetimev2(9)))
            from test_datetimev2_nano_array_functions
            order by id
        """
        exception "array_apply only accept"
    }
    test {
        sql """
            select id, array_sort(array_except(values_array, excluded_array))
            from test_datetimev2_nano_array_functions
            order by id
        """
        exception "unsupported types for function array_except"
    }
    test {
        sql """
            select array_sum(values_array)
            from test_datetimev2_nano_array_functions
        """
        exception "does not support"
    }
}
