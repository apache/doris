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

suite("test_array_except_all") {
    order_qt_partial_cancel """
        select array_sort(array_except_all(['a', 'a', 'b'], ['a']))
    """
    order_qt_preserve_duplicates """
        select array_sort(array_except_all(['a', 'a', 'b'], ['c']))
    """
    order_qt_saturating_cancel """
        select array_sort(array_except_all(['a', 'a'], ['a', 'a', 'a']))
    """
    order_qt_null_cancel """
        select array_sort(array_except_all(['a', null, 'a', null], [null]))
    """
    order_qt_null_saturating_cancel """
        select array_sort(array_except_all([null], [null, null]))
    """
    order_qt_empty_left """
        select array_except_all(cast([] as array<int>), [1])
    """
    order_qt_empty_right """
        select array_except_all([1, 1, 2], cast([] as array<int>))
    """
    order_qt_null_left """
        select array_except_all(cast(null as array<int>), [1])
    """
    order_qt_null_right """
        select array_except_all([1], cast(null as array<int>))
    """
    order_qt_implicit_type_coercion """
        select array_sort(array_except_all([1, 1, 2], array(cast(1 as bigint))))
    """
    order_qt_decimal """
        select array_sort(array_except_all(
            array(cast(1.25 as decimal(9, 2)), cast(1.25 as decimal(9, 2)),
                cast(2.50 as decimal(9, 2))),
            array(cast(1.25 as decimal(9, 2)))))
    """
    order_qt_date """
        select array_sort(array_except_all(
            array(cast('2026-08-25' as date), cast('2026-08-25' as date),
                cast('2026-08-26' as date)),
            array(cast('2026-08-25' as date))))
    """
    order_qt_datetime """
        select array_sort(array_except_all(
            array(cast('2026-08-25 10:00:00' as datetime),
                cast('2026-08-25 10:00:00' as datetime)),
            array(cast('2026-08-25 10:00:00' as datetime))))
    """
    order_qt_ipv4 """
        select array_sort(array_except_all(
            array(cast('192.168.0.1' as ipv4), cast('192.168.0.1' as ipv4),
                cast('192.168.0.2' as ipv4)),
            array(cast('192.168.0.1' as ipv4))))
    """
    order_qt_ipv6 """
        select array_sort(array_except_all(
            array(cast('2001:db8::1' as ipv6), cast('2001:db8::1' as ipv6),
                cast('2001:db8::2' as ipv6)),
            array(cast('2001:db8::1' as ipv6))))
    """
    order_qt_composed """
        select array_size(array_except_all([1, 1, 2, 3], [1, 3]))
    """
    order_qt_compare_set_semantics """
        select array_sort(array_except(['a', 'a', 'b'], ['a'])),
               array_sort(array_except_all(['a', 'a', 'b'], ['a']))
    """

    sql "drop table if exists test_array_except_all_table"
    sql """
        create table test_array_except_all_table (
            id int,
            left_int array<int>,
            right_int array<int>,
            left_string array<string>,
            right_string array<string>
        ) distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_array_except_all_table values
            (1, [1, 1, 2], [1], ['a', 'a', 'b'], ['a']),
            (2, [1, null, 1], [null], ['a', null, 'a'], [null]),
            (3, [], [1], [], ['a']),
            (4, [1, 2], [], ['a', 'b'], []),
            (5, null, [1], null, ['a'])
    """
    order_qt_column_arguments """
        select id,
               array_sort(array_except_all(left_int, right_int)),
               array_sort(array_except_all(left_string, right_string))
        from test_array_except_all_table
        order by id
    """
    order_qt_left_constant """
        select id, array_sort(array_except_all([1, 1, 2], right_int))
        from test_array_except_all_table
        order by id
    """
    order_qt_right_constant """
        select id, array_sort(array_except_all(left_int, [1]))
        from test_array_except_all_table
        order by id
    """

    test {
        sql "select array_except_all([[1], [1], [2]], [[1]])"
        exception "array_except_all does not support types"
    }
    test {
        sql "select array_except_all(array(map(1, 'a')), array(map(1, 'a')))"
        exception "array_except_all does not support types"
    }
    test {
        sql """
            select array_except_all(
                array(named_struct('a', 1)), array(named_struct('a', 1)))
        """
        exception "array_except_all does not support types"
    }
}
