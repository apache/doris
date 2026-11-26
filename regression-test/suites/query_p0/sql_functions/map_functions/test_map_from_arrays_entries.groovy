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

suite("test_map_from_arrays_entries", "p0") {
    sql "set enable_nereids_planner = true"
    sql "set enable_fallback_to_original_planner = false"
    sql "set enable_decimal256 = false"
    sql "drop table if exists test_map_from_arrays_entries"
    sql """
        create table test_map_from_arrays_entries (
            id int,
            m map<int, int>,
            decimal_m map<decimalv3(38, 0), decimalv3(38, 38)>,
            time_m map<datetimev2(0), struct<f:datetimev2(6)>>
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_map_from_arrays_entries values
            (1, map(1, 10, 2, 20),
                map(cast(1 as decimalv3(38, 0)),
                    cast(0.12345678901234567890123456789012345678 as decimalv3(38, 38))),
                map(cast('2026-01-01 00:00:00' as datetimev2(0)),
                    named_struct('f',
                        cast('2026-01-01 00:00:00.123456' as datetimev2(6))))),
            (2, cast(map() as map<int, int>), null, null),
            (3, null, null, null)
    """

    qt_map_from_arrays_1 """
        select map_size(r), r[1], r[2]
        from (select map_from_arrays([1, 2], [10, 20]) r) t
    """

    qt_map_from_arrays_2 """
        select map_size(r), r[1]
        from (select map_from_arrays([1, 1], [10, 20]) r) t
    """

    order_qt_map_from_arrays_3 """
        select id, map_size(r), r[1], r[2]
        from (
            select id, map_from_arrays(map_keys(m), map_values(m)) r
            from test_map_from_arrays_entries
        ) t
        order by id
    """

    qt_map_from_arrays_4 """
        select map_from_arrays(
            cast(null as array<int>), cast(null as array<int>))
    """

    qt_map_from_arrays_5 """
        select map_from_arrays(
            array(cast(null as int), 2), array(10, cast(null as int)))
    """

    qt_map_from_arrays_6 """
        select map_from_arrays(cast(null as array<int>), [10, 20])
    """

    qt_map_from_entries_1 """
        select map_size(r), r[1], r[2]
        from (
            select map_from_entries(array(struct(1, 10), struct(2, 20))) r
        ) t
    """

    qt_map_from_entries_2 """
        select map_size(r), r[1]
        from (
            select map_from_entries(array(struct(1, 10), struct(1, 20))) r
        ) t
    """

    order_qt_map_from_entries_3 """
        select id, map_size(r), r[1], r[2]
        from (
            select id, map_from_entries(map_entries(m)) r
            from test_map_from_arrays_entries
        ) t
        order by id
    """

    qt_map_from_entries_4 """
        select map_from_entries(cast(null as array<struct<k:int,v:int>>))
    """

    qt_map_from_entries_5 """
        select map_from_entries(null)
    """

    qt_map_from_entries_6 """
        select map_from_entries(array(
            struct(cast(null as int), 10),
            struct(2, cast(null as int))))
    """

    test {
        sql """
            select if(
                        cast(map_from_entries(map_entries(decimal_m)) as string)
                            like '%0.12345678901234567890123456789012345678%',
                        1, 0),
                    if(cast(map_from_entries(map_entries(time_m)) as string)
                            like '%.123456%', 1, 0)
            from test_map_from_arrays_entries
            where id = 1
        """
        result([[1, 1]])
    }

    test {
        sql """
            select if(cast(map_from_entries(map_entries(map(
                            cast(1 as decimalv3(38, 0)),
                            cast(0.12345678901234567890123456789012345678
                                as decimalv3(38, 38))))) as string)
                        like '%0.12345678901234567890123456789012345678%', 1, 0),
                    if(cast(map_from_entries(map_entries(map(
                            cast('2026-01-01 00:00:00' as datetimev2(0)),
                            struct(cast('2026-01-01 00:00:00.123456'
                                as datetimev2(6)))))) as string)
                        like '%.123456%', 1, 0)
        """
        result([[1, 1]])
    }

    testFoldConst("select map_from_arrays([1, 2], ['a', 'b'])")
    testFoldConst("select map_from_arrays([1, 1], [10, 20])")
    testFoldConst("select map_from_arrays(array(cast(null as int), 2),"
            + " array(10, cast(null as int)))")
    testFoldConst("select map_from_arrays(cast([] as array<int>), cast([] as array<int>))")
    testFoldConst("select map_from_arrays([null], [null])")
    testFoldConst("select map_from_arrays(null, null)")
    testFoldConst("select map_from_arrays([], [])")
    testFoldConst("select map_from_arrays(map_keys(map(null, null)),"
            + " map_values(map(null, null)))")
    testFoldConst("select map_from_arrays([1], [[null]])")
    testFoldConst("select map_values(map_from_arrays("
            + "cast([1] as array<decimalv3(38, 0)>),"
            + "cast([0.12345678901234567890123456789012345678]"
            + " as array<decimalv3(38, 38)>)))[1]")
    testFoldConst("select microsecond(struct_element(map_values(map_from_arrays("
            + "cast(['2026-01-01 00:00:00'] as array<datetimev2(0)>),"
            + "array(struct(cast('2026-01-01 00:00:00.123456' as datetimev2(6)))))"
            + ")[1], 1))")
    testFoldConst("select map_from_entries(array(struct(1, 'a'), struct(2, 'b')))")
    testFoldConst("select map_from_entries(array(struct(1, 10), struct(1, 20)))")
    testFoldConst("select map_from_entries(array(struct(cast(null as int), 10),"
            + " struct(2, cast(null as int))))")
    testFoldConst("select map_from_entries(cast([] as array<struct<k:int,v:int>>))")
    testFoldConst("select map_from_entries(cast(null as array<struct<k:int,v:int>>))")
    testFoldConst("select map_from_entries(null)")
    testFoldConst("select map_from_entries([])")
    testFoldConst("select map_from_entries(array(struct(1, null)))")
    testFoldConst("select map_from_entries(map_entries(map(null, null)))")
    testFoldConst("select map_from_entries(array(struct(1, [null])))")
    testFoldConst("select map_from_entries(map_entries(map("
            + "cast(1 as decimalv3(38, 0)),"
            + "cast(0.12345678901234567890123456789012345678"
            + " as decimalv3(38, 38)))))")
    testFoldConst("select cast(map_from_entries(map_entries(map("
            + "cast('2026-01-01 00:00:00' as datetimev2(0)),"
            + "struct(cast('2026-01-01 00:00:00.123456' as datetimev2(6))))"
            + ")) as string)")

    test {
        sql "select map_from_arrays([1, 2], [10])"
        exception "Key and value arrays of function map_from_arrays must have the same length"
    }
    test {
        sql "select map_from_arrays([[1]], [10])"
        exception "MAP key type must be a primitive type"
    }
    test {
        sql "select map_from_entries(1)"
        exception "requires an array of structs with exactly two fields"
    }
    test {
        sql "select map_from_entries(array(struct(1)))"
        exception "requires an array of structs with exactly two fields"
    }
    test {
        sql "select map_from_entries(array(struct(1, 2, 3)))"
        exception "requires an array of structs with exactly two fields"
    }
    test {
        sql "select map_from_entries(array(cast(null as struct<k:int,v:int>)))"
        exception "Map entry of function map_from_entries cannot be null"
    }
    test {
        sql "select map_from_entries([null])"
        exception "Map entry of function map_from_entries cannot be null"
    }
}
