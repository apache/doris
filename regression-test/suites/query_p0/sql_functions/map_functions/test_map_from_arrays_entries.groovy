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
    sql "drop table if exists test_map_from_arrays_entries"
    sql """
        create table test_map_from_arrays_entries (
            id int,
            m map<int, int>
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_map_from_arrays_entries values
            (1, map(1, 10, 2, 20)),
            (2, cast(map() as map<int, int>)),
            (3, null)
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
