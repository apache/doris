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

suite("array_function_invalid_argument") {
    test {
        sql "select array_flatten(1)"
        exception "array_flatten requires an ARRAY argument, but got TINYINT"
    }

    test {
        sql "select array_compact(1)"
        exception "array_compact requires an ARRAY argument, but got TINYINT"
    }

    test {
        sql "select array_compact(array(to_bitmap(1), to_bitmap(1)))"
        exception "array_compact does not support type BITMAP"
    }

    test {
        sql "select array_compact(array(hll_hash('a'), hll_hash('a')))"
        exception "array_compact does not support type HLL"
    }

    test {
        sql "select array_compact(array(to_quantile_state(1, 2048), to_quantile_state(1, 2048)))"
        exception "array_compact does not support type QUANTILE_STATE"
    }

    test {
        sql "select array_compact(array(array(to_bitmap(1)), array(to_bitmap(1))))"
        exception "array_compact does not support type ARRAY<BITMAP"
    }

    test {
        sql "select array_union(array(to_bitmap(1)), array(to_bitmap(1)))"
        exception "array_union does not support element type BITMAP"
    }

    test {
        sql "select array_union(array(cast('a' as varbinary)), array(cast('b' as varbinary)))"
        exception "array_union does not support VARBINARY arguments"
    }

    test {
        sql "select array_union(array(cast('a' as varbinary)), array('b'))"
        exception "array_union does not support VARBINARY arguments"
    }

    test {
        sql "select array_union(array(cast('12:34:56' as time(0))), array(cast('12:34:57' as time(0))))"
        exception "array_union does not support element type TIME"
    }

    test {
        sql "select array_union(array(cast('12:34:56' as time(0))), array('12:34:57'))"
        exception "array_union does not support element type TIME"
    }

    test {
        sql "select array_intersect(array(to_bitmap(1)), array(to_bitmap(1)))"
        exception "array_intersect does not support element type BITMAP"
    }

    test {
        sql "select array_intersect(array(cast('a' as varbinary)), array(cast('b' as varbinary)))"
        exception "array_intersect does not support VARBINARY arguments"
    }

    test {
        sql "select array_intersect(array(cast('12:34:56' as time(0))), array(cast('12:34:57' as time(0))))"
        exception "array_intersect does not support element type TIME"
    }

    test {
        sql "select array_except(array(to_bitmap(1)), array(to_bitmap(1)))"
        exception "array_except does not support element type BITMAP"
    }

    test {
        sql "select array_except(array(cast('a' as varbinary)), array(cast('b' as varbinary)))"
        exception "array_except does not support VARBINARY arguments"
    }

    test {
        sql "select array_except(array(cast('a' as varbinary)), array('b'))"
        exception "array_except does not support VARBINARY arguments"
    }

    test {
        sql "select array_except(array(cast('12:34:56' as time(0))), array(cast('12:34:57' as time(0))))"
        exception "array_except does not support element type TIME"
    }

    test {
        sql "select array_except(array(cast('12:34:56' as time(0))), array('12:34:57'))"
        exception "array_except does not support element type TIME"
    }

    test {
        sql "select array_distinct(array(to_bitmap(1), to_bitmap(1)))"
        exception "array_distinct does not support element type BITMAP"
    }

    test {
        sql "select array_distinct(array(cast('a' as varbinary), cast('a' as varbinary)))"
        exception "array_distinct does not support VARBINARY arguments"
    }

    test {
        sql "select array_enumerate_uniq(array(to_bitmap(1), to_bitmap(1)))"
        exception "array_enumerate_uniq does not support element type BITMAP"
    }

    test {
        sql "select array_enumerate_uniq(array(cast('a' as varbinary), cast('a' as varbinary)))"
        exception "array_enumerate_uniq does not support VARBINARY arguments"
    }

    test {
        sql """
            select array_enumerate_uniq(
                array(to_bitmap(1), to_bitmap(1)),
                array(to_bitmap(2), to_bitmap(2)))
        """
        exception "array_enumerate_uniq does not support element type BITMAP"
    }

    test {
        sql "select array_position(array(to_bitmap(1)), to_bitmap(1))"
        exception "array_position does not support element type BITMAP"
    }

    test {
        sql "select array_contains(array(to_bitmap(1)), to_bitmap(1))"
        exception "array_contains does not support element type BITMAP"
    }

    test {
        sql "select countequal(array(to_bitmap(1)), to_bitmap(1))"
        exception "countequal does not support element type BITMAP"
    }

    test {
        sql "select array_remove(array(to_bitmap(1)), to_bitmap(1))"
        exception "array_remove does not support element type BITMAP"
    }

    test {
        sql "select array_contains_all(array(to_bitmap(1)), array(to_bitmap(1)))"
        exception "array_contains_all does not support element type BITMAP"
    }

    test {
        sql "select arrays_overlap(array(to_bitmap(1)), array(to_bitmap(1)))"
        exception "arrays_overlap does not support element type BITMAP"
    }

    test {
        sql "select array_except_all(array(to_bitmap(1)), array(to_bitmap(1)))"
        exception "array_except_all does not support element type BITMAP"
    }

    test {
        sql "select array_sort(array(to_bitmap(1), to_bitmap(2)))"
        exception "array_sort does not support types"
    }

    test {
        sql "select array_sort(array(array(to_bitmap(1)), array(to_bitmap(2))))"
        exception "array_sort does not support types"
    }

    test {
        sql """
            select array_sort(
                (x, y) -> if(bitmap_count(x) < bitmap_count(y), -1,
                    if(bitmap_count(x) = bitmap_count(y), 0, 1)),
                array(to_bitmap(2), to_bitmap(1)))
        """
        exception "array_sort does not support types"
    }

    test {
        sql "select array_reverse_sort(array(to_bitmap(1), to_bitmap(2)))"
        exception "array_reverse_sort does not support types"
    }

    test {
        sql "select array_min(array(to_bitmap(1), to_bitmap(2)))"
        exception "array_min does not support element type BITMAP"
    }

    test {
        sql "select array_max(array(to_bitmap(1), to_bitmap(2)))"
        exception "array_max does not support element type BITMAP"
    }

    test {
        sql "select array_sortby([1, 2], array(to_bitmap(1), to_bitmap(2)))"
        exception "array_sortby does not support types"
    }

    qt_array_sort_lambda_empty "select array_sort((x, y) -> 0, [])"
    qt_array_sort_lambda_nulls "select array_sort((x, y) -> 0, [NULL, NULL])"
    qt_array_flatten "select array_flatten([[1, 2], [], [3]])"
    qt_array_flatten_empty "select array_flatten([])"
    qt_array_compact "select array_compact([1, 1, null, null, 2])"
    order_qt_array_compact_nested "select array_compact([[1], [1], [2]])"
    qt_array_union "select array_sort(array_union([1, 2], [2, 3]))"
    qt_array_intersect "select array_sort(array_intersect([1, 2], [2, 3]))"
    qt_array_distinct_time "select array_distinct(array(cast('12:34:56' as time(0)), cast('12:34:56' as time(0))))"
    qt_array_enumerate_uniq_time "select array_enumerate_uniq(array(cast('12:34:56' as time(0)), cast('12:34:56' as time(0))))"
    qt_array_position_time "select array_position(array(cast('12:34:56' as time(0))), cast('12:34:56' as time(0)))"
}
