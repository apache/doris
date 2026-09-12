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
        sql "select array_union(array(to_bitmap(1)), array(to_bitmap(1)))"
        exception "array_union does not support element type BITMAP"
    }

    test {
        sql "select array_intersect(array(to_bitmap(1)), array(to_bitmap(1)))"
        exception "array_intersect does not support element type BITMAP"
    }

    qt_array_flatten "select array_flatten([[1, 2], [], [3]])"
    qt_array_flatten_empty "select array_flatten([])"
    qt_array_compact "select array_compact([1, 1, null, null, 2])"
    qt_array_union "select array_sort(array_union([1, 2], [2, 3]))"
    qt_array_intersect "select array_sort(array_intersect([1, 2], [2, 3]))"
}
