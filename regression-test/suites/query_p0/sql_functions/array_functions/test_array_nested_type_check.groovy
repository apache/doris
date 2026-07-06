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

suite("test_array_nested_type_check") {
    sql "set enable_nereids_planner = true"
    sql "set enable_fallback_to_original_planner = false"

    test {
        sql "explain verbose select array_union([], [[1]])"
        exception "array_union does not support types: ARRAY<ARRAY<TINYINT>>"
    }

    test {
        sql "select array_union([], [[1]])"
        exception "array_union does not support types: ARRAY<ARRAY<TINYINT>>"
    }

    test {
        sql "select arrays_overlap([], [[1]])"
        exception "arrays_overlap does not support types: ARRAY<ARRAY<TINYINT>>"
    }

    test {
        sql "select array_position([], [1])"
        exception "array_position does not support types: ARRAY<ARRAY<TINYINT>>"
    }

    test {
        sql "select array_position([], cast('{}' as json))"
        exception "array_position does not support types: ARRAY<JSON>"
    }

    test {
        sql "select array_position([], cast('{}' as variant))"
        exception "array_position does not support types: ARRAY<variant"
    }

    test {
        sql "select array_remove([], [1])"
        exception "array_remove does not support types: ARRAY<ARRAY<TINYINT>>"
    }

    test {
        sql "select countequal([], [1])"
        exception "countequal does not support types: ARRAY<ARRAY<TINYINT>>"
    }
}
