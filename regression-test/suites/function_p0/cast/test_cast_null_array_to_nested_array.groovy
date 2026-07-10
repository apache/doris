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

suite("test_cast_null_array_to_nested_array") {
    sql "set debug_skip_fold_constant = false;"

    qt_cast_empty_array_to_nested_array_fold """
        select cast([] as array<array<int>>);
    """

    qt_cast_null_array_to_nested_array_fold """
        select cast([null] as array<array<int>>);
    """

    qt_cast_nulls_array_to_nested_array_fold """
        select cast([null, null] as array<array<int>>);
    """

    qt_cast_nested_null_array_to_deeper_array_fold """
        select cast([[null]] as array<array<array<int>>>);
    """

    test {
        sql "select cast([1] as array<array<int>>);"
        exception "can not cast from origin type ARRAY<TINYINT> to target type=ARRAY<ARRAY<INT>>"
    }

    test {
        sql "select cast([1, null] as array<array<int>>);"
        exception "can not cast from origin type ARRAY<TINYINT> to target type=ARRAY<ARRAY<INT>>"
    }

    sql "set debug_skip_fold_constant = true;"

    qt_cast_empty_array_to_nested_array_be """
        select cast([] as array<array<int>>);
    """

    qt_cast_null_array_to_nested_array_be """
        select cast([null] as array<array<int>>);
    """

    qt_cast_nulls_array_to_nested_array_be """
        select cast([null, null] as array<array<int>>);
    """

    qt_cast_nested_null_array_to_deeper_array_be """
        select cast([[null]] as array<array<array<int>>>);
    """

    test {
        sql "select cast([1] as array<array<int>>);"
        exception "can not cast from origin type ARRAY<TINYINT> to target type=ARRAY<ARRAY<INT>>"
    }

    test {
        sql "select cast([1, null] as array<array<int>>);"
        exception "can not cast from origin type ARRAY<TINYINT> to target type=ARRAY<ARRAY<INT>>"
    }
}
