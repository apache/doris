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

suite("test_complex_type") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    // array literal
    sql "select [1, 2, 3, 4]"

    // map literal
    sql "select {1:2, 3:4}"

    // struct literal
    sql "select {1, 2, 3, 4}"

    // struct constructor
    sql "select struct(1, 2, 3, 4)"

    // named struct constructor
    sql "select named_struct('x', 1, 'y', 2)"

    test {
        sql "select named_struct('x', 1, 'x', 2)"
        exception "The name of the struct field cannot be repeated"
    }

    // struct element with int
    sql "select struct_element(struct('1', '2', '3', '4'), 1);"

    test {
        sql "select struct_element(struct('1', '2', '3', '4'), 5);"
        exception "the specified field index out of bound"
    }

    test {
        sql "select struct_element(struct('1', '2', '3', '4'), -1);"
        exception "the specified field index out of bound"
    }

    // struct element with string
    sql "select struct_element(named_struct('1', '2', '3', '4'), '1');"

    test {
        sql "select struct_element(named_struct('1', '2', '3', '4'), '5')"
        exception "the specified field name 5 was not found"
    }
}

