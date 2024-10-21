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

suite("test_array_distance_functions") {
    qt_sql "SELECT l1_distance([0, 0, 0], [1, 2, 3])"
    qt_sql "SELECT l2_distance([1, 2, 3], [0, 0, 0])"
    qt_sql "SELECT cosine_distance([1, 2, 3], [3, 5, 7])"
    qt_sql "SELECT cosine_distance([0], [0])"
    qt_sql "SELECT inner_product([1, 2], [2, 3])"

    qt_sql "SELECT l2_distance([1, 2, 3], NULL)"
    qt_sql "SELECT cosine_distance([1, 2, 3], [0, NULL, 0])"

    // abnormal test cases
    try {
        sql "SELECT l2_distance([0, 0], [1])"
    } catch (Exception ex) {
        assert("${ex}".contains("function l2_distance have different input element sizes"))
    }

    try {
        sql "SELECT cosine_distance([NULL], [NULL, NULL])"
    } catch (Exception ex) {
        assert("${ex}".contains("function cosine_distance have different input element sizes"))
    }
}
