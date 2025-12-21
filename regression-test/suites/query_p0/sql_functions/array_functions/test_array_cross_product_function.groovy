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

suite("test_array_cross_product_function") {
    // normal test cases
    qt_sql "SELECT cross_product([1, 2, 3], [2, 3, 4])"
    qt_sql "SELECT cross_product([1, 2, 3], [0, 0, 0])"
    qt_sql "SELECT cross_product([0, 0, 0], [1, 2, 3])"
    qt_sql "SELECT cross_product([1, 0, 0], [0, 1, 0])"
    qt_sql "SELECT cross_product([0, 1, 0], [1, 0, 0])"

    // abnormal test cases
    try {
        sql "SELECT cross_product(NULL, [1, 2, 3])"
    } catch (Exception ex) {
        assert("${ex}".contains("First argument for function cross_product cannot be null"))
    }
    try {
        sql "SELECT cross_product([1, 2, 3], NULL)"
    } catch (Exception ex) {
        assert("${ex}".contains("Second argument for function cross_product cannot be null"))
    }
    try {
        sql "SELECT cross_product([1, NULL, 3], [1, 2, 3])"
    } catch (Exception ex) {
        assert("${ex}".contains("First argument for function cross_product cannot have null elements"))
    }
    try {
        sql "SELECT cross_product([1, 2, 3], [NULL, 2, 3])"
    } catch (Exception ex) {
        assert("${ex}".contains("Second argument for function cross_product cannot have null elements"))
    }
    try {
        sql "SELECT cross_product([1, 2, 3], [1, 2])"
    } catch (Exception ex) {
        assert("${ex}".contains("function cross_product have different input element sizes of array: 3 and 2"))
    }
    try {
        sql "SELECT cross_product([1, 2], [3, 4])"
    } catch (Exception ex) {
        assert("${ex}".contains("function cross_product requires arrays of size 3"))
    }
    try {
        sql "SELECT cross_product([1, 2, 3, 4], [1, 2, 3, 4])"
    } catch (Exception ex) {
        assert("${ex}".contains("function cross_product requires arrays of size 3"))
    }
}
