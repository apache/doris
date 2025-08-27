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

    // Test cases for nullable arrays with different null distributions
    // These test the fix for correct array size comparison when nulls are present
    qt_sql "SELECT l1_distance(NULL, NULL)"
    qt_sql "SELECT l2_distance([1.0, 2.0], [3.0, 4.0])"
    qt_sql "SELECT cosine_distance([1.0, 2.0, 3.0], [4.0, 5.0, 6.0])"
    qt_sql "SELECT inner_product([2.0, 3.0], [4.0, 5.0])"
    
    // Test arrays with NULL elements inside
    qt_sql "SELECT l1_distance([1.0, NULL, 3.0], [4.0, NULL, 6.0])"
    qt_sql "SELECT l2_distance([NULL, 2.0], [NULL, 5.0])"
    
    // Test mixed nullable scenarios - these should work correctly after the fix
    qt_sql "SELECT l1_distance([1.0, 2.0], [3.0, 4.0]) as result1, l1_distance(NULL, [5.0, 6.0]) as result2"
    qt_sql "SELECT cosine_distance([1.0], [2.0]) as result1, cosine_distance([3.0], NULL) as result2"

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
    
    // Test cases for the nullable array offset fix
    // These cases specifically test scenarios where absolute offsets might differ
    // but actual array sizes are the same (should pass) or different (should fail)
    try {
        sql "SELECT l1_distance([1.0, 2.0, 3.0], [4.0, 5.0])"
    } catch (Exception ex) {
        assert("${ex}".contains("function l1_distance have different input element sizes"))
    }
    
    try {
        sql "SELECT inner_product([1.0], [2.0, 3.0, 4.0])"
    } catch (Exception ex) {
        assert("${ex}".contains("function inner_product have different input element sizes"))
    }
    
    // Edge case: empty arrays should work
    qt_sql "SELECT l1_distance(CAST([] as ARRAY<DOUBLE>), CAST([] as ARRAY<DOUBLE>))"
    qt_sql "SELECT l2_distance(CAST([] as ARRAY<DOUBLE>), CAST([] as ARRAY<DOUBLE>))"
    
    // Comprehensive test for the offset fix: test with table data containing mixed nulls
    // This specifically tests the scenario where offsets might differ due to null distribution
    // but actual array sizes are the same
    sql """
        DROP TABLE IF EXISTS test_array_distance_nullable
    """
    sql """
        CREATE TABLE test_array_distance_nullable (
            id INT,
            arr1 ARRAY<DOUBLE>,
            arr2 ARRAY<DOUBLE>
        ) PROPERTIES (
            "replication_num" = "1"
        )
    """
    sql """
        INSERT INTO test_array_distance_nullable VALUES
        (1, [1.0, 2.0], [3.0, 4.0]),
        (2, NULL, [5.0, 6.0]),
        (3, [7.0, 8.0], NULL),
        (4, [9.0, 10.0], [11.0, 12.0]),
        (5, NULL, NULL)
    """
    
    // These queries should work correctly after the fix
    qt_sql "SELECT id, l1_distance(arr1, arr2) FROM test_array_distance_nullable ORDER BY id"
    qt_sql "SELECT id, l2_distance(arr1, arr2) FROM test_array_distance_nullable ORDER BY id"
    qt_sql "SELECT id, cosine_distance(arr1, arr2) FROM test_array_distance_nullable ORDER BY id"
    qt_sql "SELECT id, inner_product(arr1, arr2) FROM test_array_distance_nullable ORDER BY id"
}
