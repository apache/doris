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

    test {
        sql "SELECT l2_distance([1, 2, 3], NULL)"
        exception "function l2_distance cannot be null"
    }
    

    // Test cases for nullable arrays with different null distributions
    // These test the fix for correct array size comparison when nulls are present
    test {
        sql "SELECT l1_distance(NULL, NULL)"
        exception "function l1_distance cannot be null"
    } 

    qt_sql "SELECT l2_distance([1.0, 2.0], [3.0, 4.0])"
    qt_sql "SELECT cosine_distance([1.0, 2.0, 3.0], [4.0, 5.0, 6.0])"
    qt_sql "SELECT inner_product([2.0, 3.0], [4.0, 5.0])"

    // Test mixed nullable scenarios - these should work correctly after the fix
    test {
        sql "SELECT l1_distance([1.0, 2.0], [3.0, 4.0]) as result1, l1_distance(NULL, [5.0, 6.0]) as result2"
        exception "function l1_distance cannot be null"
    }

    test {
        sql "SELECT cosine_distance([1.0], [2.0]) as result1, cosine_distance([3.0], NULL) as result2"
        exception "function cosine_distance cannot be null"
    }

    // abnormal test cases
    test {
        sql "SELECT l2_distance([0, 0], [1])"
        exception "function l2_distance have different input element sizes"
    }

    test {
        sql "SELECT cosine_distance([NULL], [NULL, NULL])"
        exception "function cosine_distance cannot have null"
    }

    // Test cases for the nullable array offset fix
    // These cases specifically test scenarios where absolute offsets might differ
    // but actual array sizes are the same (should pass) or different (should fail)
    test {
        sql "SELECT l1_distance([1.0, 2.0, 3.0], [4.0, 5.0])"
        exception "function l1_distance have different input element sizes"
    }

    test {
        sql "SELECT inner_product([1.0], [2.0, 3.0, 4.0])"
        exception "function inner_product have different input element sizes"
    }

    test {
        sql "SELECT l1_distance([1, 2, 3], [0, NULL, 0])"
        exception "function l1_distance cannot have null"
    }

    // Edge case: empty arrays should work
    qt_sql "SELECT l1_distance(CAST([] as ARRAY<DOUBLE>), CAST([] as ARRAY<DOUBLE>))"
    qt_sql "SELECT l2_distance(CAST([] as ARRAY<DOUBLE>), CAST([] as ARRAY<DOUBLE>))"

    // =========================
    // cosine_similarity tests
    // =========================
    
    // Basic test: identical vectors have similarity of 1.0
    qt_cosine_sim_identical "SELECT cosine_similarity([1, 2, 3], [1, 2, 3])"
    
    // Basic test: orthogonal vectors have similarity of 0.0
    qt_cosine_sim_orthogonal "SELECT cosine_similarity([1, 0], [0, 1])"
    
    // Basic test: opposite vectors have similarity of -1.0
    qt_cosine_sim_opposite "SELECT cosine_similarity([1, 2, 3], [-1, -2, -3])"
    
    // Test with float arrays
    qt_cosine_sim_float "SELECT cosine_similarity([1.0, 2.0, 3.0], [4.0, 5.0, 6.0])"
    
    // Test known value: cos(theta) = (1*3 + 2*5 + 3*7) / (sqrt(14) * sqrt(83)) = 34 / sqrt(1162) ≈ 0.9974
    qt_cosine_sim_known "SELECT cosine_similarity([1, 2, 3], [3, 5, 7])"
    
    // Test with single element arrays
    qt_cosine_sim_single "SELECT cosine_similarity([5], [10])"
    
    // Test with 2D vectors
    qt_cosine_sim_2d "SELECT cosine_similarity([3, 4], [4, 3])"
    
    // Test with negative values
    qt_cosine_sim_negative "SELECT cosine_similarity([-1, -2], [1, 2])"
    
    // Test zero vector handling: returns 0.0 when either vector is zero
    qt_cosine_sim_zero_first "SELECT cosine_similarity([0, 0, 0], [1, 2, 3])"
    qt_cosine_sim_zero_second "SELECT cosine_similarity([1, 2, 3], [0, 0, 0])"
    qt_cosine_sim_both_zero "SELECT cosine_similarity([0, 0], [0, 0])"
    qt_cosine_sim_single_zero "SELECT cosine_similarity([0], [0])"
    
    // Test with mixed positive and negative
    qt_cosine_sim_mixed "SELECT cosine_similarity([1, -1, 1], [-1, 1, -1])"
    
    // Test relationship with cosine_distance: cosine_similarity = 1 - cosine_distance
    // For non-zero vectors, these should sum to 1.0
    qt_cosine_sim_distance_relation "SELECT cosine_similarity([1, 2, 3], [3, 5, 7]) + cosine_distance([1, 2, 3], [3, 5, 7])"
    
    // Test empty arrays
    qt_cosine_sim_empty "SELECT cosine_similarity(CAST([] as ARRAY<FLOAT>), CAST([] as ARRAY<FLOAT>))"
    
    // Test NULL handling: should throw exception
    test {
        sql "SELECT cosine_similarity([1, 2, 3], NULL)"
        exception "function cosine_similarity cannot be null"
    }
    
    test {
        sql "SELECT cosine_similarity(NULL, [1, 2, 3])"
        exception "function cosine_similarity cannot be null"
    }
    
    test {
        sql "SELECT cosine_similarity(NULL, NULL)"
        exception "function cosine_similarity cannot be null"
    }
    
    // Test array with NULL element: should throw exception
    test {
        sql "SELECT cosine_similarity([1, NULL, 3], [4, 5, 6])"
        exception "function cosine_similarity cannot have null"
    }
    
    test {
        sql "SELECT cosine_similarity([1, 2, 3], [4, NULL, 6])"
        exception "function cosine_similarity cannot have null"
    }
    
    // Test different array sizes: should throw exception
    test {
        sql "SELECT cosine_similarity([1, 2], [1, 2, 3])"
        exception "function cosine_similarity have different input element sizes"
    }
    
    test {
        sql "SELECT cosine_similarity([1, 2, 3, 4], [1, 2])"
        exception "function cosine_similarity have different input element sizes"
    }
    
    // Test large values
    qt_cosine_sim_large "SELECT cosine_similarity([1000000, 2000000], [3000000, 4000000])"
    
    // Test small values  
    qt_cosine_sim_small "SELECT cosine_similarity([0.001, 0.002], [0.003, 0.004])"
    
    // Test with multiple rows using table
    sql "DROP TABLE IF EXISTS test_cosine_similarity_table"
    sql """
        CREATE TABLE test_cosine_similarity_table (
            id INT,
            vec1 ARRAY<FLOAT>,
            vec2 ARRAY<FLOAT>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """
    
    sql """
        INSERT INTO test_cosine_similarity_table VALUES
        (1, [1, 0, 0], [1, 0, 0]),
        (2, [1, 0, 0], [0, 1, 0]),
        (3, [1, 2, 3], [4, 5, 6]),
        (4, [1, 1], [-1, -1]),
        (5, [3, 4], [4, 3])
    """
    
    qt_cosine_sim_table "SELECT id, cosine_similarity(vec1, vec2) as similarity FROM test_cosine_similarity_table ORDER BY id"
    
    sql "DROP TABLE IF EXISTS test_cosine_similarity_table"
}
