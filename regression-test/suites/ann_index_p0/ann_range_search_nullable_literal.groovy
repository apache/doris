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

// Test case for fix: Handle nullable literal gracefully in ANN range search
// When the right-side literal of comparison is nullable (e.g., from scalar subquery
// returning NULL), the query should not crash but fall back to normal execution.

suite("ann_range_search_nullable_literal") {
    sql "drop table if exists ann_nullable_test"
    sql "drop table if exists ann_nullable_threshold"

    // Main table with ANN index
    sql """
        create table ann_nullable_test (
            id int not null,
            embedding array<float> not null,
            value double null,
            INDEX ann_embedding(`embedding`) USING ANN PROPERTIES("index_type"="hnsw","metric_type"="l2_distance","dim"="4")
        ) duplicate key (`id`) 
        distributed by hash(`id`) buckets 1
        properties("replication_num"="1");
    """

    // Auxiliary table for threshold values (can be empty to produce NULL from MIN/MAX)
    sql """
        create table ann_nullable_threshold (
            id int not null,
            threshold double null
        ) duplicate key (`id`) 
        distributed by hash(`id`) buckets 1
        properties("replication_num"="1");
    """

    // Insert test data into main table
    sql """
        INSERT INTO ann_nullable_test (id, embedding, value) VALUES
            (0, [1.0, 2.0, 3.0, 4.0], 10.5),
            (1, [2.0, 3.0, 4.0, 5.0], 20.5),
            (2, [3.0, 4.0, 5.0, 6.0], 30.5),
            (3, [4.0, 5.0, 6.0, 7.0], 40.5),
            (4, [5.0, 6.0, 7.0, 8.0], 50.5);
    """

    // Test 1: Scalar subquery returning NULL (empty table case)
    // When threshold table is empty, MIN(threshold) returns NULL
    // This should not crash, just return empty result (since comparing with NULL is always false)
    qt_nullable_subquery_empty """
        select id, embedding from ann_nullable_test 
        where l2_distance_approximate(embedding, [1.0, 2.0, 3.0, 4.0]) < (select min(threshold) from ann_nullable_threshold)
        order by id;
    """

    qt_nullable_subquery_empty_ge """
        select id, embedding from ann_nullable_test 
        where l2_distance_approximate(embedding, [1.0, 2.0, 3.0, 4.0]) >= (select max(threshold) from ann_nullable_threshold)
        order by id;
    """

    // Insert some data with NULL values
    sql """
        INSERT INTO ann_nullable_threshold (id, threshold) VALUES
            (1, NULL),
            (2, NULL);
    """

    // Test 2: Scalar subquery returning NULL (all values are NULL case)
    qt_nullable_subquery_all_null """
        select id, embedding from ann_nullable_test 
        where l2_distance_approximate(embedding, [1.0, 2.0, 3.0, 4.0]) < (select min(threshold) from ann_nullable_threshold)
        order by id;
    """

    // Insert some non-NULL values
    sql """
        INSERT INTO ann_nullable_threshold (id, threshold) VALUES
            (3, 5.0),
            (4, 10.0);
    """

    // Test 3: Scalar subquery returning non-NULL value - should work normally
    qt_nullable_subquery_normal """
        select id, embedding from ann_nullable_test 
        where l2_distance_approximate(embedding, [1.0, 2.0, 3.0, 4.0]) < (select min(threshold) from ann_nullable_threshold)
        order by id;
    """

    qt_nullable_subquery_normal_max """
        select id, embedding from ann_nullable_test 
        where l2_distance_approximate(embedding, [1.0, 2.0, 3.0, 4.0]) < (select max(threshold) from ann_nullable_threshold)
        order by id;
    """

    // Test 4: COALESCE with NULL - the result type might still be nullable
    qt_coalesce_with_null """
        select id, embedding from ann_nullable_test 
        where l2_distance_approximate(embedding, [1.0, 2.0, 3.0, 4.0]) < coalesce((select min(threshold) from ann_nullable_threshold where id = 1), 5.0)
        order by id;
    """

    // Test 5: CASE expression that might return NULL
    qt_case_nullable """
        select id, embedding from ann_nullable_test 
        where l2_distance_approximate(embedding, [1.0, 2.0, 3.0, 4.0]) < 
            (case when (select count(*) from ann_nullable_threshold where threshold is not null) > 0 
                  then (select min(threshold) from ann_nullable_threshold where threshold is not null)
                  else null end)
        order by id;
    """

    // Test 6: Normal literal (not nullable) - should use ANN index
    qt_normal_literal """
        select id, embedding from ann_nullable_test 
        where l2_distance_approximate(embedding, [1.0, 2.0, 3.0, 4.0]) < 5.0
        order by id;
    """

    // Test 7: Inner product with nullable subquery
    sql "drop table if exists ann_nullable_ip_test"
    sql """
        create table ann_nullable_ip_test (
            id int not null,
            embedding array<float> not null,
            INDEX ann_embedding(`embedding`) USING ANN PROPERTIES("index_type"="hnsw","metric_type"="inner_product","dim"="4")
        ) duplicate key (`id`) 
        distributed by hash(`id`) buckets 1
        properties("replication_num"="1");
    """

    sql """
        INSERT INTO ann_nullable_ip_test (id, embedding) VALUES
            (0, [1.0, 2.0, 3.0, 4.0]),
            (1, [2.0, 3.0, 4.0, 5.0]),
            (2, [3.0, 4.0, 5.0, 6.0]);
    """

    // Empty subquery returns NULL for inner_product comparison
    qt_ip_nullable_subquery """
        select id, embedding from ann_nullable_ip_test 
        where inner_product_approximate(embedding, [1.0, 2.0, 3.0, 4.0]) > (select min(threshold) from ann_nullable_threshold where id = 999)
        order by id;
    """

    // ========== Test 8-12: Non-distance function comparisons with nullable literals ==========
    // These tests ensure that when left child is NOT a distance function,
    // the nullable literal on right side does not cause any issues.
    // The query should execute normally without crashing.

    // Test 8: Regular column comparison with nullable subquery (empty table)
    sql "truncate table ann_nullable_threshold"
    qt_non_dist_nullable_empty """
        select id, embedding from ann_nullable_test 
        where value < (select min(threshold) from ann_nullable_threshold)
        order by id;
    """

    // Test 9: Regular column comparison with nullable subquery (all NULL values)
    sql """
        INSERT INTO ann_nullable_threshold (id, threshold) VALUES (1, NULL), (2, NULL);
    """
    qt_non_dist_nullable_all_null """
        select id, embedding from ann_nullable_test 
        where value < (select min(threshold) from ann_nullable_threshold)
        order by id;
    """

    // Test 10: Regular column comparison with nullable subquery (has non-NULL values)
    sql """
        INSERT INTO ann_nullable_threshold (id, threshold) VALUES (3, 25.0), (4, 35.0);
    """
    qt_non_dist_nullable_normal """
        select id, embedding from ann_nullable_test 
        where value < (select min(threshold) from ann_nullable_threshold)
        order by id;
    """

    // Test 11: Non-distance function (abs, sqrt, etc.) with nullable literal
    qt_non_dist_func_nullable """
        select id, embedding from ann_nullable_test 
        where abs(value) < (select min(threshold) from ann_nullable_threshold where id = 999)
        order by id;
    """

    // Test 12: Arithmetic expression with nullable literal
    qt_arithmetic_nullable """
        select id, embedding from ann_nullable_test 
        where (value + 10) < (select min(threshold) from ann_nullable_threshold where id = 999)
        order by id;
    """

    // ========== Test 13-15: Mixed scenarios ==========
    // Test 13: Distance function AND regular comparison, both with nullable
    sql "truncate table ann_nullable_threshold"
    qt_mixed_dist_and_regular_nullable """
        select id, embedding from ann_nullable_test 
        where l2_distance_approximate(embedding, [1.0, 2.0, 3.0, 4.0]) < (select min(threshold) from ann_nullable_threshold)
          and value < (select max(threshold) from ann_nullable_threshold)
        order by id;
    """

    // Test 14: Distance function with non-nullable, regular with nullable
    sql """
        INSERT INTO ann_nullable_threshold (id, threshold) VALUES (1, 5.0);
    """
    qt_dist_normal_regular_nullable """
        select id, embedding from ann_nullable_test 
        where l2_distance_approximate(embedding, [1.0, 2.0, 3.0, 4.0]) < 5.0
          and value < (select min(threshold) from ann_nullable_threshold where id = 999)
        order by id;
    """

    // Test 15: OR condition with nullable literals
    qt_or_condition_nullable """
        select id, embedding from ann_nullable_test 
        where l2_distance_approximate(embedding, [1.0, 2.0, 3.0, 4.0]) < (select min(threshold) from ann_nullable_threshold where id = 999)
           or value < (select max(threshold) from ann_nullable_threshold where id = 999)
        order by id;
    """

    // Cleanup
    sql "drop table if exists ann_nullable_test"
    sql "drop table if exists ann_nullable_threshold"
    sql "drop table if exists ann_nullable_ip_test"
}
