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

// This test verifies that when both MINMAX and IN runtime filters target the same
// key column, and the IN filter's value count exceeds max_pushdown_conditions_per_column,
// the IN_LIST predicate is NOT incorrectly erased by the key range construction logic.
// Regression test for the bug where _build_key_ranges_and_filters() erased IN_LIST
// predicates when the ColumnValueRange was a scope range (from MINMAX filter).
suite("test_rf_in_list_not_erased_by_scope_range") {
    sql "drop table if exists rf_scope_probe;"
    sql "drop table if exists rf_scope_build;"

    sql """
        CREATE TABLE rf_scope_probe (
            k1 BIGINT,
            v1 INT
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """

    sql """
        CREATE TABLE rf_scope_build (
            k1 BIGINT,
            v1 INT
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """

    // Probe table: insert 20 rows with k1 from 1 to 20.
    // The build side will only match a subset (k1 in {2,4,6,8,10,12}).
    // Rows NOT in this subset (k1=1,3,5,7,9,11,13..20) should be filtered out
    // by the IN_LIST runtime filter.
    sql """
        INSERT INTO rf_scope_probe VALUES
            (1, 1), (2, 2), (3, 3), (4, 4), (5, 5),
            (6, 6), (7, 7), (8, 8), (9, 9), (10, 10),
            (11, 11), (12, 12), (13, 13), (14, 14), (15, 15),
            (16, 16), (17, 17), (18, 18), (19, 19), (20, 20);
    """

    // Build table: 6 distinct k1 values. This exceeds max_pushdown_conditions_per_column=5
    // so the IN values are NOT added to ColumnValueRange, but the IN_LIST predicate is created.
    // MINMAX range: [2, 12]
    sql """
        INSERT INTO rf_scope_build VALUES
            (2, 100), (4, 200), (6, 300), (8, 400), (10, 500), (12, 600);
    """

    sql "sync;"

    // Set max_pushdown_conditions_per_column to 5, so the 6 IN values exceed it.
    // This causes IN values to NOT be added to the ColumnValueRange (it stays as
    // a scope range from the MINMAX filter), but the IN_LIST ColumnPredicate is still created.
    sql "set max_pushdown_conditions_per_column = 5;"
    // Use both IN and MIN_MAX runtime filter types so both are generated on the join key.
    sql "set runtime_filter_type = 'IN_OR_BLOOM_FILTER,MIN_MAX';"
    sql "set runtime_filter_wait_time_ms = 10000;"
    sql "set runtime_filter_wait_infinitely = true;"
    sql "set enable_runtime_filter_prune = false;"
    sql "set enable_left_semi_direct_return_opt = true;"
    sql "set parallel_pipeline_task_num = 1;"

    // The join should only return 6 rows (matching k1 in {2,4,6,8,10,12}).
    // If the IN_LIST predicate is incorrectly erased, the MINMAX scope [2,12]
    // would let through rows with k1 in {3,5,7,9,11} as well, producing wrong results.
    // We verify correctness by checking the result.
    order_qt_join """
        SELECT p.k1, p.v1
        FROM rf_scope_probe p
        LEFT SEMI JOIN rf_scope_build b ON p.k1 = b.k1
        ORDER BY p.k1;
    """
}
