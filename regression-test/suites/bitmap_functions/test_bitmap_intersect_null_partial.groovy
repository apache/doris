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

suite("test_bitmap_intersect_null_partial", "p0") {
    sql "SET enable_agg_state=true"
    sql "SET enable_bucketed_hash_agg=false"
    sql "SET parallel_pipeline_task_num=4"
    sql "DROP TABLE IF EXISTS test_bitmap_intersect_null_partial"
    sql """
        CREATE TABLE test_bitmap_intersect_null_partial (
            id INT, g INT, s STRING NULL
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 4
        PROPERTIES('replication_num'='1')
    """
    sql """
        INSERT INTO test_bitmap_intersect_null_partial VALUES
            (1,1,'1,2'), (2,1,NULL), (3,1,'2,3'),
            (4,2,NULL), (5,2,NULL),
            (6,3,'1,2'), (7,3,''), (8,3,NULL),
            (9,4,'1'), (10,4,'2'), (11,4,NULL)
    """

    for (def nullV2 : [false, true]) {
        sql "SET enable_aggregate_function_null_v2=${nullV2}"
        for (def phase : [1, 2]) {
            sql "SET agg_phase=${phase}"
            order_qt_grouped """
                SELECT g, bitmap_to_string(bitmap_intersect(bitmap_from_string(s)))
                FROM test_bitmap_intersect_null_partial GROUP BY g
            """
            order_qt_filtered """
                SELECT g, bitmap_to_string(bitmap_intersect(bitmap_from_string(s)))
                FROM test_bitmap_intersect_null_partial WHERE s IS NOT NULL GROUP BY g
            """
            order_qt_without_key """
                SELECT bitmap_to_string(bitmap_intersect(bitmap_from_string(s)))
                FROM test_bitmap_intersect_null_partial WHERE g=1
            """
            order_qt_null_only """
                SELECT bitmap_to_string(bitmap_intersect(bitmap_from_string(s)))
                FROM test_bitmap_intersect_null_partial WHERE g=2
            """
            order_qt_empty_input """
                SELECT bitmap_to_string(bitmap_intersect(bitmap_from_string(s)))
                FROM test_bitmap_intersect_null_partial WHERE id<0
            """
            order_qt_union_control """
                SELECT bitmap_to_string(bitmap_union(bitmap_from_string(s)))
                FROM test_bitmap_intersect_null_partial WHERE g=1
            """
            order_qt_state_merge """
                SELECT bitmap_to_string(bitmap_intersect_merge(state))
                FROM (
                    SELECT bitmap_intersect_state(bitmap_from_string(s)) AS state
                    FROM test_bitmap_intersect_null_partial WHERE g=1
                ) states
            """
            order_qt_state_combine """
                SELECT g, bitmap_to_string(bitmap_intersect_finalize(state))
                FROM (
                    SELECT g, bitmap_intersect_combine(bitmap_from_string(s)) AS state
                    FROM test_bitmap_intersect_null_partial GROUP BY g
                ) states
            """
        }
    }

    sql "DROP TABLE IF EXISTS test_bitmap_intersect_not_null"
    sql """
        CREATE TABLE test_bitmap_intersect_not_null (
            id INT NOT NULL, g INT NOT NULL, b BITMAP NOT NULL
        ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 8
        PROPERTIES('replication_num'='1')
    """
    sql """
        INSERT INTO test_bitmap_intersect_not_null VALUES
            (1,1,bitmap_from_string('1,2')), (2,1,bitmap_from_string('2,3')),
            (3,2,bitmap_from_string('1,2')), (4,2,bitmap_empty())
    """
    for (def nullV2 : [false, true]) {
        sql "SET enable_aggregate_function_null_v2=${nullV2}"
        for (def phase : [1, 2]) {
            sql "SET agg_phase=${phase}"
            order_qt_not_null_grouped """
                SELECT g, bitmap_to_string(bitmap_intersect(b))
                FROM test_bitmap_intersect_not_null GROUP BY g
            """
            order_qt_not_null_without_key """
                SELECT bitmap_to_string(bitmap_intersect(b))
                FROM test_bitmap_intersect_not_null WHERE g=1
            """
            order_qt_not_null_empty_input """
                SELECT bitmap_to_string(bitmap_intersect(b))
                FROM test_bitmap_intersect_not_null WHERE id<0
            """
            order_qt_not_null_empty_state_merge """
                SELECT bitmap_to_string(bitmap_intersect_merge(state)) FROM (
                    SELECT bitmap_intersect_combine(b) AS state
                    FROM test_bitmap_intersect_not_null WHERE g=1
                    UNION ALL
                    SELECT bitmap_intersect_combine(b) AS state
                    FROM test_bitmap_intersect_not_null WHERE id<0
                ) states
            """
        }
    }

    sql "SET enable_spill=true"
    sql "SET enable_force_spill=true"
    sql "SET spill_min_revocable_mem=1"
    sql "SET agg_phase=2"
    for (def nullV2 : [false, true]) {
        sql "SET enable_aggregate_function_null_v2=${nullV2}"
        order_qt_spilled_partial """
            SELECT g, bitmap_to_string(bitmap_intersect(bitmap_from_string(s)))
            FROM test_bitmap_intersect_null_partial GROUP BY g
        """
    }
    sql "SET enable_force_spill=false"
    sql "SET enable_spill=false"

    sql "set enable_agg_state = true;"
    sql "DROP TABLE IF EXISTS test_bitmap_intersect_partial_state"
    sql """
        CREATE TABLE test_bitmap_intersect_partial_state (
            id INT, state AGG_STATE<bitmap_intersect(bitmap NULL)> GENERIC
        ) AGGREGATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 4
        PROPERTIES('replication_num'='1')
    """
    sql """
        INSERT INTO test_bitmap_intersect_partial_state
        SELECT id, bitmap_intersect_state(bitmap_from_string(s))
        FROM test_bitmap_intersect_null_partial WHERE g=1
    """
    order_qt_stored_state """
        SELECT bitmap_to_string(bitmap_intersect_merge(state))
        FROM test_bitmap_intersect_partial_state
    """

    // Stored v2 states retain their storage codec under either query wrapper option.
    for (def nullV2 : [false, true]) {
        sql "SET enable_aggregate_function_null_v2=${nullV2}"
        order_qt_stored_state_wrapper_option """
            SELECT bitmap_to_string(bitmap_intersect_merge(state))
            FROM test_bitmap_intersect_partial_state
        """
    }

    // Existing bitmap-only states keep their decoder and the explicit v1 entry point.
    sql "DROP TABLE IF EXISTS test_bitmap_intersect_legacy_state"
    sql """
        CREATE TABLE test_bitmap_intersect_legacy_state (
            id INT, state AGG_STATE<bitmap_intersect_v1(bitmap NULL)> GENERIC
        ) AGGREGATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES('replication_num'='1')
    """
    sql """
        INSERT INTO test_bitmap_intersect_legacy_state VALUES
            (1, bitmap_intersect_v1_state(bitmap_from_string('1,2'))),
            (2, bitmap_intersect_v1_state(bitmap_from_string('2,3')))
    """
    order_qt_legacy_state """
        SELECT bitmap_to_string(bitmap_intersect_v1_merge(state))
        FROM test_bitmap_intersect_legacy_state
    """
    test {
        sql """
            SELECT bitmap_to_string(bitmap_intersect_v2_merge(state))
            FROM test_bitmap_intersect_legacy_state
        """
        exception "not match function bitmap_intersect_v2_merge"
    }
    test {
        sql """
            SELECT bitmap_to_string(bitmap_intersect_v1_merge(state))
            FROM test_bitmap_intersect_partial_state
        """
        exception "not match function bitmap_intersect_merge"
    }
}
