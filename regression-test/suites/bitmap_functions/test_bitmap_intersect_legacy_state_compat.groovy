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

suite("test_bitmap_intersect_legacy_state_compat", "p0") {
    sql "SET enable_agg_state=true"
    sql "DROP TABLE IF EXISTS test_bitmap_intersect_v1_random"
    sql """
        CREATE TABLE test_bitmap_intersect_v1_random (
            id INT, state AGG_STATE<bitmap_intersect_v1(bitmap NULL)> GENERIC
        ) AGGREGATE KEY(id) DISTRIBUTED BY RANDOM BUCKETS 4
        PROPERTIES('replication_num'='1')
    """
    sql """
        INSERT INTO test_bitmap_intersect_v1_random VALUES
            (1, bitmap_intersect_v1_state(bitmap_from_string('1,2'))),
            (2, bitmap_intersect_v1_state(bitmap_from_string('4,5')))
    """
    sql """
        INSERT INTO test_bitmap_intersect_v1_random VALUES
            (1, bitmap_intersect_v1_state(bitmap_from_string('2,3'))),
            (2, bitmap_intersect_v1_state(bitmap_from_string('5,6')))
    """
    // Both explicit and canonical state consumers must also preserve the implicit RANDOM union's decoder.
    order_qt_v1_random_read """
        SELECT id, bitmap_to_string(bitmap_intersect_finalize(state))
        FROM test_bitmap_intersect_v1_random
    """
    order_qt_v1_random_merge """
        SELECT id, bitmap_to_string(bitmap_intersect_merge(state)),
            bitmap_to_string(bitmap_intersect_v1_merge(state))
        FROM test_bitmap_intersect_v1_random GROUP BY id
    """
    order_qt_v1_random_union """
        SELECT id, bitmap_to_string(bitmap_intersect_finalize(state)) FROM (
            SELECT id, bitmap_intersect_union(state) AS state
            FROM test_bitmap_intersect_v1_random GROUP BY id
        ) states
    """

    sql "DROP TABLE IF EXISTS test_bitmap_intersect_v1_hash"
    sql """
        CREATE TABLE test_bitmap_intersect_v1_hash (
            id INT, state AGG_STATE<bitmap_intersect_v1(bitmap NULL)> GENERIC
        ) AGGREGATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES('replication_num'='1')
    """
    sql "INSERT INTO test_bitmap_intersect_v1_hash SELECT * FROM test_bitmap_intersect_v1_random"

    sql "DROP TABLE IF EXISTS test_bitmap_intersect_v1_random_clone"
    sql "CREATE TABLE test_bitmap_intersect_v1_random_clone LIKE test_bitmap_intersect_v1_random"
    sql "INSERT INTO test_bitmap_intersect_v1_random_clone SELECT * FROM test_bitmap_intersect_v1_random"
    order_qt_v1_random_like """
        SELECT id, bitmap_to_string(bitmap_intersect_v1_finalize(state))
        FROM test_bitmap_intersect_v1_random_clone
    """

    sql "DROP TABLE IF EXISTS test_bitmap_intersect_v1_hash_clone"
    sql "CREATE TABLE test_bitmap_intersect_v1_hash_clone LIKE test_bitmap_intersect_v1_hash"
    sql "INSERT INTO test_bitmap_intersect_v1_hash_clone SELECT * FROM test_bitmap_intersect_v1_hash"
    order_qt_v1_hash_like """
        SELECT id, bitmap_to_string(bitmap_intersect_v1_finalize(state))
        FROM test_bitmap_intersect_v1_hash_clone
    """

    sql "DROP TABLE IF EXISTS test_bitmap_intersect_v1_replay"
    def legacyDdl = sql "SHOW CREATE TABLE test_bitmap_intersect_v1_hash"
    sql legacyDdl[0][1].replace('`test_bitmap_intersect_v1_hash`', '`test_bitmap_intersect_v1_replay`')
    sql "INSERT INTO test_bitmap_intersect_v1_replay SELECT * FROM test_bitmap_intersect_v1_hash"
    order_qt_v1_show_create_replay """
        SELECT id, bitmap_to_string(bitmap_intersect_v1_finalize(state))
        FROM test_bitmap_intersect_v1_replay
    """

    // The public alias and unversioned schema SQL continue creating v2 states.
    sql "DROP TABLE IF EXISTS test_bitmap_intersect_v2_random"
    sql """
        CREATE TABLE test_bitmap_intersect_v2_random (
            id INT, state AGG_STATE<bitmap_intersect(bitmap NULL)> GENERIC
        ) AGGREGATE KEY(id) DISTRIBUTED BY RANDOM BUCKETS 4
        PROPERTIES('replication_num'='1')
    """
    sql """
        INSERT INTO test_bitmap_intersect_v2_random VALUES
            (1, bitmap_intersect_state(bitmap_from_string('1,2'))),
            (2, bitmap_intersect_state(bitmap_from_string('4,5')))
    """
    sql """
        INSERT INTO test_bitmap_intersect_v2_random VALUES
            (1, bitmap_intersect_state(bitmap_from_string('2,3'))),
            (2, bitmap_intersect_state(bitmap_from_string('5,6')))
    """
    order_qt_v2_random_read """
        SELECT id, bitmap_to_string(bitmap_intersect_finalize(state))
        FROM test_bitmap_intersect_v2_random
    """
    order_qt_v2_random_merge """
        SELECT id, bitmap_to_string(bitmap_intersect_merge(state))
        FROM test_bitmap_intersect_v2_random GROUP BY id
    """
    sql "DROP TABLE IF EXISTS test_bitmap_intersect_v2_clone"
    sql "CREATE TABLE test_bitmap_intersect_v2_clone LIKE test_bitmap_intersect_v2_random"
    sql "INSERT INTO test_bitmap_intersect_v2_clone SELECT * FROM test_bitmap_intersect_v2_random"
    order_qt_v2_like """
        SELECT id, bitmap_to_string(bitmap_intersect_v2_finalize(state))
        FROM test_bitmap_intersect_v2_clone
    """
}
