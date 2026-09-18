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

suite("test_hash_join_probe_side_zero_copy", "query,p0") {
    sql "DROP TABLE IF EXISTS test_hash_join_probe_side_zero_copy_probe"
    sql "DROP TABLE IF EXISTS test_hash_join_probe_side_zero_copy_build_many"
    sql "DROP TABLE IF EXISTS test_hash_join_probe_side_zero_copy_build_unique"

    sql """
        CREATE TABLE test_hash_join_probe_side_zero_copy_probe (
            k INT NOT NULL,
            v INT NOT NULL,
            s VARCHAR(10) NOT NULL,
            n INT NULL
        ) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """
        CREATE TABLE test_hash_join_probe_side_zero_copy_build_many (
            k INT NOT NULL,
            v INT NOT NULL,
            s VARCHAR(10) NOT NULL
        ) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """
        CREATE TABLE test_hash_join_probe_side_zero_copy_build_unique (
            k INT NOT NULL,
            v INT NOT NULL,
            s VARCHAR(10) NOT NULL
        ) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        INSERT INTO test_hash_join_probe_side_zero_copy_probe VALUES
            (1, 10, 'a', 10), (2, 20, 'b', 20), (3, 30, 'c', 30),
            (4, 40, 'd', 40), (5, 50, 'e', 50), (6, 60, 'f', 60)
    """
    sql """
        INSERT INTO test_hash_join_probe_side_zero_copy_build_many VALUES
            (1, 100, 'x'), (2, 200, 'y'), (3, 300, 'z'), (3, 301, 'zz')
    """
    sql """
        INSERT INTO test_hash_join_probe_side_zero_copy_build_unique VALUES
            (1, 100, 'x'), (2, 15, 'y'), (3, 300, 'c')
    """

    // The first output batch has an identity probe mapping, but probe row 3 still has a pending
    // build-side match. Probe columns must not be moved until that pending state is exhausted.
    order_qt_pending_build_match """
        SELECT /*+SET_VAR(batch_size=3, disable_join_reorder=true)*/
               p.k, p.v, p.s, b.v
        FROM test_hash_join_probe_side_zero_copy_probe p
        JOIN [broadcast] test_hash_join_probe_side_zero_copy_build_many b
          ON p.k = b.k
        ORDER BY p.k, p.v, p.s, b.v
    """

    // Other join conjuncts may filter the identity-mapped output after ownership transfer. The
    // lazy probe column k is materialized from the still row-sized probe block after filtering.
    order_qt_other_join_conjunct """
        SELECT /*+SET_VAR(batch_size=3, disable_join_reorder=true)*/
               p.k, p.v, p.s, b.v
        FROM test_hash_join_probe_side_zero_copy_probe p
        JOIN [broadcast] test_hash_join_probe_side_zero_copy_build_unique b
          ON p.k = b.k AND p.v < b.v AND p.s != b.s
        ORDER BY p.k, p.v, p.s, b.v
    """

    // The IN predicate is evaluated by a mark hash join, while the OR predicate is evaluated
    // afterwards. The two probe columns used by other join conjuncts make this path eligible for
    // probe-side ownership transfer.
    order_qt_mark_and_post_join_conjunct """
        SELECT /*+SET_VAR(batch_size=3, disable_join_reorder=true)*/
               p.k, p.v, p.s
        FROM test_hash_join_probe_side_zero_copy_probe p
        WHERE p.v IN (
            SELECT b.v
            FROM test_hash_join_probe_side_zero_copy_build_unique b
            WHERE p.k = b.k + 10 AND p.s != b.s AND p.v < b.v + 1000
        ) OR p.k = 2
        ORDER BY p.k, p.v, p.s
    """

    // The first probe batch has only equality misses, so its one-to-one sentinel rows allow
    // ownership transfer. The cleared placeholders must become physical reusable columns before
    // the second probe batch, whose duplicate probe indices take the normal indexed-copy path.
    order_qt_reuse_probe_block_after_transfer """
        SELECT /*+SET_VAR(batch_size=3, disable_join_reorder=true, parallel_pipeline_task_num=1)*/
               p.k, p.n
        FROM test_hash_join_probe_side_zero_copy_probe p
        LEFT SEMI JOIN [broadcast] test_hash_join_probe_side_zero_copy_build_unique b
          ON p.k = b.k + 3 AND p.n < b.v
        ORDER BY p.k, p.n
    """
}
