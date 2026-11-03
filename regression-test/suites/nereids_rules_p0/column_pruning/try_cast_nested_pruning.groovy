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

// Regression tests for TRY_CAST under nested column pruning.
//
// TRY_CAST of a composite type keeps whole-value semantics: the conversion fails (and the
// result is NULL) if ANY field conversion fails. Nested column pruning may narrow which
// columns a scan reads, but it must never narrow the value a TRY_CAST converts, and it must
// never downgrade a TRY_CAST into a strict CAST while rebuilding the expressions whose slots
// were narrowed.
//
// Three shapes are covered:
//   1. the TRY_CAST child is the slot itself: the whole struct stays read ([s]);
//   2. the TRY_CAST child is a derived struct access: the container's sibling field is still
//      pruned ([wrapper.payload]), so the derived element_at is rebuilt and the TRY_CAST must
//      survive that rebuild. Without the fix the rebuilt expression becomes a plain CAST and
//      raises "parse number fail" under enable_strict_cast instead of returning NULL;
//   3. plain CAST over a nested type: still pruned field by field ([s.a]), unchanged.

suite("try_cast_nested_pruning") {
    sql "set enable_prune_nested_column = true"
    sql "set enable_strict_cast = true"
    sql "DROP TABLE IF EXISTS try_cast_pruning_tbl"
    sql """
        CREATE TABLE try_cast_pruning_tbl (
            id      INT,
            s       STRUCT<a: VARCHAR(10), b: VARCHAR(10)>,
            wrapper STRUCT<payload: STRUCT<good: VARCHAR(10), bad: VARCHAR(10)>, unused: INT>
        ) ENGINE = OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """

    sql """
        INSERT INTO try_cast_pruning_tbl VALUES
            (1, named_struct('a', '10', 'b', '20'),
                named_struct('payload', named_struct('good', '10', 'bad', '20'), 'unused', 1)),
            (2, named_struct('a', '10', 'b', 'bad'),
                named_struct('payload', named_struct('good', '10', 'bad', 'bad'), 'unused', 2)),
            (3, NULL, NULL),
            (4, named_struct('a', 'bad', 'b', '20'),
                named_struct('payload', named_struct('good', 'bad', 'bad', '20'), 'unused', 4))
    """

    // ── 1. TRY_CAST over the slot itself ────────────────────────────────────────
    // The whole struct is read: narrowing to s.a would drop the failing b conversion and
    // silently turn the NULL of row 2 into 10.
    explain {
        sql "select element_at(try_cast(s as struct<a:int,b:int>), 'a') from try_cast_pruning_tbl"
        contains "all access paths: [s]"
    }

    order_qt_direct_slot_try_cast """
        SELECT id, element_at(TRY_CAST(s AS STRUCT<a:INT,b:INT>), 'a') AS a_i
        FROM try_cast_pruning_tbl ORDER BY id
    """

    // ── 2. TRY_CAST over a derived struct access ────────────────────────────────
    // The whole-value context only pins payload, so the sibling field wrapper.unused is still
    // pruned away and the inner element_at is rebuilt on the narrowed wrapper slot. The cast
    // must keep both its TRY_CAST identity and its full target type across that rebuild.
    explain {
        sql """
            select element_at(try_cast(element_at(wrapper, 'payload') as struct<good:int,bad:int>), 'good')
            from try_cast_pruning_tbl
        """
        contains "all access paths: [wrapper.payload]"
    }

    order_qt_derived_child_try_cast """
        SELECT id, element_at(TRY_CAST(element_at(wrapper, 'payload') AS STRUCT<good:INT,bad:INT>), 'good') AS good_i
        FROM try_cast_pruning_tbl ORDER BY id
    """

    // ── 3. Plain CAST over a nested type is still pruned field by field ─────────
    explain {
        sql "select element_at(cast(s as struct<a:int,b:int>), 'a') from try_cast_pruning_tbl"
        contains "all access paths: [s.a]"
    }

    // ── 4. Toggling pruning must not change any result ──────────────────────────
    sql "set enable_prune_nested_column = false"
    order_qt_direct_slot_try_cast_pruning_off """
        SELECT id, element_at(TRY_CAST(s AS STRUCT<a:INT,b:INT>), 'a') AS a_i
        FROM try_cast_pruning_tbl ORDER BY id
    """
    order_qt_derived_child_try_cast_pruning_off """
        SELECT id, element_at(TRY_CAST(element_at(wrapper, 'payload') AS STRUCT<good:INT,bad:INT>), 'good') AS good_i
        FROM try_cast_pruning_tbl ORDER BY id
    """
    sql "set enable_prune_nested_column = true"
}
