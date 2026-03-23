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

// Regression tests for the string-length offset-only optimization.
//
// When length() is the *only* use of a string column (or a string field inside a
// struct), the FE should emit a DATA access path with an extra "offset" component so
// that the BE can satisfy the query by reading only the offset array instead of the
// full chars data.  The EXPLAIN plan should show:
//   nested columns: <col>:[DATA(<col>.offset)]
//
// Crucially, the slot type must remain varchar (not bigint), and any predicate
// using length() must be preserved as-is (e.g. "length(str_col) > 1"), never
// rewritten to "CAST(str_col AS int) > 1".
//
// When the same string column is also read directly (e.g. projected, passed to
// substr(), …) the optimization must be suppressed: no nested-columns entry for
// the plain string column should appear.

suite("string_length_column_pruning") {
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"

    sql """ DROP TABLE IF EXISTS slcp_str_tbl """
    sql """
        CREATE TABLE slcp_str_tbl (
            id       INT,
            str_col  STRING,
            c_struct STRUCT<f1: INT, f3: STRING>
        ) ENGINE = OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """

    // ─── Optimizable cases ──────────────────────────────────────────────────────

    // Plain string column: length() is the only use → offset access path emitted,
    // slot type stays varchar (not bigint).
    explain {
        sql "select length(str_col) from slcp_str_tbl"
        contains "nested columns"
        contains "offset"
        notContains "type=bigint"
    }

    // Struct string field: length(struct_element) is the only use
    explain {
        sql "select length(struct_element(c_struct, 'f3')) from slcp_str_tbl"
        contains "nested columns"
        contains "offset"
        notContains "type=bigint"
    }

    // length() in both SELECT and WHERE: predicate must remain length(str_col) > 1,
    // never be rewritten to CAST(str_col AS int) > 1. Slot type must stay varchar.
    explain {
        sql "select length(str_col) from slcp_str_tbl where length(str_col) > 1"
        contains "nested columns"
        contains "offset"
        contains "length(str_col"
        notContains "CAST(str_col"
        notContains "type=bigint"
    }

    // ─── Non-optimizable cases ──────────────────────────────────────────────────

    // str_col also projected directly → full chars data needed, offset path suppressed.
    // No nested-columns entry for str_col, slot type stays varchar.
    explain {
        sql "select length(str_col), str_col from slcp_str_tbl"
        notContains "type=bigint"
        notContains "CAST(str_col"
    }

    // str_col also used in substr() → full chars data needed
    explain {
        sql "select length(str_col), substr(str_col, 2) from slcp_str_tbl"
        notContains "type=bigint"
        notContains "CAST(str_col"
    }

    // Struct field also projected directly → field access is full, not offset-only
    // The struct's nested-columns entry still appears (partial struct pruning),
    // but the pruned field type must remain text (not bigint).
    explain {
        sql "select length(struct_element(c_struct, 'f3')), struct_element(c_struct, 'f3') from slcp_str_tbl"
        contains "nested columns"
        notContains "bigint"
    }

}
