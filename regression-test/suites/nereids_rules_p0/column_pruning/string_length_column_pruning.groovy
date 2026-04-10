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

// Regression tests for the string-length OFFSET-only optimization.
//
// When length() is the *only* use of a string column (or a string field inside a
// struct), the FE should emit a DATA access path with an extra "OFFSET" component so
// that the BE can satisfy the query by reading only the OFFSET array instead of the
// full chars data.  The EXPLAIN plan should show:
//   nested columns: <col>:[DATA(<col>.OFFSET)]
//
// Crucially, the slot type must remain varchar (not bigint), and any predicate
// using length() must be preserved as-is (e.g. "length(str_col) > 1"), never
// rewritten to "CAST(str_col AS int) > 1".
//
// When the same string column is also read directly (e.g. projected, passed to
// substr(), …) the optimization must be suppressed: no nested-columns entry for
// the plain string column should appear.

suite("string_length_column_pruning") {
    sql """ DROP TABLE IF EXISTS slcp_str_tbl """
    sql """
        CREATE TABLE slcp_str_tbl (
            id       INT,
            str_col  STRING,
            struct_col STRUCT<f1: INT, f3: STRING>,
            arr_col  ARRAY<INT>,
            map_col  MAP<STRING, STRING>,
            map_arr_col MAP<STRING, ARRAY<INT>>
        ) ENGINE = OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    sql """
        INSERT INTO slcp_str_tbl VALUES
            (1, 'hello', named_struct('f1', 10, 'f3', 'world'), [1, 2, 3], {'a': 'x', 'b': 'y'}, {'a': [1, 2], 'b': [3]})
    """

    // ─── Optimizable cases ──────────────────────────────────────────────────────

    // Plain string column: length() is the only use → OFFSET access path emitted,
    // slot type stays varchar (not bigint).
    explain {
        sql "select length(str_col) from slcp_str_tbl"
        contains "nested columns"
        contains "OFFSET"
        notContains "type=bigint"
    }
    sql "select length(str_col) from slcp_str_tbl"
    // Struct string field: length(struct_element) is the only use
    explain {
        sql "select length(struct_element(struct_col, 'f3')) from slcp_str_tbl"
        contains "nested columns"
        contains "OFFSET"
        notContains "type=bigint"
    }
    //sql "select length(struct_element(struct_col, 'f3')) from slcp_str_tbl"
    // length() in both SELECT and WHERE: predicate must remain length(str_col) > 1,
    // never be rewritten to CAST(str_col AS int) > 1. Slot type must stay varchar.
    explain {
        sql "select length(str_col) from slcp_str_tbl where length(str_col) > 1"
        contains "nested columns"
        contains "OFFSET"
        contains "length(str_col"
        notContains "CAST(str_col"
        notContains "type=bigint"
    }
    sql "select length(str_col) from slcp_str_tbl where length(str_col) > 1"

    // ─── Aggregate cases ─────────────────────────────────────────────────────────

    // sum(length(str_col)): length() is still the only consumer of str_col → OFFSET path applies.
    explain {
        sql "select sum(length(str_col)) from slcp_str_tbl"
        contains "nested columns"
        contains "OFFSET"
        notContains "type=bigint"
    }
    sql "select sum(length(str_col)) from slcp_str_tbl"
    // count(length(str_col))
    explain {
        sql "select count(length(str_col)) from slcp_str_tbl"
        contains "nested columns"
        contains "OFFSET"
        notContains "type=bigint"
    }

    // max(length(str_col))
    explain {
        sql "select max(length(str_col)) from slcp_str_tbl"
        contains "nested columns"
        contains "OFFSET"
        notContains "type=bigint"
    }
    sql "select max(length(str_col)) from slcp_str_tbl"
    // ─── Array column cases ──────────────────────────────────────────────────────

    // cardinality(arr_col): only the offset array is needed → OFFSET access path emitted,
    // slot type stays ARRAY (not bigint).
    explain {
        sql "select cardinality(arr_col) from slcp_str_tbl"
        contains "nested columns"
        contains "OFFSET"
        notContains "type=bigint"
    }
    sql "select cardinality(arr_col) from slcp_str_tbl"
    // cardinality(arr_col) in aggregate: OFFSET still applies.
    explain {
        sql "select sum(cardinality(arr_col)) from slcp_str_tbl"
        contains "nested columns"
        contains "OFFSET"
        notContains "type=bigint"
    }
    sql "select sum(cardinality(arr_col)) from slcp_str_tbl"
    // arr_col also accessed via element_at → full element data needed, OFFSET suppressed.
    explain {
        sql "select cardinality(arr_col), arr_col[1] from slcp_str_tbl"
        notContains "OFFSET"
        notContains "type=bigint"
    }

    // ─── Map column cases ────────────────────────────────────────────────────────

    // cardinality(map_col): only the offset array is needed → OFFSET access path emitted,
    // slot type stays MAP (not bigint).
    explain {
        sql "select cardinality(map_col) from slcp_str_tbl"
        contains "nested columns"
        contains "OFFSET"
        notContains "type=bigint"
    }
    sql "select cardinality(map_col) from slcp_str_tbl"

    // cardinality(map_col) in aggregate: OFFSET still applies.
    explain {
        sql "select sum(cardinality(map_col)) from slcp_str_tbl"
        contains "nested columns"
        contains "OFFSET"
        notContains "type=bigint"
    }

    explain {
        sql "select sum(map_size(map_col)) from slcp_str_tbl"
        contains "nested columns"
        contains "OFFSET"
        notContains "type=bigint"
    }

    // cardinality(map_keys(map_col)): only the keys offset array is needed → OFFSET access path emitted.
    explain {
        sql "select cardinality(map_keys(map_col)) from slcp_str_tbl"
        contains "nested columns"
        contains "OFFSET"
        notContains "type=bigint"
    }

    explain {
        sql "select cardinality(map_values(map_col)) from slcp_str_tbl"
        contains "nested columns"
        contains "OFFSET"
        notContains "type=bigint"
    }

    // cardinality(map_keys(map_col)) in aggregate: OFFSET still applies.
    explain {
        sql "select sum(cardinality(map_keys(map_col))) from slcp_str_tbl"
        contains "nested columns"
        contains "OFFSET"
        notContains "type=bigint"
    }

    explain {
        sql "select sum(cardinality(map_values(map_col))) from slcp_str_tbl"
        contains "nested columns"
        contains "OFFSET"
        notContains "type=bigint"
    }

    // Both map_keys and map_values sizes in the same query: both equal cardinality(map),
    // so only a single [map_col, OFFSET] path is needed.
    explain {
        sql "select sum(cardinality(map_keys(map_col))), sum(cardinality(map_values(map_col))) from slcp_str_tbl"
        contains "nested columns"
        contains "OFFSET"
        notContains "type=bigint"
    }

    // map_col also accessed via map_keys → full key data needed, OFFSET suppressed.
    explain {
        sql "select cardinality(map_col), map_keys(map_col) from slcp_str_tbl"
        notContains "type=bigint"
    }

    // ─── Map with complex value cases ────────────────────────────────────────────

    // cardinality(map_arr_col['a']): value is ARRAY<INT>.
    // Keys read in full (element lookup); values need only the OFFSET array (array size).
    // Expected paths: map_arr_col.KEYS + map_arr_col.VALUES.OFFSET
    explain {
        sql "select cardinality(map_arr_col['a']) from slcp_str_tbl"
        contains "nested columns"
        contains "KEYS"
        contains "VALUES"
        contains "OFFSET"
        notContains "type=bigint"
    }

    // same in aggregate
    explain {
        sql "select sum(cardinality(map_arr_col['a'])) from slcp_str_tbl"
        contains "nested columns"
        contains "KEYS"
        contains "VALUES"
        contains "OFFSET"
        notContains "type=bigint"
    }

    // value also accessed directly (arr[0]) → full VALUES needed, OFFSET suppressed
    explain {
        sql "select cardinality(map_arr_col['a']), map_arr_col['b'][0] from slcp_str_tbl"
        notContains "OFFSET"
        notContains "type=bigint"
    }

    // ─── Non-optimizable cases ──────────────────────────────────────────────────

    // str_col also projected directly → full chars data needed, OFFSET path suppressed.
    // No nested-columns entry for str_col, slot type stays varchar.
    explain {
        sql "select length(str_col), str_col from slcp_str_tbl"
        notContains "nested columns"
        notContains "type=bigint"
        notContains "CAST(str_col"
    }

    // str_col also used in substr() → full chars data needed
    explain {
        sql "select length(str_col), substr(str_col, 2) from slcp_str_tbl"
        notContains "nested columns"
        notContains "type=bigint"
        notContains "CAST(str_col"
    }

    // ─── StringEmptyToLength rule cases ─────────────────────────────────────────

    // str_col <> '' rewrites to length(str_col) != 0 → OFFSET optimization applies
    explain {
        sql "select length(str_col) from slcp_str_tbl where str_col <> ''"
        contains "nested columns"
        contains "OFFSET"
        notContains "type=bigint"
    }

    // str_col = '' also rewrites to length(str_col) = 0 → OFFSET applies
    explain {
        sql "select length(str_col) from slcp_str_tbl where str_col = ''"
        contains "nested columns"
        contains "OFFSET"
        notContains "type=bigint"
    }

    // aggregate + predicate rewrite: sum(length(str_col)) where str_col <> ''
    explain {
        sql "select sum(length(str_col)) from slcp_str_tbl where str_col <> ''"
        contains "nested columns"
        contains "OFFSET"
        notContains "type=bigint"
    }

    // str_col is also projected directly alongside the predicate → OFFSET suppressed
    // (str_col <> '' rewrites to length(str_col) != 0, but str_col is also projected as-is)
    explain {
        sql "select str_col, length(str_col) from slcp_str_tbl where str_col <> ''"
        notContains "nested columns"
        notContains "type=bigint"
        notContains "CAST(str_col"
    }

    // reversed operand: '' <> str_col → same rewrite
    explain {
        sql "select length(str_col) from slcp_str_tbl where '' <> str_col"
        contains "nested columns"
        contains "OFFSET"
        notContains "type=bigint"
    }

    // Struct field also projected directly → field access is full, not OFFSET-only
    // The struct's nested-columns entry still appears (partial struct pruning),
    // but the pruned field type must remain text (not bigint).
    explain {
        sql "select length(struct_element(struct_col, 'f3')), struct_element(struct_col, 'f3') from slcp_str_tbl"
        contains "nested columns"
        notContains "bigint"
    }

    // length(map_col['a']): keys read fully for element lookup, values accessed offset-only.
    // Expect access paths: map_col.KEYS (full) + map_col.VALUES.OFFSET
    explain {
        sql "select length(map_col['a']) from slcp_str_tbl"
        contains "nested columns"
        contains "KEYS"
        contains "VALUES"
        contains "OFFSET"
        notContains "bigint"
    }

    // sum(length(map_col['a'])): same optimization in aggregate context
    explain {
        sql "select sum(length(map_col['a'])) from slcp_str_tbl"
        contains "nested columns"
        contains "KEYS"
        contains "VALUES"
        contains "OFFSET"
        notContains "bigint"
    }

    // length(map_col['a']) + direct map access → OFFSET suppressed, full VALUES needed
    explain {
        sql "select length(map_col['a']), map_col['b'] from slcp_str_tbl"
        notContains "OFFSET"
        notContains "bigint"
    }
}
