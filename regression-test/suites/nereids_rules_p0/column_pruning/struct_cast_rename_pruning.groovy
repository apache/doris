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

// Regression tests for a rename-only STRUCT -> STRUCT cast.
//
// A STRUCT -> STRUCT cast converts positionally and carries the target field names, so
// element_at(cast(s as struct<b:...,a:...>), 'b') on a struct<a:...,b:...> returns the source
// field a. The cast must therefore reach BE even when the positional field types are identical:
// name based access (ELEMENT_AT, nested field references) resolves against the type attached to
// the cast result at execution time. Before the fix the FE folded such a cast into a no-op,
// leaving the value labeled with the source field names, which made
//   * nested column pruning on: the pruned slot keep the source field name and element_at fail
//     with "name b not found in Struct(a:...)";
//   * nested column pruning off: element_at silently return the source field b.
// Both settings must return the same positional result, and field level pruning must survive.

suite("struct_cast_rename_pruning") {
    sql "set enable_sql_cache = false"
    sql "DROP TABLE IF EXISTS struct_cast_rename_pruning_tbl"
    sql """
        CREATE TABLE struct_cast_rename_pruning_tbl (
            id     INT,
            s      STRUCT<a: STRING, b: STRING>,
            arr_s  ARRAY<STRUCT<a: STRING, b: STRING>>
        ) ENGINE = OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    sql """
        INSERT INTO struct_cast_rename_pruning_tbl VALUES
            (1, named_struct('a', 'left', 'b', 'right'),
                array(named_struct('a', 'left', 'b', 'right')))
    """

    // ── 1. Field level pruning still follows the positional mapping ─────────────
    // Target b is the source position 0 (a), target a is the source position 1 (b).
    explain {
        sql "select element_at(cast(s as struct<b:string,a:string>), 'b') from struct_cast_rename_pruning_tbl"
        contains "all access paths: [s.a]"
    }
    explain {
        sql "select element_at(cast(s as struct<b:string,a:string>), 'a') from struct_cast_rename_pruning_tbl"
        contains "all access paths: [s.b]"
    }

    // ── 2. Pruning on: name based access resolves against the target names ──────
    order_qt_rename_prune_on_b """
        SELECT id, element_at(CAST(s AS STRUCT<b:STRING,a:STRING>), 'b') AS b_v
        FROM struct_cast_rename_pruning_tbl ORDER BY id
    """
    order_qt_rename_prune_on_a """
        SELECT id, element_at(CAST(s AS STRUCT<b:STRING,a:STRING>), 'a') AS a_v
        FROM struct_cast_rename_pruning_tbl ORDER BY id
    """
    order_qt_rename_prune_on_both """
        SELECT id,
               element_at(CAST(s AS STRUCT<b:STRING,a:STRING>), 'b') AS b_v,
               element_at(CAST(s AS STRUCT<b:STRING,a:STRING>), 'a') AS a_v
        FROM struct_cast_rename_pruning_tbl ORDER BY id
    """
    order_qt_rename_prune_on_full_cast """
        SELECT id, CAST(s AS STRUCT<b:STRING,a:STRING>) AS renamed
        FROM struct_cast_rename_pruning_tbl ORDER BY id
    """
    // A rename nested in an array is positional as well.
    order_qt_rename_prune_on_array """
        SELECT id, element_at(element_at(CAST(arr_s AS ARRAY<STRUCT<b:STRING,a:STRING>>), 1), 'b') AS b_v
        FROM struct_cast_rename_pruning_tbl ORDER BY id
    """

    // ── 3. Pruning off: identical results ──────────────────────────────────────
    sql "set enable_prune_nested_column = false"
    order_qt_rename_prune_off_b """
        SELECT id, element_at(CAST(s AS STRUCT<b:STRING,a:STRING>), 'b') AS b_v
        FROM struct_cast_rename_pruning_tbl ORDER BY id
    """
    order_qt_rename_prune_off_a """
        SELECT id, element_at(CAST(s AS STRUCT<b:STRING,a:STRING>), 'a') AS a_v
        FROM struct_cast_rename_pruning_tbl ORDER BY id
    """
    order_qt_rename_prune_off_both """
        SELECT id,
               element_at(CAST(s AS STRUCT<b:STRING,a:STRING>), 'b') AS b_v,
               element_at(CAST(s AS STRUCT<b:STRING,a:STRING>), 'a') AS a_v
        FROM struct_cast_rename_pruning_tbl ORDER BY id
    """
    order_qt_rename_prune_off_full_cast """
        SELECT id, CAST(s AS STRUCT<b:STRING,a:STRING>) AS renamed
        FROM struct_cast_rename_pruning_tbl ORDER BY id
    """
    order_qt_rename_prune_off_array """
        SELECT id, element_at(element_at(CAST(arr_s AS ARRAY<STRUCT<b:STRING,a:STRING>>), 1), 'b') AS b_v
        FROM struct_cast_rename_pruning_tbl ORDER BY id
    """

    // ── 4. A cast whose field names already match is still a no-op ─────────────
    sql "set enable_prune_nested_column = true"
    order_qt_same_names_cast """
        SELECT id,
               element_at(CAST(s AS STRUCT<a:STRING,b:STRING>), 'a') AS a_v,
               element_at(CAST(s AS STRUCT<a:STRING,b:STRING>), 'b') AS b_v
        FROM struct_cast_rename_pruning_tbl ORDER BY id
    """
    sql "set enable_sql_cache = true"
}
