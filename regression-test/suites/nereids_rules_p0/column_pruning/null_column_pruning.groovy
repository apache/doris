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

// Regression tests for the IS NULL / IS NOT NULL column pruning optimization.
//
// When IS NULL (or IS NOT NULL) is the *only* use of a nullable column, the FE
// should emit a DATA access path with a "NULL" component so that the BE can
// satisfy the query by reading only the null flag instead of the full column data.
// The EXPLAIN plan should show:
//   nested columns:  <col>: all access paths: [<col>.NULL]
//
// When the same column is also accessed for data (e.g., projected or used in
// struct_element), the NULL-only path must be stripped from allAccessPaths but
// preserved in predicateAccessPaths.

suite("null_column_pruning") {
    sql """ DROP TABLE IF EXISTS ncp_tbl """
    sql """
        CREATE TABLE ncp_tbl (
            id          INT,
            str_col     STRING NULL,
            struct_col  STRUCT<city: STRING, zip: INT> NULL,
            arr_col     ARRAY<INT> NULL,
            map_col     MAP<STRING, INT> NULL
        ) ENGINE = OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """

    // ─── Struct IS NULL only ────────────────────────────────────────────────────
    // Only null check on struct_col → emit [struct_col, NULL] access path,
    // type stays full struct (no pruning needed).
    explain {
        sql "select 1 from ncp_tbl where struct_col is null"
        contains "nested columns"
        contains "NULL"
    }

    // ─── Struct IS NOT NULL only ────────────────────────────────────────────────
    // IS NOT NULL is the same optimization (only null flag needed).
    explain {
        sql "select 1 from ncp_tbl where struct_col is not null"
        contains "nested columns"
        contains "NULL"
    }

    // ─── Struct IS NULL in aggregate ────────────────────────────────────────────
    explain {
        sql "select count(*) from ncp_tbl where struct_col is null"
        contains "nested columns"
        contains "NULL"
    }

    // ─── Array IS NULL only ─────────────────────────────────────────────────────
    explain {
        sql "select 1 from ncp_tbl where arr_col is null"
        contains "nested columns"
        contains "NULL"
    }

    // ─── Map IS NULL only ───────────────────────────────────────────────────────
    explain {
        sql "select 1 from ncp_tbl where map_col is null"
        contains "nested columns"
        contains "NULL"
    }

    // ─── Mixed: struct IS NULL + partial field access ───────────────────────────
    // struct_col IS NULL in WHERE + struct_element in SELECT → data is also needed.
    // allAccessPaths should have field path (NULL stripped), predicateAccessPaths keeps NULL.
    explain {
        sql "select struct_element(struct_col, 'city') from ncp_tbl where struct_col is null"
        contains "nested columns"
        contains "predicate access paths"
        contains "NULL"
    }

    // ─── Non-optimizable: struct IS NULL + full struct projected ────────────────
    // Full struct access means accessAll=true; null-only optimization is suppressed.
    explain {
        sql "select struct_col from ncp_tbl where struct_col is null"
        // The predicate path [struct_col.NULL] should still appear
        contains "predicate access paths"
        contains "NULL"
    }
}
