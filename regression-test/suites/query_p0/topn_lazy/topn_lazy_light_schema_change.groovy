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

// Regression test for topn lazy materialization on non-light-schema-change tables.
//
// TopN lazy materialization appends a synthetic global row-id column
// (__DORIS_GLOBAL_ROWID_COL__<table>) to the OLAP scan. The BE only rebuilds the
// tablet schema from FE's columns_desc when columns_desc[0].col_unique_id >= 0,
// which is only true for light_schema_change tables. On a non-light-schema-change
// table every column's uniqueId is -1, so the synthetic row-id column never enters
// the BE tablet schema and the scan fails with
//   "field name is invalid. field=__DORIS_GLOBAL_ROWID_COL__<table>"
// (or, if the scan schema is forced, the second-phase fetch silently returns NULL
// for the lazily-materialized columns).
//
// The fix disables topn lazy materialization for non-light-schema-change OLAP tables
// in MaterializeProbeVisitor, falling back to normal topn. This test verifies both
// the plan shape (no lazy materialization) and correct results on such a table, and
// verifies that a light_schema_change=true table still uses lazy materialization.
suite("topn_lazy_light_schema_change") {
    // ---- light_schema_change = false : lazy materialization must be disabled ----
    sql """ drop table if exists topn_lazy_lsc_false """
    sql """
        CREATE TABLE topn_lazy_lsc_false (
            `id` INT NOT NULL,
            `name` VARCHAR(64),
            `createdate` DATETIME
        )
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "light_schema_change" = "false",
            "enable_unique_key_merge_on_write" = "false"
        );
    """
    sql """
        insert into topn_lazy_lsc_false values
            (1, 'aaa', '2024-01-01 10:00:00'),
            (2, 'bbb', '2024-01-02 10:00:00'),
            (3, 'ccc', '2024-01-03 10:00:00'),
            (4, 'ddd', '2024-01-04 10:00:00'),
            (5, 'eee', '2024-01-05 10:00:00');
    """

    // Plan shape: a non-light-schema-change table must fall back to a plain
    // PhysicalOlapScan with no PhysicalLazyMaterialize / PhysicalLazyMaterializeOlapScan.
    //
    // Only meaningful in storage-compute integrated (non-cloud) mode. In cloud mode
    // CloudPropertyAnalyzer force-rewrites light_schema_change to "true"
    // (RewriteProperty.replace), so this table is effectively light_schema_change=true
    // and lazy materialization legitimately applies -- the assertion would not hold.
    if (!isCloudMode()) {
        explain {
            sql "shape plan select name from topn_lazy_lsc_false order by createdate desc limit 3"
            notContains("PhysicalLazyMaterialize")
        }
    }

    // Correctness: the query used to error with "field name is invalid" (or return NULL
    // for name). It must now run and return the real values ordered by createdate desc.
    qt_result_lsc_false """
        select id, name from topn_lazy_lsc_false order by createdate desc limit 3
    """

    // ---- light_schema_change = true : lazy materialization must still apply ----
    sql """ drop table if exists topn_lazy_lsc_true """
    sql """
        CREATE TABLE topn_lazy_lsc_true (
            `id` INT NOT NULL,
            `name` VARCHAR(64),
            `createdate` DATETIME
        )
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "light_schema_change" = "true",
            "enable_unique_key_merge_on_write" = "false"
        );
    """
    sql """
        insert into topn_lazy_lsc_true values
            (1, 'aaa', '2024-01-01 10:00:00'),
            (2, 'bbb', '2024-01-02 10:00:00'),
            (3, 'ccc', '2024-01-03 10:00:00'),
            (4, 'ddd', '2024-01-04 10:00:00'),
            (5, 'eee', '2024-01-05 10:00:00');
    """

    // Plan shape: a light_schema_change table keeps lazy materialization
    // (PhysicalLazyMaterialize present). Checked only in non-cloud mode, for symmetry
    // with the lsc_false case above.
    if (!isCloudMode()) {
        explain {
            sql "shape plan select name from topn_lazy_lsc_true order by createdate desc limit 3"
            contains("PhysicalLazyMaterialize")
        }
    }

    // Correctness: lazy materialization returns the same real values.
    qt_result_lsc_true """
        select id, name from topn_lazy_lsc_true order by createdate desc limit 3
    """
}
