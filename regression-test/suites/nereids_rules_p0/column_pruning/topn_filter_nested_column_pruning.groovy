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

// The TopN filter is attached to the scan on BE at runtime, after the planner computed
// nested predicate access paths. When ORDER BY and WHERE touch different fields of the
// same STRUCT, the TopN filter must still see the ORDER BY field on every tablet.
suite("topn_filter_nested_column_pruning") {
    sql "set enable_prune_nested_column = true"
    sql "set topn_filter_ratio = 0.5"
    sql "set parallel_pipeline_task_num = 1"
    sql "set time_zone = '+08:00'"

    sql "DROP TABLE IF EXISTS topn_filter_nested_prune_tbl"
    sql """
        CREATE TABLE topn_filter_nested_prune_tbl (
            pk INT,
            ts STRUCT<a: TIMESTAMPTZ(0), b: TIMESTAMPTZ(0)>,
            si STRUCT<a: INT, b: INT>,
            sd STRUCT<a: DATE, b: DATE>
        )
        DUPLICATE KEY(pk)
        DISTRIBUTED BY HASH(pk) BUCKETS 2
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    sql """
        INSERT INTO topn_filter_nested_prune_tbl VALUES
        (0, struct('2000-01-01 00:00:00+08:00', '2024-01-01 00:00:00+00:00'),
            struct(1, 10), struct('2000-01-01', '2024-01-01')),
        (1, struct('2023-03-12 01:59:59-05:00', '2023-11-05 01:30:00-05:00'),
            struct(5, NULL), struct('2023-03-12', NULL)),
        (2, struct('2023-11-05 01:30:00-04:00', '2024-12-31 00:00:00+00:00'),
            struct(9, 30), struct('2023-11-05', '2024-12-31')),
        (3, struct('2030-01-01 00:00:00+08:00', '2030-01-01 00:00:00+00:00'),
            struct(20, 40), struct('2030-01-01', '2030-01-01'))
    """
    // The TopN filter is only generated when the planner has row count statistics.
    sql "ANALYZE TABLE topn_filter_nested_prune_tbl WITH SYNC"

    explain {
        sql """
            SELECT struct_element(ts, 'a')
            FROM topn_filter_nested_prune_tbl
            WHERE struct_element(ts, 'b') IS NOT NULL
            ORDER BY 1 NULLS LAST LIMIT 1
        """
        contains("TOPN OPT:1")
        contains("all access paths: [ts.a, ts.b.NULL]")
        contains("predicate access paths: [ts.b.NULL]")
    }

    qt_timestamptz_asc """
        SELECT struct_element(ts, 'a')
        FROM topn_filter_nested_prune_tbl
        WHERE struct_element(ts, 'b') IS NOT NULL
        ORDER BY 1 NULLS LAST LIMIT 1
    """
    qt_timestamptz_desc """
        SELECT struct_element(ts, 'a')
        FROM topn_filter_nested_prune_tbl
        WHERE struct_element(ts, 'b') IS NOT NULL
        ORDER BY 1 DESC NULLS LAST LIMIT 1
    """
    qt_timestamptz_asc_limit2 """
        SELECT struct_element(ts, 'a')
        FROM topn_filter_nested_prune_tbl
        WHERE struct_element(ts, 'b') IS NOT NULL
        ORDER BY 1 NULLS LAST LIMIT 2
    """

    qt_int_asc """
        SELECT struct_element(si, 'a')
        FROM topn_filter_nested_prune_tbl
        WHERE struct_element(si, 'b') IS NOT NULL
        ORDER BY 1 NULLS LAST LIMIT 1
    """
    qt_int_desc """
        SELECT struct_element(si, 'a')
        FROM topn_filter_nested_prune_tbl
        WHERE struct_element(si, 'b') IS NOT NULL
        ORDER BY 1 DESC NULLS LAST LIMIT 1
    """
    qt_int_nulls_first """
        SELECT struct_element(si, 'a')
        FROM topn_filter_nested_prune_tbl
        WHERE struct_element(si, 'b') IS NOT NULL
        ORDER BY 1 NULLS FIRST LIMIT 1
    """
    // Selective predicate on the other field.
    qt_int_selective """
        SELECT pk, struct_element(si, 'a')
        FROM topn_filter_nested_prune_tbl
        WHERE struct_element(si, 'b') > 15
        ORDER BY 2 NULLS LAST LIMIT 1
    """
    // ORDER BY and WHERE on the same field.
    qt_int_same_field """
        SELECT struct_element(si, 'b')
        FROM topn_filter_nested_prune_tbl
        WHERE struct_element(si, 'b') IS NOT NULL
        ORDER BY 1 NULLS LAST LIMIT 1
    """

    qt_date_asc """
        SELECT struct_element(sd, 'a')
        FROM topn_filter_nested_prune_tbl
        WHERE struct_element(sd, 'b') IS NOT NULL
        ORDER BY 1 NULLS LAST LIMIT 1
    """
    qt_date_desc """
        SELECT struct_element(sd, 'a')
        FROM topn_filter_nested_prune_tbl
        WHERE struct_element(sd, 'b') IS NOT NULL
        ORDER BY 1 DESC NULLS LAST LIMIT 1
    """

    qt_multi_struct """
        SELECT pk, struct_element(si, 'a'), struct_element(sd, 'a')
        FROM topn_filter_nested_prune_tbl
        WHERE struct_element(si, 'b') IS NOT NULL AND struct_element(sd, 'b') IS NOT NULL
        ORDER BY 2, 3 NULLS LAST LIMIT 1
    """

    sql "set parallel_pipeline_task_num = 0"
    qt_timestamptz_asc_default_parallel """
        SELECT struct_element(ts, 'a')
        FROM topn_filter_nested_prune_tbl
        WHERE struct_element(ts, 'b') IS NOT NULL
        ORDER BY 1 NULLS LAST LIMIT 1
    """
}
