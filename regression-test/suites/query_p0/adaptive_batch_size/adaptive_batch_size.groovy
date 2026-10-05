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

// Regression tests for the Adaptive Batch Size feature.
//
// Design notes:
//   - Each case runs the same query with the feature enabled and disabled, and
//     asserts that results are identical (correctness check).
//   - We do NOT directly assert internal block byte sizes, because the storage
//     layer does not expose them via SQL result columns.  Correctness is the
//     primary requirement; performance / memory reduction is verified manually
//     or via profile counters in a separate benchmark.

suite("adaptive_batch_size") {

    // ── helpers ────────────────────────────────────────────────────────────────

    def set_adaptive = { enabled ->
        if (enabled) {
            set_be_param("enable_adaptive_batch_size", "true")
            sql "set preferred_block_size_bytes = 8388608"     // 8 MB (default)
            sql "set batch_size = 4096"
        } else {
            set_be_param("enable_adaptive_batch_size", "false")
            sql "set preferred_block_size_bytes = 8388608"
            sql "set batch_size = 4096"
        }
    }

    try {
        // ── Test 1: wide table (VARCHAR columns) ──────────────────────────────────
        // Each row is ~10 KB; with 4096 rows that is ~40 MB/batch which OOM-risks.
        // With adaptive=on the batch is trimmed to ~8 MB worth of rows.

        sql "drop table if exists abs_wide_table"
        sql """
            create table abs_wide_table (
                id      int         not null,
                c1      varchar(4096),
                c2      varchar(4096),
                c3      varchar(4096)
            )
            ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """

        // Insert 1000 rows with ~3 KB data each.
        def wide_rows = (1..1000).collect { i ->
            "(${i}, '${('a' * 1000)}', '${('b' * 1000)}', '${('c' * 1000)}')"
        }
        sql "insert into abs_wide_table values ${wide_rows.join(',')}"

        // Run query with adaptive enabled and collect result.
        set_adaptive(true)
        def res_enabled = sql "select id, length(c1) as l1, length(c2) as l2, length(c3) as l3 from abs_wide_table order by 1, 2, 3, 4"

        order_qt_wide "select id, length(c1) as l1, length(c2) as l2, length(c3) as l3 from abs_wide_table order by 1, 2, 3, 4 limit 50"

        // Run query with adaptive disabled and collect result.
        set_adaptive(false)
        def res_disabled = sql "select id, length(c1) as l1, length(c2) as l2, length(c3) as l3 from abs_wide_table order by 1, 2, 3, 4"

        // Results must be identical.
        assertEquals(res_enabled.size(), res_disabled.size())
        for (int i = 0; i < res_enabled.size(); i++) {
            assertEquals(res_enabled[i].toString(), res_disabled[i].toString())
        }


        // ── Test 2: narrow table (INT columns) ───────────────────────────────────
        // Rows are ~12 bytes each; with adaptive=on the predictor should converge
        // toward returning close to batch_size (batch is still row-limited).

        sql "drop table if exists abs_narrow_table"
        sql """
            create table abs_narrow_table (
                id   int not null,
                c1   int,
                c2   int,
                c3   int
            )
            ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """

        sql "insert into abs_narrow_table select number, number*2, number*3, number*4 from numbers('number'='5000')"

        set_adaptive(true)
        def narrow_on  = sql "select sum(c1), sum(c2), sum(c3) from abs_narrow_table"

        order_qt_narrow "select sum(c1), sum(c2), sum(c3) from abs_narrow_table"

        set_adaptive(false)
        def narrow_off = sql "select sum(c1), sum(c2), sum(c3) from abs_narrow_table"

        assertEquals(narrow_on.toString(), narrow_off.toString())


        // ── Test 3: AGG_KEYS table ────────────────────────────────────────────────
        // Verifies that adaptive batch size does not break aggregation correctness
        // (the byte check in _agg_key_next_block must only trigger at group boundaries).

        sql "drop table if exists abs_agg_table"
        sql """
            create table abs_agg_table (
                id    int         not null,
                val   bigint      replace
            )
            ENGINE=OLAP
            AGGREGATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """

        // 2000 distinct keys, 3 rows per key → 6000 rows total.
        def agg_rows = []
        for (int k = 1; k <= 2000; k++) {
            agg_rows << "(${k}, ${k})"
            agg_rows << "(${k}, ${k * 2})"
            agg_rows << "(${k}, ${k * 3})"
        }
        sql "insert into abs_agg_table values ${agg_rows.join(',')}"

        set_adaptive(true)
        def agg_on = sql "select id, val from abs_agg_table order by 1, 2 limit 10"

        order_qt_agg "select id, val from abs_agg_table order by 1, 2 limit 10"

        set_adaptive(false)
        def agg_off = sql "select id, val from abs_agg_table order by 1, 2 limit 10"

        assertEquals(agg_on.toString(), agg_off.toString())


        // ── Test 4: UNIQUE_KEYS table ─────────────────────────────────────────────
        // Verifies that adaptive byte-stop in _unique_key_next_block does not
        // cause duplicate or missing rows.

        sql "drop table if exists abs_unique_table"
        sql """
            create table abs_unique_table (
                id   int          not null,
                name varchar(200)
            )
            ENGINE=OLAP
            UNIQUE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """

        sql "insert into abs_unique_table select number, repeat('x', 100) from numbers('number'='3000')"

        set_adaptive(true)
        def uniq_on = sql "select count(*), sum(id) from abs_unique_table"

        order_qt_unique "select count(*), sum(id) from abs_unique_table"

        set_adaptive(false)
        def uniq_off = sql "select count(*), sum(id) from abs_unique_table"

        assertEquals(uniq_on.toString(), uniq_off.toString())


        // ── Test 5: verify setting enable_adaptive_batch_size = false disables adaptive sizing ──

        sql "drop table if exists abs_flag_table"
        sql """
            create table abs_flag_table (id int not null, v int)
            ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """
        sql "insert into abs_flag_table select number, number from numbers('number'='100')"

        set_adaptive(false)
        def flag_off = sql "select sum(v) from abs_flag_table"

        order_qt_flag "select sum(v) from abs_flag_table"

        set_adaptive(true)
        def flag_on  = sql "select sum(v) from abs_flag_table"

        assertEquals(flag_off.toString(), flag_on.toString())
    } finally {
        set_adaptive(true)
        sql "set preferred_block_size_bytes = 8388608"
        sql "set batch_size = 8160"
    }
}
