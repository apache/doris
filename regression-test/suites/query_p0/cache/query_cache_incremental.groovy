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

// Correctness of the query cache incremental merge: when a partition keeps
// receiving hourly loads, a stale cache entry is reused by scanning only the
// delta rowsets since the cached version and merging them with the cached
// partial aggregation blocks. Every query below is checked against the same
// query with the cache disabled, so results stay verified even where
// incremental merge falls back to a full recompute (e.g. cloud mode). On
// local storage the suite additionally proves through the BE metrics that
// the incremental path really fires: the early stale queries must increase
// query_cache_stale_hit_total, and the two designed fallback phases (a
// delete predicate in the delta, a merge-on-write backfill that rewrites
// history) must increase query_cache_incremental_fallback_total. The final
// phase covers a long-dormant entry whose first reuse faces a large,
// many-rowset delta that goes through the parallel scanner builder.
suite("query_cache_incremental") {
    def querySql = """
        SELECT
            url,
            SUM(cost) AS total_cost,
            COUNT(*) AS cnt
        FROM test_query_cache_incremental
        WHERE dt >= '2026-01-01'
          AND dt < '2026-01-15'
        GROUP BY url
    """

    def normalize = { rows ->
        return rows.collect { row -> row.collect { col -> String.valueOf(col) }.join("|") }.sort()
    }

    // Compare the cached query result against the uncached one, twice: the
    // first cached run may fill or incrementally merge the entry, the second
    // one should serve it.
    def checkConsistency = { String sqlText ->
        sql "set enable_query_cache=false"
        def expected = normalize(sql(sqlText))
        sql "set enable_query_cache=true"
        assertEquals(expected, normalize(sql(sqlText)))
        assertEquals(expected, normalize(sql(sqlText)))
    }

    // Sum a BE counter across all backends via the webserver /metrics text.
    def sumBeMetric = { String metricName ->
        def backendIdToIp = [:]
        def backendIdToHttpPort = [:]
        getBackendIpHttpPort(backendIdToIp, backendIdToHttpPort)
        long total = 0
        backendIdToIp.each { backendId, ip ->
            def text = new URL("http://${ip}:${backendIdToHttpPort[backendId]}/metrics").getText()
            for (def line : text.split("\n")) {
                if (line.startsWith("doris_be_${metricName} ")) {
                    total += Long.parseLong(line.substring(line.lastIndexOf(' ') + 1).trim())
                }
            }
        }
        return total
    }

    // Besides result consistency, prove on local storage that the stale entry
    // was really merged incrementally (Mode::INCREMENTAL), not recomputed:
    // only the incremental path increases query_cache_stale_hit_total, and no
    // concurrent suite touches it because the session switch defaults to off.
    // Cloud mode always falls back by design, so it only checks consistency.
    // Residual caveat (here and in checkIncrementalFallback): a
    // memory-pressure prune evicting the entry inside the tiny fill-to-assert
    // window surfaces as a plain MISS and would fail the assertion; accepted
    // as rare, and a rerun re-establishes the counters from fresh deltas.
    def checkStaleIncremental = { String sqlText ->
        if (isCloudMode()) {
            checkConsistency(sqlText)
            return
        }
        def before = sumBeMetric("query_cache_stale_hit_total")
        checkConsistency(sqlText)
        def after = sumBeMetric("query_cache_stale_hit_total")
        assertTrue(after > before,
                "expected a stale incremental cache hit, but query_cache_stale_hit_total stayed at ${before}")
    }

    // Prove that a designed fallback phase really took the fallback path.
    def checkIncrementalFallback = { String sqlText ->
        if (isCloudMode()) {
            checkConsistency(sqlText)
            return
        }
        def before = sumBeMetric("query_cache_incremental_fallback_total")
        checkConsistency(sqlText)
        def after = sumBeMetric("query_cache_incremental_fallback_total")
        assertTrue(after > before,
                "expected an incremental fallback, but query_cache_incremental_fallback_total stayed at ${before}")
    }

    sql "set enable_nereids_distribute_planner=true"
    sql "set enable_query_cache=true"
    sql "set enable_query_cache_incremental=true"
    sql "set parallel_pipeline_task_num=3"
    sql "set enable_sql_cache=false"

    sql "DROP TABLE IF EXISTS test_query_cache_incremental"
    sql """
        CREATE TABLE test_query_cache_incremental (
            dt DATE,
            user_id INT,
            url STRING,
            cost BIGINT
        )
        ENGINE=OLAP
        DUPLICATE KEY(dt, user_id)
        PARTITION BY RANGE(dt)
        (
            PARTITION p20260101 VALUES LESS THAN ("2026-01-05"),
            PARTITION p20260105 VALUES LESS THAN ("2026-01-10"),
            PARTITION p20260110 VALUES LESS THAN ("2026-01-15")
        )
        DISTRIBUTED BY HASH(user_id) BUCKETS 3
        PROPERTIES
        (
            "replication_num" = "1"
        )
    """

    sql """
        INSERT INTO test_query_cache_incremental VALUES
        ('2026-01-01',1,'/a',10),
        ('2026-01-01',2,'/b',20),
        ('2026-01-02',3,'/c',30),
        ('2026-01-06',1,'/a',15),
        ('2026-01-07',3,'/c',35),
        ('2026-01-11',1,'/a',50),
        ('2026-01-12',3,'/c',70)
    """

    // The query cache participates in this plan.
    explain {
        sql(querySql)
        contains("DIGEST")
    }

    // Fill the cache, then serve from it.
    checkConsistency(querySql)
    order_qt_dup_initial "${querySql}"

    // Simulate the hourly load pattern: only the latest partition receives new
    // data, so the entries of the older partitions stay valid while the hot
    // partition entry is stale and gets incrementally merged. Crossing
    // query_cache_max_incremental_merge_count (BE config, default 8) also
    // exercises the forced full recompute that compacts the entry.
    for (int i = 1; i <= 10; i++) {
        sql """
            INSERT INTO test_query_cache_incremental VALUES
            ('2026-01-13',${i},'/a',${i}),
            ('2026-01-13',${100 + i},'/inc',${10 * i})
        """
        if (i == 1) {
            // The hot partition holds just two data rowsets here, far from
            // both the merge-count threshold and any compaction, so on local
            // storage this stale query must take the incremental path.
            checkStaleIncremental(querySql)
        } else {
            // Later rounds may legitimately recompute in full (merge-count
            // threshold, background compaction), so assert correctness only.
            checkConsistency(querySql)
        }
    }
    order_qt_dup_after_appends "${querySql}"

    // A delete in the delta cannot be merged into the cached partial result;
    // the query must fall back to a full recompute and stay correct.
    sql "DELETE FROM test_query_cache_incremental PARTITION p20260110 WHERE user_id = 1"
    checkIncrementalFallback(querySql)

    sql """
        INSERT INTO test_query_cache_incremental VALUES
        ('2026-01-14',999,'/after-delete',1)
    """
    checkConsistency(querySql)
    order_qt_dup_final "${querySql}"

    // A UNIQUE merge-on-write table takes the incremental path as long as the
    // loads only append new keys (the delete bitmap of the delta window stays
    // empty); a load that rewrites a pre-existing key falls back to one full
    // recompute and re-bases the entry, after which pure appends are
    // incremental again. Results must stay correct in every phase.
    sql "DROP TABLE IF EXISTS test_query_cache_incremental_mow"
    sql """
        CREATE TABLE test_query_cache_incremental_mow (
            dt DATE,
            user_id INT,
            cost BIGINT
        )
        ENGINE=OLAP
        UNIQUE KEY(dt, user_id)
        PARTITION BY RANGE(dt)
        (
            PARTITION p20260101 VALUES LESS THAN ("2026-01-05"),
            PARTITION p20260105 VALUES LESS THAN ("2026-01-10")
        )
        DISTRIBUTED BY HASH(user_id) BUCKETS 3
        PROPERTIES
        (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        )
    """
    def uniqueQuerySql = """
        SELECT dt, SUM(cost) AS total_cost
        FROM test_query_cache_incremental_mow
        GROUP BY dt
    """
    sql """
        INSERT INTO test_query_cache_incremental_mow VALUES
        ('2026-01-01',1,10),
        ('2026-01-02',2,20),
        ('2026-01-06',3,30)
    """
    checkConsistency(uniqueQuerySql)
    order_qt_mow_initial "${uniqueQuerySql}"
    // The hourly-append pattern on a primary-key table: every load only adds
    // brand-new keys, so the stale entry merges incrementally. The first
    // round is asserted through the metric (two data rowsets, nothing can
    // interfere); later rounds approach the compaction threshold, so they
    // assert correctness only.
    for (int i = 1; i <= 3; i++) {
        sql "INSERT INTO test_query_cache_incremental_mow VALUES ('2026-01-06',${100 + i},${i})"
        if (i == 1) {
            checkStaleIncremental(uniqueQuerySql)
        } else {
            checkConsistency(uniqueQuerySql)
        }
    }
    // A backfill rewrites history through the delete bitmap: user_id 1 gets a
    // new cost, which must fall back to one full recompute that re-bases the
    // entry.
    sql "INSERT INTO test_query_cache_incremental_mow VALUES ('2026-01-01',1,100)"
    checkIncrementalFallback(uniqueQuerySql)
    // After the re-base, pure appends take the incremental path again (not
    // asserted through the metric: by now enough rowsets piled up that a
    // background compaction could legitimately force a full recompute).
    sql "INSERT INTO test_query_cache_incremental_mow VALUES ('2026-01-06',200,7)"
    checkConsistency(uniqueQuerySql)
    order_qt_mow_final "${uniqueQuerySql}"

    // A cache entry can sit dormant while loads keep landing: the merge-count
    // threshold bounds prior write-backs, not the versions accumulated while
    // the entry is idle, so the delta of the first stale query after a long
    // idle stretch can be arbitrarily large. Such a delta must flow through
    // the parallel scanner builder like any full scan (split by segment rows)
    // and still merge correctly with the cached blocks. enable_parallel_scan
    // is on by default; the tiny per-scanner row bound forces the builder to
    // really split the delta into many scanners instead of collapsing to one.
    sql "set enable_parallel_scan=true"
    sql "set parallel_scan_min_rows_per_scanner=16"
    sql "DROP TABLE IF EXISTS test_query_cache_incremental_dormant"
    sql """
        CREATE TABLE test_query_cache_incremental_dormant (
            dt DATE,
            user_id INT,
            url STRING,
            cost BIGINT
        )
        ENGINE=OLAP
        DUPLICATE KEY(dt, user_id)
        PARTITION BY RANGE(dt)
        (
            PARTITION p20260101 VALUES LESS THAN ("2026-01-15")
        )
        DISTRIBUTED BY HASH(user_id) BUCKETS 1
        PROPERTIES
        (
            "replication_num" = "1"
        )
    """
    def dormantQuerySql = """
        SELECT
            url,
            SUM(cost) AS total_cost,
            COUNT(*) AS cnt
        FROM test_query_cache_incremental_dormant
        WHERE dt >= '2026-01-01'
          AND dt < '2026-01-15'
        GROUP BY url
    """
    sql "INSERT INTO test_query_cache_incremental_dormant VALUES ('2026-01-01',1,'/a',10)"
    // Fill the entry at the small initial version, then leave it dormant
    // while 30 loads land on the single-tablet partition: a 30-rowset,
    // couple-hundred-row suffix that splits into a dozen or more scanners
    // under the 16-row bound.
    checkConsistency(dormantQuerySql)
    for (int i = 1; i <= 30; i++) {
        sql """
            INSERT INTO test_query_cache_incremental_dormant VALUES
            ('2026-01-02',${i},'/a',${i}),
            ('2026-01-03',${i},'/b',${i}),
            ('2026-01-04',${i},'/c',${i}),
            ('2026-01-05',${i},'/d',${i}),
            ('2026-01-06',${i},'/e',${i}),
            ('2026-01-07',${i},'/f',${i}),
            ('2026-01-08',${i},'/g',${i})
        """
    }
    // The first query after the idle stretch must still take the incremental
    // path on local storage and agree with the uncached result. The loads
    // landed moments ago, so a boundary-crossing base compaction inside this
    // window is as unlikely as in the first-round assertions above (an
    // in-window cumulative compaction keeps the capture, and therefore the
    // assertion, intact).
    checkStaleIncremental(dormantQuerySql)
}
