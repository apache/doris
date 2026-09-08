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
// incremental merge falls back to a full recompute (e.g. when the delta was
// merged away by a compaction). The suite additionally proves through the BE
// metrics that the incremental path really fires: the early stale queries
// must increase query_cache_stale_hit_total, and the two designed fallback
// phases (a delete predicate in the delta, a merge-on-write backfill that
// rewrites history) must increase query_cache_incremental_fallback_total.
// The final phase covers a long-dormant entry whose first reuse faces a
// large, many-rowset delta that goes through the parallel scanner builder.
// This holds in cloud mode too: the decision first brings the tablet view up
// to the queried version (usually a no-op, the scan would sync anyway) and
// then takes the same incremental path as local storage.
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

    // Besides result consistency, prove that the stale entry was really
    // merged incrementally (Mode::INCREMENTAL), not recomputed: only the
    // incremental path increases query_cache_stale_hit_total, and no
    // concurrent suite touches it because the session switch defaults to off.
    // Compaction is kept out of the way in every mode: every table here sets
    // disable_auto_compaction, so no background compaction can merge the delta
    // away mid-window and turn the expected incremental hit into a fallback
    // (this matters most on cloud, where compaction is driven externally).
    //
    // The stale_hit assertion runs on non-cloud only. Summed across BEs it is a
    // deterministic INCREMENTAL signal solely when the fill and the reuse land
    // on the same BE, which holds on the single-BE local cluster but not on
    // multi-BE cloud. Two cloud channels flip it with no product-correctness
    // meaning: a transient meta-service hiccup during the decision's view sync
    // turns the expected stale hit into a full-recompute fallback (stale_hit
    // stays flat, the fallback counter moves instead), and because the entry is
    // BE-local a query routed to a BE that does not hold it recomputes in full
    // there. A retry repairs neither: a fallback re-bases nothing, and a
    // no-entry BE fills at the current version so its later runs are exact hits,
    // never the stale hit. Cloud still exercises the whole incremental flow
    // through checkConsistency (end-to-end result correctness on a real cloud
    // cluster); that the cloud decision truly reaches Mode::INCREMENTAL is
    // proven deterministically by the BE unit tests (QueryCacheCloudIncremental
    // Test), which drive the cloud sync-then-capture path without cluster
    // routing in play. A local memory-pressure prune evicting the entry inside
    // the tiny fill-to-assert window is the one accepted residual (rare; a rerun
    // re-establishes the counters).
    def checkStaleIncremental = { String sqlText ->
        // On cloud the metric endpoints are not even sampled: a slow or briefly
        // unreachable BE metrics port would hang or throw in sumBeMetric before
        // a merely gated assertion, re-introducing the very topology-dependent
        // flakiness this gate exists to remove (and the samples would go unused).
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

    // Prove that a designed fallback phase really took the fallback path. Like the
    // stale_hit assertion, the metric check is restricted to non-cloud: the entry
    // is BE-local, so on a multi-BE cloud cluster the query can route to a BE that
    // does not hold the stale entry, take a plain MISS (which is not a fallback,
    // so the counter does not move), and spuriously fail. Cloud still verifies the
    // fallback phase's result correctness through checkConsistency; that the cloud
    // fallback classification itself fires is proven deterministically by the BE
    // unit tests (QueryCacheCloudIncrementalTest.mow_history_rewrite_falls_back,
    // fallback_on_sync_failure, and the delete-predicate/version-gap cases).
    def checkIncrementalFallback = { String sqlText ->
        // Cloud skips the sampling too, for the same reason as above.
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
            "replication_num" = "1",
            "disable_auto_compaction" = "true"
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
            // The hot partition holds just two data rowsets here, well below
            // the merge-count threshold, and auto-compaction is disabled on the
            // table, so this stale query must take the incremental path on both
            // deployment modes.
            checkStaleIncremental(querySql)
        } else {
            // Later rounds may legitimately recompute in full (the merge-count
            // threshold forces one every query_cache_max_incremental_merge_count
            // merges), so assert correctness only.
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
            "enable_unique_key_merge_on_write" = "true",
            "disable_auto_compaction" = "true"
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
    // After the re-base, pure appends take the incremental path again. Left as
    // a correctness-only check (not metric-asserted): the earlier rounds already
    // pin the incremental path through the metric on non-cloud, so repeating a
    // stale_hit assertion here would add nothing while carrying the same cloud
    // caveats that already make checkStaleIncremental result-only there.
    sql "INSERT INTO test_query_cache_incremental_mow VALUES ('2026-01-06',200,7)"
    checkConsistency(uniqueQuerySql)
    order_qt_mow_final "${uniqueQuerySql}"

    // A cache entry can sit dormant while loads keep landing: the merge-count
    // threshold bounds prior write-backs, not the versions accumulated while
    // the entry is idle, so the delta of the first stale query after a long
    // idle stretch can be arbitrarily large. Such a delta must flow through
    // the parallel scanner builder like any full scan and still merge
    // correctly with the cached blocks. Both settings below are needed for
    // the split to actually happen at regression scale: the per-scanner row
    // floor defaults to 2M rows, and BE clamps this variable up to 1024, so
    // 1024 is the lowest floor reachable and the delta further down is sized
    // several times past it.
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
            "replication_num" = "1",
            "disable_auto_compaction" = "true"
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
    // Fill the entry at the small initial version, then leave it dormant while
    // 30 loads land on the single-tablet partition: a 30-rowset, 6000-row
    // suffix. Against the 1024-row floor the builder cuts that into four or
    // five scanners, so the delta really goes through the rowset/segment split
    // rather than through one serial scanner.
    checkConsistency(dormantQuerySql)
    for (int i = 1; i <= 30; i++) {
        def values = (1..200).collect { n ->
            "('2026-01-0${(n % 7) + 2}',${i * 1000 + n},'/u${n % 5}',${n})"
        }.join(",")
        sql "INSERT INTO test_query_cache_incremental_dormant VALUES ${values}"
    }
    // The first query after the idle stretch must still take the incremental
    // path on both deployment modes and agree with the uncached result.
    // Auto-compaction is disabled on the table, so the 30-rowset delta stays
    // intact and no boundary-crossing compaction can turn this into a fallback.
    // checkStaleIncremental asserts the stale_hit metric on non-cloud (where it
    // is deterministic) and verifies result consistency on cloud, so no
    // cluster-routing or transient-sync flake reaches this assertion.
    checkStaleIncremental(dormantQuerySql)
}
