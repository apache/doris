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

/**
 * Bucket -> local-hash parallelism upgrade.
 *
 * A pooled bucket-join fragment runs its bucket joins at bucket-count parallelism
 * (only instances owning buckets do join work). When nothing above the join needs
 * bucket alignment and per-BE instances > buckets-with-data x ratio
 * (session var local_shuffle_bucket_upgrade_ratio, > 1 enables, <= 1 disables),
 * the FE planner re-distributes both join sides with LOCAL_EXECUTION_HASH_SHUFFLE
 * so the join uses all instances.
 *
 * Shape notes (verified against a live cluster):
 *  - LocalExchangeNodes only appear in EXPLAIN DISTRIBUTED PLAN (plain EXPLAIN
 *    renders the tree before AddLocalExchange runs).
 *  - Whether a bucket-shuffle join forms is cluster-dependent (Nereids downgrades it
 *    when totalBucketNum < totalInstanceNum * bucket_shuffle_downgrade_ratio). The suite
 *    pins bucket_shuffle_downgrade_ratio=0 to keep it forming, but the plan-shape checks
 *    still gate on "did a BUCKET_HASH_SHUFFLE local exchange actually appear?" and skip
 *    if not, so the test never hard-fails on an environment where it didn't form.
 *  - The upgrade fires when min(task_num=16, cores) / min(buckets=4, cores) > 1.1, i.e.
 *    on any machine with >= 5 cores (5/4 = 1.25 > 1.1); CI and dev machines comfortably
 *    exceed this. Bucket counts are kept low (4/3/3) so a modest core count is enough.
 *  - The aggregation above must NOT group by the bucket key: a colocate agg
 *    requires bucket distribution of the join output and correctly blocks the
 *    upgrade via the parentRequire gate.
 */
suite("test_local_shuffle_bucket_upgrade") {

    def hints = { ls_on, ratio ->
        """/*+SET_VAR(
            enable_sql_cache=false, disable_join_reorder=true,
            disable_colocate_plan=true,
            auto_broadcast_join_threshold=-1, broadcast_row_count_limit=0,
            experimental_force_to_local_shuffle=true,
            experimental_enable_parallel_scan=false,
            enable_runtime_filter_prune=false,
            enable_runtime_filter_partition_prune=false,
            runtime_filter_type='IN,MIN_MAX',
            parallel_pipeline_task_num=16,
            parallel_exchange_instance_num=8,
            query_timeout=600,
            bucket_shuffle_downgrade_ratio=0,
            local_shuffle_bucket_upgrade_ratio=${ratio},
            enable_local_shuffle=${ls_on},
            enable_local_shuffle_planner=${ls_on}
        )*/"""
    }

    sql "DROP TABLE IF EXISTS lsbu_fact"
    sql "DROP TABLE IF EXISTS lsbu_probe"
    sql "DROP TABLE IF EXISTS lsbu_probe2"
    sql """CREATE TABLE lsbu_fact (k INT, v BIGINT)
           ENGINE=OLAP DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 4
           PROPERTIES ("replication_num"="1")"""
    sql """CREATE TABLE lsbu_probe (pk INT, k INT, w BIGINT)
           ENGINE=OLAP DUPLICATE KEY(pk) DISTRIBUTED BY HASH(pk) BUCKETS 3
           PROPERTIES ("replication_num"="1")"""
    sql """CREATE TABLE lsbu_probe2 (pk INT, k INT, w BIGINT)
           ENGINE=OLAP DUPLICATE KEY(pk) DISTRIBUTED BY HASH(pk) BUCKETS 3
           PROPERTIES ("replication_num"="1")"""
    sql """INSERT INTO lsbu_fact
           SELECT CAST(number%50 AS INT), number*10+1
           FROM numbers("number"="200")"""
    sql """INSERT INTO lsbu_probe
           SELECT CAST(number AS INT), CAST(number%50 AS INT), 1000+number
           FROM numbers("number"="300")"""
    sql """INSERT INTO lsbu_probe2
           SELECT CAST(number AS INT), CAST(number%50 AS INT), 2000+number
           FROM numbers("number"="170")"""

    // group key pk%10 is NOT the bucket key, so the agg above does not require
    // bucket distribution and the upgrade is allowed.
    def singleJoin = { h ->
        """SELECT ${h} p.pk % 10 AS g, COUNT(*) c, SUM(f.v) sv, SUM(p.w) sw
           FROM lsbu_fact f JOIN lsbu_probe p ON p.k = f.k
           GROUP BY g ORDER BY g"""
    }

    // ---------- 1. plan shape (EXPLAIN DISTRIBUTED PLAN: post-AddLocalExchange) ----------
    // The upgrade replaces the bucket join's BUCKET_HASH_SHUFFLE local exchange with
    // LOCAL_EXECUTION_HASH_SHUFFLE. "BUCKET_HASH_SHUFFLE" names ONLY a local-exchange type
    // (the join op prints "BUCKET_SHUFFLE", the network sink "BUCKET_SHFFULE_HASH_PARTITIONED"),
    // so it is an unambiguous, fragment-local signal of the thing being upgraded — unlike
    // LOCAL_EXECUTION_HASH, which an agg-finalize fragment may also carry on a multi-BE
    // cluster regardless of the gate.
    //
    // First confirm a bucket-shuffle local exchange actually formed; if it did not (cluster
    // shaped the join differently), there is nothing to upgrade, so skip rather than fail.
    def countBucketHashLe = { String planText -> planText.split("BUCKET_HASH_SHUFFLE").length - 1 }

    def bucketText = (sql "EXPLAIN DISTRIBUTED PLAN ${singleJoin(hints('true', '0'))}").toString()
    int bucketLeCount = countBucketHashLe(bucketText)
    if (bucketLeCount == 0) {
        logger.warn("bucket-shuffle join did not form in this environment; "
            + "skipping single-join upgrade plan-shape checks")
    } else {
        // ratio=1.1 upgrades the bucket join → all BUCKET_HASH local exchanges are gone
        def upgradedText = (sql "EXPLAIN DISTRIBUTED PLAN ${singleJoin(hints('true', '1.1'))}").toString()
        assertEquals(0, countBucketHashLe(upgradedText),
            "ratio=1.1 must upgrade away the bucket join's BUCKET_HASH_SHUFFLE local exchanges")

        // ratio <= 1 disables the upgrade → bucket-hash local exchanges unchanged
        def ratioOneText = (sql "EXPLAIN DISTRIBUTED PLAN ${singleJoin(hints('true', '1'))}").toString()
        assertEquals(bucketLeCount, countBucketHashLe(ratioOneText),
            "ratio=1 must keep the upgrade off (<=1 disables)")
    }

    // Note: whether a group-by-bucket-key agg blocks the upgrade depends on the agg
    // shape the optimizer picks (a colocate one-phase agg requires bucket distribution
    // and blocks it; a two-phase agg does not). That parentRequire gate is covered
    // deterministically by LocalShuffleNodeCoverageTest; here we only pin correctness.
    def bucketKeyAgg = { h ->
        """SELECT ${h} f.k AS g, COUNT(*) c, SUM(p.w) sw
           FROM lsbu_fact f JOIN lsbu_probe p ON p.k = f.k
           GROUP BY g ORDER BY g"""
    }
    def bka_baseline = sql bucketKeyAgg(hints('false', '0'))
    def bka_upgraded = sql bucketKeyAgg(hints('true', '1.1'))
    assertEquals(50, bka_baseline.size())
    assertEquals(bka_baseline, bka_upgraded,
        "group-by-bucket-key agg over (possibly upgraded) bucket join must stay correct")

    // ---------- 2. correctness: single bucket join ----------
    def single_baseline = sql singleJoin(hints('false', '0'))
    def single_bucket = sql singleJoin(hints('true', '0'))
    def single_upgraded = sql singleJoin(hints('true', '1.1'))

    assertEquals(10, single_baseline.size())
    assertEquals(single_baseline, single_bucket,
        "bucket join (upgrade off) must match local-shuffle-off baseline")
    assertEquals(single_baseline, single_upgraded,
        "upgraded bucket join must match local-shuffle-off baseline")

    // ---------- 3. correctness: stacked bucket joins ----------
    def stackedJoin = { h ->
        """SELECT ${h} p1.pk % 10 AS g, COUNT(*) c, SUM(f.v) sv, SUM(p1.w) s1, SUM(p2.w) s2
           FROM lsbu_fact f
           JOIN lsbu_probe p1 ON p1.k = f.k
           JOIN lsbu_probe2 p2 ON p2.k = f.k
           GROUP BY g ORDER BY g"""
    }

    // whole-chain shape: at an eligible ratio every level of the stacked bucket chain
    // upgrades (the lower join reports NOOP so the upper re-align LE is kept), so all
    // BUCKET_HASH local exchanges are upgraded away. Skip if the chain didn't form here.
    def stackedBucketText = (sql "EXPLAIN DISTRIBUTED PLAN ${stackedJoin(hints('true', '0'))}").toString()
    if (countBucketHashLe(stackedBucketText) == 0) {
        logger.warn("stacked bucket-shuffle chain did not form in this environment; "
            + "skipping stacked upgrade plan-shape check")
    } else {
        def stackedUpgradedText = (sql "EXPLAIN DISTRIBUTED PLAN ${stackedJoin(hints('true', '1.1'))}").toString()
        assertEquals(0, countBucketHashLe(stackedUpgradedText),
            "ratio=1.1 must upgrade away the stacked bucket chain's BUCKET_HASH local exchanges")
    }

    // Forced-RF killer case: with the upgrade, the join build is hash-sliced; the
    // per-instance IN/MIN_MAX partial filters MUST be merged before application
    // (TRuntimeFilterDesc.force_local_merge). Before that fix this query silently
    // lost up to 96% of its rows.
    def rfHints = { ratio ->
        hints('true', ratio).replace(")*/",
            ", enable_runtime_filter_prune=false, runtime_filter_type='IN,MIN_MAX')*/")
    }
    def single_up_rf = sql "SELECT ${rfHints('1.1')} p.pk % 10 AS g, COUNT(*) c, SUM(f.v) sv, SUM(p.w) sw FROM lsbu_fact f JOIN lsbu_probe p ON p.k = f.k GROUP BY g ORDER BY g"
    assertEquals(single_baseline, single_up_rf,
        "upgraded bucket join with forced IN/MIN_MAX runtime filters must stay correct")

    def stacked_baseline = sql stackedJoin(hints('false', '0'))
    def stacked_bucket = sql stackedJoin(hints('true', '0'))
    def stacked_upgraded = sql stackedJoin(hints('true', '1.1'))

    assertEquals(10, stacked_baseline.size())
    assertEquals(stacked_baseline, stacked_bucket,
        "stacked bucket joins (upgrade off) must match local-shuffle-off baseline")
    assertEquals(stacked_baseline, stacked_upgraded,
        "stacked bucket joins (upgrade on) must match local-shuffle-off baseline")
}
