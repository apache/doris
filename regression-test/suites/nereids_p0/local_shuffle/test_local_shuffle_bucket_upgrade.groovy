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
 * DORIS-24902 part 2: bucket -> local-hash parallelism upgrade.
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
 *  - Nereids bucket-shuffle downgrade (bucket shuffle only forms when
 *    totalBucketNum >= totalInstanceNum * bucket_shuffle_downgrade_ratio) depends on
 *    the alive BE count, so the suite pins bucket_shuffle_downgrade_ratio=0 to keep
 *    the bucket join forming in any environment. ratio=1.1 fires on any BE count
 *    (per-BE instances 16 > bucketsPerWorker * 1.1 for 13 buckets on >= 1 BE);
 *    assertions about ratios that should NOT fire are limited to <= 1 because
 *    thresholds above 1 depend on how many buckets land on each BE.
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
           ENGINE=OLAP DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 13
           PROPERTIES ("replication_num"="1")"""
    sql """CREATE TABLE lsbu_probe (pk INT, k INT, w BIGINT)
           ENGINE=OLAP DUPLICATE KEY(pk) DISTRIBUTED BY HASH(pk) BUCKETS 7
           PROPERTIES ("replication_num"="1")"""
    sql """CREATE TABLE lsbu_probe2 (pk INT, k INT, w BIGINT)
           ENGINE=OLAP DUPLICATE KEY(pk) DISTRIBUTED BY HASH(pk) BUCKETS 5
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
    def upgradedPlan = sql "EXPLAIN DISTRIBUTED PLAN ${singleJoin(hints('true', '1.1'))}"
    def upgradedText = upgradedPlan.toString()
    assertTrue(upgradedText.contains("BUCKET_SHUFFLE"),
        "precondition: the join must be a bucket-shuffle join")
    assertTrue(upgradedText.contains("LOCAL_EXECUTION_HASH_SHUFFLE"),
        "ratio=1.1 must upgrade the bucket join's local exchanges to LOCAL hash")

    def bucketPlan = sql "EXPLAIN DISTRIBUTED PLAN ${singleJoin(hints('true', '0'))}"
    def bucketText = bucketPlan.toString()
    assertTrue(bucketText.contains("BUCKET_SHUFFLE"),
        "precondition: the join must be a bucket-shuffle join")
    assertFalse(bucketText.contains("LOCAL_EXECUTION_HASH_SHUFFLE"),
        "ratio=0 disables the upgrade: no LOCAL hash exchanges for the bucket join")

    // ratio exactly 1 also keeps the upgrade off (<=1 disables)
    def ratioOnePlan = sql "EXPLAIN DISTRIBUTED PLAN ${singleJoin(hints('true', '1'))}"
    assertFalse(ratioOnePlan.toString().contains("LOCAL_EXECUTION_HASH_SHUFFLE"),
        "ratio=1 must keep the upgrade off (<=1 disables)")

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

    def stacked_baseline = sql stackedJoin(hints('false', '0'))
    def stacked_bucket = sql stackedJoin(hints('true', '0'))
    def stacked_upgraded = sql stackedJoin(hints('true', '1.1'))

    assertEquals(10, stacked_baseline.size())
    assertEquals(stacked_baseline, stacked_bucket,
        "stacked bucket joins (upgrade off) must match local-shuffle-off baseline")
    assertEquals(stacked_baseline, stacked_upgraded,
        "stacked bucket joins (upgrade on) must match local-shuffle-off baseline")
}
