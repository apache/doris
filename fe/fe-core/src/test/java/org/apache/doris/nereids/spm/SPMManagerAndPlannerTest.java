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

package org.apache.doris.nereids.spm;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.spm.manager.BaselineManager;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTE;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * M3 milestone test: BaselineManager (storage + index + ordering) + SPMPlanner (whole-query
 * bind + rewrite controller).
 *
 * Verifies:
 *
 * 1. BaselineManager CRUD, duplicate detection, the three-level filter (hash + digest),
 *    and priority ordering
 * 2. SPMPlanner whole-query rewrite: create a baseline from SQL texts -> structurally
 *    identical user query -> hit -> the user's literal values are substituted into the
 *    whole plan (filters of every query block, CTE bodies, nested subqueries - including
 *    a query whose ONLY predicates live inside a subquery)
 */
public class SPMManagerAndPlannerTest {

    private BaselineManager manager;

    @BeforeEach
    public void setUp() {
        manager = BaselineManager.getInstance();
        manager.clearForTest();
    }

    // ==================== BaselineManager ====================

    @Test
    public void testConcurrentCreateAndLookup() throws Exception {
        // The rewrite hot path (hasBaselines + findCandidateBaselines) runs concurrently on
        // many query threads while DDL / capture threads mutate the store. The fine-grained
        // store lock must keep lookups correct (no lost or duplicated baselines) and keep
        // concurrent lookups from serializing on each other.
        int threads = 4;
        int perThread = 25;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            futures.add(pool.submit(() -> {
                start.await();
                for (int i = 0; i < perThread; i++) {
                    String tag = threadId + "_" + i;
                    // interleave lookups with creates so reads race the writes
                    manager.hasBaselines();
                    Assertions.assertNull(manager.getBaseline(-1L));
                    BaselinePlan plan = new BaselinePlan();
                    plan.setBindSql("select " + tag);
                    plan.setBindSqlDigest("digest_" + tag);
                    plan.setBindSqlHash(tag.hashCode());
                    plan.setPlanSql("plan_" + tag);
                    manager.createBaseline(plan);
                    manager.getAllBaselines();
                }
                return null;
            }));
        }
        start.countDown();
        for (Future<?> future : futures) {
            future.get(); // rethrows any failure from inside the worker threads
        }
        pool.shutdown();

        Assertions.assertEquals(threads * perThread, manager.getAllBaselines().size());
        // every created baseline is indexed: its digest resolves to exactly one candidate
        for (int t = 0; t < threads; t++) {
            for (int i = 0; i < perThread; i++) {
                String tag = t + "_" + i;
                List<BaselinePlan> candidates =
                        manager.findCandidateBaselines("digest_" + tag, tag.hashCode());
                Assertions.assertEquals(1, candidates.size(),
                        "digest " + tag + " must resolve to exactly one candidate");
            }
        }
    }

    @Test
    public void testCreateAndFindCandidate() throws Exception {
        SPMPlanner planner = new SPMPlanner();
        // baseline: WHERE a = 100
        long id = manager.createBaseline(planner.buildBaseline(
                "SELECT * FROM t1 WHERE a = 100", "SELECT * FROM t1 WHERE a = 100"));
        Assertions.assertTrue(id > 0);
        // baselines owned by the global manager are GLOBAL-scope
        Assertions.assertEquals(BaselineScope.GLOBAL, manager.getBaseline(id).getScope());

        // A structurally identical user query with a different value (WHERE a = 42) -> the
        // same value-free digest/hash -> the candidate is found
        LogicalPlan userPlan = parse("SELECT * FROM t1 WHERE a = 42");
        List<BaselinePlan> candidates = manager.findCandidateBaselines(
                userPlan.toSpmDigest(), SPMUtils.hashOf(userPlan.toSpmDigest()));
        Assertions.assertEquals(1, candidates.size());
        Assertions.assertEquals(id, candidates.get(0).getId());
    }

    @Test
    public void testDuplicateBaselineSkipped() throws Exception {
        SPMPlanner planner = new SPMPlanner();
        BaselinePlan b1 = planner.buildBaseline(
                "SELECT * FROM t1 WHERE a = 100", "SELECT * FROM t1 WHERE a = 100");
        long id1 = manager.createBaseline(b1);
        // exact duplicate (same digest + planSql) -> returns the existing id, no new row
        BaselinePlan b2 = planner.buildBaseline(
                "SELECT * FROM t1 WHERE a = 100", "SELECT * FROM t1 WHERE a = 100");
        long id2 = manager.createBaseline(b2);
        Assertions.assertEquals(id1, id2);
        Assertions.assertEquals(1, manager.getAllBaselines().size());
    }

    @Test
    public void testComparatorUserBaselineFirst() throws Exception {
        SPMPlanner planner = new SPMPlanner();
        // Auto-captured baseline (has queryMs)
        BaselinePlan captured = planner.buildBaseline(
                "SELECT * FROM t1 WHERE a = 100", "SELECT * FROM t1 WHERE a = 100");
        captured.setSource(BaselineSource.CAPTURE);
        captured.setQueryTimeMs(2000);

        // Manually created baseline (no queryMs, default -1)
        BaselinePlan manual = planner.buildBaseline(
                "SELECT * FROM t1 WHERE a = 100", "SELECT * FROM t1 WHERE a = 200");
        manual.setSource(BaselineSource.USER);

        manager.createBaseline(captured);
        manager.createBaseline(manual);

        LogicalPlan userPlan = parse("SELECT * FROM t1 WHERE a = 42");
        List<BaselinePlan> candidates = manager.findCandidateBaselines(
                userPlan.toSpmDigest(), SPMUtils.hashOf(userPlan.toSpmDigest()));

        // The manual baseline without time wins
        Assertions.assertEquals(2, candidates.size());
        Assertions.assertEquals(manual.getPlanSql(), candidates.get(0).getPlanSql());
    }

    @Test
    public void testDropAndDisable() throws Exception {
        SPMPlanner planner = new SPMPlanner();
        long id = manager.createBaseline(planner.buildBaseline(
                "SELECT * FROM t1 WHERE a = 100", "SELECT * FROM t1 WHERE a = 100"));

        // DISABLED no longer participates in matching
        manager.updateStatus(id, BaselineStatus.DISABLED);
        LogicalPlan userPlan = parse("SELECT * FROM t1 WHERE a = 42");
        Assertions.assertTrue(manager.findCandidateBaselines(
                userPlan.toSpmDigest(), SPMUtils.hashOf(userPlan.toSpmDigest())).isEmpty());

        // DROP removes it completely
        manager.updateStatus(id, BaselineStatus.ENABLED);
        Assertions.assertTrue(manager.dropBaseline(id));
        Assertions.assertTrue(manager.getAllBaselines().isEmpty());
    }

    @Test
    public void testRefreshMergeAddUpdateRemove() {
        BaselinePlan remote1 = remoteBaseline(101L, "digest_a", 0xA1L, BaselineStatus.ENABLED);
        BaselinePlan remote2 = remoteBaseline(102L, "digest_b", 0xB2L, BaselineStatus.ENABLED);

        // add: ids created on another FE become visible and searchable through the index
        manager.applyRefreshedBaselines(Map.of(101L, remote1, 102L, remote2));
        Assertions.assertSame(remote1, manager.getBaseline(101L));
        Assertions.assertEquals(2, manager.getAllBaselines().size());
        Assertions.assertEquals(1, manager.findCandidateBaselines("digest_a", 0xA1L).size());

        // update: same id with changed persisted content (status) replaces the object
        BaselinePlan disabled = remoteBaseline(101L, "digest_a", 0xA1L, BaselineStatus.DISABLED);
        manager.applyRefreshedBaselines(Map.of(101L, disabled, 102L, remote2));
        Assertions.assertSame(disabled, manager.getBaseline(101L));
        Assertions.assertTrue(manager.findCandidateBaselines("digest_a", 0xA1L).isEmpty());

        // unchanged rows keep their object identity (timestamps are intentionally ignored)
        BaselinePlan current = manager.getBaseline(102L);
        manager.applyRefreshedBaselines(Map.of(101L, disabled, 102L, remote2));
        Assertions.assertSame(current, manager.getBaseline(102L));

        // remove: ids dropped on another FE vanish from memory and from the index
        manager.applyRefreshedBaselines(Map.of(102L, remote2));
        Assertions.assertNull(manager.getBaseline(101L));
        Assertions.assertTrue(manager.findCandidateBaselines("digest_a", 0xA1L).isEmpty());
        Assertions.assertEquals(1, manager.getAllBaselines().size());
    }

    @Test
    public void testRefreshAdvancesIdGeneratorPastRemoteIds() {
        manager.applyRefreshedBaselines(Map.of(500L,
                remoteBaseline(500L, "digest_c", 0xC3L, BaselineStatus.ENABLED)));
        long id = manager.createBaseline(
                remoteBaseline(0L, "digest_d", 0xD4L, BaselineStatus.ENABLED));
        // never collides with ids created on another FE
        Assertions.assertEquals(501L, id);
    }

    /**
     * A refresh reads its snapshot OUTSIDE the state lock, so a status update can land
     * while that read is in flight. updateStatus writes the durable row and bumps the
     * version, but it does not republish the in-memory row - applying the STALE snapshot
     * (the OLD status) would therefore leave this FE matching a disabled baseline until
     * the next refresh. The version guard must reject the stale snapshot.
     */
    @Test
    public void testStaleRefreshSnapshotCannotOverwriteStatusUpdate() {
        long id = manager.createBaseline(
                remoteBaseline(0L, "digest_race", 0xE5L, BaselineStatus.ENABLED));
        // the snapshot was read while the baseline was still ENABLED
        long versionAtRead = manager.getStateVersion();
        BaselinePlan staleRow = remoteBaseline(id, "digest_race", 0xE5L, BaselineStatus.ENABLED);

        Assertions.assertTrue(manager.updateStatus(id, BaselineStatus.DISABLED));

        Assertions.assertFalse(manager.applyRefreshedSnapshotIfUnchanged(versionAtRead,
                Map.of(id, staleRow)),
                "a snapshot read before the status update must be rejected");
        Assertions.assertEquals(BaselineStatus.DISABLED, manager.getBaseline(id).getStatus(),
                "the local status update must survive the stale refresh snapshot");

        // the guard is not a blanket rejection: with the current version a content-equal
        // snapshot is applied (nothing changes, the status stays DISABLED)
        Assertions.assertTrue(manager.applyRefreshedSnapshotIfUnchanged(
                manager.getStateVersion(),
                Map.of(id, remoteBaseline(id, "digest_race", 0xE5L, BaselineStatus.DISABLED))));
        Assertions.assertEquals(BaselineStatus.DISABLED, manager.getBaseline(id).getStatus());
    }

    /** Builds a baseline as it would come back from a refresh (no transient trees needed). */
    private static BaselinePlan remoteBaseline(long id, String digest, long hash,
            BaselineStatus status) {
        BaselinePlan p = new BaselinePlan();
        p.setId(id);
        p.setBindSql("SELECT 1 FROM t WHERE k = ?");
        p.setBindSqlDigest(digest);
        p.setBindSqlHash(hash);
        p.setPlanSql("SELECT 1 FROM t WHERE k = _spm_const_var(1)");
        p.setCost(1.0D);
        p.setQueryTimeMs(-1L);
        p.setStatus(status);
        return p;
    }

    // ==================== whole-query rewrite ====================

    @Test
    public void testTryRewritePlanEndToEnd() throws Exception {
        SPMPlanner planner = new SPMPlanner();
        // 1. create a baseline from SQL text; the baseline carries the value-free
        //    full-query digest of the bind tree
        manager.createBaseline(planner.buildBaseline(
                "SELECT * FROM t1 WHERE a = 100",
                "SELECT * FROM t1 WHERE a = 100"));

        // 2. a structurally identical user query with a different value hits the baseline
        LogicalPlan userPlan = parse("SELECT * FROM t1 WHERE a = 42");
        long deadline = System.currentTimeMillis() + 5000;
        LogicalPlan rewritten = planner.tryRewritePlan(userPlan, deadline);

        Assertions.assertNotNull(rewritten, "structurally identical query must be rewritten");
        Assertions.assertTrue(planner.getUsedBaselineId() > 0, "used baseline id must be set");

        // the rewritten plan carries the user's value 42 (not the capture-time 100)
        String rewrittenSql = allExprSqls(rewritten);
        Assertions.assertTrue(rewrittenSql.contains("42"), rewrittenSql);
        Assertions.assertFalse(rewrittenSql.contains("_spm_const_var"),
                "no placeholder may remain in the rewritten plan: " + rewrittenSql);
    }

    @Test
    public void testTryRewritePlanNoMatch() throws Exception {
        SPMPlanner planner = new SPMPlanner();
        manager.createBaseline(planner.buildBaseline(
                "SELECT * FROM t1 WHERE a = 100", "SELECT * FROM t1 WHERE a = 100"));

        // structurally different user query (different column) -> no rewrite
        LogicalPlan userPlan = parse("SELECT * FROM t1 WHERE b = 42");
        long deadline = System.currentTimeMillis() + 5000;
        Assertions.assertNull(planner.tryRewritePlan(userPlan, deadline));
    }

    @Test
    public void testTryRewritePlanRewritesSubqueryFilter() throws Exception {
        SPMPlanner planner = new SPMPlanner();
        // baseline whose topmost filter contains an IN subquery with a constant inside
        String subqueryBindSql = "SELECT * FROM t1 WHERE a = 100 "
                + "AND t1.c IN (SELECT t2.c FROM t2 WHERE t2.b = 1)";
        manager.createBaseline(planner.buildBaseline(subqueryBindSql, subqueryBindSql));

        // a structurally identical user query whose literals differ BOTH at the top
        // level (a: 100 -> 42) and inside the subquery (b: 1 -> 2) must be rewritten
        // with both values substituted (subquery constants are parameterized too)
        LogicalPlan userPlan = parse("SELECT * FROM t1 WHERE a = 42 "
                + "AND t1.c IN (SELECT t2.c FROM t2 WHERE t2.b = 2)");
        long deadline = System.currentTimeMillis() + 5000;
        LogicalPlan rewritten = planner.tryRewritePlan(userPlan, deadline);

        Assertions.assertNotNull(rewritten, "subquery-containing query must be rewritten");
        Assertions.assertTrue(planner.getUsedBaselineId() > 0, "used baseline id must be set");
        String rewrittenSql = allExprSqls(rewritten);
        Assertions.assertTrue(rewrittenSql.contains("42"),
                "top-level literal must be substituted: " + rewrittenSql);
        Assertions.assertFalse(rewrittenSql.contains("_spm_const_var(2, 1)"),
                "subquery placeholder must be substituted: " + rewrittenSql);
    }

    // ==================== multi-level binding (every query block, incl. CTE bodies) ====================

    @Test
    public void testTryRewritePlanMultiLevelFilter() throws Exception {
        SPMPlanner planner = new SPMPlanner();
        // bindSql with three nested query blocks, each with its own filter
        String bindSql = "SELECT c1 FROM (SELECT c1, c2 FROM (SELECT c1, c2, c3 FROM t1 "
                + "WHERE c3 > 1) tx WHERE tx.c2 > 3) ty WHERE ty.c1 > 4";
        manager.createBaseline(planner.buildBaseline(bindSql, bindSql));

        // user query: same structure, different values in ALL three filters
        LogicalPlan userPlan = parse("SELECT c1 FROM (SELECT c1, c2 FROM (SELECT c1, c2, c3 FROM t1 "
                + "WHERE c3 > 7) tx WHERE tx.c2 > 8) ty WHERE ty.c1 > 9");
        long deadline = System.currentTimeMillis() + 5000;
        LogicalPlan rewritten = planner.tryRewritePlan(userPlan, deadline);

        Assertions.assertNotNull(rewritten, "multi-level filter query must be rewritten");
        Assertions.assertTrue(planner.getUsedBaselineId() > 0, "used baseline id must be set");
        String rewrittenSql = allExprSqls(rewritten);
        // ALL THREE user values must be substituted into their respective filters
        Assertions.assertTrue(rewrittenSql.contains("9"),
                "topmost filter value must be substituted: " + rewrittenSql);
        Assertions.assertTrue(rewrittenSql.contains("8"),
                "middle (derived table) filter value must be substituted: " + rewrittenSql);
        Assertions.assertTrue(rewrittenSql.contains("7"),
                "inner (derived table) filter value must be substituted: " + rewrittenSql);
        Assertions.assertFalse(rewrittenSql.contains("_spm_const_var"),
                "no placeholder may remain in the rewritten plan: " + rewrittenSql);
    }

    @Test
    public void testTryRewritePlanCteFilter() throws Exception {
        SPMPlanner planner = new SPMPlanner();
        // bindSql: a filter in the main query AND a filter inside the CTE definition
        String bindSql = "WITH cte AS (SELECT c1, c2 FROM t1 WHERE c2 > 1) "
                + "SELECT c1 FROM cte WHERE c1 > 2";
        manager.createBaseline(planner.buildBaseline(bindSql, bindSql));

        LogicalPlan userPlan = parse("WITH cte AS (SELECT c1, c2 FROM t1 WHERE c2 > 5) "
                + "SELECT c1 FROM cte WHERE c1 > 6");
        long deadline = System.currentTimeMillis() + 5000;
        LogicalPlan rewritten = planner.tryRewritePlan(userPlan, deadline);

        Assertions.assertNotNull(rewritten, "CTE filter query must be rewritten");
        String rewrittenSql = allExprSqls(rewritten);
        Assertions.assertTrue(rewrittenSql.contains("6"),
                "main query filter value must be substituted: " + rewrittenSql);
        Assertions.assertTrue(rewrittenSql.contains("5"),
                "CTE definition filter value must be substituted: " + rewrittenSql);
        Assertions.assertFalse(rewrittenSql.contains("_spm_const_var"),
                "no placeholder may remain in the rewritten plan: " + rewrittenSql);
    }

    @Test
    public void testTryRewritePlanSameBlockMultiConjuncts() throws Exception {
        SPMPlanner planner = new SPMPlanner();
        // one query block with several conjuncts: WHERE a > 1 AND b < 2
        String bindSql = "SELECT * FROM t1 WHERE a > 1 AND b < 2";
        manager.createBaseline(planner.buildBaseline(bindSql, bindSql));

        // user query with the same conjuncts but different values
        LogicalPlan userPlan = parse("SELECT * FROM t1 WHERE a > 10 AND b < 20");
        long deadline = System.currentTimeMillis() + 5000;
        LogicalPlan rewritten = planner.tryRewritePlan(userPlan, deadline);
        Assertions.assertNotNull(rewritten, "multi-conjunct filter query must be rewritten");
        String rewrittenSql = allExprSqls(rewritten);
        Assertions.assertTrue(rewrittenSql.contains("10"),
                "first conjunct value must be substituted: " + rewrittenSql);
        Assertions.assertTrue(rewrittenSql.contains("20"),
                "second conjunct value must be substituted: " + rewrittenSql);
        Assertions.assertFalse(rewrittenSql.contains("_spm_const_var"),
                "no placeholder may remain in the rewritten plan: " + rewrittenSql);
    }

    // ==================== recursive CTE (WITH RECURSIVE) ====================

    /**
     * A recursive CTE baseline must be built and matched like any other query: the
     * anchor branch and the recursive branch carry literals (initial value 1, recursion
     * guard WHERE k1 < 5), and a structurally identical user query with different
     * values (WHERE k1 < 10) must hit the baseline and get the USER values substituted
     * into both the anchor and the recursive part.
     */
    @Test
    public void testRecursiveCteBindAndRewrite() throws Exception {
        SPMPlanner planner = new SPMPlanner();
        String bindSql = "WITH RECURSIVE t1(k1) AS ("
                + "SELECT cast(1 as bigint) k1 "
                + "UNION ALL SELECT k1 + 1 FROM t1 WHERE k1 < 5"
                + ") SELECT * FROM t1";
        manager.createBaseline(planner.buildBaseline(bindSql, bindSql));

        LogicalPlan userPlan = parse("WITH RECURSIVE t1(k1) AS ("
                + "SELECT cast(1 as bigint) k1 "
                + "UNION ALL SELECT k1 + 1 FROM t1 WHERE k1 < 10"
                + ") SELECT * FROM t1");
        long deadline = System.currentTimeMillis() + 5000;
        LogicalPlan rewritten = planner.tryRewritePlan(userPlan, deadline);

        Assertions.assertNotNull(rewritten,
                "recursive-CTE value variant must be rewritten: " + allExprSqls(userPlan));
        String rewrittenSql = allExprSqls(rewritten);
        Assertions.assertTrue(rewrittenSql.contains("10"),
                "recursive-guard literal must be substituted: " + rewrittenSql);
        Assertions.assertFalse(rewrittenSql.contains("_spm_const_var"),
                "no placeholder may remain in the rewritten recursive CTE: " + rewrittenSql);
    }

    // ==================== subquery-only binding (no query-block WHERE) ====================

    @Test
    public void testSubqueryOnlyWhereBindAndRewrite() throws Exception {
        SPMPlanner planner = new SPMPlanner();
        // A query whose ONLY predicates live inside a SELECT-list scalar subquery has no
        // query-block WHERE at all. The whole-query binding handles it naturally: the
        // subquery's filter is part of the tree and is parameterized / matched / rewritten
        // like any other literal position.
        String bindSql = "SELECT (SELECT count(*) FROM t2 WHERE t2.b = 1) AS c FROM t1";
        BaselinePlan baseline = planner.buildBaseline(bindSql, bindSql);
        long id = manager.createBaseline(baseline);
        Assertions.assertTrue(id > 0, "a subquery-only-WHERE query must be creatable");

        // structurally identical user query with a different subquery literal -> hit
        LogicalPlan userPlan = parse("SELECT (SELECT count(*) FROM t2 WHERE t2.b = 5) AS c FROM t1");
        long deadline = System.currentTimeMillis() + 5000;
        LogicalPlan rewritten = planner.tryRewritePlan(userPlan, deadline);

        Assertions.assertNotNull(rewritten,
                "subquery-only-WHERE query must be rewritten (whole-query binding)");
        Assertions.assertEquals(id, planner.getUsedBaselineId(),
                "used baseline id must be set");
        String rewrittenSql = allExprSqls(rewritten);
        Assertions.assertTrue(rewrittenSql.contains("5"),
                "the subquery literal must be substituted: " + rewrittenSql);
        Assertions.assertFalse(rewrittenSql.contains("_spm_const_var"),
                "no placeholder may remain in the rewritten plan: " + rewrittenSql);
    }

    @Test
    public void testSubqueryOnlyWhereValueIndependence() throws Exception {
        SPMPlanner planner = new SPMPlanner();
        String bindSql = "SELECT c1 FROM t1 WHERE (SELECT max(t2.b) FROM t2 WHERE t2.id = 1) > 0";
        manager.createBaseline(planner.buildBaseline(bindSql, bindSql));

        // scalar subquery in the SELECT list, no query-block WHERE at all
        LogicalPlan userPlan = parse(
                "SELECT (SELECT max(t2.b) FROM t2 WHERE t2.id = 2) AS m FROM t1");
        long deadline = System.currentTimeMillis() + 5000;
        LogicalPlan rewritten = planner.tryRewritePlan(userPlan, deadline);
        Assertions.assertNull(rewritten,
                "a structurally different query must NOT hit the baseline");
    }

    @Test
    public void testTryRewritePlanScalarSubqueryFilter() throws Exception {
        SPMPlanner planner = new SPMPlanner();
        // a scalar subquery inside the topmost filter carries its own constant (t2.id = 1)
        String bindSql = "SELECT * FROM t1 WHERE a = 100 "
                + "AND (SELECT max(t2.b) FROM t2 WHERE t2.id = 1) > 5";
        manager.createBaseline(planner.buildBaseline(bindSql, bindSql));

        // user query: top-level literal differs (a: 100 -> 42) AND the scalar subquery's
        // literal differs (t2.id: 1 -> 2, and the > 5 comparison -> 6)
        LogicalPlan userPlan = parse("SELECT * FROM t1 WHERE a = 42 "
                + "AND (SELECT max(t2.b) FROM t2 WHERE t2.id = 2) > 6");
        long deadline = System.currentTimeMillis() + 5000;
        LogicalPlan rewritten = planner.tryRewritePlan(userPlan, deadline);

        Assertions.assertNotNull(rewritten,
                "scalar-subquery filter query must be rewritten");
        Assertions.assertTrue(planner.getUsedBaselineId() > 0, "used baseline id must be set");
        String rewrittenSql = allExprSqls(rewritten);
        Assertions.assertTrue(rewrittenSql.contains("42"),
                "top-level literal must be substituted: " + rewrittenSql);
        Assertions.assertFalse(rewrittenSql.contains("_spm_const_var"),
                "no placeholder may remain in the rewritten plan: " + rewrittenSql);
    }

    /**
     * End-to-end check that constants INSIDE a subquery are substituted with the exact
     * user values (whole-query binding): the IN-subquery's inner filter predicate must
     * carry the user's value 2 (not the capture-time 1), and the top-level literal must
     * carry 42 (not 100).
     */
    @Test
    public void testInSubqueryConstantSubstitutedPrecisely() throws Exception {
        SPMPlanner planner = new SPMPlanner();
        String bindSql = "SELECT * FROM t1 WHERE a = 100 "
                + "AND t1.c IN (SELECT t2.c FROM t2 WHERE t2.b = 1)";
        manager.createBaseline(planner.buildBaseline(bindSql, bindSql));

        LogicalPlan userPlan = parse("SELECT * FROM t1 WHERE a = 42 "
                + "AND t1.c IN (SELECT t2.c FROM t2 WHERE t2.b = 2)");
        long deadline = System.currentTimeMillis() + 5000;
        LogicalPlan rewritten = planner.tryRewritePlan(userPlan, deadline);

        Assertions.assertNotNull(rewritten, "IN-subquery query must be rewritten");
        Assertions.assertEquals(planner.getUsedBaselineId() > 0, true,
                "used baseline id must be set");
        String allExprs = allExprSqls(rewritten);
        Assertions.assertTrue(allExprs.contains("t2.b = 2"),
                "subquery inner constant must be substituted with the user value: " + allExprs);
        Assertions.assertTrue(allExprs.contains("a = 42"),
                "top-level constant must be substituted with the user value: " + allExprs);
        Assertions.assertFalse(allExprs.contains("_spm_const_var"),
                "no placeholder may remain in the rewritten plan: " + allExprs);
    }

    /**
     * End-to-end check for a SELECT-list scalar subquery (the whole query has NO
     * query-block WHERE): the scalar subquery's own inner filter predicate must carry the
     * user's value 5 (not the capture-time 1).
     */
    @Test
    public void testScalarSelectListSubqueryConstantSubstitutedPrecisely() throws Exception {
        SPMPlanner planner = new SPMPlanner();
        String bindSql = "SELECT (SELECT count(*) FROM t2 WHERE t2.b = 1) AS c FROM t1";
        manager.createBaseline(planner.buildBaseline(bindSql, bindSql));

        LogicalPlan userPlan = parse("SELECT (SELECT count(*) FROM t2 WHERE t2.b = 5) AS c FROM t1");
        long deadline = System.currentTimeMillis() + 5000;
        LogicalPlan rewritten = planner.tryRewritePlan(userPlan, deadline);

        Assertions.assertNotNull(rewritten,
                "SELECT-list scalar subquery query must be rewritten (whole-query binding)");
        Assertions.assertEquals(planner.getUsedBaselineId() > 0, true,
                "used baseline id must be set");
        String allExprs = allExprSqls(rewritten);
        Assertions.assertTrue(allExprs.contains("t2.b = 5"),
                "scalar subquery inner constant must be substituted with the user value: "
                        + allExprs);
        Assertions.assertFalse(allExprs.contains("_spm_const_var"),
                "no placeholder may remain in the rewritten plan: " + allExprs);
    }

    /**
     * End-to-end check that non-expression literals (LIMIT / OFFSET) of the user query
     * are adopted by the rewrite: the rewritten plan must carry the USER limit (5), not
     * the captured one (100) - otherwise a structurally identical query with a different
     * limit would return a truncated result.
     */
    @Test
    public void testUserLimitAdoptedInRewrite() throws Exception {
        SPMPlanner planner = new SPMPlanner();
        String bindSql = "SELECT * FROM t1 WHERE a = 100 ORDER BY a LIMIT 100";
        manager.createBaseline(planner.buildBaseline(bindSql, bindSql));

        LogicalPlan userPlan = parse("SELECT * FROM t1 WHERE a = 42 ORDER BY a LIMIT 5");
        long deadline = System.currentTimeMillis() + 5000;
        LogicalPlan rewritten = planner.tryRewritePlan(userPlan, deadline);

        Assertions.assertNotNull(rewritten, "limit-variant query must be rewritten");
        Assertions.assertTrue(planner.getUsedBaselineId() > 0, "used baseline id must be set");
        // the rewritten tree must carry the user's limit (5)
        Assertions.assertEquals(5L, findLimit(rewritten),
                "rewritten plan must adopt the user LIMIT: " + allExprSqls(rewritten));
        Assertions.assertTrue(allExprSqls(rewritten).contains("42"),
                "top-level literal must still be substituted: " + allExprSqls(rewritten));
    }

    /**
     * ORDER BY keys can carry literals (substr(c_customer_id, 1, 20) -> user changes the
     * length to 21). Those literals must be parameterized like any other expression and
     * substituted with the USER value, otherwise the rewritten tree keeps the bind-side
     * sort key while group-by / project get the user value -> analyze error.
     */
    @Test
    public void testOrderByConstantSubstitutedPrecisely() throws Exception {
        SPMPlanner planner = new SPMPlanner();
        String bindSql = "SELECT substr(c_customer_id, 1, 2) a, count(*) c "
                + "FROM customer GROUP BY substr(c_customer_id, 1, 2) "
                + "ORDER BY substr(c_customer_id, 1, 2) LIMIT 10";
        manager.createBaseline(planner.buildBaseline(bindSql, bindSql));

        LogicalPlan userPlan = parse("SELECT substr(c_customer_id, 1, 3) a, count(*) c "
                + "FROM customer GROUP BY substr(c_customer_id, 1, 3) "
                + "ORDER BY substr(c_customer_id, 1, 3) LIMIT 10");
        long deadline = System.currentTimeMillis() + 5000;
        LogicalPlan rewritten = planner.tryRewritePlan(userPlan, deadline);

        Assertions.assertNotNull(rewritten,
                "ORDER BY literal variant must be rewritten: " + allExprSqls(userPlan));
        String orderKeys = allOrderKeySqls(rewritten);
        Assertions.assertTrue(orderKeys.contains("substr(c_customer_id, 1, 3)"),
                "rewritten ORDER BY must carry the USER literal: " + orderKeys);
        Assertions.assertFalse(orderKeys.contains("_spm_const_var"),
                "no placeholder may remain in rewritten ORDER BY: " + orderKeys);
        // group-by must carry the user value too (both sides stay consistent)
        Assertions.assertTrue(allExprSqls(rewritten).contains("substr(c_customer_id, 1, 3)"),
                "group-by / project must carry the USER literal: " + allExprSqls(rewritten));
    }

    /**
     * An ORDER BY key can be a bare literal (ORDER BY 1 = order by the first column).
     * Parameterizing such a literal walks it with a null parent, while the same value
     * may already be registered under a real parent (e.g. WHERE x > 1). The placeholder
     * dedup must not crash comparing a null parent against a non-null one and must not
     * reuse the id across different nesting levels.
     */
    @Test
    public void testOrderByBareLiteralParameterized() throws Exception {
        SPMPlanner planner = new SPMPlanner();
        String bindSql = "SELECT c_customer_sk FROM customer "
                + "WHERE c_customer_sk > 1 ORDER BY 1 LIMIT 10";
        manager.createBaseline(planner.buildBaseline(bindSql, bindSql));

        LogicalPlan userPlan = parse("SELECT c_customer_sk FROM customer "
                + "WHERE c_customer_sk > 5 ORDER BY 1 LIMIT 10");
        long deadline = System.currentTimeMillis() + 5000;
        LogicalPlan rewritten = planner.tryRewritePlan(userPlan, deadline);

        Assertions.assertNotNull(rewritten,
                "bare-literal ORDER BY variant must be rewritten: " + allExprSqls(userPlan));
        String orderKeys = allOrderKeySqls(rewritten);
        Assertions.assertFalse(orderKeys.contains("_spm_const_var"),
                "no placeholder may remain in rewritten ORDER BY: " + orderKeys);
        Assertions.assertTrue(allExprSqls(rewritten).contains("5"),
                "filter literal must still be substituted: " + allExprSqls(rewritten));
    }

    /**
     * Simulates an FE restart: the two transient parameterized trees are not persisted and
     * start as null, and are rebuilt at load from the stored bindSql / planSql text
     * (SPMPlanner#rebuildParameterizedTrees as called by
     * BaselineManager#readPersistedSnapshot). Verifies the rebuild restores BOTH trees and
     * that the tree-substitution rewrite path (the tryRewritePlan fallback) keeps working
     * afterwards.
     */
    @Test
    public void testRebuildParameterizedTreesAfterRestart() throws Exception {
        SPMPlanner planner = new SPMPlanner();
        String bindSql = "SELECT * FROM t1 WHERE a = 100";
        BaselinePlan baseline = planner.buildBaseline(bindSql, bindSql);
        Assertions.assertTrue(manager.createBaseline(baseline) > 0);

        // restart: only the scalar columns survive, the transient trees are null again
        BaselinePlan persisted = manager.getAllBaselines().iterator().next();
        persisted.setParameterizedBindPlan(null);
        persisted.setParameterizedPlanPlan(null);
        Assertions.assertNull(persisted.getParameterizedBindPlan());
        Assertions.assertNull(persisted.getParameterizedPlanPlan());

        // the load rebuilds the trees from the persisted SQL texts with one shared builder
        Pair<LogicalPlan, LogicalPlan> trees =
                SPMPlanner.rebuildParameterizedTrees(persisted.getBindSql(), persisted.getPlanSql());
        Assertions.assertNotNull(trees.first);
        Assertions.assertNotNull(trees.second);
        persisted.setParameterizedBindPlan(trees.first);
        persisted.setParameterizedPlanPlan(trees.second);

        // a structurally identical user query is still rewritten (value 42 substituted)
        LogicalPlan userPlan = parse("SELECT * FROM t1 WHERE a = 42");
        LogicalPlan rewritten = planner.tryRewritePlan(userPlan, System.currentTimeMillis() + 5000);
        Assertions.assertNotNull(rewritten,
                "rewrite must still work after the trees are rebuilt from the stored SQL");
        String rewrittenSql = allExprSqls(rewritten);
        Assertions.assertTrue(rewrittenSql.contains("42"), rewrittenSql);
        Assertions.assertFalse(rewrittenSql.contains("_spm_const_var"),
                "no placeholder may remain in the rewritten plan: " + rewrittenSql);
    }

    /**
     * The shared-builder rebuild keeps the placeholder ids of the two trees aligned even
     * when the stored texts differ: the CREATE path assigns the plan ids AFTER the bind ids
     * (one shared builder), so a per-text rebuild that restarted plan ids at 1 could
     * silently map a user value onto a different literal slot of the plan text. With the
     * shared builder the plan-only ids stay unmapped and the fallback substitution is
     * rejected instead of mis-substituting.
     */
    @Test
    public void testRebuildSharedBuilderRejectsCrossTreeMisSubstitution() throws Exception {
        SPMPlanner planner = new SPMPlanner();
        String bindSql = "SELECT * FROM t1 WHERE a = 1 AND b = 2";
        // the plan text reorders the literals: a per-text rebuild would collide ids 1 / 2
        String planSql = "SELECT * FROM t1 WHERE b = 1 AND a = 2";
        Assertions.assertTrue(manager.createBaseline(planner.buildBaseline(bindSql, planSql)) > 0);

        // the same must already hold for the freshly created baseline (CREATE path): the
        // plan literals (different parent slots) never reuse the bind-side ids, so the
        // substitution is rejected instead of silently swapping values between slots
        Assertions.assertNull(planner.tryRewritePlan(
                parse("SELECT * FROM t1 WHERE a = 5 AND b = 6"),
                System.currentTimeMillis() + 5000),
                "the CREATE path must not mis-substitute values between reordered slots");

        // simulate a restart: rebuild both trees exactly like the load does
        BaselinePlan persisted = manager.getAllBaselines().iterator().next();
        Pair<LogicalPlan, LogicalPlan> trees =
                SPMPlanner.rebuildParameterizedTrees(persisted.getBindSql(), persisted.getPlanSql());
        Assertions.assertNotNull(trees.first);
        Assertions.assertNotNull(trees.second);
        persisted.setParameterizedBindPlan(trees.first);
        persisted.setParameterizedPlanPlan(trees.second);

        // the user query matches the bind tree; the substitution into the reordered plan
        // tree must be rejected rather than silently swapping values between the slots
        LogicalPlan rewritten = planner.tryRewritePlan(
                parse("SELECT * FROM t1 WHERE a = 5 AND b = 6"), System.currentTimeMillis() + 5000);
        Assertions.assertNull(rewritten,
                "a cross-tree id collision must reject the rewrite, never mis-substitute values");
    }

    /** Returns the limit of the (single) LogicalLimit of the tree, or -1 when absent. */
    private static long findLimit(LogicalPlan plan) {
        return findLimitIn(plan);
    }

    private static long findLimitIn(Plan plan) {
        if (plan instanceof org.apache.doris.nereids.trees.plans.logical.LogicalLimit) {
            return ((org.apache.doris.nereids.trees.plans.logical.LogicalLimit<?>) plan).getLimit();
        }
        for (Plan child : plan.children()) {
            long found = findLimitIn(child);
            if (found >= 0) {
                return found;
            }
        }
        return -1;
    }

    /** SQL text of every ORDER BY key expression of the tree (newline separated). */
    private static String allOrderKeySqls(LogicalPlan plan) {
        StringBuilder sb = new StringBuilder();
        collectOrderKeySqls(plan, sb);
        return sb.toString();
    }

    private static void collectOrderKeySqls(Plan plan, StringBuilder sb) {
        if (plan instanceof org.apache.doris.nereids.trees.plans.logical.LogicalSort) {
            for (org.apache.doris.nereids.properties.OrderKey orderKey
                    : ((org.apache.doris.nereids.trees.plans.logical.LogicalSort<?>) plan)
                            .getOrderKeys()) {
                sb.append(orderKey.getExpr().toSql()).append('\n');
            }
        }
        for (Plan child : plan.children()) {
            collectOrderKeySqls(child, sb);
        }
    }

    /** Parses a single SELECT SQL into an unbound logical plan. */
    private static LogicalPlan parse(String sql) {
        return (LogicalPlan) new NereidsParser().parseSingle(sql);
    }

    /** Concatenates the SQL text of every expression, recursing into subquery plans. */
    private static String allExprSqls(LogicalPlan plan) {
        StringBuilder sb = new StringBuilder();
        collectExprSqls(plan, sb);
        return sb.toString();
    }

    private static void collectExprSqls(Plan plan, StringBuilder sb) {
        for (Expression expr : plan.getExpressions()) {
            sb.append(expr.toSql()).append('\n');
            collectSubquerySqls(expr, sb);
        }
        if (plan instanceof LogicalCTE) {
            for (Plan aliasQuery : ((LogicalCTE<?>) plan).getAliasQueries()) {
                collectExprSqls(aliasQuery, sb);
            }
        }
        for (Plan child : plan.children()) {
            collectExprSqls(child, sb);
        }
    }

    private static void collectSubquerySqls(Expression expr, StringBuilder sb) {
        if (expr instanceof SubqueryExpr) {
            collectExprSqls(((SubqueryExpr) expr).getQueryPlan(), sb);
        }
        for (Expression child : expr.children()) {
            collectSubquerySqls(child, sb);
        }
    }
}
