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

import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.spm.manager.BaselineManager;
import org.apache.doris.nereids.spm.manager.SessionBaselineStore;
import org.apache.doris.nereids.spm.placeholder.SPMPlaceholderBuilder;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.SessionVariable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * SESSION-scope baseline storage test (SessionBaselineStore + its wiring into the
 * rewrite matcher).
 *
 * Verifies:
 *
 * 1. store semantics: create / dedup / candidate lookup / disable / drop
 * 2. rewrite precedence: a SESSION baseline of the current connection wins over a
 *    structurally identical GLOBAL baseline of the shared manager
 * 3. isolation: a NEW connection (fresh ConnectContext) neither sees nor uses the
 *    session baseline created by another connection
 */
public class SessionBaselineStoreTest {

    private BaselineManager manager;

    @BeforeEach
    public void setUp() {
        manager = BaselineManager.getInstance();
        manager.clearForTest();
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    @Test
    public void testCreateFindDedupDisableDrop() throws Exception {
        SessionBaselineStore store = new SessionBaselineStore();
        String bindSql = "SELECT * FROM t1 WHERE a = 100";
        String frozen = "SELECT * FROM t1 WHERE (a = CAST(_spm_const_var(1) AS INT))";

        long id = store.createBaseline(frozenBaseline(bindSql, frozen));
        // the store marks everything it owns as SESSION-scope, and the id comes from the
        // SESSION id range (>= 2^62), so the scope derived from the id is exact
        Assertions.assertEquals(BaselineScope.SESSION, store.getBaseline(id).getScope());
        Assertions.assertTrue(id >= BaselineScope.SESSION_ID_BASE,
                "a session baseline id must stay in the session id range, got " + id);
        Assertions.assertEquals(BaselineScope.SESSION, BaselineScope.ofId(id));
        // dedup: identical (hash, digest, planSql) returns the existing id
        Assertions.assertEquals(id, store.createBaseline(frozenBaseline(bindSql, frozen)));
        Assertions.assertEquals(1, store.getAllBaselines().size());

        // candidate lookup through the hash index + digest filter
        LogicalPlan userPlan = parse("SELECT * FROM t1 WHERE a = 42");
        String digest = userPlan.toSpmDigest();
        long hash = SPMUtils.hashOf(digest);
        Assertions.assertEquals(1, store.findCandidateBaselines(digest, hash).size());

        // disable -> no longer a candidate; enable restores it
        Assertions.assertTrue(store.updateStatus(id, BaselineStatus.DISABLED));
        Assertions.assertTrue(store.findCandidateBaselines(digest, hash).isEmpty());
        Assertions.assertTrue(store.updateStatus(id, BaselineStatus.ENABLED));
        Assertions.assertEquals(1, store.findCandidateBaselines(digest, hash).size());

        // drop; unknown ids are misses (the commands route session-range ids exclusively
        // into this store - a miss is final and never resolved against the global store)
        Assertions.assertTrue(store.dropBaseline(id));
        Assertions.assertFalse(store.dropBaseline(id));
        Assertions.assertFalse(store.updateStatus(id, BaselineStatus.DISABLED));
        Assertions.assertTrue(store.getAllBaselines().isEmpty());
        Assertions.assertEquals(0, store.findCandidateBaselines(digest, hash).size());
    }

    @Test
    public void testSessionBaselineWinsOverGlobalForSameShape() throws Exception {
        installConnectContext();
        SessionBaselineStore store = ConnectContext.get().getSessionBaselineStore();

        // structurally identical GLOBAL baseline; its frozen text keeps "="
        long globalId = manager.createBaseline(frozenBaseline(
                "SELECT * FROM t1 WHERE a = 100",
                "SELECT * FROM t1 WHERE (a = CAST(_spm_const_var(1) AS INT))"));
        // SESSION baseline of this connection; its frozen text uses ">" so the replay
        // result identifies which baseline produced the rewrite
        long sessionId = store.createBaseline(frozenBaseline(
                "SELECT * FROM t1 WHERE a = 100",
                "SELECT * FROM t1 WHERE (a > CAST(_spm_const_var(1) AS INT))"));
        Assertions.assertTrue(globalId != sessionId, "both scopes must draw distinct ids");
        // the id ranges are disjoint: GLOBAL below 2^62, SESSION from 2^62 on
        Assertions.assertTrue(globalId < BaselineScope.SESSION_ID_BASE,
                "a GLOBAL id must stay below the session id range, got " + globalId);
        Assertions.assertTrue(sessionId >= BaselineScope.SESSION_ID_BASE,
                "a SESSION id must start at 2^62, got " + sessionId);
        Assertions.assertEquals(BaselineScope.GLOBAL, BaselineScope.ofId(globalId));
        Assertions.assertEquals(BaselineScope.SESSION, BaselineScope.ofId(sessionId));
        // a session-range id is invisible to the global store and vice versa
        Assertions.assertNull(manager.getBaseline(sessionId),
                "the global store must not contain a session-range id");
        Assertions.assertNull(store.getBaseline(globalId),
                "the session store must not contain a global-range id");

        // the owning store decides the scope: manager -> GLOBAL, session store -> SESSION
        Assertions.assertEquals(BaselineScope.GLOBAL, manager.getBaseline(globalId).getScope());
        Assertions.assertEquals(BaselineScope.SESSION, store.getBaseline(sessionId).getScope());

        SPMPlanner planner = new SPMPlanner();
        LogicalPlan rewritten = planner.tryRewritePlan(
                parse("SELECT * FROM t1 WHERE a = 42"), System.currentTimeMillis() + 5000);

        Assertions.assertNotNull(rewritten, "the user query must hit a baseline");
        Assertions.assertEquals(sessionId, planner.getUsedBaselineId(),
                "the SESSION baseline must win over the GLOBAL one");
        String exprSqls = allExprSqls(rewritten);
        Assertions.assertTrue(exprSqls.contains(">"),
                "the rewrite must replay the SESSION baseline's frozen text: " + exprSqls);
        Assertions.assertTrue(exprSqls.contains("42"),
                "the user value must be substituted: " + exprSqls);
        Assertions.assertFalse(SPMPlanTreeSupport.containsFrozenPlaceholder(rewritten),
                "no placeholder call may remain: " + exprSqls);
    }

    @Test
    public void testSessionBaselineIsInvisibleToAnotherSession() throws Exception {
        installConnectContext();
        long sessionId = ConnectContext.get().getSessionBaselineStore().createBaseline(
                frozenBaseline("SELECT * FROM t1 WHERE a = 100",
                        "SELECT * FROM t1 WHERE (a > CAST(_spm_const_var(1) AS INT))"));
        Assertions.assertTrue(sessionId > 0);

        // a NEW connection gets a fresh, empty store
        ConnectContext.remove();
        installConnectContext();
        Assertions.assertTrue(ConnectContext.get().getSessionBaselineStore()
                .getAllBaselines().isEmpty(), "a new connection must start with an empty store");

        // and the rewrite in the new session must not use the other session's baseline
        SPMPlanner planner = new SPMPlanner();
        LogicalPlan rewritten = planner.tryRewritePlan(
                parse("SELECT * FROM t1 WHERE a = 42"), System.currentTimeMillis() + 5000);
        Assertions.assertNull(rewritten,
                "another session's SESSION baseline must be invisible");
        Assertions.assertEquals(-1, planner.getUsedBaselineId());
    }

    @Test
    public void testBaselineScopeOfIdBoundaries() {
        // the id range is the single authoritative scope discriminator: everything below
        // 2^62 is GLOBAL, everything from 2^62 on is SESSION
        Assertions.assertEquals(BaselineScope.GLOBAL, BaselineScope.ofId(0));
        Assertions.assertEquals(BaselineScope.GLOBAL, BaselineScope.ofId(1));
        Assertions.assertEquals(BaselineScope.GLOBAL,
                BaselineScope.ofId(BaselineScope.SESSION_ID_BASE - 1));
        Assertions.assertEquals(BaselineScope.SESSION,
                BaselineScope.ofId(BaselineScope.SESSION_ID_BASE));
        Assertions.assertEquals(BaselineScope.SESSION, BaselineScope.ofId(Long.MAX_VALUE));
    }

    // ==================== helpers ====================

    /**
     * Installs a minimal ConnectContext on this thread (the frozen-text replay needs
     * it, exactly like the real StmtExecutor path).
     */
    private static void installConnectContext() {
        ConnectContext ctx = new ConnectContext();
        ctx.setSessionVariable(new SessionVariable());
        ctx.setThreadLocalInfo();
        ctx.setStatementContext(new StatementContext(ctx, new OriginStatement("SELECT 1", 0)));
    }

    /**
     * Hand-builds a baseline whose planSql is a frozen (placeholder-carrying) plan text -
     * the equivalent of what buildBaselineFromSql stores after a successful decompile.
     */
    private static BaselinePlan frozenBaseline(String bindSql, String frozenPlanSql) {
        LogicalPlan bindPlan = parse(bindSql);
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        LogicalPlan parameterizedBind = SPMPlanTreeSupport.transform(
                bindPlan, expr -> expr.accept(builder, null));

        BaselinePlan baseline = new BaselinePlan();
        baseline.setBindSql(bindSql);
        baseline.setBindSqlDigest(bindPlan.toSpmDigest());
        baseline.setBindSqlHash(SPMUtils.hashOf(bindPlan.toSpmDigest()));
        baseline.setPlanSql(frozenPlanSql);
        baseline.setParameterizedBindPlan(parameterizedBind);
        baseline.setCost(0.0);
        return baseline;
    }

    /** Parses a single SELECT SQL into an unbound logical plan. */
    private static LogicalPlan parse(String sql) {
        return (LogicalPlan) new NereidsParser().parseSingle(sql);
    }

    /** Concatenates the SQL text of every expression, recursing into child plans. */
    private static String allExprSqls(Plan plan) {
        StringBuilder sb = new StringBuilder();
        for (Expression expr : plan.getExpressions()) {
            sb.append(expr.toSql()).append('\n');
        }
        for (Plan child : plan.children()) {
            sb.append(allExprSqls(child));
        }
        return sb.toString();
    }
}
