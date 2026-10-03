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
import org.apache.doris.nereids.spm.manager.SessionBaselineStore;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * Eighteenth review round: the CREATE inputs (row policies), the CTE merge guard, the
 * SPM settings a forwarded statement must carry, and the session-baseline carrier.
 *
 * - The frozen plan is optimized in the CREATOR's context: leaving the CHECK ROW POLICY /
 *   DATA MASK markers in place resolved the creator's row filter / mask into the frozen
 *   SQL, so any user matching the same bind query replayed the creator's policy.
 * - Separately declared bind / plan texts can have different CTE counts; the limit merge
 *   must not index past the shorter list (an IndexOutOfBounds silently ignored the
 *   accepted baseline on every match).
 * - enable_spm_rewrite / spm_rewrite_timeout_ms / enable_spm_fallback drive PLANNING and
 *   must survive FE forwarding, or the master's defaults decide whether a baseline is
 *   used.
 * - A SESSION-scope baseline lives in the forwarding connection's store; the master needs
 *   it carried along, otherwise it never applies to a forwarded SELECT.
 */
public class SPMRound18SafetyTest {

    private static LogicalPlan parse(String sql) {
        return (LogicalPlan) new NereidsParser().parseSingle(sql);
    }

    // ==================== #1: creator policies never enter the frozen plan ====================

    /**
     * The optimizer must not see the CHECK ROW POLICY / DATA MASK markers: resolving them
     * under the CREATOR's identity froze the creator's filter / mask into the planSql as
     * an ordinary predicate, and another user matching the same bind query replayed it.
     * A frozen text is re-parsed at replay (and the stored parameterized trees keep their
     * markers), so the EXECUTING user's own policy checks still run.
     */
    @Test
    public void testCheckPolicyMarkersAreStrippedForOptimization() {
        LogicalPlan bound = parse("SELECT k FROM t1");
        Assertions.assertTrue(bound.treeString().contains("LogicalCheckPolicy"),
                "the parser must mark relations for the policy rule: " + bound.treeString());

        LogicalPlan stripped = SPMPlanTreeSupport.stripCheckPolicy(bound);
        Assertions.assertFalse(stripped.treeString().contains("LogicalCheckPolicy"),
                "no policy marker may reach the SPM optimizer: " + stripped.treeString());
        Assertions.assertTrue(stripped.treeString().contains("UnboundRelation"),
                "the relation itself must stay: " + stripped.treeString());
        Assertions.assertEquals(stripped.treeString(),
                SPMPlanTreeSupport.stripCheckPolicy(stripped).treeString(),
                "stripping an already stripped tree is a no-op");
    }

    // ==================== #8: CTE alias counts are guarded ====================

    /**
     * bindSql and planSql are parsed separately, and the optimizer keeps the CTE
     * definitions of both in the frozen SQL: a matching query with FEWER CTEs must not
     * make the positional merge read past the user's alias list (the thrown
     * IndexOutOfBounds aborted the rewrite and the accepted baseline was silently ignored
     * on every match - even without any LIMIT).
     */
    @Test
    public void testCteAliasCountMismatchDoesNotAbortTheMerge() {
        LogicalPlan frozenTree = parse(
                "WITH a AS (SELECT 1 AS x), b AS (SELECT 2 AS y) SELECT x FROM a");
        LogicalPlan userTree = parse("WITH a AS (SELECT 1 AS x) SELECT x FROM a");

        LogicalPlan merged = SPMPlanTreeSupport.mergeLimits(frozenTree, userTree);
        Assertions.assertSame(frozenTree, merged,
                "a subtree without a positional counterpart stays unchanged");

        // the equal-size case still adopts the user's limit value
        LogicalPlan equalFrozen = parse("WITH a AS (SELECT 1 AS x) SELECT x FROM a LIMIT 3");
        LogicalPlan equalUser = parse("WITH a AS (SELECT 1 AS x) SELECT x FROM a LIMIT 7");
        LogicalPlan mergedEqual = SPMPlanTreeSupport.mergeLimits(equalFrozen, equalUser);
        Assertions.assertNotNull(mergedEqual);
        Assertions.assertEquals(7L, findLimit(mergedEqual).getLimit(),
                "the user's limit must be merged when the shapes line up: "
                        + mergedEqual.treeString());
    }

    /** First LogicalLimit of the tree (test helper). */
    private static org.apache.doris.nereids.trees.plans.logical.LogicalLimit<?> findLimit(
            org.apache.doris.nereids.trees.plans.Plan plan) {
        if (plan instanceof org.apache.doris.nereids.trees.plans.logical.LogicalLimit) {
            return (org.apache.doris.nereids.trees.plans.logical.LogicalLimit<?>) plan;
        }
        for (org.apache.doris.nereids.trees.plans.Plan child
                : plan.children()) {
            org.apache.doris.nereids.trees.plans.logical.LogicalLimit<?> limit =
                    findLimit(child);
            if (limit != null) {
                return limit;
            }
        }
        return null;
    }

    // ==================== #6: SPM planning settings are forwarded ====================

    /**
     * A forwarded statement is planned by the MASTER: its own enable_spm_rewrite / timeout
     * / fallback defaults decided whether the connection's baseline was used. The three
     * planning switches must therefore be part of the forwarded variables.
     */
    @Test
    public void testSpmSettingsAreForwarded() {
        Map<String, String> forwarded = new SessionVariable().getForwardVariables();
        Assertions.assertTrue(forwarded.containsKey(SessionVariable.ENABLE_SPM_REWRITE),
                "enable_spm_rewrite must be forwarded: " + forwarded.keySet());
        Assertions.assertTrue(forwarded.containsKey(SessionVariable.SPM_REWRITE_TIMEOUT_MS),
                "spm_rewrite_timeout_ms must be forwarded");
        Assertions.assertTrue(forwarded.containsKey(SessionVariable.ENABLE_SPM_FALLBACK),
                "enable_spm_fallback must be forwarded");
        Assertions.assertTrue(
                forwarded.containsKey(SessionVariable.SPM_FORWARDED_SESSION_BASELINES),
                "the session-baseline carrier must be forwarded");

        SessionVariable master = new SessionVariable();
        master.setForwardedSessionVariables(Map.of(
                SessionVariable.ENABLE_SPM_REWRITE, "true",
                SessionVariable.SPM_FORWARDED_SESSION_BASELINES, "payload"));
        Assertions.assertTrue(master.isEnableSpmRewrite(),
                "a forwarded request must restore the connection's switch");
        Assertions.assertEquals("payload", master.getSpmForwardedSessionBaselines());
    }

    // ==================== #7: session baselines travel with the forward ====================

    /**
     * round-23 #6: the post-plan replay validation must keep the matched baseline (or
     * fail): re-fetching by id can silently miss it after a concurrent DROP / refresh,
     * and returning then would skip the schema check exactly when a table DDL may have
     * committed between the pre-match validation and the replay planning.
     */
    @Test
    public void testReplayValidationFailsWhenTheBaselineIsGone() {
        ConnectContext ctx = new ConnectContext();
        ctx.setStatementContext(new StatementContext(ctx, null));
        LogicalPlan plan = parse("SELECT 1");
        Assertions.assertThrows(RuntimeException.class,
                () -> SPMPlanner.verifyReplayMetadata(ctx, 777L, plan),
                "a disappeared baseline must not silently skip the replay validation");

        // the retained incarnation (set at match time) keeps the validation alive
        BaselinePlan retained = new BaselinePlan();
        retained.setId(777L);
        ctx.getStatementContext().setSpmUsedBaseline(retained);
        SPMPlanner.verifyReplayMetadata(ctx, 777L, plan);
    }

    /**
     * The observer attaches the ENABLED rows of its session store to the forwarded
     * request; the master rebuilds them (parameterized trees included) into its context's
     * store before planning, and an empty payload clears whatever the context held (the
     * payload mirrors the source store, so a row dropped there must not survive).
     */
    @Test
    public void testForwardedSessionBaselineRoundTrip() {
        SessionBaselineStore store = new SessionBaselineStore();
        BaselinePlan plan = new BaselinePlan();
        plan.setBindSql("SELECT * FROM t1 WHERE a > 100");
        plan.setPlanSql("SELECT * FROM t1 WHERE a > CAST(_spm_const_var(1) AS INT)");
        LogicalPlan bindPlan = parse(plan.getBindSql());
        plan.setBindSqlDigest(bindPlan.toSpmDigest());
        plan.setBindSqlHash(SPMUtils.hashOf(bindPlan.toSpmDigest()));
        plan.setPlanFrozen(Boolean.TRUE);
        plan.setStatus(BaselineStatus.ENABLED);
        plan.setSource(BaselineSource.USER);
        store.createBaseline(plan);

        String payload = SPMForwardedSession.serialize(store);
        Assertions.assertFalse(payload.isEmpty(), "an enabled row must be carried");

        ConnectContext masterCtx = new ConnectContext();
        // the import follows the statement's enable_spm_rewrite (round-26: with rewrite
        // disabled a baseline can never be consulted, so nothing is rebuilt)
        masterCtx.getSessionVariable().setEnableSpmRewrite(true);
        SPMForwardedSession.importInto(masterCtx, payload);
        Assertions.assertEquals(1, masterCtx.getSessionBaselineStore().getAllBaselines().size(),
                "the master context must see the connection's session baseline");
        BaselinePlan restored = masterCtx.getSessionBaselineStore().getAllBaselines().get(0);
        Assertions.assertEquals(plan.getBindSqlDigest(), restored.getBindSqlDigest());
        Assertions.assertTrue(Boolean.TRUE.equals(restored.getPlanFrozen()),
                "the frozen provenance must survive the carrier");
        Assertions.assertEquals(plan.getId(), restored.getId(),
                "the ORIGINAL session id must travel with the payload: a forwarded EXPLAIN"
                        + " must report the id SHOW / ALTER / DROP use on the connection");
        // repeated imports are deterministic: no fresh id per forwarded request
        SPMForwardedSession.importInto(masterCtx, payload);
        Assertions.assertEquals(plan.getId(),
                masterCtx.getSessionBaselineStore().getAllBaselines().get(0).getId(),
                "every forwarded request must import under the same id");
        Assertions.assertNotNull(restored.getParameterizedBindPlan(),
                "the transient trees are rebuilt on the master");
        Assertions.assertEquals(BaselineStatus.ENABLED, restored.getStatus());

        SPMForwardedSession.importInto(masterCtx, "");
        Assertions.assertTrue(masterCtx.getSessionBaselineStore().isEmpty(),
                "an empty payload mirrors an empty source store");

        // malformed payloads never break the statement they ride on
        SPMForwardedSession.importInto(new ConnectContext(), "{not json");

        // a DISABLED row is not carried (it could not match anyway)
        store.clear();
        plan.setStatus(BaselineStatus.DISABLED);
        store.createBaseline(plan);
        Assertions.assertEquals("", SPMForwardedSession.serialize(store));
    }
}
