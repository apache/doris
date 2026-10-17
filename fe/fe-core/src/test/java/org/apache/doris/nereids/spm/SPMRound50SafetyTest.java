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
import org.apache.doris.nereids.spm.manager.SessionBaselineStore;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Round-50 review fixes of the forwarding / reload paths:
 *
 * - #6 (BaselineManager#parsePersistedRow): the PERSISTED plan_frozen provenance is
 *   authoritative. A legal unqualified UDF call _spm_const_var(1) forces the raw fallback
 *   and stores plan_frozen=false, but the parse-based marker check re-classified that
 *   text as frozen on reload - a matching caller using _spm_const_var(2) then had the
 *   real UDF call replaced by the literal 2 in frozen replay.
 * - #11 (FEOpExecutor#buildStmtForwardParams): only the SESSION baselines the forwarded
 *   statement can actually match are carried to the master. The master rebuilds every
 *   carried row (re-parsing both SQL texts) before it even classifies the statement, so
 *   forwarding the whole store of a large session made every forwarded statement pay
 *   thousands of parses - outside spm_rewrite_timeout_ms. Statements that address rows
 *   by id (the baseline management commands) keep the full payload.
 */
public class SPMRound50SafetyTest {

    private static BaselinePlan enabledRow(String bindSql, String digest, long hash) {
        BaselinePlan plan = new BaselinePlan();
        plan.setBindSql(bindSql);
        plan.setPlanSql(bindSql);
        plan.setBindSqlDigest(digest);
        plan.setBindSqlHash(hash);
        plan.setStatus(BaselineStatus.ENABLED);
        return plan;
    }

    private static SessionBaselineStore storeWith(BaselinePlan... plans) {
        SessionBaselineStore store = new SessionBaselineStore();
        for (BaselinePlan plan : plans) {
            store.createBaseline(plan);
        }
        return store;
    }

    // ==================== #11: only applicable candidates travel ====================

    /**
     * A forwardsable QUERY carries exactly the rows whose (hash, digest) equal its own
     * match key - the contract the rewrite looks candidates up with.
     */
    @Test
    public void testForwardedSessionCarriesOnlyApplicableRows() {
        Pair<String, Long> key = SPMPlanner.queryMatchKey(null, "SELECT k FROM t");
        Assertions.assertNotNull(key, "a query must have a match key");
        SessionBaselineStore store = storeWith(
                enabledRow("SELECT k FROM t", key.first, key.second),
                enabledRow("SELECT k FROM other", "another-digest", key.second + 1));
        String payload = SPMForwardedSession.serializeForStatement(store, null,
                "SELECT k FROM t");
        Assertions.assertTrue(payload.contains("SELECT k FROM t"),
                "the matching row must travel: " + payload);
        Assertions.assertFalse(payload.contains("SELECT k FROM other"),
                "a row that can never match must not be rebuilt on the master: " + payload);
    }

    /**
     * Statements that address rows BY ID on the master (the baseline management
     * commands) keep the full payload - and a query matching nothing carries nothing,
     * so the master's store is left empty instead of being rebuilt for nothing.
     */
    @Test
    public void testForwardedSessionFallsBackToTheFullStoreForNonQueries() {
        SessionBaselineStore store = storeWith(
                enabledRow("SELECT k FROM t", "d1", 1L),
                enabledRow("SELECT k FROM other", "d2", 2L));
        String ddlPayload = SPMForwardedSession.serializeForStatement(store, null,
                "DROP BASELINE PLAN IF EXISTS 42");
        Assertions.assertTrue(ddlPayload.contains("SELECT k FROM t")
                        && ddlPayload.contains("SELECT k FROM other"),
                "a management command keeps the full payload: " + ddlPayload);
        String batchPayload = SPMForwardedSession.serializeForStatement(store, null,
                "SELECT k FROM t; SELECT k FROM other");
        Assertions.assertTrue(batchPayload.contains("SELECT k FROM other"),
                "a statement batch keeps the full payload: " + batchPayload);
        Assertions.assertEquals("", SPMForwardedSession.serializeForStatement(store, null,
                        "SELECT unrelated FROM elsewhere"),
                "a query that can match no row carries nothing");
        Assertions.assertEquals("", SPMForwardedSession.serializeForStatement(null, null,
                "SELECT k FROM t"));
    }

    /** Only a plan-rewritable query has a match key; commands / batches / junk do not. */
    @Test
    public void testQueryMatchKeyOnlyForQueries() {
        Assertions.assertNotNull(SPMPlanner.queryMatchKey(null, "SELECT 1"));
        Assertions.assertNull(SPMPlanner.queryMatchKey(null, "DROP BASELINE PLAN IF EXISTS 1"),
                "a command is never rewritten: no key, the full store travels");
        Assertions.assertNull(SPMPlanner.queryMatchKey(null, "SELECT 1; SELECT 2"),
                "a batch cannot be classified per statement");
        Assertions.assertNull(SPMPlanner.queryMatchKey(null, "this is not sql"),
                "unparsable text has no key");
    }

    // ==================== #6: persisted non-frozen provenance ====================

    /**
     * An explicit plan_frozen=false row keeps its raw text even when that text is
     * marker-SHAPED: the call is the user's own UDF (the decompiler refused it, which is
     * why the fallback stored the raw text), so the reload must not flip the row into
     * the frozen replay path where the call would be replaced by the caller's literal.
     */
    @Test
    public void testExplicitNonFrozenProvenanceWinsOverMarkerShape() {
        String rawFallback = "SELECT _spm_const_var(1) FROM t";
        Assertions.assertTrue(SPMPlanner.isFrozenPlanSql(rawFallback, null),
                "without provenance the marker-shaped text classifies as frozen");
        Assertions.assertFalse(SPMPlanner.isFrozenPlanSql(rawFallback, Boolean.FALSE),
                "an explicit NOT-frozen row keeps its raw text");
        Assertions.assertTrue(SPMPlanner.isFrozenPlanSql(rawFallback, Boolean.TRUE));
    }
}
