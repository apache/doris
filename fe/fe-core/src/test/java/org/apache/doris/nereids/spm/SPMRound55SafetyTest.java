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
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.properties.SelectHintSetVar;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.SqlModeHelper;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

/**
 * Round-55 #1 / #9: the SPM forwarding path must compute the session-baseline SUBSET key
 * from the statement as the rewrite itself sees it - the QUERY an EXPLAIN wraps, parsed
 * with the mode its text was written for.
 */
public class SPMRound55SafetyTest {

    /**
     * #1: a forwarded EXPLAIN SELECT is treated as a query by the forwarding policy, so its
     * key must be the explained SELECT's. Rejecting the ExplainCommand made the branch fall
     * back to the WHOLE enabled session store: the master re-parses every row before EXPLAIN
     * even reaches the rewrite timeout, and a diagnostic for one query scales with
     * thousands of unrelated baselines.
     */
    @Test
    public void testExplainQueryGetsTheExplainedSelectsMatchKey() {
        Pair<String, Long> selectKey =
                SPMPlanner.queryMatchKey(null, "SELECT k FROM t1");
        Assertions.assertNotNull(selectKey, "a plain SELECT gets a key");
        Pair<String, Long> explainKey =
                SPMPlanner.queryMatchKey(null, "EXPLAIN SELECT k FROM t1");
        Assertions.assertNotNull(explainKey,
                "EXPLAIN SELECT must get the explained SELECT's key instead of null (which"
                        + " serializes the whole session store)");
        Assertions.assertEquals(selectKey.first, explainKey.first,
                "the EXPLAIN key is the explained SELECT's bucket, exactly like the rewrite"
                        + " classifies it (ExplainCommand#run hands getLogicalPlan() to"
                        + " tryRewritePlan)");
        Assertions.assertEquals(selectKey.second, explainKey.second);

        // a statement no baseline can ever apply to still yields no key: it must keep the
        // unfiltered store (a management command addresses rows by id)
        Assertions.assertNull(SPMPlanner.queryMatchKey(null, "CREATE TABLE t2 (k int)"),
                "a DDL is not plan-rewritable");
    }

    /**
     * #9: the subset key must be computed under the mode the statement's TEXT was written
     * for. The statement's own {@code SET_VAR(sql_mode=...)} hint writes the SESSION mode
     * while it is being parsed (SelectHintSetVar#setVarOnceInSql, reverted only when the
     * statement finishes), so reading the session mode here built the key under a mode the
     * text was never parsed with - a query whose hint turned PIPES_AS_CONCAT on produced a
     * different digest than the rewrite had matched (or no match at all), and the master,
     * which receives the same polluted mode, re-parsed the SQL as concat.
     */
    @Test
    public void testOriginParseModeSurvivesTheSetVarHint() {
        ConnectContext ctx = new ConnectContext();
        ctx.setSessionVariable(new SessionVariable());
        StatementContext statementContext = new StatementContext(ctx, null);
        ctx.setStatementContext(statementContext);

        // the mode in force right BEFORE the hint is applied: the ambient mode the text is
        // parsed under (the thread-bound session's, or the global default without one)
        long preHint = SqlModeHelper.currentMode();
        SelectHintSetVar hint = new SelectHintSetVar("set_var",
                ImmutableMap.of("sql_mode", Optional.of("PIPES_AS_CONCAT")));
        Assertions.assertDoesNotThrow(() -> hint.setVarOnceInSql(statementContext));
        Assertions.assertEquals(preHint, statementContext.getSpmOriginSqlMode(),
                "the mode in force BEFORE the hint is the mode the text is parsed under");
        Assertions.assertEquals(preHint, SPMPlanner.originSqlMode(ctx),
                "the captured mode is what the re-parse / the forwarded statement uses");

        // a SECOND hint of the same statement must not re-record (the session value is
        // already polluted by the first one)
        statementContext.recordSpmOriginSqlMode(SqlModeHelper.MODE_PIPES_AS_CONCAT);
        Assertions.assertEquals(preHint, statementContext.getSpmOriginSqlMode(),
                "only the FIRST capture counts");

        // a captured mode that DIFFERS from the ambient one is reported as-is: the
        // statement's own (pre-hint) mode wins over the possibly polluted session one
        StatementContext capturing = new StatementContext(ctx, null);
        ctx.setStatementContext(capturing);
        long captured = SqlModeHelper.MODE_PIPES_AS_CONCAT ^ SqlModeHelper.MODE_DEFAULT;
        capturing.recordSpmOriginSqlMode(captured);
        Assertions.assertEquals(captured, SPMPlanner.originSqlMode(ctx));
    }

    /** Without a hint (or a context) the session's own mode is the statement's mode. */
    @Test
    public void testOriginParseModeFallsBackToTheSession() {
        ConnectContext ctx = new ConnectContext();
        ctx.setSessionVariable(new SessionVariable());
        StatementContext statementContext = new StatementContext(ctx, null);
        ctx.setStatementContext(statementContext);
        Assertions.assertEquals(SqlModeHelper.currentMode(), SPMPlanner.originSqlMode(ctx),
                "nothing was captured, so the ambient (session) mode is reported");
        Assertions.assertEquals(SqlModeHelper.currentMode(), SPMPlanner.originSqlMode(null),
                "a missing context falls back to the ambient mode (tests / internal paths)");
    }

    /**
     * Round-57 C4: the forwarded CREATE's three canonical identities (bind digest, plan
     * digest, schema fingerprint) must be computed under ONE originating parse mode.
     * Parsing the hinted bind SQL applies its SET_VAR(sql_mode=...) to the OBSERVER's live
     * session, and the parser's mode window restores only that one parse - so a plan digest
     * computed in a SEPARATE call afterwards saw the HINTED mode, while the master parsed
     * BOTH CREATE texts under the pre-hint one. A duplicate CREATE's row carries the FIRST
     * statement's query id and a different frozen plan text, so the mismatched plan digest
     * made a successful master response fail follower confirmation.
     */
    @Test
    public void testForwardedCreateIdentitiesUseOneOriginatingMode() {
        ConnectContext ctx = new ConnectContext();
        ctx.setSessionVariable(new SessionVariable());
        ctx.setStatementContext(new StatementContext(ctx, null));
        ctx.setThreadLocalInfo();
        try {
            // `||` parses as a CONCAT under PIPES_AS_CONCAT and as a logical OR under the
            // default mode: the two digests of the SAME plan text differ
            String bind = "SELECT /*+ SET_VAR(sql_mode='PIPES_AS_CONCAT') */ a || b FROM t1";
            String plan = "SELECT a || b FROM t1";
            String preHintPlanDigest = SqlModeHelper.withSqlMode(SqlModeHelper.MODE_DEFAULT,
                    () -> SPMPlanner.canonicalPlanDigest(ctx, plan));
            String hintedPlanDigest = SqlModeHelper
                    .withSqlMode(SqlModeHelper.MODE_PIPES_AS_CONCAT,
                            () -> SPMPlanner.canonicalPlanDigest(ctx, plan));
            Assertions.assertNotEquals(preHintPlanDigest, hintedPlanDigest,
                    "precondition: the two modes produce different digests");

            // the fixed path: ONE originating mode around all three computations, so the
            // bind parse's hint cannot leak into the plan digest / fingerprint
            String[] identities = SPMPlanner.canonicalForwardedCreateIdentities(ctx, bind,
                    plan);
            Assertions.assertEquals(preHintPlanDigest, identities[1],
                    "the plan digest must be computed under the PRE-hint mode, exactly like"
                            + " the master's parse of the same CREATE text");

            // the sequence the fix replaced: the bind parse DID write the session (that is
            // inherent to parsing a hinted statement), so a call OUTSIDE the wrapper takes
            // the hinted mode - which is what made the two sides disagree
            Assertions.assertEquals(hintedPlanDigest,
                    SPMPlanner.canonicalPlanDigest(ctx, plan),
                    "without the originating-mode window the next digest sees the HINTED"
                            + " mode (the reviewer's C4 report)");
        } finally {
            ConnectContext.remove();
        }
    }
}
