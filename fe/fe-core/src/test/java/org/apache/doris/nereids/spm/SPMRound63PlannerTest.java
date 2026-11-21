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
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Round-63 review fixes on the replay / alignment side:
 *
 * - a STORED SQL text (bindSql / planSql / fingerprint text) is parsed with the caller's
 *   session isolated: its captured SET_VAR payloads used to be EXECUTED on whatever
 *   ConnectContext the thread carried, so rebuilding a baseline changed the CALLER's
 *   session variables (a stored SET_VAR(time_zone='+09:30') moved the query's zone before
 *   the fallback replay even ran);
 * - a star over a CTE keeps the CTE's EXPLICIT column aliases: the outer project of a
 *   WITH c(x, y) AS (...) SELECT * FROM c froze as Project(x, y), while the alignment's
 *   star expansion re-derived the INNER projection's labels (a, b) and renamed the frozen
 *   headers;
 * - a CTE is referenced by its NAME: an UNQUALIFIED reference resolves against the CTE
 *   scope, while a QUALIFIED name (db.c) is a base-table reference and must NOT be
 *   expanded through the same-named CTE (SELECT db.c.* from a join with c q used to
 *   rename the frozen k to the CTE's inner alias).
 */
public class SPMRound63PlannerTest {

    private static LogicalPlan parse(String sql) {
        return (LogicalPlan) new NereidsParser().parseSingle(sql);
    }

    private static ConnectContext context() {
        ConnectContext ctx = new ConnectContext();
        StatementContext statementContext = new StatementContext();
        statementContext.setConnectContext(ctx);
        ctx.setStatementContext(statementContext);
        ctx.setSessionVariable(new SessionVariable());
        ctx.setDatabase("spm_db");
        ctx.setThreadLocalInfo();
        return ctx;
    }

    /** The labels of the FIRST projection under the root (the main query's output). */
    private static List<String> mainProjectLabels(LogicalPlan root) {
        Plan node = root;
        while (!(node instanceof LogicalProject)) {
            node = node.child(0);
        }
        List<String> labels = new ArrayList<>();
        for (NamedExpression expr : ((LogicalProject<?>) node).getProjects()) {
            labels.add(expr.getName());
        }
        return labels;
    }

    // ==================== #3: stored parses never write the caller's session ====================

    /**
     * CONTROL: parsing the SAME text as an ordinary statement DOES execute its SET_VAR
     * payload (this is the behavior the stored parse must not inherit). Then the stored
     * parses - the cold rebuild of a baseline's trees and the warm fingerprint recompute -
     * leave the session variable untouched.
     */
    @Test
    public void testStoredPlanParseDoesNotWriteSessionSetVars() {
        String planText = "SELECT /*+ SET_VAR(time_zone='+09:30') */ k FROM t";

        // control: a REGULAR parse on the caller's thread applies the payload
        ConnectContext controlCtx = context();
        try {
            String before = controlCtx.getSessionVariable().getTimeZone();
            new NereidsParser().parseSingle(planText);
            Assertions.assertNotEquals(before, controlCtx.getSessionVariable().getTimeZone(),
                    "the SET_VAR payload of a plain parse must reach the session (the"
                            + " stored-parse suppression is what keeps the caller intact)");
        } finally {
            ConnectContext.remove();
        }

        ConnectContext ctx = context();
        try {
            String before = ctx.getSessionVariable().getTimeZone();

            // cold: the CREATE/LOAD rebuild of one baseline's parameterized trees
            Pair<LogicalPlan, LogicalPlan> trees = SPMPlanner.rebuildParameterizedTrees(
                    "SELECT k FROM t", planText, 0L, 0L);
            Assertions.assertNotNull(trees.first, "the bind text must still parse");
            Assertions.assertNotNull(trees.second, "the stored plan text must still parse");
            Assertions.assertEquals(before, ctx.getSessionVariable().getTimeZone(),
                    "rebuilding a stored baseline must not execute its SET_VAR payload on"
                            + " the caller's session");
            Assertions.assertNotEquals("+09:30", ctx.getSessionVariable().getTimeZone());

            // warm: the post-plan fingerprint parse of the stored planSql
            String fingerprint = SPMPlanTreeSupport.schemaFingerprintForReplay(
                    ctx, null, null, planText);
            Assertions.assertNotNull(fingerprint);
            Assertions.assertEquals(before, ctx.getSessionVariable().getTimeZone(),
                    "the fingerprint parse must not repeat the write on warm replay");
        } finally {
            ConnectContext.remove();
        }
    }

    // ==================== #6: the CTE's explicit column aliases ====================

    /**
     * The caller's `WITH c(x, y) AS (SELECT k AS a, v AS b FROM t) SELECT * FROM c` exposes
     * the headers x, y - the frozen replay of the same statement is a Project(x, y). The
     * caller-side star expansion used to peel the CTE's alias node and derive the INNER
     * project's labels (a, b), so alignment renamed the correctly frozen Project(x, y)
     * into Project(x AS a, y AS b).
     */
    @Test
    public void testCteColumnAliasesDriveTheStarExpansion() {
        LogicalPlan userPlan = parse(
                "WITH c(x, y) AS (SELECT k AS a, v AS b FROM t)\n"
                        + "SELECT * FROM c");
        LogicalPlan frozenReplay = parse(
                "WITH c(x, y) AS (SELECT k AS x, v AS y FROM t)\n"
                        + "SELECT x, y FROM c");

        Assertions.assertEquals(List.of("x", "y"), mainProjectLabels(frozenReplay),
                "precondition: the frozen tree reports the caller-visible headers");
        LogicalPlan aligned = SPMPlanTreeSupport.alignRootOutputLabels(frozenReplay, userPlan);
        Assertions.assertEquals(List.of("x", "y"), mainProjectLabels(aligned),
                "the CTE's explicit column aliases must survive the star expansion: the"
                        + " frozen headers must not be renamed to the CTE body's labels");
    }

    // ==================== #7: a CTE is referenced by NAME, not by a name suffix ====================

    /**
     * The caller's `WITH c AS (SELECT k AS x FROM u) SELECT db.c.* FROM db.c CROSS JOIN c q`
     * exposes `k` (the base table's column); the frozen replay projects k. The qualified
     * star expansion used to resolve the LAST name part (c) against the CTE scope, so
     * `db.c.*` was derived through the SAME-NAMED CTE and alignment renamed the frozen k
     * to the CTE's inner alias x.
     */
    @Test
    public void testQualifiedStarOnACteNamedTable() {
        LogicalPlan userPlan = parse(
                "WITH c AS (SELECT k AS x FROM u)\n"
                        + "SELECT db.c.* FROM db.c CROSS JOIN c q");
        LogicalPlan frozenReplay = parse(
                "WITH c AS (SELECT k AS x FROM u)\n"
                        + "SELECT db.c.k FROM db.c CROSS JOIN c q");

        Assertions.assertEquals(List.of("db.c.k"), mainProjectLabels(frozenReplay),
                "precondition: the frozen tree reports the base table's column");
        LogicalPlan aligned = SPMPlanTreeSupport.alignRootOutputLabels(frozenReplay, userPlan);
        Assertions.assertEquals(List.of("db.c.k"), mainProjectLabels(aligned),
                "a QUALIFIED relation is not a CTE reference: the frozen base-table"
                        + " header must not be renamed to the CTE body's alias");
    }
}
