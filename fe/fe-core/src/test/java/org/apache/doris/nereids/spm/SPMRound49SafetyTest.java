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

import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.OneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * The round-49 contracts:
 *
 * - the create-time predicate / join-condition comparisons must keep the COMPONENT
 *   boundary of every slot: a column literally named `a.b` (ONE component) and the
 *   qualified reference a.b (two components) render identically through toSql while
 *   filtering different columns, so a manual plan reading alias a's column b passed
 *   the WHERE / JOIN ON checks as if it carried the bind's filter on the dotted
 *   column;
 * - a standalone one-row replay (SELECT 1 matching a later SELECT 2) has no
 *   output-carrying unary node: the alignment must rebuild the one-row relation's own
 *   item list with the caller's label (or skip the rewrite).
 */
public class SPMRound49SafetyTest {

    private static LogicalPlan parse(String sql) {
        return (LogicalPlan) new NereidsParser().parseSingle(sql);
    }

    private static RuntimeException buildFails(String bindSql, String planSql) {
        return Assertions.assertThrows(RuntimeException.class,
                () -> new SPMPlanner().buildBaseline(bindSql, planSql));
    }

    // ==================== #1: predicate / join-condition slot boundaries ====================

    /**
     * The bind filters the column literally named `a.b`; the plan filters alias a's
     * column b. Both render "a.b > a.k", so the WHERE containment used to accept the
     * pair - a matching caller then received rows its own filter excluded.
     */
    @Test
    public void testDottedColumnFilterIsNotTheQualifiedReference() {
        RuntimeException failure = buildFails(
                "SELECT a.k FROM t a WHERE `a.b` > a.k",
                "SELECT a.k FROM t a WHERE a.b > a.k");
        Assertions.assertNotNull(failure.getMessage());
        Assertions.assertTrue(failure.getMessage().contains("drops row filter"),
                failure.getMessage());
    }

    /** The same boundary loss on a JOIN ON condition must reject the pair as well. */
    @Test
    public void testDottedColumnJoinConditionIsNotTheQualifiedReference() {
        RuntimeException failure = buildFails(
                "SELECT a.k FROM t a JOIN u b ON `a.b` = b.k",
                "SELECT a.k FROM t a JOIN u b ON a.b = b.k");
        Assertions.assertNotNull(failure.getMessage());
        Assertions.assertTrue(failure.getMessage().contains("ON condition"),
                failure.getMessage());
    }

    /** Identical spellings stay accepted: the boundary check must not over-reject. */
    @Test
    public void testIdenticalPredicateSpellingsStayAccepted() {
        Assertions.assertDoesNotThrow(() -> new SPMPlanner().buildBaseline(
                "SELECT a.k FROM t a WHERE `a.b` > 1",
                "SELECT a.k FROM t a WHERE `a.b` > 1"),
                "the same dotted column on both sides is one contract");
        Assertions.assertDoesNotThrow(() -> new SPMPlanner().buildBaseline(
                "SELECT a.k FROM t a WHERE a.b > 1",
                "SELECT a.k FROM t a WHERE a.b > 1"),
                "the same qualified reference on both sides is one contract");
    }

    // ==================== #4: standalone one-row replay labels ====================

    /**
     * CREATE SESSION BASELINE PLAN 'SELECT 1' can match a later SELECT 2: the value is
     * substituted, and the alignment must rebuild the one-row header with the CALLER's
     * own label instead of keeping the captured one.
     */
    @Test
    public void testOneRowReplayAdoptsTheCallerLabel() {
        LogicalPlan rewritten = parse("SELECT 1");
        LogicalPlan user = parse("SELECT 2");
        LogicalPlan aligned = SPMPlanTreeSupport.alignRootOutputLabels(rewritten, user);
        Assertions.assertNotSame(rewritten, aligned,
                "the captured one-row label must be replaced by the caller's");
        Assertions.assertEquals("2", oneRowLabel(aligned),
                "the caller's own label is the visible header: " + aligned);
        Assertions.assertEquals("1", oneRowExpression(aligned),
                "the substituted expression itself stays untouched: " + aligned);
    }

    /** An identical label stays untouched (no needless rebuild). */
    @Test
    public void testOneRowReplayWithTheSameLabelStaysUntouched() {
        LogicalPlan rewritten = parse("SELECT 1 AS c");
        Assertions.assertSame(rewritten,
                SPMPlanTreeSupport.alignRootOutputLabels(rewritten, parse("SELECT 9 AS c")),
                "the label already equals the caller's");
    }

    /** An arity mismatch cannot be aligned: the rewrite is skipped, never half-renamed. */
    @Test
    public void testOneRowReplayWithDifferentArityIsSkipped() {
        LogicalPlan rewritten = parse("SELECT 1");
        LogicalPlan user = parse("SELECT 2, 3");
        Assertions.assertThrows(
                SPMPlanTreeSupport.UnalignableOutputLabelsException.class,
                () -> SPMPlanTreeSupport.alignRootOutputLabels(rewritten, user));
    }

    private static NamedExpression oneRowItem(Plan plan) {
        Plan node = plan;
        while (!(node instanceof OneRowRelation)) {
            node = node.child(0);
        }
        return ((OneRowRelation) node).getProjects().get(0);
    }

    private static String oneRowLabel(Plan plan) {
        NamedExpression item = oneRowItem(plan);
        if (item instanceof UnboundAlias) {
            return ((UnboundAlias) item).getAlias().orElse(null);
        }
        if (item instanceof Alias) {
            return ((Alias) item).getName();
        }
        return null;
    }

    private static String oneRowExpression(Plan plan) {
        NamedExpression item = oneRowItem(plan);
        if (item instanceof UnboundAlias) {
            return ((UnboundAlias) item).child().toSql();
        }
        if (item instanceof Alias) {
            return ((Alias) item).child().toSql();
        }
        return item.toSql();
    }
}
