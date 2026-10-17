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

import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.spm.builder.SPMExprSqlBuilder;
import org.apache.doris.nereids.spm.builder.SQLRelation;
import org.apache.doris.nereids.spm.matcher.SPMFrozenTreeReplacer;
import org.apache.doris.nereids.spm.placeholder.SpmConstList;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.WindowFrame;
import org.apache.doris.nereids.trees.expressions.WindowFrame.FrameBoundary;
import org.apache.doris.nereids.trees.expressions.WindowFrame.FrameUnitsType;
import org.apache.doris.nereids.trees.expressions.functions.window.Lag;
import org.apache.doris.nereids.trees.expressions.functions.window.Lead;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.commons.text.StringSubstitutor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Round-65 review fixes on the rendering side:
 *
 * - #1 WindowFunctionChecker attaches an INTERNAL ROWS frame to LAG/LEAD; the window
 *   renderer used to emit it as explicit SQL, and the frozen text is rejected by the
 *   checker itself (WindowFrame for LAG() must be null) when the frozen plan is planned.
 *   The frame must follow the user-level absence for these functions; the retained
 *   SpmConstList values must also be collectable so a frozen plan containing an IN-list
 *   placeholder is validated at CREATE time / substituted at replay time;
 * - #13 persisted SQL screens are rendered through a StringSubstitutor; a VALID alias may
 *   contain the literal `${id}` template, so the replacement values must not be
 *   re-expanded (they used to corrupt the bind / digest / plan SQL).
 */
public class SPMRound65BuilderTest {

    private static Plan findFilter(Plan plan) {
        if (plan instanceof LogicalFilter) {
            return plan;
        }
        for (Plan child : plan.children()) {
            Plan found = findFilter(child);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    // ==================== #1: LAG / LEAD serialize without the internal frame ====================

    /**
     * WindowFunctionChecker.visitLag/visitLead add a ROWS frame (unbounded preceding to
     * offset preceding) BEFORE planning, and the frozen planSql is re-parsed and planned
     * on replay: emitting the frame produced SQL that the checker itself rejects
     * ("WindowFrame for LAG() must be null"). The renderer must keep the user-level
     * absence of a frame for these two functions.
     */
    @Test
    public void testLagAndLeadOmitTheInternalFrame() {
        WindowFrame internalFrame = new WindowFrame(FrameUnitsType.ROWS,
                FrameBoundary.newPrecedingBoundary(),
                FrameBoundary.newPrecedingBoundary(new IntegerLiteral(1)));
        List<OrderExpression> orderKeys = List.of(
                new OrderExpression(new OrderKey(new IntegerLiteral(1), true, true)));

        WindowExpression lag = new WindowExpression(
                new Lag(new IntegerLiteral(1), new IntegerLiteral(1), new IntegerLiteral(0)),
                List.of(), orderKeys, internalFrame);
        String lagSql = new SPMExprSqlBuilder().visitWindow(lag, new SQLRelation());
        Assertions.assertTrue(lagSql.contains("OVER (ORDER BY"),
                "the LAG window must still be rendered: " + lagSql);
        Assertions.assertFalse(lagSql.contains("ROWS"),
                "the checker's internal LAG frame must not surface in the frozen SQL: " + lagSql);

        WindowExpression lead = new WindowExpression(
                new Lead(new IntegerLiteral(1), new IntegerLiteral(1), new IntegerLiteral(0)),
                List.of(), orderKeys, internalFrame);
        String leadSql = new SPMExprSqlBuilder().visitWindow(lead, new SQLRelation());
        Assertions.assertFalse(leadSql.contains("ROWS"),
                "the checker's internal LEAD frame must not surface in the frozen SQL: " + leadSql);
    }

    /**
     * The frozen text of an IN-list baseline carries `_spm_const_list(id)`; the CREATE-time
     * replan probe hands the placeholder's OWN retained values to the replacer, which must
     * adopt them as the option list so the frozen text is validated instead of accepted
     * unvalidated (the retained SpmConstList used to have no collector at all).
     */
    @Test
    public void testFrozenInListPlaceholderAdoptsItsRetainedValues() {
        LogicalPlan parsed = (LogicalPlan) new NereidsParser()
                .parseSingle("SELECT * FROM t WHERE k IN (_spm_const_list(7))");
        Plan filter = findFilter(parsed);
        Assertions.assertNotNull(filter, "the frozen text must produce a filter");
        Expression predicate = ((LogicalFilter<?>) filter).getPredicate();
        Assertions.assertTrue(predicate instanceof InPredicate,
                "precondition: the parsed predicate is the IN placeholder: " + predicate);

        Map<Long, Expression> values = new HashMap<>();
        values.put(7L, SpmConstList.of(7L, List.of(new IntegerLiteral(1), new IntegerLiteral(2))));
        Expression substituted = new SPMFrozenTreeReplacer()
                .visitInPredicate((InPredicate) predicate, values);
        Assertions.assertTrue(substituted instanceof InPredicate,
                "the IN predicate must be rebuilt: " + substituted);
        Assertions.assertEquals(2, ((InPredicate) substituted).getOptions().size(),
                "the retained values must become the option list: " + substituted);
    }

    // ==================== #13: literal template syntax in persisted SQL ====================

    /**
     * A valid alias may be `SELECT k AS `${id}` FROM t`; its SQL text is stored verbatim
     * in bind / digest / plan columns. The rendering substitutor (used for every
     * statement the manager executes) must NOT recursively expand `${...}` occurrences
     * INSIDE the replacement values - the INSERT map defines `id`, so the alias used to
     * be rewritten with the allocated number and the visibility confirmation compared the
     * original digest against the corrupted row.
     */
    @Test
    public void testSqlSubstitutorDoesNotExpandTemplateSyntaxInsideValues() throws Exception {
        Method factory = StatisticsUtil.class.getDeclaredMethod("sqlSubstitutor", Map.class);
        factory.setAccessible(true);
        Map<String, String> params = new HashMap<>();
        params.put("id", "7");
        params.put("bindSql", "SELECT k AS `${id}` FROM t");
        StringSubstitutor substitutor = (StringSubstitutor) factory.invoke(null, params);

        String rendered = substitutor.replace("VALUES (${id}, '${bindSql}')");
        Assertions.assertEquals("VALUES (7, 'SELECT k AS `${id}` FROM t')", rendered,
                "the replacement values must be inserted VERBATIM: literal template syntax"
                        + " inside persisted SQL must survive the rendering");
    }
}
