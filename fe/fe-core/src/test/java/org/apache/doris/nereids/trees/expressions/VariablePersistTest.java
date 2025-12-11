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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.analysis.SessionVarGuardRewriter;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteTestHelper;
import org.apache.doris.nereids.rules.expression.MergeGuardExpr;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.util.MemoTestUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class VariablePersistTest extends ExpressionRewriteTestHelper {
    private static final NereidsParser PARSER = new NereidsParser();

    @BeforeEach
    public void setUp() {
        cascadesContext = MemoTestUtils.createCascadesContext(
                new UnboundRelation(new RelationId(1), ImmutableList.of("tbl")));
        context = new ExpressionRewriteContext(cascadesContext);
    }

    // Test point 2: When persisted sessionVars in view differ from current sessionVar,
    // GuardExpr should be added for protection
    @Test
    public void testAddGuardWhenSessionVarsDifferent() {
        Map<String, String> persistSessionVars = ImmutableMap.of("enable_decimal256", "true");
        Map<String, String> currentSessionVars = ImmutableMap.of("enable_decimal256", "false");
        boolean matches = SessionVarGuardRewriter.checkSessionVariablesMatch(
                currentSessionVars, persistSessionVars);
        Assertions.assertFalse(matches, "Should return false when sessionVars differ");
        SessionVarGuardRewriter.AddSessionVarGuardRewriter rewriter =
                new SessionVarGuardRewriter.AddSessionVarGuardRewriter(persistSessionVars);
        Expression expr = PARSER.parseExpression("a * b");
        expr = ExpressionRewriteTestHelper.replaceUnboundSlot(expr, new HashMap<>());
        expr = ExpressionRewriteTestHelper.typeCoercion(expr);
        Expression rewritten = expr.accept(rewriter, Boolean.FALSE);
        Assertions.assertTrue(rewritten instanceof SessionVarGuardExpr,
                "Should add GuardExpr when sessionVars differ");
        SessionVarGuardExpr guardExpr = (SessionVarGuardExpr) rewritten;
        Assertions.assertEquals(persistSessionVars, guardExpr.getSessionVars());
    }

    // Test point 3: When persisted sessionVars in view match current sessionVar, GuardExpr should not be added
    @Test
    public void testNoGuardWhenSessionVarsSame() {
        Map<String, String> persistSessionVars = ImmutableMap.of("enable_decimal256", "true");
        Map<String, String> currentSessionVars = ImmutableMap.of("enable_decimal256", "true");
        boolean matches = SessionVarGuardRewriter.checkSessionVariablesMatch(
                currentSessionVars, persistSessionVars);
        Assertions.assertTrue(matches, "Should return true when sessionVars match");
        SessionVarGuardRewriter.AddSessionVarGuardRewriter rewriter =
                new SessionVarGuardRewriter.AddSessionVarGuardRewriter(null);
        Expression expr = PARSER.parseExpression("a * b");
        expr = ExpressionRewriteTestHelper.replaceUnboundSlot(expr, new HashMap<>());
        expr = ExpressionRewriteTestHelper.typeCoercion(expr);
        Expression rewritten = expr.accept(rewriter, Boolean.FALSE);
        Assertions.assertFalse(rewritten instanceof SessionVarGuardExpr,
                "Should not add GuardExpr when sessionVar is null");
        Assertions.assertEquals(expr, rewritten);
    }

    // Test when persisted sessionVars are empty or null
    @Test
    public void testCheckSessionVariablesMatchWithEmptyPersistVars() {
        Map<String, String> currentSessionVars = ImmutableMap.of("enable_decimal256", "true");
        boolean matches1 = SessionVarGuardRewriter.checkSessionVariablesMatch(
                currentSessionVars, null);
        Assertions.assertTrue(matches1, "Should return true when persistSessionVars is null");

        Map<String, String> emptyPersistVars = new HashMap<>();
        boolean matches2 = SessionVarGuardRewriter.checkSessionVariablesMatch(
                currentSessionVars, emptyPersistVars);
        Assertions.assertTrue(matches2, "Should return true when persistSessionVars is empty");
    }

    @Test
    public void testMultipleSessionVars() {
        Map<String, String> persistSessionVars = ImmutableMap.of(
                "enable_decimal256", "true",
                "decimal_overflow_scale", "10"
        );
        Map<String, String> partialMatchVars = ImmutableMap.of("enable_decimal256", "true");
        boolean matches1 = SessionVarGuardRewriter.checkSessionVariablesMatch(
                partialMatchVars, persistSessionVars);
        Assertions.assertFalse(matches1, "Should return false when only partial variables match");

        Map<String, String> fullMatchVars = ImmutableMap.of(
                "enable_decimal256", "true",
                "decimal_overflow_scale", "10"
        );
        boolean matches2 = SessionVarGuardRewriter.checkSessionVariablesMatch(
                fullMatchVars, persistSessionVars);
        Assertions.assertTrue(matches2, "Should return true when all variables match");

        Map<String, String> valueMismatchVars = ImmutableMap.of(
                "enable_decimal256", "true",
                "decimal_overflow_scale", "8"
        );
        boolean matches3 = SessionVarGuardRewriter.checkSessionVariablesMatch(
                valueMismatchVars, persistSessionVars);
        Assertions.assertFalse(matches3, "Should return false when variable values do not match");
    }

    // guard(guard(child)) -> guard(child)
    @Test
    public void testMergeGuardExprRemoveSame() {
        Map<String, String> sessionVars1 = ImmutableMap.of("enable_decimal256", "true");
        Map<String, String> sessionVars2 = ImmutableMap.of("enable_decimal256", "true");
        Expression child = PARSER.parseExpression("a + b");
        child = ExpressionRewriteTestHelper.replaceUnboundSlot(child, new HashMap<>());
        child = ExpressionRewriteTestHelper.typeCoercion(child);
        SessionVarGuardExpr innerGuard = new SessionVarGuardExpr(child, sessionVars2);
        SessionVarGuardExpr outerGuard = new SessionVarGuardExpr(innerGuard, sessionVars1);
        Expression rewritten = outerGuard.accept(MergeGuardExpr.INSTANCE, null);

        Assertions.assertTrue(rewritten instanceof SessionVarGuardExpr);
        SessionVarGuardExpr resultGuard = (SessionVarGuardExpr) rewritten;
        Assertions.assertEquals(child, resultGuard.child());
        Assertions.assertEquals(sessionVars1, resultGuard.getSessionVars());
    }

    // guard(guard(child)) -> child, slot need not be guarded
    @Test
    public void testMergeGuardExprRemoveUseless() {
        Map<String, String> sessionVars1 = ImmutableMap.of("enable_decimal256", "true");
        Map<String, String> sessionVars2 = ImmutableMap.of("enable_decimal256", "true");
        Expression child = new SlotReference("a", BooleanType.INSTANCE);
        child = ExpressionRewriteTestHelper.replaceUnboundSlot(child, new HashMap<>());
        child = ExpressionRewriteTestHelper.typeCoercion(child);
        SessionVarGuardExpr innerGuard = new SessionVarGuardExpr(child, sessionVars2);
        SessionVarGuardExpr outerGuard = new SessionVarGuardExpr(innerGuard, sessionVars1);
        Expression rewritten = outerGuard.accept(MergeGuardExpr.INSTANCE, null);

        Assertions.assertEquals(child, rewritten);
    }

    // guard(guard(and(guard(guard(child1), child2)))) -> and(child1,child2)
    @Test
    public void testMergeGuardExprComplicated() {
        Map<String, String> sessionVars1 = ImmutableMap.of("enable_decimal256", "true");
        Map<String, String> sessionVars2 = ImmutableMap.of("enable_decimal256", "true");
        SlotReference slotA = new SlotReference("a", BooleanType.INSTANCE);
        SessionVarGuardExpr innerGuard = new SessionVarGuardExpr(slotA, sessionVars2);
        SessionVarGuardExpr outerGuard = new SessionVarGuardExpr(innerGuard, sessionVars1);
        SlotReference slotB = new SlotReference("b", BooleanType.INSTANCE);
        And and = new And(outerGuard, slotB);
        SessionVarGuardExpr innerGuard2 = new SessionVarGuardExpr(and, sessionVars2);
        SessionVarGuardExpr outerGuard2 = new SessionVarGuardExpr(innerGuard2, sessionVars1);
        Expression rewritten = outerGuard2.accept(MergeGuardExpr.INSTANCE, null);

        Assertions.assertEquals(rewritten, new And(slotA, slotB));
    }


    // guard1(guard2(child)) -> exception
    @Test
    public void testMergeGuardExprException() {
        Map<String, String> sessionVars1 = ImmutableMap.of("enable_decimal256", "true");
        Map<String, String> sessionVars2 = ImmutableMap.of("enable_decimal256", "false");
        Expression child = PARSER.parseExpression("a");
        child = ExpressionRewriteTestHelper.replaceUnboundSlot(child, new HashMap<>());
        child = ExpressionRewriteTestHelper.typeCoercion(child);
        SessionVarGuardExpr innerGuard = new SessionVarGuardExpr(child, sessionVars2);
        SessionVarGuardExpr outerGuard = new SessionVarGuardExpr(innerGuard, sessionVars1);
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, () ->
                outerGuard.accept(MergeGuardExpr.INSTANCE, null));
        Assertions.assertTrue(exception.getMessage().contains("Conflicting session variable guards"));
    }

    // Test deep nesting (3 layers) - should not cause infinite loop
    @Test
    public void testMergeGuardExprDeepNesting() {
        Map<String, String> sessionVars = ImmutableMap.of("enable_decimal256", "true");
        Expression child = PARSER.parseExpression("a + b");
        child = ExpressionRewriteTestHelper.replaceUnboundSlot(child, new HashMap<>());
        child = ExpressionRewriteTestHelper.typeCoercion(child);
        SessionVarGuardExpr guard1 = new SessionVarGuardExpr(child, sessionVars);
        SessionVarGuardExpr guard2 = new SessionVarGuardExpr(guard1, sessionVars);
        SessionVarGuardExpr guard3 = new SessionVarGuardExpr(guard2, sessionVars);
        Expression rewritten = guard3.accept(MergeGuardExpr.INSTANCE, null);

        Assertions.assertTrue(rewritten instanceof SessionVarGuardExpr);
        SessionVarGuardExpr resultGuard = (SessionVarGuardExpr) rewritten;
        Assertions.assertEquals(child, resultGuard.child());
        Assertions.assertEquals(sessionVars, resultGuard.getSessionVars());
    }
}
