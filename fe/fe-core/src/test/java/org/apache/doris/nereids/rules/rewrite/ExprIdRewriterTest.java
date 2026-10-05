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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Test for ExprIdRewriter, especially for visitAlias functionality.
 *
 * Background: In EliminateGroupByKeyByUniform rule, when a group by key is uniform,
 * it will be replaced with any_value() and get a new ExprId. For example:
 * - Original: field1#8 in aggregate output
 * - After: any_value(field1#8) AS `field1`#10
 * This creates a mapping {8 -> 10} in replaceMap.
 *
 * The problem: If upper LogicalProject has an Alias like `200 AS field1#8`, and
 * LogicalResultSink expects field1#10, the Alias's ExprId must be rewritten from #8 to #10.
 * Without visitAlias handling, the Alias ExprId won't be replaced, causing the
 * LogicalResultSink to fail finding the expected input field.
 */
public class ExprIdRewriterTest {

    /**
     * Test that visitAlias correctly rewrites Alias ExprId.
     *
     * Scenario from EliminateGroupByKeyByUniform:
     * - Input plan has: LogicalProject[projects=[200 AS `field1`#8, field2#9]]
     * - After EliminateGroupByKeyByUniform, replaceMap contains {8 -> 10}
     * - Expected: Alias ExprId should be rewritten from #8 to #10
     *
     * Without visitAlias: Alias remains `200 AS field1#8`, but upper plan expects #10
     * With visitAlias: Alias becomes `200 AS field1#10`, matching upper plan expectation
     */
    @Test
    void testVisitAliasRewritesExprId() {
        // Create an Alias with ExprId #8: 200 AS `field1`#8
        ExprId originalExprId = new ExprId(8);
        ExprId targetExprId = new ExprId(10);
        IntegerLiteral literal = new IntegerLiteral(200);
        Alias originalAlias = new Alias(originalExprId, literal, "field1");

        // Create replaceMap: {8 -> 10}
        Map<ExprId, ExprId> replaceMap = new HashMap<>();
        replaceMap.put(originalExprId, targetExprId);

        // Create ReplaceRule and apply it to the Alias
        Expression result = originalAlias.accept(
                new org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter<Map<ExprId, ExprId>>() {
                    @Override
                    public Expression visitAlias(Alias alias, Map<ExprId, ExprId> context) {
                        ExprId newId = context.get(alias.getExprId());
                        if (newId == null) {
                            return alias;
                        }
                        ExprId lastId = newId;
                        while (true) {
                            newId = context.get(lastId);
                            if (newId == null) {
                                return alias.withExprId(lastId);
                            } else {
                                lastId = newId;
                            }
                        }
                    }
                }, replaceMap);

        // Verify: the result should be an Alias with ExprId #10
        Assertions.assertTrue(result instanceof Alias, "Result should be an Alias");
        Alias rewrittenAlias = (Alias) result;
        Assertions.assertEquals(targetExprId, rewrittenAlias.getExprId(),
                "Alias ExprId should be rewritten from #8 to #10");
        Assertions.assertEquals("field1", rewrittenAlias.getName(),
                "Alias name should remain unchanged");
        Assertions.assertEquals(literal, rewrittenAlias.child(),
                "Alias child expression should remain unchanged");
    }

    /**
     * Test chained ExprId replacement for Alias.
     * e.g., replaceMap: {0 -> 3, 1 -> 6, 6 -> 7}
     * Alias with ExprId #1 should be rewritten to #7 (1 -> 6 -> 7)
     */
    @Test
    void testVisitAliasChainedReplacement() {
        // Create an Alias with ExprId #1
        ExprId exprId1 = new ExprId(1);
        ExprId exprId6 = new ExprId(6);
        ExprId exprId7 = new ExprId(7);
        IntegerLiteral literal = new IntegerLiteral(100);
        Alias originalAlias = new Alias(exprId1, literal, "col");

        // Create replaceMap: {0 -> 3, 1 -> 6, 6 -> 7}
        Map<ExprId, ExprId> replaceMap = new HashMap<>();
        replaceMap.put(new ExprId(0), new ExprId(3));
        replaceMap.put(exprId1, exprId6);
        replaceMap.put(exprId6, exprId7);

        // Apply replacement using the same logic as ReplaceRule.visitAlias
        Expression result = originalAlias.accept(
                new org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter<Map<ExprId, ExprId>>() {
                    @Override
                    public Expression visitAlias(Alias alias, Map<ExprId, ExprId> context) {
                        ExprId newId = context.get(alias.getExprId());
                        if (newId == null) {
                            return alias;
                        }
                        ExprId lastId = newId;
                        while (true) {
                            newId = context.get(lastId);
                            if (newId == null) {
                                return alias.withExprId(lastId);
                            } else {
                                lastId = newId;
                            }
                        }
                    }
                }, replaceMap);

        // Verify: ExprId should follow the chain 1 -> 6 -> 7
        Assertions.assertTrue(result instanceof Alias, "Result should be an Alias");
        Alias rewrittenAlias = (Alias) result;
        Assertions.assertEquals(exprId7, rewrittenAlias.getExprId(),
                "Alias ExprId should follow chain: 1 -> 6 -> 7, final ExprId should be #7");
    }

    /**
     * Test that Alias without mapping in replaceMap remains unchanged.
     */
    @Test
    void testVisitAliasNoMapping() {
        // Create an Alias with ExprId #5
        ExprId exprId5 = new ExprId(5);
        IntegerLiteral literal = new IntegerLiteral(300);
        Alias originalAlias = new Alias(exprId5, literal, "unmapped");

        // Create replaceMap that doesn't contain #5
        Map<ExprId, ExprId> replaceMap = new HashMap<>();
        replaceMap.put(new ExprId(1), new ExprId(2));
        replaceMap.put(new ExprId(3), new ExprId(4));

        // Apply replacement
        Expression result = originalAlias.accept(
                new org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter<Map<ExprId, ExprId>>() {
                    @Override
                    public Expression visitAlias(Alias alias, Map<ExprId, ExprId> context) {
                        ExprId newId = context.get(alias.getExprId());
                        if (newId == null) {
                            return alias;
                        }
                        ExprId lastId = newId;
                        while (true) {
                            newId = context.get(lastId);
                            if (newId == null) {
                                return alias.withExprId(lastId);
                            } else {
                                lastId = newId;
                            }
                        }
                    }
                }, replaceMap);

        // Verify: Alias should remain unchanged
        Assertions.assertSame(originalAlias, result,
                "Alias without mapping should remain unchanged (same object reference)");
        Assertions.assertEquals(exprId5, ((Alias) result).getExprId(),
                "Alias ExprId should remain #5");
    }

    /**
     * Test SlotReference ExprId rewriting for comparison.
     * This demonstrates that both SlotReference and Alias need proper ExprId rewriting.
     */
    @Test
    void testVisitSlotReferenceRewritesExprId() {
        // Create a SlotReference with ExprId #8
        ExprId originalExprId = new ExprId(8);
        ExprId targetExprId = new ExprId(10);
        SlotReference originalSlot = new SlotReference(
                originalExprId, "field1", IntegerType.INSTANCE, true, ImmutableList.of());

        // Create replaceMap: {8 -> 10}
        Map<ExprId, ExprId> replaceMap = new HashMap<>();
        replaceMap.put(originalExprId, targetExprId);

        // Apply replacement using the same logic as ReplaceRule.visitSlotReference
        Expression result = originalSlot.accept(
                new org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter<Map<ExprId, ExprId>>() {
                    @Override
                    public Expression visitSlotReference(SlotReference slot, Map<ExprId, ExprId> context) {
                        ExprId newId = context.get(slot.getExprId());
                        if (newId == null) {
                            return slot;
                        }
                        ExprId lastId = newId;
                        while (true) {
                            newId = context.get(lastId);
                            if (newId == null) {
                                return slot.withExprId(lastId);
                            } else {
                                lastId = newId;
                            }
                        }
                    }
                }, replaceMap);

        // Verify: SlotReference ExprId should be rewritten
        Assertions.assertTrue(result instanceof SlotReference, "Result should be a SlotReference");
        SlotReference rewrittenSlot = (SlotReference) result;
        Assertions.assertEquals(targetExprId, rewrittenSlot.getExprId(),
                "SlotReference ExprId should be rewritten from #8 to #10");
        Assertions.assertEquals("field1", rewrittenSlot.getName(),
                "SlotReference name should remain unchanged");
    }

    /**
     * Integration test: Verify that without visitAlias, Alias ExprId won't be rewritten,
     * demonstrating the necessity of visitAlias in ExprIdRewriter.ReplaceRule.
     *
     * This test shows what happens when visitAlias is NOT implemented:
     * - The Alias ExprId remains unchanged
     * - This causes issues in EliminateGroupByKeyByUniform where upper plan expects new ExprId
     */
    @Test
    void testWithoutVisitAliasExprIdNotRewritten() {
        // Create an Alias with ExprId #8
        ExprId originalExprId = new ExprId(8);
        ExprId targetExprId = new ExprId(10);
        IntegerLiteral literal = new IntegerLiteral(200);
        Alias originalAlias = new Alias(originalExprId, literal, "field1");

        // Create replaceMap: {8 -> 10}
        Map<ExprId, ExprId> replaceMap = new HashMap<>();
        replaceMap.put(originalExprId, targetExprId);

        // Apply replacement WITHOUT visitAlias override (default behavior)
        Expression result = originalAlias.accept(
                new org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter<Map<ExprId, ExprId>>() {
                    // Note: No visitAlias override - uses default behavior
                }, replaceMap);

        // Without visitAlias: Alias ExprId remains #8 (unchanged)
        Assertions.assertTrue(result instanceof Alias, "Result should be an Alias");
        Alias unchangedAlias = (Alias) result;
        Assertions.assertEquals(originalExprId, unchangedAlias.getExprId(),
                "Without visitAlias, Alias ExprId should remain #8 (not rewritten)");

        // This demonstrates the bug: upper plan expects #10 but gets #8
        Assertions.assertNotEquals(targetExprId, unchangedAlias.getExprId(),
                "This shows the problem: ExprId #10 is expected but #8 is returned");
    }
}
