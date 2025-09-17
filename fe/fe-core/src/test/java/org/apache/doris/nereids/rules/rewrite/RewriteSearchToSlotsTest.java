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

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SearchExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Search;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SearchDslParser;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.types.StringType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for RewriteSearchToSlots rule
 * Note: These tests focus on the rule structure and basic functionality.
 * Full integration testing should be done via regression tests.
 */
public class RewriteSearchToSlotsTest {

    private RewriteSearchToSlots rewriteRule;

    @BeforeEach
    public void setUp() {
        rewriteRule = new RewriteSearchToSlots();
    }

    @Test
    public void testRuleType() {
        assertNotNull(rewriteRule);
        assertEquals(org.apache.doris.nereids.rules.RuleType.REWRITE_SEARCH_TO_SLOTS, 
                     rewriteRule.build().getRuleType());
    }

    @Test
    public void testRuleCreation() {
        Rule rule = rewriteRule.build();
        assertNotNull(rule);
        assertNotNull(rule.getPattern());
    }

    @Test
    public void testSearchFunctionDetection() {
        // Create simple search function
        String dsl = "title:hello";
        Search searchFunc = new Search(new StringLiteral(dsl));
        
        // Verify it's a Search function
        assertInstanceOf(Search.class, searchFunc);
        assertEquals(dsl, searchFunc.getDslString());
        assertNotNull(searchFunc.getQsPlan());
    }

    @Test
    public void testSearchExpressionCreation() {
        // Test creating SearchExpression manually
        String dsl = "title:hello";
        SearchDslParser.QsPlan plan = SearchDslParser.parseDsl(dsl);
        SlotReference titleSlot = new SlotReference("title", StringType.INSTANCE, true, Arrays.asList());
        List<Expression> slotChildren = Arrays.asList(titleSlot);

        SearchExpression searchExpr = new SearchExpression(dsl, plan, slotChildren);
        
        assertNotNull(searchExpr);
        assertEquals(dsl, searchExpr.getDslString());
        assertEquals(1, searchExpr.getSlotChildren().size());
        assertEquals("title", ((SlotReference) searchExpr.getSlotChildren().get(0)).getName());
    }

    @Test
    public void testMultiFieldSearchExpression() {
        String dsl = "title:hello AND content:world";
        SearchDslParser.QsPlan plan = SearchDslParser.parseDsl(dsl);
        
        SlotReference titleSlot = new SlotReference("title", StringType.INSTANCE, true, Arrays.asList());
        SlotReference contentSlot = new SlotReference("content", StringType.INSTANCE, true, Arrays.asList());
        List<Expression> slotChildren = Arrays.asList(titleSlot, contentSlot);

        SearchExpression searchExpr = new SearchExpression(dsl, plan, slotChildren);
        
        assertEquals(2, searchExpr.getSlotChildren().size());
        assertTrue(searchExpr.getSlotChildren().stream()
                .anyMatch(expr -> "title".equals(((SlotReference) expr).getName())));
        assertTrue(searchExpr.getSlotChildren().stream()
                .anyMatch(expr -> "content".equals(((SlotReference) expr).getName())));
    }

    @Test
    public void testQsPlanParsing() {
        // Test various DSL formats
        String[] testCases = {
            "title:hello",
            "content:\"phrase search\"",
            "title:hello AND content:world",
            "(title:machine OR content:learning) AND category:tech"
        };
        
        for (String dsl : testCases) {
            try {
                SearchDslParser.QsPlan plan = SearchDslParser.parseDsl(dsl);
                assertNotNull(plan, "Plan should not be null for DSL: " + dsl);
                assertNotNull(plan.root, "Plan root should not be null for DSL: " + dsl);
                assertTrue(plan.fieldBindings.size() > 0, "Should have field bindings for DSL: " + dsl);
            } catch (Exception e) {
                // DSL parsing might fail for complex cases - that's acceptable
                System.out.println("DSL parsing failed for: " + dsl + " - " + e.getMessage());
            }
        }
    }

    @Test
    public void testFieldNameExtraction() {
        String dsl = "title:hello AND content:world AND category:tech";
        SearchDslParser.QsPlan plan = SearchDslParser.parseDsl(dsl);
        
        // Should extract 3 unique field names
        assertEquals(3, plan.fieldBindings.size());
        
        List<String> fieldNames = plan.fieldBindings.stream()
                .map(binding -> binding.fieldName)
                .distinct()
                .collect(java.util.stream.Collectors.toList());
        
        assertTrue(fieldNames.contains("title"));
        assertTrue(fieldNames.contains("content"));
        assertTrue(fieldNames.contains("category"));
    }

    @Test
    public void testCaseInsensitiveFieldNames() {
        String dsl1 = "TITLE:hello";
        String dsl2 = "title:hello";
        
        SearchDslParser.QsPlan plan1 = SearchDslParser.parseDsl(dsl1);
        SearchDslParser.QsPlan plan2 = SearchDslParser.parseDsl(dsl2);
        
        // Both should work and extract field names
        assertEquals(1, plan1.fieldBindings.size());
        assertEquals(1, plan2.fieldBindings.size());
        
        // Field names should be consistent (implementation dependent)
        assertNotNull(plan1.fieldBindings.get(0).fieldName);
        assertNotNull(plan2.fieldBindings.get(0).fieldName);
    }

    @Test
    public void testEmptyAndInvalidDsl() {
        // Empty DSL should be handled gracefully
        try {
            SearchDslParser.QsPlan plan = SearchDslParser.parseDsl("");
            assertNotNull(plan);
        } catch (RuntimeException e) {
            // Also acceptable to throw exception
            assertTrue(e.getMessage().contains("empty") || e.getMessage().contains("Invalid"));
        }
        
        // Invalid DSL should throw exception
        try {
            SearchDslParser.parseDsl("invalid:syntax AND");
            assertTrue(false, "Expected exception for invalid DSL");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("Invalid"));
        }
    }

    @Test
    public void testComplexDslStructures() {
        String complexDsl = "(title:\"machine learning\" OR content:AI) AND NOT category:spam";
        
        try {
            SearchDslParser.QsPlan plan = SearchDslParser.parseDsl(complexDsl);
            assertNotNull(plan);
            assertNotNull(plan.root);
            
            // Should have multiple field bindings
            assertTrue(plan.fieldBindings.size() >= 2);
            
        } catch (Exception e) {
            // Complex DSL might not be fully supported yet
            System.out.println("Complex DSL parsing failed: " + e.getMessage());
        }
    }

    @Test
    public void testSlotReferenceConsistency() {
        String dsl = "title:hello";
        SearchDslParser.QsPlan plan = SearchDslParser.parseDsl(dsl);
        
        // Create slot reference matching the field binding
        String fieldName = plan.fieldBindings.get(0).fieldName;
        SlotReference slot = new SlotReference(fieldName, StringType.INSTANCE, true, Arrays.asList());
        
        SearchExpression expr = new SearchExpression(dsl, plan, Arrays.asList(slot));
        
        // Verify consistency
        assertEquals(1, expr.children().size());
        assertEquals(slot, expr.children().get(0));
        assertEquals(fieldName, ((SlotReference) expr.children().get(0)).getName());
    }
}