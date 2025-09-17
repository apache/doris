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

import org.apache.doris.nereids.trees.expressions.functions.scalar.SearchDslParser;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.StringType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for SearchExpression
 */
public class SearchExpressionTest {

    private SearchDslParser.QsPlan createTestPlan() {
        SearchDslParser.QsNode root = new SearchDslParser.QsNode(
                SearchDslParser.QsClauseType.TERM, "title", "hello");
        List<SearchDslParser.QsFieldBinding> bindings = Arrays.asList(
                new SearchDslParser.QsFieldBinding("title", 0));
        return new SearchDslParser.QsPlan(root, bindings);
    }

    private SlotReference createTestSlot(String name) {
        return new SlotReference(name, StringType.INSTANCE, true, Arrays.asList());
    }

    @Test
    public void testSearchExpressionCreation() {
        String dsl = "title:hello";
        SearchDslParser.QsPlan plan = createTestPlan();
        SlotReference titleSlot = createTestSlot("title");
        List<Expression> slotChildren = Arrays.asList(titleSlot);

        SearchExpression searchExpr = new SearchExpression(dsl, plan, slotChildren);

        assertNotNull(searchExpr);
        assertEquals(dsl, searchExpr.getDslString());
        assertEquals(plan, searchExpr.getQsPlan());
        assertEquals(slotChildren, searchExpr.getSlotChildren());
        assertEquals(1, searchExpr.children().size());
        assertEquals(titleSlot, searchExpr.children().get(0));
    }

    @Test
    public void testDataType() {
        String dsl = "title:hello";
        SearchDslParser.QsPlan plan = createTestPlan();
        SlotReference titleSlot = createTestSlot("title");
        List<Expression> slotChildren = Arrays.asList(titleSlot);

        SearchExpression searchExpr = new SearchExpression(dsl, plan, slotChildren);

        assertEquals(BooleanType.INSTANCE, searchExpr.getDataType());
    }

    @Test
    public void testFoldable() {
        String dsl = "title:hello";
        SearchDslParser.QsPlan plan = createTestPlan();
        SlotReference titleSlot = createTestSlot("title");
        List<Expression> slotChildren = Arrays.asList(titleSlot);

        SearchExpression searchExpr = new SearchExpression(dsl, plan, slotChildren);

        // SearchExpression should never be foldable to prevent constant evaluation
        assertFalse(searchExpr.foldable());
    }

    @Test
    public void testWithChildren() {
        String dsl = "title:hello";
        SearchDslParser.QsPlan plan = createTestPlan();
        SlotReference titleSlot = createTestSlot("title");
        SlotReference contentSlot = createTestSlot("content");
        List<Expression> originalChildren = Arrays.asList(titleSlot);
        List<Expression> newChildren = Arrays.asList(contentSlot);

        SearchExpression originalExpr = new SearchExpression(dsl, plan, originalChildren);
        SearchExpression newExpr = originalExpr.withChildren(newChildren);

        assertEquals(dsl, newExpr.getDslString());
        assertEquals(plan, newExpr.getQsPlan());
        assertEquals(newChildren, newExpr.getSlotChildren());
        assertEquals(1, newExpr.children().size());
        assertEquals(contentSlot, newExpr.children().get(0));
    }

    @Test
    public void testWithChildrenValidation() {
        String dsl = "title:hello";
        SearchDslParser.QsPlan plan = createTestPlan();
        SlotReference titleSlot = createTestSlot("title");
        List<Expression> slotChildren = Arrays.asList(titleSlot);

        SearchExpression searchExpr = new SearchExpression(dsl, plan, slotChildren);

        // Non-slot children should throw exception
        Expression nonSlotExpr = new org.apache.doris.nereids.trees.expressions.literal.StringLiteral("test");
        List<Expression> invalidChildren = Arrays.asList(nonSlotExpr);

        assertThrows(IllegalArgumentException.class, () -> {
            searchExpr.withChildren(invalidChildren);
        });
    }

    @Test
    public void testToString() {
        String dsl = "title:hello";
        SearchDslParser.QsPlan plan = createTestPlan();
        SlotReference titleSlot = createTestSlot("title");
        List<Expression> slotChildren = Arrays.asList(titleSlot);

        SearchExpression searchExpr = new SearchExpression(dsl, plan, slotChildren);

        String str = searchExpr.toString();
        assertEquals("search('title:hello')", str);
    }

    @Test
    public void testEquals() {
        String dsl1 = "title:hello";
        String dsl2 = "title:hello";
        String dsl3 = "content:world";

        SearchDslParser.QsPlan plan1 = createTestPlan();
        SearchDslParser.QsPlan plan2 = createTestPlan();

        SlotReference titleSlot = createTestSlot("title");
        List<Expression> slotChildren = Arrays.asList(titleSlot);

        SearchExpression expr1 = new SearchExpression(dsl1, plan1, slotChildren);
        SearchExpression expr2 = new SearchExpression(dsl2, plan2, slotChildren);
        SearchExpression expr3 = new SearchExpression(dsl3, plan1, slotChildren);

        assertEquals(expr1, expr2);
        assertEquals(expr1.hashCode(), expr2.hashCode());

        assertFalse(expr1.equals(expr3));
    }

    @Test
    public void testVisitorPattern() {
        String dsl = "title:hello";
        SearchDslParser.QsPlan plan = createTestPlan();
        SlotReference titleSlot = createTestSlot("title");
        List<Expression> slotChildren = Arrays.asList(titleSlot);

        SearchExpression searchExpr = new SearchExpression(dsl, plan, slotChildren);

        // Create a visitor that counts SearchExpression occurrences
        ExpressionVisitor<Integer, Void> visitor = new ExpressionVisitor<Integer, Void>() {
            @Override
            public Integer visit(org.apache.doris.nereids.trees.expressions.Expression expr, Void context) {
                return expr.accept(this, context);
            }
            
            @Override
            public Integer visitSearchExpression(SearchExpression searchExpression, Void context) {
                return 1;
            }
        };

        Integer result = searchExpr.accept(visitor, null);
        assertEquals(Integer.valueOf(1), result);
    }

    @Test
    public void testMultipleSlotChildren() {
        String dsl = "title:hello AND content:world";

        // Create complex plan with AND clause
        SearchDslParser.QsNode leftChild = new SearchDslParser.QsNode(
                SearchDslParser.QsClauseType.TERM, "title", "hello");
        SearchDslParser.QsNode rightChild = new SearchDslParser.QsNode(
                SearchDslParser.QsClauseType.TERM, "content", "world");
        SearchDslParser.QsNode root = new SearchDslParser.QsNode(
                SearchDslParser.QsClauseType.AND, Arrays.asList(leftChild, rightChild));

        List<SearchDslParser.QsFieldBinding> bindings = Arrays.asList(
                new SearchDslParser.QsFieldBinding("title", 0),
                new SearchDslParser.QsFieldBinding("content", 1));
        SearchDslParser.QsPlan plan = new SearchDslParser.QsPlan(root, bindings);

        SlotReference titleSlot = createTestSlot("title");
        SlotReference contentSlot = createTestSlot("content");
        List<Expression> slotChildren = Arrays.asList(titleSlot, contentSlot);

        SearchExpression searchExpr = new SearchExpression(dsl, plan, slotChildren);

        assertEquals(2, searchExpr.children().size());
        assertEquals(titleSlot, searchExpr.children().get(0));
        assertEquals(contentSlot, searchExpr.children().get(1));
        assertEquals(2, searchExpr.getQsPlan().fieldBindings.size());
    }

    @Test
    public void testNullValidation() {
        SlotReference titleSlot = createTestSlot("title");
        List<Expression> slotChildren = Arrays.asList(titleSlot);

        // Null DSL string should throw exception
        assertThrows(NullPointerException.class, () -> {
            new SearchExpression(null, createTestPlan(), slotChildren);
        });

        // Null QsPlan should throw exception
        assertThrows(NullPointerException.class, () -> {
            new SearchExpression("title:hello", null, slotChildren);
        });

        // Null slot children should throw exception
        assertThrows(NullPointerException.class, () -> {
            new SearchExpression("title:hello", createTestPlan(), null);
        });
    }

    @Test
    public void testEmptySlotChildren() {
        String dsl = "title:hello";
        SearchDslParser.QsPlan plan = createTestPlan();
        List<Expression> emptyChildren = Collections.emptyList();

        SearchExpression searchExpr = new SearchExpression(dsl, plan, emptyChildren);

        assertEquals(0, searchExpr.children().size());
        assertTrue(searchExpr.getSlotChildren().isEmpty());
    }
}
