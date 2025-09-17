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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SearchDslParser;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TSearchParam;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for SearchPredicate
 */
public class SearchPredicateTest {

    private SearchDslParser.QsPlan createTestPlan() {
        SearchDslParser.QsNode root = new SearchDslParser.QsNode(
                SearchDslParser.QsClauseType.TERM, "title", "hello");
        List<SearchDslParser.QsFieldBinding> bindings = Arrays.asList(
                new SearchDslParser.QsFieldBinding("title", 0));
        return new SearchDslParser.QsPlan(root, bindings);
    }

    private SlotRef createTestSlotRef(String columnName) {
        SlotRef slotRef = new SlotRef(null, columnName);
        // Mock basic properties for testing
        try {
            java.lang.reflect.Field typeField = SlotRef.class.getDeclaredField("type");
            typeField.setAccessible(true);
            typeField.set(slotRef, Type.STRING);
            
            // Set a dummy SlotId to prevent getSlotId() from throwing
            java.lang.reflect.Field slotIdField = SlotRef.class.getDeclaredField("slotId");
            slotIdField.setAccessible(true);
            slotIdField.set(slotRef, new org.apache.doris.analysis.SlotId(0));
        } catch (Exception e) {
            // Ignore reflection errors in test - tests may fail but won't crash
            System.out.println("Warning: Could not set SlotRef properties: " + e.getMessage());
        }
        return slotRef;
    }

    @Test
    public void testSearchPredicateCreation() {
        String dsl = "title:hello";
        SearchDslParser.QsPlan plan = createTestPlan();
        List<Expr> children = Arrays.asList(createTestSlotRef("title"));

        SearchPredicate predicate = new SearchPredicate(dsl, plan, children);

        assertNotNull(predicate);
        assertEquals(Type.BOOLEAN, predicate.getType());
        assertEquals(1, predicate.getChildren().size());
    }

    @Test
    public void testToSqlImpl() {
        String dsl = "title:hello";
        SearchDslParser.QsPlan plan = createTestPlan();
        List<Expr> children = Arrays.asList(createTestSlotRef("title"));

        SearchPredicate predicate = new SearchPredicate(dsl, plan, children);

        String sql = predicate.toSqlImpl(false, false, null, null);
        assertEquals("search('title:hello')", sql);
    }

    @Test
    public void testToThrift() {
        String dsl = "title:hello";
        SearchDslParser.QsPlan plan = createTestPlan();
        SlotRef titleSlot = createTestSlotRef("title");
        List<Expr> children = Arrays.asList(titleSlot);

        SearchPredicate predicate = new SearchPredicate(dsl, plan, children);

        TExprNode thriftNode = new TExprNode();
        predicate.toThrift(thriftNode);

        assertEquals(TExprNodeType.SEARCH_EXPR, thriftNode.node_type);
        assertNotNull(thriftNode.search_param);
        assertEquals(dsl, thriftNode.search_param.original_dsl);
        assertEquals(1, thriftNode.search_param.field_bindings.size());
        assertEquals("title", thriftNode.search_param.field_bindings.get(0).field_name);
    }

    @Test
    public void testBuildThriftParam() {
        String dsl = "title:hello AND content:world";

        // Create complex plan
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

        SlotRef titleSlot = createTestSlotRef("title");
        SlotRef contentSlot = createTestSlotRef("content");
        List<Expr> children = Arrays.asList(titleSlot, contentSlot);

        SearchPredicate predicate = new SearchPredicate(dsl, plan, children);

        TExprNode thriftNode = new TExprNode();
        predicate.toThrift(thriftNode);

        TSearchParam param = thriftNode.search_param;
        assertEquals(dsl, param.original_dsl);
        assertEquals("AND", param.root.clause_type);
        assertEquals(2, param.root.children.size());
        assertEquals(2, param.field_bindings.size());

        // Verify field bindings
        assertEquals("title", param.field_bindings.get(0).field_name);
        assertEquals("content", param.field_bindings.get(1).field_name);
    }

    @Test
    public void testClone() {
        String dsl = "title:hello";
        SearchDslParser.QsPlan plan = createTestPlan();
        List<Expr> children = Arrays.asList(createTestSlotRef("title"));

        SearchPredicate original = new SearchPredicate(dsl, plan, children);
        SearchPredicate cloned = (SearchPredicate) original.clone();

        assertNotNull(cloned);
        assertEquals(original.toSqlImpl(false, false, null, null),
                cloned.toSqlImpl(false, false, null, null));
        assertEquals(original.getChildren().size(), cloned.getChildren().size());
    }

    @Test
    public void testEquals() {
        String dsl1 = "title:hello";
        String dsl2 = "title:hello";
        String dsl3 = "content:world";

        SearchDslParser.QsPlan plan = createTestPlan();
        List<Expr> children = Arrays.asList(createTestSlotRef("title"));

        SearchPredicate pred1 = new SearchPredicate(dsl1, plan, children);
        SearchPredicate pred2 = new SearchPredicate(dsl2, plan, children);
        SearchPredicate pred3 = new SearchPredicate(dsl3, plan, children);

        assertTrue(pred1.equals(pred2));
        assertTrue(!pred1.equals(pred3));
    }

    @Test
    public void testComplexDslConversion() {
        String dsl = "(title:\"machine learning\" OR content:AI) AND NOT category:spam";

        // Create complex AST structure
        SearchDslParser.QsNode titlePhrase = new SearchDslParser.QsNode(
                SearchDslParser.QsClauseType.PHRASE, "title", "machine learning");
        SearchDslParser.QsNode contentTerm = new SearchDslParser.QsNode(
                SearchDslParser.QsClauseType.TERM, "content", "AI");
        SearchDslParser.QsNode orClause = new SearchDslParser.QsNode(
                SearchDslParser.QsClauseType.OR, Arrays.asList(titlePhrase, contentTerm));

        SearchDslParser.QsNode categoryTerm = new SearchDslParser.QsNode(
                SearchDslParser.QsClauseType.TERM, "category", "spam");
        SearchDslParser.QsNode notClause = new SearchDslParser.QsNode(
                SearchDslParser.QsClauseType.NOT, Arrays.asList(categoryTerm));

        SearchDslParser.QsNode root = new SearchDslParser.QsNode(
                SearchDslParser.QsClauseType.AND, Arrays.asList(orClause, notClause));

        List<SearchDslParser.QsFieldBinding> bindings = Arrays.asList(
                new SearchDslParser.QsFieldBinding("title", 0),
                new SearchDslParser.QsFieldBinding("content", 1),
                new SearchDslParser.QsFieldBinding("category", 2));
        SearchDslParser.QsPlan plan = new SearchDslParser.QsPlan(root, bindings);

        List<Expr> children = Arrays.asList(
                createTestSlotRef("title"),
                createTestSlotRef("content"),
                createTestSlotRef("category"));

        SearchPredicate predicate = new SearchPredicate(dsl, plan, children);

        TExprNode thriftNode = new TExprNode();
        predicate.toThrift(thriftNode);

        TSearchParam param = thriftNode.search_param;
        assertEquals(dsl, param.original_dsl);
        assertEquals("AND", param.root.clause_type);
        assertEquals(2, param.root.children.size());

        // Verify OR clause
        assertEquals("OR", param.root.children.get(0).clause_type);
        assertEquals(2, param.root.children.get(0).children.size());
        assertEquals("PHRASE", param.root.children.get(0).children.get(0).clause_type);
        assertEquals("TERM", param.root.children.get(0).children.get(1).clause_type);

        // Verify NOT clause
        assertEquals("NOT", param.root.children.get(1).clause_type);
        assertEquals(1, param.root.children.get(1).children.size());
        assertEquals("TERM", param.root.children.get(1).children.get(0).clause_type);

        assertEquals(3, param.field_bindings.size());
    }

    @Test
    public void testEmptyChildren() {
        String dsl = "title:hello";
        SearchDslParser.QsPlan plan = createTestPlan();
        List<Expr> emptyChildren = Collections.emptyList();

        SearchPredicate predicate = new SearchPredicate(dsl, plan, emptyChildren);

        assertEquals(0, predicate.getChildren().size());

        TExprNode thriftNode = new TExprNode();
        predicate.toThrift(thriftNode);

        assertNotNull(thriftNode.search_param);
        assertEquals(dsl, thriftNode.search_param.original_dsl);
    }
}
