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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.nereids.trees.expressions.functions.scalar.SearchDslParser.QsClauseType;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SearchDslParser.QsFieldBinding;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SearchDslParser.QsNode;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SearchDslParser.QsPlan;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for SearchDslParser
 */
public class SearchDslParserTest {

    @Test
    public void testSimpleTermQuery() {
        String dsl = "title:hello";
        QsPlan plan = SearchDslParser.parseDsl(dsl);
        
        assertNotNull(plan);
        assertNotNull(plan.root);
        assertEquals(QsClauseType.TERM, plan.root.type);
        assertEquals("title", plan.root.field);
        assertEquals("hello", plan.root.value);
        
        assertEquals(1, plan.fieldBindings.size());
        QsFieldBinding binding = plan.fieldBindings.get(0);
        assertEquals("title", binding.fieldName);
    }

    @Test
    public void testPhraseQuery() {
        String dsl = "content:\"hello world\"";
        QsPlan plan = SearchDslParser.parseDsl(dsl);
        
        assertNotNull(plan);
        assertEquals(QsClauseType.PHRASE, plan.root.type);
        assertEquals("content", plan.root.field);
        assertEquals("hello world", plan.root.value);
    }

    @Test
    public void testPrefixQuery() {
        String dsl = "title:hello*";
        QsPlan plan = SearchDslParser.parseDsl(dsl);
        
        assertNotNull(plan);
        assertEquals(QsClauseType.PREFIX, plan.root.type);
        assertEquals("title", plan.root.field);
        assertEquals("hello*", plan.root.value);
    }

    @Test
    public void testWildcardQuery() {
        String dsl = "title:h*llo";
        QsPlan plan = SearchDslParser.parseDsl(dsl);
        
        assertNotNull(plan);
        assertEquals(QsClauseType.WILDCARD, plan.root.type);
        assertEquals("title", plan.root.field);
        assertEquals("h*llo", plan.root.value);
    }

    @Test
    public void testRegexpQuery() {
        String dsl = "title:/[a-z]+/";
        QsPlan plan = SearchDslParser.parseDsl(dsl);
        
        assertNotNull(plan);
        assertEquals(QsClauseType.REGEXP, plan.root.type);
        assertEquals("title", plan.root.field);
        assertEquals("[a-z]+", plan.root.value); // slashes removed
    }

    @Test
    public void testRangeQuery() {
        String dsl = "age:[18 TO 65]";
        QsPlan plan = SearchDslParser.parseDsl(dsl);
        
        assertNotNull(plan);
        assertEquals(QsClauseType.RANGE, plan.root.type);
        assertEquals("age", plan.root.field);
        assertEquals("[18 TO 65]", plan.root.value);
    }

    @Test
    public void testListQuery() {
        String dsl = "category:IN(tech news)";
        QsPlan plan = SearchDslParser.parseDsl(dsl);
        
        assertNotNull(plan);
        assertEquals(QsClauseType.LIST, plan.root.type);
        assertEquals("category", plan.root.field);
        assertEquals("IN(tech news)", plan.root.value);
    }

    @Test
    public void testAnyQuery() {
        String dsl = "tags:ANY(java python)";
        QsPlan plan = SearchDslParser.parseDsl(dsl);
        
        assertNotNull(plan);
        assertEquals(QsClauseType.ANY, plan.root.type);
        assertEquals("tags", plan.root.field);
        assertEquals("java python", plan.root.value);
    }

    @Test
    public void testAllQuery() {
        String dsl = "tags:ALL(programming language)";
        QsPlan plan = SearchDslParser.parseDsl(dsl);
        
        assertNotNull(plan);
        assertEquals(QsClauseType.ALL, plan.root.type);
        assertEquals("tags", plan.root.field);
        assertEquals("programming language", plan.root.value);
    }

    @Test
    public void testAndQuery() {
        String dsl = "title:hello AND content:world";
        QsPlan plan = SearchDslParser.parseDsl(dsl);
        
        assertNotNull(plan);
        assertEquals(QsClauseType.AND, plan.root.type);
        assertEquals(2, plan.root.children.size());
        
        QsNode leftChild = plan.root.children.get(0);
        assertEquals(QsClauseType.TERM, leftChild.type);
        assertEquals("title", leftChild.field);
        assertEquals("hello", leftChild.value);
        
        QsNode rightChild = plan.root.children.get(1);
        assertEquals(QsClauseType.TERM, rightChild.type);
        assertEquals("content", rightChild.field);
        assertEquals("world", rightChild.value);
        
        // Should have 2 field bindings
        assertEquals(2, plan.fieldBindings.size());
        assertTrue(plan.fieldBindings.stream().anyMatch(b -> "title".equals(b.fieldName)));
        assertTrue(plan.fieldBindings.stream().anyMatch(b -> "content".equals(b.fieldName)));
    }

    @Test
    public void testOrQuery() {
        String dsl = "title:hello OR title:hi";
        QsPlan plan = SearchDslParser.parseDsl(dsl);
        
        assertNotNull(plan);
        assertEquals(QsClauseType.OR, plan.root.type);
        assertEquals(2, plan.root.children.size());
    }

    @Test
    public void testNotQuery() {
        String dsl = "NOT title:spam";
        QsPlan plan = SearchDslParser.parseDsl(dsl);
        
        assertNotNull(plan);
        assertEquals(QsClauseType.NOT, plan.root.type);
        assertEquals(1, plan.root.children.size());
        
        QsNode child = plan.root.children.get(0);
        assertEquals(QsClauseType.TERM, child.type);
        assertEquals("title", child.field);
        assertEquals("spam", child.value);
    }

    @Test
    public void testComplexQuery() {
        String dsl = "(title:\"machine learning\" OR content:AI) AND NOT category:spam";
        QsPlan plan = SearchDslParser.parseDsl(dsl);
        
        assertNotNull(plan);
        assertEquals(QsClauseType.AND, plan.root.type);
        assertEquals(2, plan.root.children.size());
        
        // Should have 3 field bindings
        assertEquals(3, plan.fieldBindings.size());
        assertTrue(plan.fieldBindings.stream().anyMatch(b -> "title".equals(b.fieldName)));
        assertTrue(plan.fieldBindings.stream().anyMatch(b -> "content".equals(b.fieldName)));
        assertTrue(plan.fieldBindings.stream().anyMatch(b -> "category".equals(b.fieldName)));
    }

    @Test
    public void testEmptyDsl() {
        String dsl = "";
        QsPlan plan = SearchDslParser.parseDsl(dsl);
        
        assertNotNull(plan);
        assertEquals(QsClauseType.TERM, plan.root.type);
        assertEquals("error", plan.root.field);
        assertEquals("empty_dsl", plan.root.value);
    }

    @Test
    public void testInvalidDsl() {
        String dsl = "invalid:syntax AND";
        
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            SearchDslParser.parseDsl(dsl);
        });
        
        assertTrue(exception.getMessage().contains("Invalid search DSL syntax"));
    }

    @Test
    public void testQsPlanSerialization() {
        String dsl = "title:hello";
        QsPlan plan = SearchDslParser.parseDsl(dsl);
        
        String json = plan.toJson();
        assertNotNull(json);
        assertTrue(json.contains("TERM"));
        assertTrue(json.contains("title"));
        assertTrue(json.contains("hello"));
        
        QsPlan deserialized = QsPlan.fromJson(json);
        assertNotNull(deserialized);
        assertEquals(plan.root.type, deserialized.root.type);
        assertEquals(plan.root.field, deserialized.root.field);
        assertEquals(plan.root.value, deserialized.root.value);
    }

    @Test
    public void testQuotedFieldNames() {
        String dsl = "\"field name\":value";
        QsPlan plan = SearchDslParser.parseDsl(dsl);
        
        assertNotNull(plan);
        assertEquals("field name", plan.root.field);
        assertEquals(1, plan.fieldBindings.size());
        assertEquals("field name", plan.fieldBindings.get(0).fieldName);
    }
}
