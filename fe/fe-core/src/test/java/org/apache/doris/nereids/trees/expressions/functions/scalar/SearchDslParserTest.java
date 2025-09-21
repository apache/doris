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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for SearchDslParser
 */
public class SearchDslParserTest {

    @Test
    public void testSimpleTermQuery() {
        String dsl = "title:hello";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertNotNull(plan.root);
        Assertions.assertEquals(QsClauseType.TERM, plan.root.type);
        Assertions.assertEquals("title", plan.root.field);
        Assertions.assertEquals("hello", plan.root.value);

        Assertions.assertEquals(1, plan.fieldBindings.size());
        QsFieldBinding binding = plan.fieldBindings.get(0);
        Assertions.assertEquals("title", binding.fieldName);
    }

    @Test
    public void testPhraseQuery() {
        String dsl = "content:\"hello world\"";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.PHRASE, plan.root.type);
        Assertions.assertEquals("content", plan.root.field);
        Assertions.assertEquals("hello world", plan.root.value);
    }

    @Test
    public void testPrefixQuery() {
        String dsl = "title:hello*";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.PREFIX, plan.root.type);
        Assertions.assertEquals("title", plan.root.field);
        Assertions.assertEquals("hello*", plan.root.value);
    }

    @Test
    public void testWildcardQuery() {
        String dsl = "title:h*llo";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.WILDCARD, plan.root.type);
        Assertions.assertEquals("title", plan.root.field);
        Assertions.assertEquals("h*llo", plan.root.value);
    }

    @Test
    public void testRegexpQuery() {
        String dsl = "title:/[a-z]+/";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.REGEXP, plan.root.type);
        Assertions.assertEquals("title", plan.root.field);
        Assertions.assertEquals("[a-z]+", plan.root.value); // slashes removed
    }

    @Test
    public void testRangeQuery() {
        String dsl = "age:[18 TO 65]";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.RANGE, plan.root.type);
        Assertions.assertEquals("age", plan.root.field);
        Assertions.assertEquals("[18 TO 65]", plan.root.value);
    }

    @Test
    public void testListQuery() {
        String dsl = "category:IN(tech news)";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.LIST, plan.root.type);
        Assertions.assertEquals("category", plan.root.field);
        Assertions.assertEquals("IN(tech news)", plan.root.value);
    }

    @Test
    public void testAnyQuery() {
        String dsl = "tags:ANY(java python)";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.ANY, plan.root.type);
        Assertions.assertEquals("tags", plan.root.field);
        Assertions.assertEquals("java python", plan.root.value);
    }

    @Test
    public void testAllQuery() {
        String dsl = "tags:ALL(programming language)";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.ALL, plan.root.type);
        Assertions.assertEquals("tags", plan.root.field);
        Assertions.assertEquals("programming language", plan.root.value);
    }

    @Test
    public void testAndQuery() {
        String dsl = "title:hello AND content:world";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.AND, plan.root.type);
        Assertions.assertEquals(2, plan.root.children.size());

        QsNode leftChild = plan.root.children.get(0);
        Assertions.assertEquals(QsClauseType.TERM, leftChild.type);
        Assertions.assertEquals("title", leftChild.field);
        Assertions.assertEquals("hello", leftChild.value);

        QsNode rightChild = plan.root.children.get(1);
        Assertions.assertEquals(QsClauseType.TERM, rightChild.type);
        Assertions.assertEquals("content", rightChild.field);
        Assertions.assertEquals("world", rightChild.value);

        // Should have 2 field bindings
        Assertions.assertEquals(2, plan.fieldBindings.size());
        Assertions.assertTrue(plan.fieldBindings.stream().anyMatch(b -> "title".equals(b.fieldName)));
        Assertions.assertTrue(plan.fieldBindings.stream().anyMatch(b -> "content".equals(b.fieldName)));
    }

    @Test
    public void testOrQuery() {
        String dsl = "title:hello OR title:hi";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OR, plan.root.type);
        Assertions.assertEquals(2, plan.root.children.size());
    }

    @Test
    public void testNotQuery() {
        String dsl = "NOT title:spam";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.NOT, plan.root.type);
        Assertions.assertEquals(1, plan.root.children.size());

        QsNode child = plan.root.children.get(0);
        Assertions.assertEquals(QsClauseType.TERM, child.type);
        Assertions.assertEquals("title", child.field);
        Assertions.assertEquals("spam", child.value);
    }

    @Test
    public void testComplexQuery() {
        String dsl = "(title:\"machine learning\" OR content:AI) AND NOT category:spam";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.AND, plan.root.type);
        Assertions.assertEquals(2, plan.root.children.size());

        // Should have 3 field bindings
        Assertions.assertEquals(3, plan.fieldBindings.size());
        Assertions.assertTrue(plan.fieldBindings.stream().anyMatch(b -> "title".equals(b.fieldName)));
        Assertions.assertTrue(plan.fieldBindings.stream().anyMatch(b -> "content".equals(b.fieldName)));
        Assertions.assertTrue(plan.fieldBindings.stream().anyMatch(b -> "category".equals(b.fieldName)));
    }

    @Test
    public void testEmptyDsl() {
        String dsl = "";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.TERM, plan.root.type);
        Assertions.assertEquals("error", plan.root.field);
        Assertions.assertEquals("empty_dsl", plan.root.value);
    }

    @Test
    public void testInvalidDsl() {
        String dsl = "invalid:syntax AND";

        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            SearchDslParser.parseDsl(dsl);
        });

        Assertions.assertTrue(exception.getMessage().contains("Invalid search DSL syntax"));
    }

    @Test
    public void testQsPlanSerialization() {
        String dsl = "title:hello";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        String json = plan.toJson();
        Assertions.assertNotNull(json);
        Assertions.assertTrue(json.contains("TERM"));
        Assertions.assertTrue(json.contains("title"));
        Assertions.assertTrue(json.contains("hello"));

        QsPlan deserialized = QsPlan.fromJson(json);
        Assertions.assertNotNull(deserialized);
        Assertions.assertEquals(plan.root.type, deserialized.root.type);
        Assertions.assertEquals(plan.root.field, deserialized.root.field);
        Assertions.assertEquals(plan.root.value, deserialized.root.value);
    }

    @Test
    public void testQuotedFieldNames() {
        String dsl = "\"field name\":value";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals("field name", plan.root.field);
        Assertions.assertEquals(1, plan.fieldBindings.size());
        Assertions.assertEquals("field name", plan.fieldBindings.get(0).fieldName);
    }
}
