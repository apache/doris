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
    public void testAllQueryWithQuotes() {
        String dsl = "redirect:ALL(\"Rainbowman\")";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.ALL, plan.root.type);
        Assertions.assertEquals("redirect", plan.root.field);
        Assertions.assertEquals("Rainbowman", plan.root.value);
    }

    @Test
    public void testAnyQueryWithQuotes() {
        String dsl = "tags:ANY(\"Mandy Patinkin\")";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.ANY, plan.root.type);
        Assertions.assertEquals("tags", plan.root.field);
        Assertions.assertEquals("Mandy Patinkin", plan.root.value);
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

    // ============ Tests for Default Field and Operator Support ============

    @Test
    public void testDefaultFieldWithSimpleTerm() {
        // Test: "foo" + field="tags" → "tags:foo"
        String dsl = "foo";
        QsPlan plan = SearchDslParser.parseDsl(dsl, "tags", null);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.TERM, plan.root.type);
        Assertions.assertEquals("tags", plan.root.field);
        Assertions.assertEquals("foo", plan.root.value);
        Assertions.assertEquals(1, plan.fieldBindings.size());
        Assertions.assertEquals("tags", plan.fieldBindings.get(0).fieldName);
    }

    @Test
    public void testDefaultFieldWithMultiTermAnd() {
        // Test: "foo bar" + field="tags" + operator="and" → "tags:ALL(foo bar)"
        String dsl = "foo bar";
        QsPlan plan = SearchDslParser.parseDsl(dsl, "tags", "and");

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.ALL, plan.root.type);
        Assertions.assertEquals("tags", plan.root.field);
        Assertions.assertEquals("foo bar", plan.root.value);
    }

    @Test
    public void testDefaultFieldWithMultiTermOr() {
        // Test: "foo bar" + field="tags" + operator="or" → "tags:ANY(foo bar)"
        String dsl = "foo bar";
        QsPlan plan = SearchDslParser.parseDsl(dsl, "tags", "or");

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.ANY, plan.root.type);
        Assertions.assertEquals("tags", plan.root.field);
        Assertions.assertEquals("foo bar", plan.root.value);
    }

    @Test
    public void testDefaultFieldWithMultiTermDefaultOr() {
        // Test: "foo bar" + field="tags" (no operator, defaults to OR) → "tags:ANY(foo bar)"
        String dsl = "foo bar";
        QsPlan plan = SearchDslParser.parseDsl(dsl, "tags", null);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.ANY, plan.root.type);
        Assertions.assertEquals("tags", plan.root.field);
        Assertions.assertEquals("foo bar", plan.root.value);
    }

    @Test
    public void testDefaultFieldWithWildcardSingleTerm() {
        // Test: "foo*" + field="tags" → "tags:foo*"
        String dsl = "foo*";
        QsPlan plan = SearchDslParser.parseDsl(dsl, "tags", null);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.PREFIX, plan.root.type);
        Assertions.assertEquals("tags", plan.root.field);
        Assertions.assertEquals("foo*", plan.root.value);
    }

    @Test
    public void testDefaultFieldWithWildcardMultiTermAnd() {
        // Test: "foo* bar*" + field="tags" + operator="and" → "tags:foo* AND tags:bar*"
        String dsl = "foo* bar*";
        QsPlan plan = SearchDslParser.parseDsl(dsl, "tags", "and");

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.AND, plan.root.type);
        Assertions.assertEquals(2, plan.root.children.size());

        QsNode firstChild = plan.root.children.get(0);
        Assertions.assertEquals(QsClauseType.PREFIX, firstChild.type);
        Assertions.assertEquals("tags", firstChild.field);
        Assertions.assertEquals("foo*", firstChild.value);

        QsNode secondChild = plan.root.children.get(1);
        Assertions.assertEquals(QsClauseType.PREFIX, secondChild.type);
        Assertions.assertEquals("tags", secondChild.field);
        Assertions.assertEquals("bar*", secondChild.value);
    }

    @Test
    public void testDefaultFieldWithWildcardMultiTermOr() {
        // Test: "foo* bar*" + field="tags" + operator="or" → "tags:foo* OR tags:bar*"
        String dsl = "foo* bar*";
        QsPlan plan = SearchDslParser.parseDsl(dsl, "tags", "or");

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OR, plan.root.type);
        Assertions.assertEquals(2, plan.root.children.size());
    }

    @Test
    public void testDefaultFieldWithExplicitOperatorOverride() {
        // Test: "foo OR bar" + field="tags" + operator="and" → explicit OR takes precedence
        String dsl = "foo OR bar";
        QsPlan plan = SearchDslParser.parseDsl(dsl, "tags", "and");

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OR, plan.root.type);
        Assertions.assertEquals(2, plan.root.children.size());

        QsNode firstChild = plan.root.children.get(0);
        Assertions.assertEquals("tags", firstChild.field);
        Assertions.assertEquals("foo", firstChild.value);

        QsNode secondChild = plan.root.children.get(1);
        Assertions.assertEquals("tags", secondChild.field);
        Assertions.assertEquals("bar", secondChild.value);
    }

    @Test
    public void testDefaultFieldWithExplicitAndOperator() {
        // Test: "foo AND bar" + field="tags" + operator="or" → explicit AND takes precedence
        String dsl = "foo AND bar";
        QsPlan plan = SearchDslParser.parseDsl(dsl, "tags", "or");

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.AND, plan.root.type);
        Assertions.assertEquals(2, plan.root.children.size());
    }

    @Test
    public void testDefaultFieldWithExactFunction() {
        // Test: "EXACT(foo bar)" + field="tags" → "tags:EXACT(foo bar)"
        String dsl = "EXACT(foo bar)";
        QsPlan plan = SearchDslParser.parseDsl(dsl, "tags", null);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.EXACT, plan.root.type);
        Assertions.assertEquals("tags", plan.root.field);
        Assertions.assertEquals("foo bar", plan.root.value);
    }

    @Test
    public void testDefaultFieldWithAnyFunction() {
        // Test: "ANY(foo bar)" + field="tags" → "tags:ANY(foo bar)"
        String dsl = "ANY(foo bar)";
        QsPlan plan = SearchDslParser.parseDsl(dsl, "tags", null);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.ANY, plan.root.type);
        Assertions.assertEquals("tags", plan.root.field);
        Assertions.assertEquals("foo bar", plan.root.value);
    }

    @Test
    public void testDefaultFieldWithAllFunction() {
        // Test: "ALL(foo bar)" + field="tags" → "tags:ALL(foo bar)"
        String dsl = "ALL(foo bar)";
        QsPlan plan = SearchDslParser.parseDsl(dsl, "tags", null);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.ALL, plan.root.type);
        Assertions.assertEquals("tags", plan.root.field);
        Assertions.assertEquals("foo bar", plan.root.value);
    }

    @Test
    public void testDefaultFieldIgnoredWhenDslHasFieldReference() {
        // Test: DSL with field reference should ignore default field
        String dsl = "title:hello";
        QsPlan plan = SearchDslParser.parseDsl(dsl, "tags", "and");

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.TERM, plan.root.type);
        Assertions.assertEquals("title", plan.root.field); // Should be "title", not "tags"
        Assertions.assertEquals("hello", plan.root.value);
    }

    @Test
    public void testInvalidDefaultOperator() {
        // Test: invalid operator should throw exception
        String dsl = "foo bar";

        IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class, () -> {
            SearchDslParser.parseDsl(dsl, "tags", "invalid");
        });

        Assertions.assertTrue(exception.getMessage().contains("Invalid default operator"));
        Assertions.assertTrue(exception.getMessage().contains("Must be 'and' or 'or'"));
    }

    @Test
    public void testDefaultOperatorCaseInsensitive() {
        // Test: operator should be case-insensitive
        String dsl = "foo bar";

        // Test "AND"
        QsPlan plan1 = SearchDslParser.parseDsl(dsl, "tags", "AND");
        Assertions.assertEquals(QsClauseType.ALL, plan1.root.type);

        // Test "Or"
        QsPlan plan2 = SearchDslParser.parseDsl(dsl, "tags", "Or");
        Assertions.assertEquals(QsClauseType.ANY, plan2.root.type);

        // Test "aNd"
        QsPlan plan3 = SearchDslParser.parseDsl(dsl, "tags", "aNd");
        Assertions.assertEquals(QsClauseType.ALL, plan3.root.type);
    }

    @Test
    public void testDefaultFieldWithComplexWildcard() {
        // Test: "*foo*" (middle wildcard) + field="tags"
        String dsl = "*foo*";
        QsPlan plan = SearchDslParser.parseDsl(dsl, "tags", null);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.WILDCARD, plan.root.type);
        Assertions.assertEquals("tags", plan.root.field);
        Assertions.assertEquals("*foo*", plan.root.value);
    }

    @Test
    public void testDefaultFieldWithMixedWildcards() {
        // Test: "foo* *bar baz" (mixed wildcards and regular terms) + field="tags" + operator="and"
        String dsl = "foo* bar baz";
        QsPlan plan = SearchDslParser.parseDsl(dsl, "tags", "and");

        Assertions.assertNotNull(plan);
        // Should create AND query because it contains wildcards
        Assertions.assertEquals(QsClauseType.AND, plan.root.type);
        Assertions.assertEquals(3, plan.root.children.size());
    }

    @Test
    public void testDefaultFieldWithQuotedPhrase() {
        // Test: quoted phrase should be treated as PHRASE
        String dsl = "\"hello world\"";
        QsPlan plan = SearchDslParser.parseDsl(dsl, "tags", "and");

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.PHRASE, plan.root.type);
        Assertions.assertEquals("tags", plan.root.field);
        Assertions.assertEquals("hello world", plan.root.value);
    }

    @Test
    public void testDefaultFieldWithNotOperator() {
        // Test: "NOT foo" + field="tags" → "NOT tags:foo"
        String dsl = "NOT foo";
        QsPlan plan = SearchDslParser.parseDsl(dsl, "tags", null);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.NOT, plan.root.type);
        Assertions.assertEquals(1, plan.root.children.size());

        QsNode child = plan.root.children.get(0);
        Assertions.assertEquals(QsClauseType.TERM, child.type);
        Assertions.assertEquals("tags", child.field);
        Assertions.assertEquals("foo", child.value);
    }

    @Test
    public void testDefaultFieldWithEmptyString() {
        // Test: empty default field should not expand DSL, causing parse error
        // for incomplete DSL like "foo" (no field specified)
        String dsl = "foo";

        // This should throw an exception because "foo" alone is not valid DSL
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            SearchDslParser.parseDsl(dsl, "", "and");
        });

        Assertions.assertTrue(exception.getMessage().contains("Invalid search DSL syntax"));
    }

    @Test
    public void testDefaultFieldWithNullOperator() {
        // Test: null operator should default to OR
        String dsl = "foo bar";
        QsPlan plan = SearchDslParser.parseDsl(dsl, "tags", null);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.ANY, plan.root.type); // Defaults to OR/ANY
    }

    @Test
    public void testDefaultFieldWithSingleWildcardTerm() {
        // Test: single term with wildcard should not use ANY/ALL
        String dsl = "f?o";
        QsPlan plan = SearchDslParser.parseDsl(dsl, "tags", "and");

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.WILDCARD, plan.root.type);
        Assertions.assertEquals("tags", plan.root.field);
        Assertions.assertEquals("f?o", plan.root.value);
    }

    @Test
    public void testDefaultFieldPreservesFieldBindings() {
        // Test: field bindings should be correctly generated with default field
        String dsl = "foo bar";
        QsPlan plan = SearchDslParser.parseDsl(dsl, "tags", "and");

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(1, plan.fieldBindings.size());
        Assertions.assertEquals("tags", plan.fieldBindings.get(0).fieldName);
        Assertions.assertEquals(0, plan.fieldBindings.get(0).slotIndex);
    }
}
