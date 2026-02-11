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
import org.apache.doris.nereids.trees.expressions.functions.scalar.SearchDslParser.QsOccur;
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
        Assertions.assertNotNull(plan.getRoot());
        Assertions.assertEquals(QsClauseType.TERM, plan.getRoot().getType());
        Assertions.assertEquals("title", plan.getRoot().getField());
        Assertions.assertEquals("hello", plan.getRoot().getValue());

        Assertions.assertEquals(1, plan.getFieldBindings().size());
        QsFieldBinding binding = plan.getFieldBindings().get(0);
        Assertions.assertEquals("title", binding.getFieldName());
    }

    @Test
    public void testPhraseQuery() {
        String dsl = "content:\"hello world\"";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.PHRASE, plan.getRoot().getType());
        Assertions.assertEquals("content", plan.getRoot().getField());
        Assertions.assertEquals("hello world", plan.getRoot().getValue());
    }

    @Test
    public void testPrefixQuery() {
        String dsl = "title:hello*";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.PREFIX, plan.getRoot().getType());
        Assertions.assertEquals("title", plan.getRoot().getField());
        Assertions.assertEquals("hello*", plan.getRoot().getValue());
    }

    @Test
    public void testWildcardQuery() {
        String dsl = "title:h*llo";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.WILDCARD, plan.getRoot().getType());
        Assertions.assertEquals("title", plan.getRoot().getField());
        Assertions.assertEquals("h*llo", plan.getRoot().getValue());
    }

    @Test
    public void testRegexpQuery() {
        String dsl = "title:/[a-z]+/";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.REGEXP, plan.getRoot().getType());
        Assertions.assertEquals("title", plan.getRoot().getField());
        Assertions.assertEquals("[a-z]+", plan.getRoot().getValue()); // slashes removed
    }

    @Test
    public void testRangeQuery() {
        String dsl = "age:[18 TO 65]";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.RANGE, plan.getRoot().getType());
        Assertions.assertEquals("age", plan.getRoot().getField());
        Assertions.assertEquals("[18 TO 65]", plan.getRoot().getValue());
    }

    @Test
    public void testListQuery() {
        String dsl = "category:IN(tech news)";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.LIST, plan.getRoot().getType());
        Assertions.assertEquals("category", plan.getRoot().getField());
        Assertions.assertEquals("IN(tech news)", plan.getRoot().getValue());
    }

    @Test
    public void testAnyQuery() {
        String dsl = "tags:ANY(java python)";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.ANY, plan.getRoot().getType());
        Assertions.assertEquals("tags", plan.getRoot().getField());
        Assertions.assertEquals("java python", plan.getRoot().getValue());
    }

    @Test
    public void testAllQuery() {
        String dsl = "tags:ALL(programming language)";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.ALL, plan.getRoot().getType());
        Assertions.assertEquals("tags", plan.getRoot().getField());
        Assertions.assertEquals("programming language", plan.getRoot().getValue());
    }

    @Test
    public void testAllQueryWithQuotes() {
        String dsl = "redirect:ALL(\"Rainbowman\")";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.ALL, plan.getRoot().getType());
        Assertions.assertEquals("redirect", plan.getRoot().getField());
        Assertions.assertEquals("Rainbowman", plan.getRoot().getValue());
    }

    @Test
    public void testAnyQueryWithQuotes() {
        String dsl = "tags:ANY(\"Mandy Patinkin\")";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.ANY, plan.getRoot().getType());
        Assertions.assertEquals("tags", plan.getRoot().getField());
        Assertions.assertEquals("Mandy Patinkin", plan.getRoot().getValue());
    }

    @Test
    public void testAndQuery() {
        String dsl = "title:hello AND content:world";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.AND, plan.getRoot().getType());
        Assertions.assertEquals(2, plan.getRoot().getChildren().size());

        QsNode leftChild = plan.getRoot().getChildren().get(0);
        Assertions.assertEquals(QsClauseType.TERM, leftChild.getType());
        Assertions.assertEquals("title", leftChild.getField());
        Assertions.assertEquals("hello", leftChild.getValue());

        QsNode rightChild = plan.getRoot().getChildren().get(1);
        Assertions.assertEquals(QsClauseType.TERM, rightChild.getType());
        Assertions.assertEquals("content", rightChild.getField());
        Assertions.assertEquals("world", rightChild.getValue());

        // Should have 2 field bindings
        Assertions.assertEquals(2, plan.getFieldBindings().size());
        Assertions.assertTrue(plan.getFieldBindings().stream().anyMatch(b -> "title".equals(b.getFieldName())));
        Assertions.assertTrue(plan.getFieldBindings().stream().anyMatch(b -> "content".equals(b.getFieldName())));
    }

    @Test
    public void testOrQuery() {
        String dsl = "title:hello OR title:hi";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OR, plan.getRoot().getType());
        Assertions.assertEquals(2, plan.getRoot().getChildren().size());
    }

    @Test
    public void testNotQuery() {
        String dsl = "NOT title:spam";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.NOT, plan.getRoot().getType());
        Assertions.assertEquals(1, plan.getRoot().getChildren().size());

        QsNode child = plan.getRoot().getChildren().get(0);
        Assertions.assertEquals(QsClauseType.TERM, child.getType());
        Assertions.assertEquals("title", child.getField());
        Assertions.assertEquals("spam", child.getValue());
    }

    @Test
    public void testComplexQuery() {
        String dsl = "(title:\"machine learning\" OR content:AI) AND NOT category:spam";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.AND, plan.getRoot().getType());
        Assertions.assertEquals(2, plan.getRoot().getChildren().size());

        // Should have 3 field bindings
        Assertions.assertEquals(3, plan.getFieldBindings().size());
        Assertions.assertTrue(plan.getFieldBindings().stream().anyMatch(b -> "title".equals(b.getFieldName())));
        Assertions.assertTrue(plan.getFieldBindings().stream().anyMatch(b -> "content".equals(b.getFieldName())));
        Assertions.assertTrue(plan.getFieldBindings().stream().anyMatch(b -> "category".equals(b.getFieldName())));
    }

    @Test
    public void testEmptyDsl() {
        String dsl = "";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.TERM, plan.getRoot().getType());
        Assertions.assertEquals("error", plan.getRoot().getField());
        Assertions.assertEquals("empty_dsl", plan.getRoot().getValue());
    }

    @Test
    public void testInvalidDsl() {
        String dsl = "invalid:syntax AND";

        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            SearchDslParser.parseDsl(dsl);
        });

        Assertions.assertTrue(exception.getMessage().contains("Invalid search DSL"));
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
        Assertions.assertEquals(plan.getRoot().getType(), deserialized.getRoot().getType());
        Assertions.assertEquals(plan.getRoot().getField(), deserialized.getRoot().getField());
        Assertions.assertEquals(plan.getRoot().getValue(), deserialized.getRoot().getValue());
    }

    @Test
    public void testQuotedFieldNames() {
        String dsl = "\"field name\":value";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals("field name", plan.getRoot().getField());
        Assertions.assertEquals(1, plan.getFieldBindings().size());
        Assertions.assertEquals("field name", plan.getFieldBindings().get(0).getFieldName());
    }

    // ============ Tests for Default Field and Operator Support ============

    @Test
    public void testDefaultFieldWithSimpleTerm() {
        // Test: "foo" + field="tags" → "tags:foo"
        String dsl = "foo";
        QsPlan plan = SearchDslParser.parseDsl(dsl, "tags", null);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.TERM, plan.getRoot().getType());
        Assertions.assertEquals("tags", plan.getRoot().getField());
        Assertions.assertEquals("foo", plan.getRoot().getValue());
        Assertions.assertEquals(1, plan.getFieldBindings().size());
        Assertions.assertEquals("tags", plan.getFieldBindings().get(0).getFieldName());
    }

    @Test
    public void testDefaultFieldWithMultiTermAnd() {
        // Test: "foo bar" + field="tags" + operator="and" → "tags:foo AND tags:bar"
        // With single-phase parsing, multi-term queries are parsed as separate terms
        String dsl = "foo bar";
        QsPlan plan = SearchDslParser.parseDsl(dsl, "tags", "and");

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.AND, plan.getRoot().getType());
        Assertions.assertEquals(2, plan.getRoot().getChildren().size());

        QsNode firstChild = plan.getRoot().getChildren().get(0);
        Assertions.assertEquals(QsClauseType.TERM, firstChild.getType());
        Assertions.assertEquals("tags", firstChild.getField());
        Assertions.assertEquals("foo", firstChild.getValue());

        QsNode secondChild = plan.getRoot().getChildren().get(1);
        Assertions.assertEquals(QsClauseType.TERM, secondChild.getType());
        Assertions.assertEquals("tags", secondChild.getField());
        Assertions.assertEquals("bar", secondChild.getValue());
    }

    @Test
    public void testDefaultFieldWithMultiTermOr() {
        // Test: "foo bar" + field="tags" + operator="or" → "tags:foo OR tags:bar"
        // With single-phase parsing, multi-term queries are parsed as separate terms
        String dsl = "foo bar";
        QsPlan plan = SearchDslParser.parseDsl(dsl, "tags", "or");

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OR, plan.getRoot().getType());
        Assertions.assertEquals(2, plan.getRoot().getChildren().size());

        QsNode firstChild = plan.getRoot().getChildren().get(0);
        Assertions.assertEquals(QsClauseType.TERM, firstChild.getType());
        Assertions.assertEquals("tags", firstChild.getField());
        Assertions.assertEquals("foo", firstChild.getValue());

        QsNode secondChild = plan.getRoot().getChildren().get(1);
        Assertions.assertEquals(QsClauseType.TERM, secondChild.getType());
        Assertions.assertEquals("tags", secondChild.getField());
        Assertions.assertEquals("bar", secondChild.getValue());
    }

    @Test
    public void testDefaultFieldWithMultiTermDefaultOr() {
        // Test: "foo bar" + field="tags" (no operator, defaults to OR) → "tags:foo OR tags:bar"
        // With single-phase parsing, multi-term queries are parsed as separate terms
        String dsl = "foo bar";
        QsPlan plan = SearchDslParser.parseDsl(dsl, "tags", null);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OR, plan.getRoot().getType());
        Assertions.assertEquals(2, plan.getRoot().getChildren().size());

        QsNode firstChild = plan.getRoot().getChildren().get(0);
        Assertions.assertEquals(QsClauseType.TERM, firstChild.getType());
        Assertions.assertEquals("tags", firstChild.getField());
        Assertions.assertEquals("foo", firstChild.getValue());

        QsNode secondChild = plan.getRoot().getChildren().get(1);
        Assertions.assertEquals(QsClauseType.TERM, secondChild.getType());
        Assertions.assertEquals("tags", secondChild.getField());
        Assertions.assertEquals("bar", secondChild.getValue());
    }

    @Test
    public void testDefaultFieldWithWildcardSingleTerm() {
        // Test: "foo*" + field="tags" → "tags:foo*"
        String dsl = "foo*";
        QsPlan plan = SearchDslParser.parseDsl(dsl, "tags", null);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.PREFIX, plan.getRoot().getType());
        Assertions.assertEquals("tags", plan.getRoot().getField());
        Assertions.assertEquals("foo*", plan.getRoot().getValue());
    }

    @Test
    public void testDefaultFieldWithWildcardMultiTermAnd() {
        // Test: "foo* bar*" + field="tags" + operator="and" → "tags:foo* AND tags:bar*"
        String dsl = "foo* bar*";
        QsPlan plan = SearchDslParser.parseDsl(dsl, "tags", "and");

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.AND, plan.getRoot().getType());
        Assertions.assertEquals(2, plan.getRoot().getChildren().size());

        QsNode firstChild = plan.getRoot().getChildren().get(0);
        Assertions.assertEquals(QsClauseType.PREFIX, firstChild.getType());
        Assertions.assertEquals("tags", firstChild.getField());
        Assertions.assertEquals("foo*", firstChild.getValue());

        QsNode secondChild = plan.getRoot().getChildren().get(1);
        Assertions.assertEquals(QsClauseType.PREFIX, secondChild.getType());
        Assertions.assertEquals("tags", secondChild.getField());
        Assertions.assertEquals("bar*", secondChild.getValue());
    }

    @Test
    public void testDefaultFieldWithWildcardMultiTermOr() {
        // Test: "foo* bar*" + field="tags" + operator="or" → "tags:foo* OR tags:bar*"
        String dsl = "foo* bar*";
        QsPlan plan = SearchDslParser.parseDsl(dsl, "tags", "or");

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OR, plan.getRoot().getType());
        Assertions.assertEquals(2, plan.getRoot().getChildren().size());
    }

    @Test
    public void testDefaultFieldWithExplicitOperatorOverride() {
        // Test: "foo OR bar" + field="tags" + operator="and" → explicit OR takes precedence
        String dsl = "foo OR bar";
        QsPlan plan = SearchDslParser.parseDsl(dsl, "tags", "and");

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OR, plan.getRoot().getType());
        Assertions.assertEquals(2, plan.getRoot().getChildren().size());

        QsNode firstChild = plan.getRoot().getChildren().get(0);
        Assertions.assertEquals("tags", firstChild.getField());
        Assertions.assertEquals("foo", firstChild.getValue());

        QsNode secondChild = plan.getRoot().getChildren().get(1);
        Assertions.assertEquals("tags", secondChild.getField());
        Assertions.assertEquals("bar", secondChild.getValue());
    }

    @Test
    public void testDefaultFieldWithExplicitAndOperator() {
        // Test: "foo AND bar" + field="tags" + operator="or" → explicit AND takes precedence
        String dsl = "foo AND bar";
        QsPlan plan = SearchDslParser.parseDsl(dsl, "tags", "or");

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.AND, plan.getRoot().getType());
        Assertions.assertEquals(2, plan.getRoot().getChildren().size());
    }

    @Test
    public void testDefaultFieldWithExactFunction() {
        // Test: "EXACT(foo bar)" + field="tags" → "tags:EXACT(foo bar)"
        String dsl = "EXACT(foo bar)";
        QsPlan plan = SearchDslParser.parseDsl(dsl, "tags", null);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.EXACT, plan.getRoot().getType());
        Assertions.assertEquals("tags", plan.getRoot().getField());
        Assertions.assertEquals("foo bar", plan.getRoot().getValue());
    }

    @Test
    public void testDefaultFieldWithAnyFunction() {
        // Test: "ANY(foo bar)" + field="tags" → "tags:ANY(foo bar)"
        String dsl = "ANY(foo bar)";
        QsPlan plan = SearchDslParser.parseDsl(dsl, "tags", null);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.ANY, plan.getRoot().getType());
        Assertions.assertEquals("tags", plan.getRoot().getField());
        Assertions.assertEquals("foo bar", plan.getRoot().getValue());
    }

    @Test
    public void testDefaultFieldWithAllFunction() {
        // Test: "ALL(foo bar)" + field="tags" → "tags:ALL(foo bar)"
        String dsl = "ALL(foo bar)";
        QsPlan plan = SearchDslParser.parseDsl(dsl, "tags", null);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.ALL, plan.getRoot().getType());
        Assertions.assertEquals("tags", plan.getRoot().getField());
        Assertions.assertEquals("foo bar", plan.getRoot().getValue());
    }

    @Test
    public void testDefaultFieldIgnoredWhenDslHasFieldReference() {
        // Test: DSL with field reference should ignore default field
        String dsl = "title:hello";
        QsPlan plan = SearchDslParser.parseDsl(dsl, "tags", "and");

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.TERM, plan.getRoot().getType());
        Assertions.assertEquals("title", plan.getRoot().getField()); // Should be "title", not "tags"
        Assertions.assertEquals("hello", plan.getRoot().getValue());
    }

    @Test
    public void testInvalidDefaultOperator() {
        // Test: invalid operator should throw IllegalArgumentException
        String dsl = "foo bar";

        // Invalid operator should throw exception
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            SearchDslParser.parseDsl(dsl, "tags", "invalid");
        });
    }

    @Test
    public void testDefaultFieldWithInFunction() {
        // Test: "IN(value1 value2)" + field="tags" → "tags:IN(value1 value2)"
        String dsl = "IN(value1 value2)";
        QsPlan plan = SearchDslParser.parseDsl(dsl, "tags", null);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.LIST, plan.getRoot().getType());
        Assertions.assertEquals("tags", plan.getRoot().getField());
        Assertions.assertEquals("IN(value1 value2)", plan.getRoot().getValue());
    }

    @Test
    public void testDefaultOperatorCaseInsensitive() {
        // Test: operator should be case-insensitive
        // With single-phase parsing, multi-term queries produce AND/OR nodes
        String dsl = "foo bar";

        // Test "AND"
        QsPlan plan1 = SearchDslParser.parseDsl(dsl, "tags", "AND");
        Assertions.assertEquals(QsClauseType.AND, plan1.getRoot().getType());

        // Test "Or"
        QsPlan plan2 = SearchDslParser.parseDsl(dsl, "tags", "Or");
        Assertions.assertEquals(QsClauseType.OR, plan2.getRoot().getType());

        // Test "aNd"
        QsPlan plan3 = SearchDslParser.parseDsl(dsl, "tags", "aNd");
        Assertions.assertEquals(QsClauseType.AND, plan3.getRoot().getType());
    }

    @Test
    public void testDefaultFieldWithComplexWildcard() {
        // Test: "*foo*" (middle wildcard) + field="tags"
        String dsl = "*foo*";
        QsPlan plan = SearchDslParser.parseDsl(dsl, "tags", null);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.WILDCARD, plan.getRoot().getType());
        Assertions.assertEquals("tags", plan.getRoot().getField());
        Assertions.assertEquals("*foo*", plan.getRoot().getValue());
    }

    @Test
    public void testDefaultFieldWithMixedWildcards() {
        // Test: "foo* *bar baz" (mixed wildcards and regular terms) + field="tags" + operator="and"
        String dsl = "foo* bar baz";
        QsPlan plan = SearchDslParser.parseDsl(dsl, "tags", "and");

        Assertions.assertNotNull(plan);
        // Should create AND query because it contains wildcards
        Assertions.assertEquals(QsClauseType.AND, plan.getRoot().getType());
        Assertions.assertEquals(3, plan.getRoot().getChildren().size());
    }

    @Test
    public void testDefaultFieldWithQuotedPhrase() {
        // Test: quoted phrase should be treated as PHRASE
        String dsl = "\"hello world\"";
        QsPlan plan = SearchDslParser.parseDsl(dsl, "tags", "and");

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.PHRASE, plan.getRoot().getType());
        Assertions.assertEquals("tags", plan.getRoot().getField());
        Assertions.assertEquals("hello world", plan.getRoot().getValue());
    }

    @Test
    public void testDefaultFieldWithNotOperator() {
        // Test: "NOT foo" + field="tags" → "NOT tags:foo"
        String dsl = "NOT foo";
        QsPlan plan = SearchDslParser.parseDsl(dsl, "tags", null);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.NOT, plan.getRoot().getType());
        Assertions.assertEquals(1, plan.getRoot().getChildren().size());

        QsNode child = plan.getRoot().getChildren().get(0);
        Assertions.assertEquals(QsClauseType.TERM, child.getType());
        Assertions.assertEquals("tags", child.getField());
        Assertions.assertEquals("foo", child.getValue());
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

        Assertions.assertTrue(exception.getMessage().contains("Invalid search DSL"));
    }

    @Test
    public void testDefaultFieldWithNullOperator() {
        // Test: null operator should default to OR
        // With single-phase parsing, multi-term queries produce OR nodes
        String dsl = "foo bar";
        QsPlan plan = SearchDslParser.parseDsl(dsl, "tags", null);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OR, plan.getRoot().getType()); // Defaults to OR
    }

    @Test
    public void testDefaultFieldWithSingleWildcardTerm() {
        // Test: single term with wildcard should not use ANY/ALL
        String dsl = "f?o";
        QsPlan plan = SearchDslParser.parseDsl(dsl, "tags", "and");

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.WILDCARD, plan.getRoot().getType());
        Assertions.assertEquals("tags", plan.getRoot().getField());
        Assertions.assertEquals("f?o", plan.getRoot().getValue());
    }

    @Test
    public void testDefaultFieldPreservesFieldBindings() {
        // Test: field bindings should be correctly generated with default field
        String dsl = "foo bar";
        QsPlan plan = SearchDslParser.parseDsl(dsl, "tags", "and");

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(1, plan.getFieldBindings().size());
        Assertions.assertEquals("tags", plan.getFieldBindings().get(0).getFieldName());
        Assertions.assertEquals(0, plan.getFieldBindings().get(0).getSlotIndex());
    }

    // ============ Tests for Lucene Mode Parsing ============

    @Test
    public void testLuceneModeSimpleAndQuery() {
        // Test: "a AND b" in Lucene mode → both MUST
        String dsl = "field:a AND field:b";
        String options = "{\"mode\":\"lucene\",\"minimum_should_match\":0}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OCCUR_BOOLEAN, plan.getRoot().getType());
        Assertions.assertEquals(2, plan.getRoot().getChildren().size());
        Assertions.assertEquals(Integer.valueOf(0), plan.getRoot().getMinimumShouldMatch());

        // Both children should have MUST occur
        for (QsNode child : plan.getRoot().getChildren()) {
            Assertions.assertEquals(SearchDslParser.QsOccur.MUST, child.getOccur());
        }
    }

    @Test
    public void testLuceneModeSimpleOrQuery() {
        // Test: "a OR b OR c" in Lucene mode → all SHOULD, at least one must match
        String dsl = "field:a OR field:b OR field:c";
        String options = "{\"mode\":\"lucene\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OCCUR_BOOLEAN, plan.getRoot().getType());
        Assertions.assertEquals(3, plan.getRoot().getChildren().size());

        // All children should have SHOULD occur
        for (QsNode child : plan.getRoot().getChildren()) {
            Assertions.assertEquals(SearchDslParser.QsOccur.SHOULD, child.getOccur());
        }

        // minimum_should_match should be 1 (at least one must match)
        Assertions.assertEquals(Integer.valueOf(1), plan.getRoot().getMinimumShouldMatch());
    }

    @Test
    public void testLuceneModeAndOrMixed() {
        // Test: "a AND b OR c" in Lucene mode with minimum_should_match=0
        // Expected: +a (SHOULD terms discarded because MUST exists)
        String dsl = "field:a AND field:b OR field:c";
        String options = "{\"mode\":\"lucene\",\"minimum_should_match\":0}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        // With minimum_should_match=0 and MUST clauses present, SHOULD is discarded
        // Only "a" remains with MUST
        Assertions.assertEquals(QsClauseType.TERM, plan.getRoot().getType());
        Assertions.assertEquals("field", plan.getRoot().getField());
        Assertions.assertEquals("a", plan.getRoot().getValue());
    }

    @Test
    public void testLuceneModeAndOrNotMixed() {
        // Test: "a AND b OR NOT c AND d" in Lucene mode
        // Expected processing:
        // - a: MUST (first term, default_operator=AND)
        // - b: MUST (AND introduces)
        // - c: MUST_NOT (OR + NOT, but OR makes preceding SHOULD, NOT makes current MUST_NOT)
        // - d: MUST (AND introduces)
        // With minimum_should_match=0: b becomes SHOULD and is discarded
        // Result: +a -c +d
        String dsl = "field:a AND field:b OR NOT field:c AND field:d";
        String options = "{\"mode\":\"lucene\",\"minimum_should_match\":0}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OCCUR_BOOLEAN, plan.getRoot().getType());

        // Should have 3 children: a(MUST), c(MUST_NOT), d(MUST)
        // b is filtered out because it becomes SHOULD
        Assertions.assertEquals(3, plan.getRoot().getChildren().size());

        QsNode nodeA = plan.getRoot().getChildren().get(0);
        Assertions.assertEquals("a", nodeA.getValue());
        Assertions.assertEquals(SearchDslParser.QsOccur.MUST, nodeA.getOccur());

        QsNode nodeC = plan.getRoot().getChildren().get(1);
        Assertions.assertEquals("c", nodeC.getValue());
        Assertions.assertEquals(SearchDslParser.QsOccur.MUST_NOT, nodeC.getOccur());

        QsNode nodeD = plan.getRoot().getChildren().get(2);
        Assertions.assertEquals("d", nodeD.getValue());
        Assertions.assertEquals(SearchDslParser.QsOccur.MUST, nodeD.getOccur());
    }

    @Test
    public void testLuceneModeWithDefaultField() {
        // Test: Lucene mode with default field expansion
        String dsl = "aterm AND bterm OR cterm";
        // Now default_field and default_operator are inside the options JSON
        String options = "{\"default_field\":\"firstname\",\"default_operator\":\"and\","
                + "\"mode\":\"lucene\",\"minimum_should_match\":0}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        // With minimum_should_match=0, only aterm (MUST) remains
        Assertions.assertEquals(QsClauseType.TERM, plan.getRoot().getType());
        Assertions.assertEquals("firstname", plan.getRoot().getField());
        Assertions.assertEquals("aterm", plan.getRoot().getValue());
    }

    @Test
    public void testLuceneModeNotOperator() {
        // Test: "NOT a" in Lucene mode
        // In Lucene mode, single NOT produces OCCUR_BOOLEAN with a MUST_NOT child
        // (wrapped for BE to handle the negation properly)
        String dsl = "NOT field:a";
        String options = "{\"mode\":\"lucene\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OCCUR_BOOLEAN, plan.getRoot().getType());
        Assertions.assertEquals(1, plan.getRoot().getChildren().size());
        Assertions.assertEquals(QsClauseType.TERM, plan.getRoot().getChildren().get(0).getType());
        Assertions.assertEquals(QsOccur.MUST_NOT, plan.getRoot().getChildren().get(0).getOccur());
    }

    @Test
    public void testLuceneModeMinimumShouldMatchExplicit() {
        // Test: explicit minimum_should_match=1 keeps SHOULD clauses
        String dsl = "field:a AND field:b OR field:c";
        String options = "{\"mode\":\"lucene\",\"minimum_should_match\":1}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OCCUR_BOOLEAN, plan.getRoot().getType());
        // All 3 terms should be present
        Assertions.assertEquals(3, plan.getRoot().getChildren().size());
        Assertions.assertEquals(Integer.valueOf(1), plan.getRoot().getMinimumShouldMatch());
    }

    @Test
    public void testLuceneModeSingleTerm() {
        // Test: single term should not create OCCUR_BOOLEAN wrapper
        String dsl = "field:hello";
        String options = "{\"mode\":\"lucene\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.TERM, plan.getRoot().getType());
        Assertions.assertEquals("field", plan.getRoot().getField());
        Assertions.assertEquals("hello", plan.getRoot().getValue());
    }

    @Test
    public void testStandardModeUnchanged() {
        // Test: standard mode (default) should work as before
        String dsl = "field:a AND field:b OR field:c";
        QsPlan plan = SearchDslParser.parseDsl(dsl, (String) null);

        Assertions.assertNotNull(plan);
        // Standard mode uses traditional boolean algebra: OR at top level
        Assertions.assertEquals(QsClauseType.OR, plan.getRoot().getType());
    }

    @Test
    public void testLuceneModeInvalidJson() {
        // Test: invalid JSON options should throw an exception
        String dsl = "field:a AND field:b";
        String options = "not valid json";
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            SearchDslParser.parseDsl(dsl, options);
        });
    }

    @Test
    public void testLuceneModeEmptyOptions() {
        // Test: empty options string should use standard mode
        String dsl = "field:a AND field:b";
        QsPlan plan = SearchDslParser.parseDsl(dsl, "");

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.AND, plan.getRoot().getType());
    }

    // ============ Tests for Escape Handling ============

    @Test
    public void testEscapedSpaceInTerm() {
        // Test: "First\ Value" should be treated as a single term "First Value"
        // The escape sequence is processed: \ + space -> space
        String dsl = "field:First\\ Value";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.TERM, plan.getRoot().getType());
        Assertions.assertEquals("field", plan.getRoot().getField());
        // After unescape: "First\ Value" -> "First Value"
        Assertions.assertEquals("First Value", plan.getRoot().getValue());
    }

    @Test
    public void testEscapedParentheses() {
        // Test: \( and \) should be treated as literal characters, not grouping
        // The escape sequence is processed: \( -> ( and \) -> )
        String dsl = "field:hello\\(world\\)";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.TERM, plan.getRoot().getType());
        Assertions.assertEquals("field", plan.getRoot().getField());
        // After unescape: "hello\(world\)" -> "hello(world)"
        Assertions.assertEquals("hello(world)", plan.getRoot().getValue());
    }

    @Test
    public void testEscapedColon() {
        // Test: \: should be treated as literal colon, not field separator
        // The escape sequence is processed: \: -> :
        String dsl = "field:value\\:with\\:colons";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.TERM, plan.getRoot().getType());
        Assertions.assertEquals("field", plan.getRoot().getField());
        // After unescape: "value\:with\:colons" -> "value:with:colons"
        Assertions.assertEquals("value:with:colons", plan.getRoot().getValue());
    }

    @Test
    public void testEscapedBackslash() {
        // Test: \\ should be treated as a literal backslash
        // The escape sequence is processed: \\ -> \
        String dsl = "field:path\\\\to\\\\file";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.TERM, plan.getRoot().getType());
        Assertions.assertEquals("field", plan.getRoot().getField());
        // After unescape: "path\\to\\file" -> "path\to\file"
        Assertions.assertEquals("path\\to\\file", plan.getRoot().getValue());
    }

    @Test
    public void testUppercaseAndOperator() {
        // Test: uppercase AND should be treated as operator
        String dsl = "field:a AND field:b";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.AND, plan.getRoot().getType());
        Assertions.assertEquals(2, plan.getRoot().getChildren().size());
    }

    @Test
    public void testLowercaseAndOperator() {
        // Test: Currently lowercase 'and' is also treated as operator
        // According to PDF requirement, only uppercase should be operators
        // This test documents current behavior - may need to change
        String dsl = "field:a and field:b";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        // Current behavior: lowercase 'and' IS an operator
        Assertions.assertEquals(QsClauseType.AND, plan.getRoot().getType());
        // TODO: If PDF requires only uppercase, this should fail and return OR or different structure
    }

    @Test
    public void testUppercaseOrOperator() {
        // Test: uppercase OR should be treated as operator
        String dsl = "field:a OR field:b";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OR, plan.getRoot().getType());
        Assertions.assertEquals(2, plan.getRoot().getChildren().size());
    }

    @Test
    public void testLowercaseOrOperator() {
        // Test: Currently lowercase 'or' is also treated as operator
        // According to PDF requirement, only uppercase should be operators
        String dsl = "field:a or field:b";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        // Current behavior: lowercase 'or' IS an operator
        Assertions.assertEquals(QsClauseType.OR, plan.getRoot().getType());
        // TODO: If PDF requires only uppercase, this should fail
    }

    @Test
    public void testUppercaseNotOperator() {
        // Test: uppercase NOT should be treated as operator
        String dsl = "NOT field:spam";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.NOT, plan.getRoot().getType());
    }

    @Test
    public void testLowercaseNotOperator() {
        // Test: Currently lowercase 'not' is also treated as operator
        // According to PDF requirement, only uppercase should be operators
        String dsl = "not field:spam";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        // Current behavior: lowercase 'not' IS an operator
        Assertions.assertEquals(QsClauseType.NOT, plan.getRoot().getType());
        // TODO: If PDF requires only uppercase, this should fail
    }

    @Test
    public void testExclamationNotOperator() {
        // Test: ! should be treated as NOT operator
        String dsl = "!field:spam";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        // Current behavior: ! IS a NOT operator
        Assertions.assertEquals(QsClauseType.NOT, plan.getRoot().getType());
    }

    @Test
    public void testEscapedSpecialCharactersInQuoted() {
        // Test: escaped characters inside quoted strings
        // Note: For PHRASE queries, escape handling is preserved as-is for now
        // The backend will handle escape processing for phrase queries
        String dsl = "field:\"hello\\\"world\"";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.PHRASE, plan.getRoot().getType());
        Assertions.assertEquals("hello\\\"world", plan.getRoot().getValue());
    }

    @Test
    public void testNoEscapeWithoutBackslash() {
        // Test: normal term without escape characters
        String dsl = "field:normalterm";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.TERM, plan.getRoot().getType());
        Assertions.assertEquals("normalterm", plan.getRoot().getValue());
    }

    // ============ Tests for Multi-Field Search ============

    @Test
    public void testMultiFieldSimpleTerm() {
        // Test: "hello" + fields=["title","content"] → "(title:hello OR content:hello)"
        String dsl = "hello";
        String options = "{\"fields\":[\"title\",\"content\"]}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OR, plan.getRoot().getType());
        Assertions.assertEquals(2, plan.getRoot().getChildren().size());

        // Verify both fields are in bindings
        Assertions.assertEquals(2, plan.getFieldBindings().size());
        Assertions.assertTrue(plan.getFieldBindings().stream()
                .anyMatch(b -> "title".equals(b.getFieldName())));
        Assertions.assertTrue(plan.getFieldBindings().stream()
                .anyMatch(b -> "content".equals(b.getFieldName())));
    }

    @Test
    public void testMultiFieldMultiTermAnd() {
        // Test: "hello world" + fields=["title","content"] + default_operator="and" + type="cross_fields"
        // → "(title:hello OR content:hello) AND (title:world OR content:world)"
        String dsl = "hello world";
        String options = "{\"fields\":[\"title\",\"content\"],\"default_operator\":\"and\",\"type\":\"cross_fields\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.AND, plan.getRoot().getType());
        Assertions.assertEquals(2, plan.getRoot().getChildren().size());

        // Each child should be an OR of two fields
        for (QsNode child : plan.getRoot().getChildren()) {
            Assertions.assertEquals(QsClauseType.OR, child.getType());
            Assertions.assertEquals(2, child.getChildren().size());
        }
    }

    @Test
    public void testMultiFieldMultiTermOr() {
        // Test: "hello world" + fields=["title","content"] + default_operator="or"
        // → "(title:hello OR content:hello) OR (title:world OR content:world)"
        String dsl = "hello world";
        String options = "{\"fields\":[\"title\",\"content\"],\"default_operator\":\"or\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OR, plan.getRoot().getType());
    }

    @Test
    public void testMultiFieldExplicitAndOperator() {
        // Test: "hello AND world" + fields=["title","content"] + cross_fields
        // → "(title:hello OR content:hello) AND (title:world OR content:world)"
        String dsl = "hello AND world";
        String options = "{\"fields\":[\"title\",\"content\"],\"type\":\"cross_fields\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.AND, plan.getRoot().getType());
    }

    @Test
    public void testMultiFieldMixedWithExplicitField() {
        // Test: "hello AND category:tech" + fields=["title","content"] + cross_fields
        // → "(title:hello OR content:hello) AND category:tech"
        String dsl = "hello AND category:tech";
        String options = "{\"fields\":[\"title\",\"content\"],\"type\":\"cross_fields\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.AND, plan.getRoot().getType());
        Assertions.assertEquals(2, plan.getRoot().getChildren().size());

        // Verify "category" is preserved
        Assertions.assertTrue(plan.getFieldBindings().stream()
                .anyMatch(b -> "category".equals(b.getFieldName())));
    }

    @Test
    public void testMultiFieldWithWildcard() {
        // Test: "hello*" + fields=["title","content"]
        String dsl = "hello*";
        String options = "{\"fields\":[\"title\",\"content\"]}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OR, plan.getRoot().getType());
        Assertions.assertEquals(2, plan.getRoot().getChildren().size());

        // Both should be PREFIX type
        for (QsNode child : plan.getRoot().getChildren()) {
            Assertions.assertEquals(QsClauseType.PREFIX, child.getType());
        }
    }

    @Test
    public void testMultiFieldWithExactFunction() {
        // Test: "EXACT(foo bar)" + fields=["title","content"]
        String dsl = "EXACT(foo bar)";
        String options = "{\"fields\":[\"title\",\"content\"]}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OR, plan.getRoot().getType());
        Assertions.assertEquals(2, plan.getRoot().getChildren().size());

        // Both should be EXACT type
        for (QsNode child : plan.getRoot().getChildren()) {
            Assertions.assertEquals(QsClauseType.EXACT, child.getType());
        }
    }

    @Test
    public void testMultiFieldThreeFields() {
        // Test: "hello" + fields=["title","content","tags"]
        String dsl = "hello";
        String options = "{\"fields\":[\"title\",\"content\",\"tags\"]}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OR, plan.getRoot().getType());
        Assertions.assertEquals(3, plan.getRoot().getChildren().size());
        Assertions.assertEquals(3, plan.getFieldBindings().size());
    }

    @Test
    public void testFieldsAndDefaultFieldMutuallyExclusive() {
        // Test: specifying both fields and default_field should throw error
        String dsl = "hello";
        String options = "{\"fields\":[\"title\",\"content\"],\"default_field\":\"tags\"}";

        IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class, () -> {
            SearchDslParser.parseDsl(dsl, options);
        });
        Assertions.assertTrue(exception.getMessage().contains("mutually exclusive"));
    }

    @Test
    public void testSingleFieldInArray() {
        // Test: single field in array should work like default_field
        String dsl = "hello";
        String options = "{\"fields\":[\"title\"]}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.TERM, plan.getRoot().getType());
        Assertions.assertEquals("title", plan.getRoot().getField());
        Assertions.assertEquals(1, plan.getFieldBindings().size());
    }

    @Test
    public void testMultiFieldNotOperator() {
        // Test: "NOT hello" + fields=["title","content"] with cross_fields type
        // cross_fields: NOT (title:hello OR content:hello)
        String dsl = "NOT hello";
        String options = "{\"fields\":[\"title\",\"content\"],\"type\":\"cross_fields\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.NOT, plan.getRoot().getType());
        Assertions.assertEquals(1, plan.getRoot().getChildren().size());
        Assertions.assertEquals(QsClauseType.OR, plan.getRoot().getChildren().get(0).getType());
    }

    // ============ Tests for Multi-Field + Lucene Mode ============

    @Test
    public void testMultiFieldLuceneModeSimpleAnd() {
        // Test: "a AND b" + fields=["title","content"] + lucene mode + cross_fields
        // Expanded: "(title:a OR content:a) AND (title:b OR content:b)"
        // With Lucene semantics: both groups are MUST
        String dsl = "a AND b";
        String options = "{\"fields\":[\"title\",\"content\"],\"mode\":\"lucene\",\"minimum_should_match\":0,\"type\":\"cross_fields\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OCCUR_BOOLEAN, plan.getRoot().getType());

        // Should have 2 children (two OR groups), both with MUST
        // Note: In Lucene mode, OR groups are also wrapped as OCCUR_BOOLEAN
        Assertions.assertEquals(2, plan.getRoot().getChildren().size());
        for (QsNode child : plan.getRoot().getChildren()) {
            Assertions.assertEquals(QsOccur.MUST, child.getOccur());
            // The child is OCCUR_BOOLEAN wrapping the OR group
            Assertions.assertEquals(QsClauseType.OCCUR_BOOLEAN, child.getType());
        }
    }

    @Test
    public void testMultiFieldLuceneModeSimpleOr() {
        // Test: "a OR b" + fields=["title","content"] + lucene mode
        // Expanded: "(title:a OR content:a) OR (title:b OR content:b)"
        // With Lucene semantics: both groups are SHOULD
        String dsl = "a OR b";
        String options = "{\"fields\":[\"title\",\"content\"],\"mode\":\"lucene\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OCCUR_BOOLEAN, plan.getRoot().getType());

        // Should have 2 children, both with SHOULD
        Assertions.assertEquals(2, plan.getRoot().getChildren().size());
        for (QsNode child : plan.getRoot().getChildren()) {
            Assertions.assertEquals(QsOccur.SHOULD, child.getOccur());
        }

        // minimum_should_match should be 1
        Assertions.assertEquals(Integer.valueOf(1), plan.getRoot().getMinimumShouldMatch());
    }

    @Test
    public void testMultiFieldLuceneModeAndOrMixed() {
        // Test: "a AND b OR c" + fields=["title","content"] + lucene mode + minimum_should_match=0 + cross_fields
        // With Lucene semantics and minimum_should_match=0: SHOULD groups are discarded
        // Only "a" (MUST) remains - wrapped in OCCUR_BOOLEAN
        String dsl = "a AND b OR c";
        String options = "{\"fields\":[\"title\",\"content\"],\"mode\":\"lucene\",\"minimum_should_match\":0,\"type\":\"cross_fields\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        // With minimum_should_match=0, only (title:a OR content:a) remains
        // In Lucene mode, this is wrapped as OCCUR_BOOLEAN
        Assertions.assertEquals(QsClauseType.OCCUR_BOOLEAN, plan.getRoot().getType());
    }

    @Test
    public void testMultiFieldLuceneModeWithNot() {
        // Test: "a AND NOT b" + fields=["title","content"] + lucene mode + cross_fields
        // Expanded: "(title:a OR content:a) AND NOT (title:b OR content:b)"
        String dsl = "a AND NOT b";
        String options = "{\"fields\":[\"title\",\"content\"],\"mode\":\"lucene\",\"minimum_should_match\":0,\"type\":\"cross_fields\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OCCUR_BOOLEAN, plan.getRoot().getType());

        // Should have 2 children: a (MUST), b (MUST_NOT)
        Assertions.assertEquals(2, plan.getRoot().getChildren().size());

        // Find MUST and MUST_NOT children
        boolean hasMust = plan.getRoot().getChildren().stream().anyMatch(c -> c.getOccur() == QsOccur.MUST);
        boolean hasMustNot = plan.getRoot().getChildren().stream().anyMatch(c -> c.getOccur() == QsOccur.MUST_NOT);
        Assertions.assertTrue(hasMust);
        Assertions.assertTrue(hasMustNot);
    }

    @Test
    public void testMultiFieldLuceneModeSingleTerm() {
        // Test: single term with multi-field + lucene mode
        String dsl = "hello";
        String options = "{\"fields\":[\"title\",\"content\"],\"mode\":\"lucene\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        // In Lucene mode, even single term OR groups are wrapped as OCCUR_BOOLEAN
        Assertions.assertEquals(QsClauseType.OCCUR_BOOLEAN, plan.getRoot().getType());
        // The OCCUR_BOOLEAN contains the OR group's children with SHOULD occur
        Assertions.assertEquals(2, plan.getRoot().getChildren().size());
    }

    @Test
    public void testMultiFieldLuceneModeComplexQuery() {
        // Test: "(a OR b) AND NOT c" + fields=["f1","f2"] + lucene mode + cross_fields
        String dsl = "(a OR b) AND NOT c";
        String options = "{\"fields\":[\"f1\",\"f2\"],\"mode\":\"lucene\",\"minimum_should_match\":0,\"type\":\"cross_fields\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        // Should have proper structure with MUST and MUST_NOT
        Assertions.assertEquals(QsClauseType.OCCUR_BOOLEAN, plan.getRoot().getType());
    }

    @Test
    public void testMultiFieldLuceneModeMinimumShouldMatchOne() {
        // Test: "a AND b OR c" with minimum_should_match=1 keeps all clauses + cross_fields
        String dsl = "a AND b OR c";
        String options = "{\"fields\":[\"title\",\"content\"],\"mode\":\"lucene\",\"minimum_should_match\":1,\"type\":\"cross_fields\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OCCUR_BOOLEAN, plan.getRoot().getType());
        // All 3 groups should be present
        Assertions.assertEquals(3, plan.getRoot().getChildren().size());
        Assertions.assertEquals(Integer.valueOf(1), plan.getRoot().getMinimumShouldMatch());
    }

    // ============ Tests for type parameter (best_fields vs cross_fields) ============

    @Test
    public void testMultiFieldBestFieldsDefault() {
        // Test: best_fields is the default when type is not specified
        // "hello world" with fields ["title", "content"] and default_operator "and"
        // Expands to: (title:hello AND title:world) OR (content:hello AND content:world)
        String dsl = "hello world";
        String options = "{\"fields\":[\"title\",\"content\"],\"default_operator\":\"and\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        // Root should be OR (joining fields)
        Assertions.assertEquals(QsClauseType.OR, plan.getRoot().getType());
        Assertions.assertEquals(2, plan.getRoot().getChildren().size()); // 2 fields

        // Each child should be an AND of terms for that field
        for (QsNode fieldGroup : plan.getRoot().getChildren()) {
            Assertions.assertEquals(QsClauseType.AND, fieldGroup.getType());
            Assertions.assertEquals(2, fieldGroup.getChildren().size()); // 2 terms
        }
    }

    @Test
    public void testMultiFieldBestFieldsExplicit() {
        // Test: explicitly specify type=best_fields
        String dsl = "hello world";
        String options = "{\"fields\":[\"title\",\"content\"],\"default_operator\":\"and\",\"type\":\"best_fields\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OR, plan.getRoot().getType());
        Assertions.assertEquals(2, plan.getRoot().getChildren().size());
    }

    @Test
    public void testMultiFieldCrossFields() {
        // Test: cross_fields mode
        // "hello world" with fields ["title", "content"] and default_operator "and"
        // Expands to: (title:hello OR content:hello) AND (title:world OR content:world)
        String dsl = "hello world";
        String options = "{\"fields\":[\"title\",\"content\"],\"default_operator\":\"and\",\"type\":\"cross_fields\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        // Root should be AND (joining term groups)
        Assertions.assertEquals(QsClauseType.AND, plan.getRoot().getType());
        Assertions.assertEquals(2, plan.getRoot().getChildren().size()); // 2 term groups

        // Each child should be an OR of the same term across fields
        for (QsNode termGroup : plan.getRoot().getChildren()) {
            Assertions.assertEquals(QsClauseType.OR, termGroup.getType());
            Assertions.assertEquals(2, termGroup.getChildren().size()); // 2 fields
        }
    }

    @Test
    public void testMultiFieldBestFieldsLuceneMode() {
        // Test: best_fields with Lucene mode
        String dsl = "hello world";
        String options = "{\"fields\":[\"title\",\"content\"],\"default_operator\":\"and\",\"mode\":\"lucene\",\"type\":\"best_fields\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OCCUR_BOOLEAN, plan.getRoot().getType());
    }

    @Test
    public void testMultiFieldCrossFieldsLuceneMode() {
        // Test: cross_fields with Lucene mode
        String dsl = "hello world";
        String options = "{\"fields\":[\"title\",\"content\"],\"default_operator\":\"and\",\"mode\":\"lucene\",\"type\":\"cross_fields\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OCCUR_BOOLEAN, plan.getRoot().getType());
    }

    @Test
    public void testMultiFieldInvalidType() {
        // Test: invalid type value should throw exception
        String dsl = "hello";
        String options = "{\"fields\":[\"title\",\"content\"],\"type\":\"invalid_type\"}";

        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            SearchDslParser.parseDsl(dsl, options);
        });
    }

    @Test
    public void testMultiFieldSingleTermSameResultForBothTypes() {
        // Test: single term should have same structure for both types
        // since there's only one term, no difference between best_fields and cross_fields
        String dsl = "hello";
        String optionsBestFields = "{\"fields\":[\"title\",\"content\"],\"type\":\"best_fields\"}";
        String optionsCrossFields = "{\"fields\":[\"title\",\"content\"],\"type\":\"cross_fields\"}";

        QsPlan planBest = SearchDslParser.parseDsl(dsl, optionsBestFields);
        QsPlan planCross = SearchDslParser.parseDsl(dsl, optionsCrossFields);

        Assertions.assertNotNull(planBest);
        Assertions.assertNotNull(planCross);
        // Both should have same structure: (title:hello OR content:hello)
        Assertions.assertEquals(planBest.getRoot().getType(), planCross.getRoot().getType());
        Assertions.assertEquals(planBest.getRoot().getChildren().size(), planCross.getRoot().getChildren().size());
    }

    // =====================================================================
    // Tests for bare query support (without field: prefix)
    // These tests verify the single-phase parsing where ANTLR grammar
    // natively supports bareQuery and visitor fills in default field
    // =====================================================================

    @Test
    public void testBareTermWithDefaultField() {
        // Bare term without field prefix - uses default_field
        String dsl = "hello";
        String options = "{\"default_field\":\"title\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.TERM, plan.getRoot().getType());
        Assertions.assertEquals("title", plan.getRoot().getField());
        Assertions.assertEquals("hello", plan.getRoot().getValue());
    }

    @Test
    public void testBareRegexWithDefaultField() {
        // Bare regex without field prefix - uses default_field
        String dsl = "/[a-z]+/";
        String options = "{\"default_field\":\"title\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.REGEXP, plan.getRoot().getType());
        Assertions.assertEquals("title", plan.getRoot().getField());
        Assertions.assertEquals("[a-z]+", plan.getRoot().getValue());
    }

    @Test
    public void testBareTermNotWithRegex() {
        // DORIS-24368 scenario: bare term NOT bare regex
        // "Anthony NOT /(\\d{1,2}:\\d{2} [AP]M)/"
        String dsl = "Anthony NOT /(\\d{1,2}:\\d{2} [AP]M)/";
        String options = "{\"default_field\":\"title\",\"default_operator\":\"AND\",\"mode\":\"lucene\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        // Should parse correctly without mangling the regex
        Assertions.assertEquals(QsClauseType.OCCUR_BOOLEAN, plan.getRoot().getType());
    }

    @Test
    public void testMixedFieldAndBareQuery() {
        // Mixed: explicit field + bare query
        // "content:/[a-z]+/ AND hello" where hello uses default_field=title
        String dsl = "content:/[a-z]+/ AND hello";
        String options = "{\"default_field\":\"title\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.AND, plan.getRoot().getType());
        Assertions.assertEquals(2, plan.getRoot().getChildren().size());

        // First child: content:/[a-z]+/
        QsNode first = plan.getRoot().getChildren().get(0);
        Assertions.assertEquals(QsClauseType.REGEXP, first.getType());
        Assertions.assertEquals("content", first.getField());

        // Second child: hello with default_field=title
        QsNode second = plan.getRoot().getChildren().get(1);
        Assertions.assertEquals(QsClauseType.TERM, second.getType());
        Assertions.assertEquals("title", second.getField());
    }

    @Test
    public void testRegexWithOperatorKeywordsInside() {
        // Regex containing AND/OR keywords - should not be treated as operators
        String dsl = "/pattern AND stuff/";
        String options = "{\"default_field\":\"title\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.REGEXP, plan.getRoot().getType());
        Assertions.assertEquals("title", plan.getRoot().getField());
        // The AND inside regex should be preserved literally
        Assertions.assertEquals("pattern AND stuff", plan.getRoot().getValue());
    }

    @Test
    public void testRegexWithSpacesAndColons() {
        // Regex containing spaces and colons - should not break parsing
        String dsl = "/\\d{1,2}:\\d{2} [AP]M/";
        String options = "{\"default_field\":\"title\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.REGEXP, plan.getRoot().getType());
        Assertions.assertEquals("title", plan.getRoot().getField());
        Assertions.assertEquals("\\d{1,2}:\\d{2} [AP]M", plan.getRoot().getValue());
    }

    @Test
    public void testBarePhraseWithDefaultField() {
        // Bare phrase without field prefix
        String dsl = "\"hello world\"";
        String options = "{\"default_field\":\"content\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.PHRASE, plan.getRoot().getType());
        Assertions.assertEquals("content", plan.getRoot().getField());
        Assertions.assertEquals("hello world", plan.getRoot().getValue());
    }

    @Test
    public void testBarePrefixWithDefaultField() {
        // Bare prefix query without field prefix
        String dsl = "hello*";
        String options = "{\"default_field\":\"title\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.PREFIX, plan.getRoot().getType());
        Assertions.assertEquals("title", plan.getRoot().getField());
        Assertions.assertEquals("hello*", plan.getRoot().getValue());
    }

    @Test
    public void testBareWildcardWithDefaultField() {
        // Bare wildcard query without field prefix
        String dsl = "h*llo";
        String options = "{\"default_field\":\"title\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.WILDCARD, plan.getRoot().getType());
        Assertions.assertEquals("title", plan.getRoot().getField());
        Assertions.assertEquals("h*llo", plan.getRoot().getValue());
    }

    @Test
    public void testBareQueryWithoutDefaultFieldThrows() {
        // Bare query without default_field should throw an error
        String dsl = "hello";
        String options = "{}";  // No default_field

        Assertions.assertThrows(RuntimeException.class, () -> {
            SearchDslParser.parseDsl(dsl, options);
        });
    }

    @Test
    public void testBareAndOperatorWithDefaultField() {
        // "hello AND world" with default_field
        String dsl = "hello AND world";
        String options = "{\"default_field\":\"title\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.AND, plan.getRoot().getType());
        Assertions.assertEquals(2, plan.getRoot().getChildren().size());

        for (QsNode child : plan.getRoot().getChildren()) {
            Assertions.assertEquals(QsClauseType.TERM, child.getType());
            Assertions.assertEquals("title", child.getField());
        }
    }

    @Test
    public void testBareOrOperatorWithDefaultField() {
        // "hello OR world" with default_field
        String dsl = "hello OR world";
        String options = "{\"default_field\":\"title\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OR, plan.getRoot().getType());
        Assertions.assertEquals(2, plan.getRoot().getChildren().size());

        for (QsNode child : plan.getRoot().getChildren()) {
            Assertions.assertEquals(QsClauseType.TERM, child.getType());
            Assertions.assertEquals("title", child.getField());
        }
    }

    @Test
    public void testBareNotOperatorWithDefaultField() {
        // "NOT hello" with default_field
        String dsl = "NOT hello";
        String options = "{\"default_field\":\"title\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.NOT, plan.getRoot().getType());
        Assertions.assertEquals(1, plan.getRoot().getChildren().size());

        QsNode child = plan.getRoot().getChildren().get(0);
        Assertions.assertEquals(QsClauseType.TERM, child.getType());
        Assertions.assertEquals("title", child.getField());
    }

    @Test
    public void testComplexBareQueryLuceneMode() {
        // Complex bare query in Lucene mode
        // "a AND b OR NOT c" with default_field
        String dsl = "a AND b OR NOT c";
        String options = "{\"default_field\":\"title\",\"mode\":\"lucene\",\"minimum_should_match\":0}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OCCUR_BOOLEAN, plan.getRoot().getType());
    }

    @Test
    public void testParenthesizedBareQuery() {
        // "(hello OR world) AND foo" with default_field
        String dsl = "(hello OR world) AND foo";
        String options = "{\"default_field\":\"title\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.AND, plan.getRoot().getType());
        Assertions.assertEquals(2, plan.getRoot().getChildren().size());

        // First child should be OR
        QsNode orNode = plan.getRoot().getChildren().get(0);
        Assertions.assertEquals(QsClauseType.OR, orNode.getType());

        // Second child should be TERM
        QsNode termNode = plan.getRoot().getChildren().get(1);
        Assertions.assertEquals(QsClauseType.TERM, termNode.getType());
        Assertions.assertEquals("title", termNode.getField());
    }
}
