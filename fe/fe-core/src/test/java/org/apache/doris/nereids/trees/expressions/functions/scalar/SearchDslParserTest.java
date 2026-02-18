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
        // Lucene addClause semantics (left-to-right, no precedence, default_operator=OR):
        //   a(CONJ_NONE)→SHOULD, b(CONJ_AND)→prev MUST, b MUST, c(CONJ_OR)→SHOULD (prev unchanged)
        //   Result: [MUST(a), MUST(b), SHOULD(c)] with msm=0
        //   ES: +a +b c  (SHOULD(c) kept, not filtered — msm=0 means optional, not removed)
        String dsl = "field:a AND field:b OR field:c";
        String options = "{\"mode\":\"lucene\",\"minimum_should_match\":0}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OCCUR_BOOLEAN, plan.getRoot().getType());
        Assertions.assertEquals(3, plan.getRoot().getChildren().size());

        QsNode nodeA = plan.getRoot().getChildren().get(0);
        Assertions.assertEquals("a", nodeA.getValue());
        Assertions.assertEquals(SearchDslParser.QsOccur.MUST, nodeA.getOccur());

        QsNode nodeB = plan.getRoot().getChildren().get(1);
        Assertions.assertEquals("b", nodeB.getValue());
        Assertions.assertEquals(SearchDslParser.QsOccur.MUST, nodeB.getOccur());

        QsNode nodeC = plan.getRoot().getChildren().get(2);
        Assertions.assertEquals("c", nodeC.getValue());
        Assertions.assertEquals(SearchDslParser.QsOccur.SHOULD, nodeC.getOccur());
    }

    @Test
    public void testLuceneModeAndOrNotMixed() {
        // Test: "a AND b OR NOT c AND d" in Lucene mode
        // Lucene addClause semantics (left-to-right, no precedence):
        //   a(CONJ_NONE)→SHOULD, b(CONJ_AND)→prev MUST, b MUST,
        //   NOT c(CONJ_OR, MOD_NOT)→MUST_NOT (prev unchanged with OR_OPERATOR),
        //   d(CONJ_AND)→prev(c) skip (MUST_NOT), d MUST
        //   Result: [MUST(a), MUST(b), MUST_NOT(c), MUST(d)] = +a +b -c +d
        String dsl = "field:a AND field:b OR NOT field:c AND field:d";
        String options = "{\"mode\":\"lucene\",\"minimum_should_match\":0}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OCCUR_BOOLEAN, plan.getRoot().getType());

        // Should have 4 children: a(MUST), b(MUST), c(MUST_NOT), d(MUST)
        Assertions.assertEquals(4, plan.getRoot().getChildren().size());

        QsNode nodeA = plan.getRoot().getChildren().get(0);
        Assertions.assertEquals("a", nodeA.getValue());
        Assertions.assertEquals(SearchDslParser.QsOccur.MUST, nodeA.getOccur());

        QsNode nodeB = plan.getRoot().getChildren().get(1);
        Assertions.assertEquals("b", nodeB.getValue());
        Assertions.assertEquals(SearchDslParser.QsOccur.MUST, nodeB.getOccur());

        QsNode nodeC = plan.getRoot().getChildren().get(2);
        Assertions.assertEquals("c", nodeC.getValue());
        Assertions.assertEquals(SearchDslParser.QsOccur.MUST_NOT, nodeC.getOccur());

        QsNode nodeD = plan.getRoot().getChildren().get(3);
        Assertions.assertEquals("d", nodeD.getValue());
        Assertions.assertEquals(SearchDslParser.QsOccur.MUST, nodeD.getOccur());
    }

    @Test
    public void testLuceneModeWithDefaultField() {
        // Test: Lucene mode with default field expansion
        // Lucene addClause semantics with default_operator=AND (AND_OPERATOR):
        //   aterm(CONJ_NONE)→MUST, bterm(CONJ_AND)→prev MUST, bterm MUST,
        //   cterm(CONJ_OR)→SHOULD + prev(bterm) becomes SHOULD (AND_OPERATOR + CONJ_OR)
        //   Result: [MUST(aterm), SHOULD(bterm), SHOULD(cterm)] with msm=0
        //   ES: +aterm bterm cterm
        String dsl = "aterm AND bterm OR cterm";
        String options = "{\"default_field\":\"firstname\",\"default_operator\":\"and\","
                + "\"mode\":\"lucene\",\"minimum_should_match\":0}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OCCUR_BOOLEAN, plan.getRoot().getType());
        Assertions.assertEquals(3, plan.getRoot().getChildren().size());

        QsNode nodeA = plan.getRoot().getChildren().get(0);
        Assertions.assertEquals("firstname", nodeA.getField());
        Assertions.assertEquals("aterm", nodeA.getValue());
        Assertions.assertEquals(SearchDslParser.QsOccur.MUST, nodeA.getOccur());

        QsNode nodeB = plan.getRoot().getChildren().get(1);
        Assertions.assertEquals("bterm", nodeB.getValue());
        Assertions.assertEquals(SearchDslParser.QsOccur.SHOULD, nodeB.getOccur());

        QsNode nodeC = plan.getRoot().getChildren().get(2);
        Assertions.assertEquals("cterm", nodeC.getValue());
        Assertions.assertEquals(SearchDslParser.QsOccur.SHOULD, nodeC.getOccur());
    }

    @Test
    public void testLuceneModeNotOperator() {
        // Test: "NOT a" in Lucene mode
        // Pure NOT queries are rewritten to: SHOULD(MATCH_ALL_DOCS) + MUST_NOT(term)
        // with minimum_should_match=1, following ES/Lucene semantics where pure NOT
        // should return all documents EXCEPT those matching the NOT clause
        String dsl = "NOT field:a";
        String options = "{\"mode\":\"lucene\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OCCUR_BOOLEAN, plan.getRoot().getType());
        Assertions.assertEquals(2, plan.getRoot().getChildren().size());
        Assertions.assertEquals(Integer.valueOf(1), plan.getRoot().getMinimumShouldMatch());

        // First child: MATCH_ALL_DOCS with SHOULD
        QsNode matchAllNode = plan.getRoot().getChildren().get(0);
        Assertions.assertEquals(QsClauseType.MATCH_ALL_DOCS, matchAllNode.getType());
        Assertions.assertEquals(QsOccur.SHOULD, matchAllNode.getOccur());

        // Second child: TERM with MUST_NOT
        QsNode termNode = plan.getRoot().getChildren().get(1);
        Assertions.assertEquals(QsClauseType.TERM, termNode.getType());
        Assertions.assertEquals(QsOccur.MUST_NOT, termNode.getOccur());
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
    public void testEscapedSpaceInBareQueryLuceneMode() {
        // Test: "Josh\ Brolin" (bare query, no field prefix) in lucene mode
        // Should be treated as a single term "Josh Brolin", not split into two terms
        String dsl = "Josh\\ Brolin";
        String optionsJson = "{\"default_field\":\"title\",\"default_operator\":\"AND\","
                + "\"mode\":\"lucene\",\"minimum_should_match\":0}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, optionsJson);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.TERM, plan.getRoot().getType());
        Assertions.assertEquals("title", plan.getRoot().getField());
        Assertions.assertEquals("Josh Brolin", plan.getRoot().getValue());
        // defaultOperator must be lowercase for BE case-sensitive comparison
        Assertions.assertEquals("and", plan.getDefaultOperator());
    }

    @Test
    public void testDefaultOperatorNormalization() {
        // Verify defaultOperator is always normalized to lowercase in the plan,
        // regardless of the case used in the options JSON.
        // BE compares case-sensitively: (default_operator == "and")
        String dsl = "foo bar";
        String optionsJson = "{\"default_field\":\"title\",\"default_operator\":\"AND\","
                + "\"mode\":\"lucene\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, optionsJson);
        Assertions.assertEquals("and", plan.getDefaultOperator());

        optionsJson = "{\"default_field\":\"title\",\"default_operator\":\"OR\","
                + "\"mode\":\"lucene\"}";
        plan = SearchDslParser.parseDsl(dsl, optionsJson);
        Assertions.assertEquals("or", plan.getDefaultOperator());
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
        // Lowercase 'and' is NOT an operator in ANTLR grammar (case-sensitive).
        // With bareQuery rule, it's parsed as a bare term without field.
        // Without default_field, bare term throws exception.
        String dsl = "field:a and field:b";
        Assertions.assertThrows(RuntimeException.class, () -> {
            SearchDslParser.parseDsl(dsl);
        });
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
        // Lowercase 'or' is NOT an operator in ANTLR grammar (case-sensitive).
        // With bareQuery rule, it's parsed as a bare term without field.
        // Without default_field, bare term throws exception.
        String dsl = "field:a or field:b";
        Assertions.assertThrows(RuntimeException.class, () -> {
            SearchDslParser.parseDsl(dsl);
        });
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
        // Lowercase 'not' is NOT an operator in ANTLR grammar (case-sensitive).
        // With bareQuery rule, it's parsed as a bare term without field.
        // Without default_field, bare term throws exception.
        String dsl = "not field:spam";
        Assertions.assertThrows(RuntimeException.class, () -> {
            SearchDslParser.parseDsl(dsl);
        });
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
    public void testMultiFieldExplicitFieldInFieldsList() {
        // Bug fix: explicit field prefix should NOT be expanded even when the field IS in the fields list
        // ES query_string always respects explicit "field:term" syntax regardless of the fields parameter.
        // "title:music AND content:history" with fields=["title","content"]
        // → title:music AND content:history (NOT expanded to multi-field OR)
        String dsl = "title:music AND content:history";
        String options = "{\"fields\":[\"title\",\"content\"],\"type\":\"cross_fields\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.AND, plan.getRoot().getType());
        Assertions.assertEquals(2, plan.getRoot().getChildren().size());

        // First child: title:music - NOT expanded
        QsNode first = plan.getRoot().getChildren().get(0);
        Assertions.assertEquals(QsClauseType.TERM, first.getType());
        Assertions.assertEquals("title", first.getField());
        Assertions.assertEquals("music", first.getValue());

        // Second child: content:history - NOT expanded
        QsNode second = plan.getRoot().getChildren().get(1);
        Assertions.assertEquals(QsClauseType.TERM, second.getType());
        Assertions.assertEquals("content", second.getField());
        Assertions.assertEquals("history", second.getValue());
    }

    @Test
    public void testMultiFieldExplicitFieldInFieldsListBestFields() {
        // Same test as above but with best_fields type
        String dsl = "title:music AND content:history";
        String options = "{\"fields\":[\"title\",\"content\"],\"type\":\"best_fields\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        // best_fields wraps in OR for multi-field, but explicit fields should be preserved in each copy
        QsNode root = plan.getRoot();
        Assertions.assertEquals(QsClauseType.OR, root.getType());
        Assertions.assertEquals(2, root.getChildren().size());

        // Each OR branch should have AND(title:music, content:history) - both explicit fields preserved
        for (QsNode branch : root.getChildren()) {
            Assertions.assertEquals(QsClauseType.AND, branch.getType());
            Assertions.assertEquals(2, branch.getChildren().size());

            QsNode titleNode = branch.getChildren().get(0);
            Assertions.assertEquals("title", titleNode.getField());
            Assertions.assertEquals("music", titleNode.getValue());

            QsNode contentNode = branch.getChildren().get(1);
            Assertions.assertEquals("content", contentNode.getField());
            Assertions.assertEquals("history", contentNode.getValue());
        }
    }

    @Test
    public void testMultiFieldMixedExplicitAndBareQuery() {
        // "title:football AND american" with fields=["title","content"]
        // → title:football AND (title:american OR content:american)
        // title:football should NOT be expanded; "american" (bare) should be expanded
        String dsl = "title:football AND american";
        String options = "{\"fields\":[\"title\",\"content\"],\"type\":\"cross_fields\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.AND, plan.getRoot().getType());
        Assertions.assertEquals(2, plan.getRoot().getChildren().size());

        // First child: title:football - NOT expanded (explicit field)
        QsNode first = plan.getRoot().getChildren().get(0);
        Assertions.assertEquals(QsClauseType.TERM, first.getType());
        Assertions.assertEquals("title", first.getField());
        Assertions.assertEquals("football", first.getValue());

        // Second child: (title:american OR content:american) - expanded (bare term)
        QsNode second = plan.getRoot().getChildren().get(1);
        Assertions.assertEquals(QsClauseType.OR, second.getType());
        Assertions.assertEquals(2, second.getChildren().size());
    }

    @Test
    public void testMultiFieldLuceneModeExplicitFieldInFieldsList() {
        // Lucene mode: "title:music AND content:history" with fields=["title","content"]
        // Explicit fields should be preserved, not expanded
        String dsl = "title:music AND content:history";
        String options = "{\"fields\":[\"title\",\"content\"],\"default_operator\":\"and\","
                + "\"mode\":\"lucene\",\"type\":\"cross_fields\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        QsNode root = plan.getRoot();
        Assertions.assertEquals(QsClauseType.OCCUR_BOOLEAN, root.getType());
        Assertions.assertEquals(2, root.getChildren().size());

        // Both children should be leaf TERM nodes (not expanded to OCCUR_BOOLEAN wrappers)
        QsNode first = root.getChildren().get(0);
        Assertions.assertEquals(QsClauseType.TERM, first.getType());
        Assertions.assertEquals("title", first.getField());
        Assertions.assertEquals("music", first.getValue());

        QsNode second = root.getChildren().get(1);
        Assertions.assertEquals(QsClauseType.TERM, second.getType());
        Assertions.assertEquals("content", second.getField());
        Assertions.assertEquals("history", second.getValue());
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
        // With no default_operator (default is OR_OPERATOR in Lucene):
        //   a=MUST (promoted by AND), b=MUST (from AND), c=SHOULD (from OR)
        //   With OR_OPERATOR, OR does NOT change preceding term's occur
        // msm is ignored for multi-field mode, node-level msm defaults to 0 (since MUST exists)
        String dsl = "a AND b OR c";
        String options = "{\"fields\":[\"title\",\"content\"],\"mode\":\"lucene\",\"minimum_should_match\":0,\"type\":\"cross_fields\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        // Root is OCCUR_BOOLEAN with 3 children: MUST(a), MUST(b), SHOULD(c)
        Assertions.assertEquals(QsClauseType.OCCUR_BOOLEAN, plan.getRoot().getType());
        Assertions.assertEquals(3, plan.getRoot().getChildren().size());
        // a and b are MUST, c is SHOULD
        Assertions.assertEquals(QsOccur.MUST, plan.getRoot().getChildren().get(0).getOccur());
        Assertions.assertEquals(QsOccur.MUST, plan.getRoot().getChildren().get(1).getOccur());
        Assertions.assertEquals(QsOccur.SHOULD, plan.getRoot().getChildren().get(2).getOccur());
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
        // Test: "a AND b OR c" with minimum_should_match=1 + cross_fields + multi-field
        // For multi-field mode (fields.size() > 1), minimum_should_match is nullified.
        // Lucene addClause with default_operator=OR: [MUST(a), MUST(b), SHOULD(c)] msm=0
        // No SHOULD filtering — all 3 terms kept, each expanded to 2 fields via cross_fields
        String dsl = "a AND b OR c";
        String options = "{\"fields\":[\"title\",\"content\"],\"mode\":\"lucene\",\"minimum_should_match\":1,\"type\":\"cross_fields\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OCCUR_BOOLEAN, plan.getRoot().getType());
        // 3 terms (a, b, c), each expanded to cross_fields OCCUR_BOOLEAN
        Assertions.assertEquals(3, plan.getRoot().getChildren().size());
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
        // Test: best_fields with Lucene mode uses per-clause expansion (matching ES query_string)
        // "hello world" with AND → each term independently expanded across fields:
        //   MUST(SHOULD(title:hello, content:hello)) AND MUST(SHOULD(title:world, content:world))
        String dsl = "hello world";
        String options = "{\"fields\":[\"title\",\"content\"],\"default_operator\":\"and\",\"mode\":\"lucene\",\"type\":\"best_fields\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OCCUR_BOOLEAN, plan.getRoot().getType());
        // Per-clause expansion: 2 children (one per term), each expanded across fields
        Assertions.assertEquals(2, plan.getRoot().getChildren().size());
        for (QsNode child : plan.getRoot().getChildren()) {
            // Each child is an OCCUR_BOOLEAN wrapping the per-field expansion
            Assertions.assertEquals(QsClauseType.OCCUR_BOOLEAN, child.getType());
            Assertions.assertEquals(2, child.getChildren().size()); // one per field
        }
    }

    @Test
    public void testMultiFieldBestFieldsLuceneModePerClauseExpansion() {
        // Test: best_fields with phrase + regex uses per-clause expansion (not per-field)
        // ES query_string expands each clause independently across fields:
        //   ("Costner" AND /Li../) → MUST(title:"Costner" | content:"Costner") AND MUST(title:/Li../ | content:/Li../)
        // NOT: (title:"Costner" AND title:/Li../) OR (content:"Costner" AND content:/Li../)
        String dsl = "\"Costner\" /Li../";
        String options = "{\"fields\":[\"title\",\"content\"],\"default_operator\":\"and\",\"mode\":\"lucene\",\"type\":\"best_fields\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        QsNode root = plan.getRoot();
        Assertions.assertEquals(QsClauseType.OCCUR_BOOLEAN, root.getType());
        // 2 children: one for phrase "Costner", one for regex /Li../
        Assertions.assertEquals(2, root.getChildren().size());

        // First child: phrase "Costner" expanded across fields
        QsNode phraseGroup = root.getChildren().get(0);
        Assertions.assertEquals(QsClauseType.OCCUR_BOOLEAN, phraseGroup.getType());
        Assertions.assertEquals(2, phraseGroup.getChildren().size());
        Assertions.assertEquals(QsClauseType.PHRASE, phraseGroup.getChildren().get(0).getType());
        Assertions.assertEquals(QsClauseType.PHRASE, phraseGroup.getChildren().get(1).getType());

        // Second child: regex /Li../ expanded across fields
        QsNode regexpGroup = root.getChildren().get(1);
        Assertions.assertEquals(QsClauseType.OCCUR_BOOLEAN, regexpGroup.getType());
        Assertions.assertEquals(2, regexpGroup.getChildren().size());
        Assertions.assertEquals(QsClauseType.REGEXP, regexpGroup.getChildren().get(0).getType());
        Assertions.assertEquals(QsClauseType.REGEXP, regexpGroup.getChildren().get(1).getType());
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

    // =====================================================================
    // Hubspot-specific tests
    // =====================================================================

    @Test
    public void testPhraseWithImplicitOrOperator() {
        // Test: '"2003 NBA draft" Darrell' with default_operator=OR should produce same result as
        // '"2003 NBA draft" OR Darrell'
        String dsl1 = "\"2003 NBA draft\" Darrell";
        String dsl2 = "\"2003 NBA draft\" OR Darrell";
        String options = "{\"default_field\":\"title\",\"default_operator\":\"OR\","
                + "\"mode\":\"lucene\",\"minimum_should_match\":0}";

        QsPlan plan1 = SearchDslParser.parseDsl(dsl1, options);
        QsPlan plan2 = SearchDslParser.parseDsl(dsl2, options);

        Assertions.assertNotNull(plan1);
        Assertions.assertNotNull(plan2);

        // Both should have the same structure - OCCUR_BOOLEAN with 2 SHOULD children
        Assertions.assertEquals(plan2.getRoot().getType(), plan1.getRoot().getType());
        Assertions.assertEquals(plan2.getRoot().getChildren().size(), plan1.getRoot().getChildren().size());

        // Verify the phrase is preserved as PHRASE type, not broken into terms
        boolean hasPhrase1 = plan1.getRoot().getChildren().stream()
                .anyMatch(n -> n.getType() == QsClauseType.PHRASE);
        boolean hasPhrase2 = plan2.getRoot().getChildren().stream()
                .anyMatch(n -> n.getType() == QsClauseType.PHRASE);
        Assertions.assertTrue(hasPhrase1, "Plan 1 should contain a PHRASE node");
        Assertions.assertTrue(hasPhrase2, "Plan 2 should contain a PHRASE node");
    }

    @Test
    public void testPhraseWithImplicitAndOperator() {
        // Test: '"hello world" foo' with default_operator=AND
        String dsl = "\"hello world\" foo";
        String options = "{\"default_field\":\"title\",\"default_operator\":\"AND\"}";

        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        // Should create AND query: title:"hello world" AND title:foo
        Assertions.assertEquals(QsClauseType.AND, plan.getRoot().getType());
        Assertions.assertEquals(2, plan.getRoot().getChildren().size());

        // Verify the phrase is preserved
        boolean hasPhrase = plan.getRoot().getChildren().stream()
                .anyMatch(n -> n.getType() == QsClauseType.PHRASE);
        Assertions.assertTrue(hasPhrase, "Should contain a PHRASE node");
    }

    @Test
    public void testMultiplePhrases() {
        // Test: '"hello world" "foo bar"' with default_operator=OR
        String dsl = "\"hello world\" \"foo bar\"";
        String options = "{\"default_field\":\"title\",\"default_operator\":\"OR\"}";

        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OR, plan.getRoot().getType());
        Assertions.assertEquals(2, plan.getRoot().getChildren().size());

        // Both children should be PHRASE type
        for (QsNode child : plan.getRoot().getChildren()) {
            Assertions.assertEquals(QsClauseType.PHRASE, child.getType());
        }
    }

    // ============ Tests for Standalone Wildcard * ============

    @Test
    public void testStandaloneWildcardWithAnd() {
        // Test: "Dollar AND *" should produce: MUST(title:Dollar) AND MUST(MATCH_ALL_DOCS)
        // Standalone "*" becomes MATCH_ALL_DOCS (matches ES behavior: field:* → ExistsQuery)
        String dsl = "Dollar AND *";
        String options = "{\"default_field\":\"title\",\"default_operator\":\"OR\","
                + "\"mode\":\"lucene\",\"minimum_should_match\":0}";

        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OCCUR_BOOLEAN, plan.getRoot().getType());
        Assertions.assertEquals(2, plan.getRoot().getChildren().size());

        // Both children should have MUST occur (AND)
        for (QsNode child : plan.getRoot().getChildren()) {
            Assertions.assertEquals(QsOccur.MUST, child.getOccur());
        }

        // One should be TERM (Dollar), one should be MATCH_ALL_DOCS
        boolean hasTerm = plan.getRoot().getChildren().stream()
                .anyMatch(n -> n.getType() == QsClauseType.TERM && "Dollar".equals(n.getValue()));
        boolean hasMatchAll = plan.getRoot().getChildren().stream()
                .anyMatch(n -> n.getType() == QsClauseType.MATCH_ALL_DOCS);

        Assertions.assertTrue(hasTerm, "Should contain TERM node for 'Dollar'");
        Assertions.assertTrue(hasMatchAll, "Should contain MATCH_ALL_DOCS node for '*'");
    }

    @Test
    public void testStandaloneWildcardAlone() {
        // Test: "*" alone becomes MATCH_ALL_DOCS (matches ES behavior: field:* → ExistsQuery)
        String dsl = "*";
        String options = "{\"default_field\":\"title\",\"default_operator\":\"OR\"}";

        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.MATCH_ALL_DOCS, plan.getRoot().getType());
    }

    @Test
    public void testStandaloneWildcardWithOr() {
        // Test: "Dollar OR *" should produce: SHOULD(title:Dollar) OR SHOULD(MATCH_ALL_DOCS)
        // Standalone "*" becomes MATCH_ALL_DOCS (matches ES behavior: field:* → ExistsQuery)
        String dsl = "Dollar OR *";
        String options = "{\"default_field\":\"title\",\"default_operator\":\"OR\","
                + "\"mode\":\"lucene\",\"minimum_should_match\":0}";

        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OCCUR_BOOLEAN, plan.getRoot().getType());
        Assertions.assertEquals(2, plan.getRoot().getChildren().size());

        // Both children should have SHOULD occur (OR)
        for (QsNode child : plan.getRoot().getChildren()) {
            Assertions.assertEquals(QsOccur.SHOULD, child.getOccur());
        }

        // One should be TERM (Dollar), one should be MATCH_ALL_DOCS
        boolean hasTerm = plan.getRoot().getChildren().stream()
                .anyMatch(n -> n.getType() == QsClauseType.TERM && "Dollar".equals(n.getValue()));
        boolean hasMatchAll = plan.getRoot().getChildren().stream()
                .anyMatch(n -> n.getType() == QsClauseType.MATCH_ALL_DOCS);

        Assertions.assertTrue(hasTerm, "Should contain TERM node for 'Dollar'");
        Assertions.assertTrue(hasMatchAll, "Should contain MATCH_ALL_DOCS node for '*'");
    }
}
