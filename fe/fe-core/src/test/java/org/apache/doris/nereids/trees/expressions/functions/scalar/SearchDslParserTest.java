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

    // ============ Tests for Lucene Mode Parsing ============

    @Test
    public void testLuceneModeSimpleAndQuery() {
        // Test: "a AND b" in Lucene mode → both MUST
        String dsl = "field:a AND field:b";
        String options = "{\"mode\":\"lucene\",\"minimum_should_match\":0}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OCCUR_BOOLEAN, plan.root.type);
        Assertions.assertEquals(2, plan.root.children.size());
        Assertions.assertEquals(Integer.valueOf(0), plan.root.minimumShouldMatch);

        // Both children should have MUST occur
        for (QsNode child : plan.root.children) {
            Assertions.assertEquals(SearchDslParser.QsOccur.MUST, child.occur);
        }
    }

    @Test
    public void testLuceneModeSimpleOrQuery() {
        // Test: "a OR b OR c" in Lucene mode → all SHOULD, at least one must match
        String dsl = "field:a OR field:b OR field:c";
        String options = "{\"mode\":\"lucene\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OCCUR_BOOLEAN, plan.root.type);
        Assertions.assertEquals(3, plan.root.children.size());

        // All children should have SHOULD occur
        for (QsNode child : plan.root.children) {
            Assertions.assertEquals(SearchDslParser.QsOccur.SHOULD, child.occur);
        }

        // minimum_should_match should be 1 (at least one must match)
        Assertions.assertEquals(Integer.valueOf(1), plan.root.minimumShouldMatch);
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
        Assertions.assertEquals(QsClauseType.TERM, plan.root.type);
        Assertions.assertEquals("field", plan.root.field);
        Assertions.assertEquals("a", plan.root.value);
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
        Assertions.assertEquals(QsClauseType.OCCUR_BOOLEAN, plan.root.type);

        // Should have 3 children: a(MUST), c(MUST_NOT), d(MUST)
        // b is filtered out because it becomes SHOULD
        Assertions.assertEquals(3, plan.root.children.size());

        QsNode nodeA = plan.root.children.get(0);
        Assertions.assertEquals("a", nodeA.value);
        Assertions.assertEquals(SearchDslParser.QsOccur.MUST, nodeA.occur);

        QsNode nodeC = plan.root.children.get(1);
        Assertions.assertEquals("c", nodeC.value);
        Assertions.assertEquals(SearchDslParser.QsOccur.MUST_NOT, nodeC.occur);

        QsNode nodeD = plan.root.children.get(2);
        Assertions.assertEquals("d", nodeD.value);
        Assertions.assertEquals(SearchDslParser.QsOccur.MUST, nodeD.occur);
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
        Assertions.assertEquals(QsClauseType.TERM, plan.root.type);
        Assertions.assertEquals("firstname", plan.root.field);
        Assertions.assertEquals("aterm", plan.root.value);
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
        Assertions.assertEquals(QsClauseType.OCCUR_BOOLEAN, plan.root.type);
        Assertions.assertEquals(1, plan.root.children.size());
        Assertions.assertEquals(QsClauseType.TERM, plan.root.children.get(0).type);
        Assertions.assertEquals(QsOccur.MUST_NOT, plan.root.children.get(0).occur);
    }

    @Test
    public void testLuceneModeMinimumShouldMatchExplicit() {
        // Test: explicit minimum_should_match=1 keeps SHOULD clauses
        String dsl = "field:a AND field:b OR field:c";
        String options = "{\"mode\":\"lucene\",\"minimum_should_match\":1}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OCCUR_BOOLEAN, plan.root.type);
        // All 3 terms should be present
        Assertions.assertEquals(3, plan.root.children.size());
        Assertions.assertEquals(Integer.valueOf(1), plan.root.minimumShouldMatch);
    }

    @Test
    public void testLuceneModeSingleTerm() {
        // Test: single term should not create OCCUR_BOOLEAN wrapper
        String dsl = "field:hello";
        String options = "{\"mode\":\"lucene\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.TERM, plan.root.type);
        Assertions.assertEquals("field", plan.root.field);
        Assertions.assertEquals("hello", plan.root.value);
    }

    @Test
    public void testStandardModeUnchanged() {
        // Test: standard mode (default) should work as before
        String dsl = "field:a AND field:b OR field:c";
        QsPlan plan = SearchDslParser.parseDsl(dsl, (String) null);

        Assertions.assertNotNull(plan);
        // Standard mode uses traditional boolean algebra: OR at top level
        Assertions.assertEquals(QsClauseType.OR, plan.root.type);
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
        Assertions.assertEquals(QsClauseType.AND, plan.root.type);
    }

    // ============ Tests for Escape Handling ============

    @Test
    public void testEscapedSpaceInTerm() {
        // Test: "First\ Value" should be treated as a single term "First Value"
        // The escape sequence is processed: \ + space -> space
        String dsl = "field:First\\ Value";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.TERM, plan.root.type);
        Assertions.assertEquals("field", plan.root.field);
        // After unescape: "First\ Value" -> "First Value"
        Assertions.assertEquals("First Value", plan.root.value);
    }

    @Test
    public void testEscapedParentheses() {
        // Test: \( and \) should be treated as literal characters, not grouping
        // The escape sequence is processed: \( -> ( and \) -> )
        String dsl = "field:hello\\(world\\)";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.TERM, plan.root.type);
        Assertions.assertEquals("field", plan.root.field);
        // After unescape: "hello\(world\)" -> "hello(world)"
        Assertions.assertEquals("hello(world)", plan.root.value);
    }

    @Test
    public void testEscapedColon() {
        // Test: \: should be treated as literal colon, not field separator
        // The escape sequence is processed: \: -> :
        String dsl = "field:value\\:with\\:colons";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.TERM, plan.root.type);
        Assertions.assertEquals("field", plan.root.field);
        // After unescape: "value\:with\:colons" -> "value:with:colons"
        Assertions.assertEquals("value:with:colons", plan.root.value);
    }

    @Test
    public void testEscapedBackslash() {
        // Test: \\ should be treated as a literal backslash
        // The escape sequence is processed: \\ -> \
        String dsl = "field:path\\\\to\\\\file";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.TERM, plan.root.type);
        Assertions.assertEquals("field", plan.root.field);
        // After unescape: "path\\to\\file" -> "path\to\file"
        Assertions.assertEquals("path\\to\\file", plan.root.value);
    }

    @Test
    public void testUppercaseAndOperator() {
        // Test: uppercase AND should be treated as operator
        String dsl = "field:a AND field:b";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.AND, plan.root.type);
        Assertions.assertEquals(2, plan.root.children.size());
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
        Assertions.assertEquals(QsClauseType.AND, plan.root.type);
        // TODO: If PDF requires only uppercase, this should fail and return OR or different structure
    }

    @Test
    public void testUppercaseOrOperator() {
        // Test: uppercase OR should be treated as operator
        String dsl = "field:a OR field:b";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OR, plan.root.type);
        Assertions.assertEquals(2, plan.root.children.size());
    }

    @Test
    public void testLowercaseOrOperator() {
        // Test: Currently lowercase 'or' is also treated as operator
        // According to PDF requirement, only uppercase should be operators
        String dsl = "field:a or field:b";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        // Current behavior: lowercase 'or' IS an operator
        Assertions.assertEquals(QsClauseType.OR, plan.root.type);
        // TODO: If PDF requires only uppercase, this should fail
    }

    @Test
    public void testUppercaseNotOperator() {
        // Test: uppercase NOT should be treated as operator
        String dsl = "NOT field:spam";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.NOT, plan.root.type);
    }

    @Test
    public void testLowercaseNotOperator() {
        // Test: Currently lowercase 'not' is also treated as operator
        // According to PDF requirement, only uppercase should be operators
        String dsl = "not field:spam";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        // Current behavior: lowercase 'not' IS an operator
        Assertions.assertEquals(QsClauseType.NOT, plan.root.type);
        // TODO: If PDF requires only uppercase, this should fail
    }

    @Test
    public void testExclamationNotOperator() {
        // Test: ! should be treated as NOT operator
        String dsl = "!field:spam";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        // Current behavior: ! IS a NOT operator
        Assertions.assertEquals(QsClauseType.NOT, plan.root.type);
    }

    @Test
    public void testEscapedSpecialCharactersInQuoted() {
        // Test: escaped characters inside quoted strings
        // Note: For PHRASE queries, escape handling is preserved as-is for now
        // The backend will handle escape processing for phrase queries
        String dsl = "field:\"hello\\\"world\"";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.PHRASE, plan.root.type);
        Assertions.assertEquals("hello\\\"world", plan.root.value);
    }

    @Test
    public void testNoEscapeWithoutBackslash() {
        // Test: normal term without escape characters
        String dsl = "field:normalterm";
        QsPlan plan = SearchDslParser.parseDsl(dsl);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.TERM, plan.root.type);
        Assertions.assertEquals("normalterm", plan.root.value);
    }

    // ============ Tests for Multi-Field Search ============

    @Test
    public void testMultiFieldSimpleTerm() {
        // Test: "hello" + fields=["title","content"] → "(title:hello OR content:hello)"
        String dsl = "hello";
        String options = "{\"fields\":[\"title\",\"content\"]}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OR, plan.root.type);
        Assertions.assertEquals(2, plan.root.children.size());

        // Verify both fields are in bindings
        Assertions.assertEquals(2, plan.fieldBindings.size());
        Assertions.assertTrue(plan.fieldBindings.stream()
                .anyMatch(b -> "title".equals(b.fieldName)));
        Assertions.assertTrue(plan.fieldBindings.stream()
                .anyMatch(b -> "content".equals(b.fieldName)));
    }

    @Test
    public void testMultiFieldMultiTermAnd() {
        // Test: "hello world" + fields=["title","content"] + default_operator="and"
        // → "(title:hello OR content:hello) AND (title:world OR content:world)"
        String dsl = "hello world";
        String options = "{\"fields\":[\"title\",\"content\"],\"default_operator\":\"and\"}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.AND, plan.root.type);
        Assertions.assertEquals(2, plan.root.children.size());

        // Each child should be an OR of two fields
        for (QsNode child : plan.root.children) {
            Assertions.assertEquals(QsClauseType.OR, child.type);
            Assertions.assertEquals(2, child.children.size());
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
        Assertions.assertEquals(QsClauseType.OR, plan.root.type);
    }

    @Test
    public void testMultiFieldExplicitAndOperator() {
        // Test: "hello AND world" + fields=["title","content"]
        String dsl = "hello AND world";
        String options = "{\"fields\":[\"title\",\"content\"]}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.AND, plan.root.type);
    }

    @Test
    public void testMultiFieldMixedWithExplicitField() {
        // Test: "hello AND category:tech" + fields=["title","content"]
        // → "(title:hello OR content:hello) AND category:tech"
        String dsl = "hello AND category:tech";
        String options = "{\"fields\":[\"title\",\"content\"]}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.AND, plan.root.type);
        Assertions.assertEquals(2, plan.root.children.size());

        // Verify "category" is preserved
        Assertions.assertTrue(plan.fieldBindings.stream()
                .anyMatch(b -> "category".equals(b.fieldName)));
    }

    @Test
    public void testMultiFieldWithWildcard() {
        // Test: "hello*" + fields=["title","content"]
        String dsl = "hello*";
        String options = "{\"fields\":[\"title\",\"content\"]}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OR, plan.root.type);
        Assertions.assertEquals(2, plan.root.children.size());

        // Both should be PREFIX type
        for (QsNode child : plan.root.children) {
            Assertions.assertEquals(QsClauseType.PREFIX, child.type);
        }
    }

    @Test
    public void testMultiFieldWithExactFunction() {
        // Test: "EXACT(foo bar)" + fields=["title","content"]
        String dsl = "EXACT(foo bar)";
        String options = "{\"fields\":[\"title\",\"content\"]}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OR, plan.root.type);
        Assertions.assertEquals(2, plan.root.children.size());

        // Both should be EXACT type
        for (QsNode child : plan.root.children) {
            Assertions.assertEquals(QsClauseType.EXACT, child.type);
        }
    }

    @Test
    public void testMultiFieldThreeFields() {
        // Test: "hello" + fields=["title","content","tags"]
        String dsl = "hello";
        String options = "{\"fields\":[\"title\",\"content\",\"tags\"]}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OR, plan.root.type);
        Assertions.assertEquals(3, plan.root.children.size());
        Assertions.assertEquals(3, plan.fieldBindings.size());
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
        Assertions.assertEquals(QsClauseType.TERM, plan.root.type);
        Assertions.assertEquals("title", plan.root.field);
        Assertions.assertEquals(1, plan.fieldBindings.size());
    }

    @Test
    public void testMultiFieldNotOperator() {
        // Test: "NOT hello" + fields=["title","content"]
        String dsl = "NOT hello";
        String options = "{\"fields\":[\"title\",\"content\"]}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.NOT, plan.root.type);
        Assertions.assertEquals(1, plan.root.children.size());
        Assertions.assertEquals(QsClauseType.OR, plan.root.children.get(0).type);
    }

    // ============ Tests for Multi-Field + Lucene Mode ============

    @Test
    public void testMultiFieldLuceneModeSimpleAnd() {
        // Test: "a AND b" + fields=["title","content"] + lucene mode
        // Expanded: "(title:a OR content:a) AND (title:b OR content:b)"
        // With Lucene semantics: both groups are MUST
        String dsl = "a AND b";
        String options = "{\"fields\":[\"title\",\"content\"],\"mode\":\"lucene\",\"minimum_should_match\":0}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OCCUR_BOOLEAN, plan.root.type);

        // Should have 2 children (two OR groups), both with MUST
        // Note: In Lucene mode, OR groups are also wrapped as OCCUR_BOOLEAN
        Assertions.assertEquals(2, plan.root.children.size());
        for (QsNode child : plan.root.children) {
            Assertions.assertEquals(QsOccur.MUST, child.occur);
            // The child is OCCUR_BOOLEAN wrapping the OR group
            Assertions.assertEquals(QsClauseType.OCCUR_BOOLEAN, child.type);
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
        Assertions.assertEquals(QsClauseType.OCCUR_BOOLEAN, plan.root.type);

        // Should have 2 children, both with SHOULD
        Assertions.assertEquals(2, plan.root.children.size());
        for (QsNode child : plan.root.children) {
            Assertions.assertEquals(QsOccur.SHOULD, child.occur);
        }

        // minimum_should_match should be 1
        Assertions.assertEquals(Integer.valueOf(1), plan.root.minimumShouldMatch);
    }

    @Test
    public void testMultiFieldLuceneModeAndOrMixed() {
        // Test: "a AND b OR c" + fields=["title","content"] + lucene mode + minimum_should_match=0
        // With Lucene semantics and minimum_should_match=0: SHOULD groups are discarded
        // Only "a" (MUST) remains - wrapped in OCCUR_BOOLEAN
        String dsl = "a AND b OR c";
        String options = "{\"fields\":[\"title\",\"content\"],\"mode\":\"lucene\",\"minimum_should_match\":0}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        // With minimum_should_match=0, only (title:a OR content:a) remains
        // In Lucene mode, this is wrapped as OCCUR_BOOLEAN
        Assertions.assertEquals(QsClauseType.OCCUR_BOOLEAN, plan.root.type);
    }

    @Test
    public void testMultiFieldLuceneModeWithNot() {
        // Test: "a AND NOT b" + fields=["title","content"] + lucene mode
        // Expanded: "(title:a OR content:a) AND NOT (title:b OR content:b)"
        String dsl = "a AND NOT b";
        String options = "{\"fields\":[\"title\",\"content\"],\"mode\":\"lucene\",\"minimum_should_match\":0}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OCCUR_BOOLEAN, plan.root.type);

        // Should have 2 children: a (MUST), b (MUST_NOT)
        Assertions.assertEquals(2, plan.root.children.size());

        // Find MUST and MUST_NOT children
        boolean hasMust = plan.root.children.stream().anyMatch(c -> c.occur == QsOccur.MUST);
        boolean hasMustNot = plan.root.children.stream().anyMatch(c -> c.occur == QsOccur.MUST_NOT);
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
        Assertions.assertEquals(QsClauseType.OCCUR_BOOLEAN, plan.root.type);
        // The OCCUR_BOOLEAN contains the OR group's children with SHOULD occur
        Assertions.assertEquals(2, plan.root.children.size());
    }

    @Test
    public void testMultiFieldLuceneModeComplexQuery() {
        // Test: "(a OR b) AND NOT c" + fields=["f1","f2"] + lucene mode
        String dsl = "(a OR b) AND NOT c";
        String options = "{\"fields\":[\"f1\",\"f2\"],\"mode\":\"lucene\",\"minimum_should_match\":0}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        // Should have proper structure with MUST and MUST_NOT
        Assertions.assertEquals(QsClauseType.OCCUR_BOOLEAN, plan.root.type);
    }

    @Test
    public void testMultiFieldLuceneModeMinimumShouldMatchOne() {
        // Test: "a AND b OR c" with minimum_should_match=1 keeps all clauses
        String dsl = "a AND b OR c";
        String options = "{\"fields\":[\"title\",\"content\"],\"mode\":\"lucene\",\"minimum_should_match\":1}";
        QsPlan plan = SearchDslParser.parseDsl(dsl, options);

        Assertions.assertNotNull(plan);
        Assertions.assertEquals(QsClauseType.OCCUR_BOOLEAN, plan.root.type);
        // All 3 groups should be present
        Assertions.assertEquals(3, plan.root.children.size());
        Assertions.assertEquals(Integer.valueOf(1), plan.root.minimumShouldMatch);
    }
}
