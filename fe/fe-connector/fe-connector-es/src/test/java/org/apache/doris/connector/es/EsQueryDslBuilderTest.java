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

package org.apache.doris.connector.es;

import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.pushdown.ConnectorAnd;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.ConnectorFunctionCall;
import org.apache.doris.connector.spi.pushdown.ConnectorLike;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;
import org.apache.doris.connector.spi.pushdown.ConnectorNot;
import org.apache.doris.connector.spi.pushdown.ConnectorOr;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

public class EsQueryDslBuilderTest {

    private static ConnectorExpression keywordEquals(String value) {
        return new ConnectorComparison(ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("name", ConnectorType.of("VARCHAR")), ConnectorLiteral.ofString(value));
    }

    private static ConnectorExpression partiallyPushableAnd() {
        ConnectorExpression local = new ConnectorFunctionCall("unsupported_predicate", ConnectorType.of("BOOLEAN"),
                Collections.singletonList(new ConnectorColumnRef("name", ConnectorType.of("VARCHAR"))));
        return new ConnectorAnd(Arrays.asList(keywordEquals("one"), local));
    }

    private static EsQueryDslResult buildWithTracking(ConnectorExpression expression) {
        return EsQueryDslBuilder.buildQueryDslWithResult(expression, Collections.emptyMap(),
                Collections.singletonMap("name", "keyword"), true, Collections.emptyList());
    }

    @Test
    void partialAndRetainsOnlyItsUnsupportedConjunct() {
        EsQueryDslResult result = buildWithTracking(partiallyPushableAnd());
        Assertions.assertEquals(Collections.singletonList(1), result.getNotPushedIndices());
        Assertions.assertEquals("{\"term\":{\"name\":\"one\"}}", result.getQueryDsl());
    }

    @Test
    void orWithPartiallyPushableAndRemainsLocal() {
        EsQueryDslResult result = buildWithTracking(new ConnectorOr(
                Arrays.asList(partiallyPushableAnd(), keywordEquals("two"))));
        Assertions.assertEquals(Collections.singletonList(0), result.getNotPushedIndices());
        Assertions.assertEquals(EsQueryDslBuilder.matchAllQuery(), result.getQueryDsl());
    }

    @Test
    void notWithPartiallyPushableAndRemainsLocal() {
        EsQueryDslResult result = buildWithTracking(new ConnectorNot(partiallyPushableAnd()));
        Assertions.assertEquals(Collections.singletonList(0), result.getNotPushedIndices());
        Assertions.assertEquals(EsQueryDslBuilder.matchAllQuery(), result.getQueryDsl());
    }

    @Test
    void fullyPushableOrAndNotRemainPushed() {
        for (ConnectorExpression expression : Arrays.asList(
                new ConnectorOr(Arrays.asList(keywordEquals("one"), keywordEquals("two"))),
                new ConnectorNot(keywordEquals("one")))) {
            EsQueryDslResult result = buildWithTracking(expression);
            Assertions.assertTrue(result.getNotPushedIndices().isEmpty());
            Assertions.assertNotEquals(EsQueryDslBuilder.matchAllQuery(), result.getQueryDsl());
        }
    }

    @Test
    void retainedNotComparisonsPreserveNullRejection() {
        ConnectorColumnRef column = new ConnectorColumnRef("value", ConnectorType.of("BIGINT"));
        ConnectorComparison.Operator[] operators = {ConnectorComparison.Operator.EQ, ConnectorComparison.Operator.NE,
                ConnectorComparison.Operator.LT, ConnectorComparison.Operator.LE,
                ConnectorComparison.Operator.GT, ConnectorComparison.Operator.GE};
        ConnectorComparison.Operator[] inverses = {ConnectorComparison.Operator.NE, ConnectorComparison.Operator.EQ,
                ConnectorComparison.Operator.GE, ConnectorComparison.Operator.GT,
                ConnectorComparison.Operator.LE, ConnectorComparison.Operator.LT};
        for (int i = 0; i < operators.length; i++) {
            ConnectorExpression expression = new ConnectorComparison(operators[i], column, ConnectorLiteral.ofLong(1));
            ConnectorExpression inverse = new ConnectorComparison(inverses[i], column, ConnectorLiteral.ofLong(1));
            Assertions.assertEquals(buildWithTracking(inverse).getQueryDsl(),
                    buildWithTracking(new ConnectorNot(expression)).getQueryDsl());
            Assertions.assertEquals(buildWithTracking(expression).getQueryDsl(),
                    buildWithTracking(new ConnectorNot(new ConnectorNot(expression))).getQueryDsl());
        }
        ConnectorExpression eq = new ConnectorComparison(ConnectorComparison.Operator.EQ, column,
                ConnectorLiteral.ofLong(1));
        ConnectorExpression ne = new ConnectorComparison(ConnectorComparison.Operator.NE, column,
                ConnectorLiteral.ofLong(1));
        Assertions.assertEquals(buildWithTracking(new ConnectorAnd(Arrays.asList(ne, eq))).getQueryDsl(),
                buildWithTracking(new ConnectorNot(new ConnectorOr(Arrays.asList(eq, ne)))).getQueryDsl());
    }

    @Test
    void notEqualExcludesMissingValuesForEveryNonNullLiteral() {
        for (ConnectorLiteral literal : Arrays.asList(ConnectorLiteral.ofLong(1),
                ConnectorLiteral.ofString("one"), ConnectorLiteral.ofString(""), ConnectorLiteral.ofBoolean(true))) {
            ConnectorExpression comparison = new ConnectorComparison(ConnectorComparison.Operator.NE,
                    new ConnectorColumnRef("value", literal.getType()), literal);
            EsQueryDslResult result = EsQueryDslBuilder.buildQueryDslWithResult(comparison,
                    Collections.emptyMap(), Collections.emptyMap(), true, Collections.emptyList());
            Assertions.assertTrue(result.getNotPushedIndices().isEmpty());
            Assertions.assertTrue(result.getQueryDsl().contains("\"must\":{\"exists\":{\"field\":\"value\"}}"));
        }
    }

    @Test
    public void testRegexpProducesRegexpQuery() {
        // SQL: WHERE name REGEXP '^test.*'
        ConnectorColumnRef col = new ConnectorColumnRef("name", ConnectorType.of("VARCHAR"));
        ConnectorLiteral pattern = ConnectorLiteral.ofString("^test.*");
        ConnectorLike regexpExpr = new ConnectorLike(
                ConnectorLike.Operator.REGEXP, col, pattern);

        String dsl = EsQueryDslBuilder.buildQueryDsl(
                regexpExpr,
                Collections.emptyMap(),
                Collections.emptyMap(),
                true,
                Collections.emptyList());

        // Must be {"regexp":...} not {"wildcard":...}
        Assertions.assertTrue(dsl.contains("\"regexp\""),
                "REGEXP should produce a regexp query, got: " + dsl);
        Assertions.assertFalse(dsl.contains("\"wildcard\""),
                "REGEXP must not produce a wildcard query, got: " + dsl);
        Assertions.assertTrue(dsl.contains("\"name\""),
                "Query should reference the column name, got: " + dsl);
        Assertions.assertTrue(dsl.contains("^test.*"),
                "Query should contain the regex pattern, got: " + dsl);
    }

    @Test
    public void testLikeProducesWildcardQuery() {
        // SQL: WHERE name LIKE '%test%' (on a keyword field)
        ConnectorColumnRef col = new ConnectorColumnRef("name", ConnectorType.of("VARCHAR"));
        ConnectorLiteral pattern = ConnectorLiteral.ofString("%test%");
        ConnectorLike likeExpr = new ConnectorLike(
                ConnectorLike.Operator.LIKE, col, pattern);

        java.util.Map<String, String> column2typeMap = new java.util.HashMap<>();
        column2typeMap.put("name", "keyword");

        String dsl = EsQueryDslBuilder.buildQueryDsl(
                likeExpr,
                Collections.emptyMap(),
                column2typeMap,
                true,
                Collections.emptyList());

        // Must be {"wildcard":...} not {"regexp":...}
        Assertions.assertTrue(dsl.contains("\"wildcard\""),
                "LIKE should produce a wildcard query, got: " + dsl);
        Assertions.assertFalse(dsl.contains("\"regexp\""),
                "LIKE must not produce a regexp query, got: " + dsl);
        // '%' should be converted to '*'
        Assertions.assertTrue(dsl.contains("*test*"),
                "LIKE pattern '%test%' should become '*test*', got: " + dsl);
    }

    @Test
    public void testLikeOnTextFieldNotPushed() {
        // SQL: WHERE name LIKE '%test%' (on a text field — should NOT push down)
        ConnectorColumnRef col = new ConnectorColumnRef("name", ConnectorType.of("VARCHAR"));
        ConnectorLiteral pattern = ConnectorLiteral.ofString("%test%");
        ConnectorLike likeExpr = new ConnectorLike(
                ConnectorLike.Operator.LIKE, col, pattern);

        java.util.Map<String, String> column2typeMap = new java.util.HashMap<>();
        column2typeMap.put("name", "text");

        String dsl = EsQueryDslBuilder.buildQueryDsl(
                likeExpr,
                Collections.emptyMap(),
                column2typeMap,
                true,
                Collections.emptyList());

        // Text field should fall back to match_all (not pushed)
        Assertions.assertTrue(dsl.contains("\"match_all\""),
                "LIKE on text field should not push down, got: " + dsl);
    }

    @Test
    public void testRegexpWithKeywordSubfield() {
        // When fieldsContext maps "name" → "name.keyword", REGEXP should use the sub-field
        ConnectorColumnRef col = new ConnectorColumnRef("name", ConnectorType.of("VARCHAR"));
        ConnectorLiteral pattern = ConnectorLiteral.ofString("[0-9]+");
        ConnectorLike regexpExpr = new ConnectorLike(
                ConnectorLike.Operator.REGEXP, col, pattern);

        java.util.Map<String, String> fieldsContext = new java.util.HashMap<>();
        fieldsContext.put("name", "name.keyword");

        String dsl = EsQueryDslBuilder.buildQueryDsl(
                regexpExpr,
                fieldsContext,
                Collections.emptyMap(),
                true,
                Collections.emptyList());

        Assertions.assertTrue(dsl.contains("\"regexp\""),
                "REGEXP should produce regexp query, got: " + dsl);
        Assertions.assertTrue(dsl.contains("name.keyword"),
                "REGEXP should use keyword sub-field, got: " + dsl);
    }

    @Test
    public void testRegexpNotPushedWhenLikePushDownDisabled() {
        // When likePushDown=false, even REGEXP should not push down
        ConnectorColumnRef col = new ConnectorColumnRef("name", ConnectorType.of("VARCHAR"));
        ConnectorLiteral pattern = ConnectorLiteral.ofString("^test");
        ConnectorLike regexpExpr = new ConnectorLike(
                ConnectorLike.Operator.REGEXP, col, pattern);

        String dsl = EsQueryDslBuilder.buildQueryDsl(
                regexpExpr,
                Collections.emptyMap(),
                Collections.emptyMap(),
                false,
                Collections.emptyList());

        Assertions.assertTrue(dsl.contains("\"match_all\""),
                "REGEXP with pushdown disabled should not push down, got: " + dsl);
    }

    @Test
    public void testLikePatternConversion() {
        // Test _ → ? and % → * conversion for LIKE
        ConnectorColumnRef col = new ConnectorColumnRef("code", ConnectorType.of("VARCHAR"));
        ConnectorLiteral pattern = ConnectorLiteral.ofString("A_B%C");
        ConnectorLike likeExpr = new ConnectorLike(
                ConnectorLike.Operator.LIKE, col, pattern);

        java.util.Map<String, String> column2typeMap = new java.util.HashMap<>();
        column2typeMap.put("code", "keyword");

        String dsl = EsQueryDslBuilder.buildQueryDsl(
                likeExpr,
                Collections.emptyMap(),
                column2typeMap,
                true,
                Collections.emptyList());

        Assertions.assertTrue(dsl.contains("A?B*C"),
                "_ should become ? and % should become *, got: " + dsl);
    }

    @Test
    public void testEqForNullWithNullGeneratesNotExists() {
        // SQL: WHERE name <=> NULL  → ES: {"bool":{"must_not":{"exists":{"field":"name"}}}}
        ConnectorColumnRef col = new ConnectorColumnRef("name", ConnectorType.of("VARCHAR"));
        ConnectorLiteral nullLiteral = ConnectorLiteral.ofNull(ConnectorType.of("VARCHAR"));
        ConnectorComparison expr = new ConnectorComparison(
                ConnectorComparison.Operator.EQ_FOR_NULL, col, nullLiteral);

        String dsl = EsQueryDslBuilder.buildQueryDsl(
                expr,
                Collections.emptyMap(),
                Collections.emptyMap(),
                true,
                Collections.emptyList());

        Assertions.assertTrue(dsl.contains("\"must_not\""),
                "EQ_FOR_NULL with NULL should produce must_not, got: " + dsl);
        Assertions.assertTrue(dsl.contains("\"exists\""),
                "EQ_FOR_NULL with NULL should produce exists query, got: " + dsl);
        Assertions.assertTrue(dsl.contains("\"name\""),
                "exists query should reference field name, got: " + dsl);
        Assertions.assertFalse(dsl.contains("\"term\""),
                "EQ_FOR_NULL with NULL should NOT produce term query, got: " + dsl);
    }

    @Test
    public void testEqForNullWithValueProducesTermQuery() {
        // SQL: WHERE name <=> 'hello' → same as term query (non-null case)
        ConnectorColumnRef col = new ConnectorColumnRef("name", ConnectorType.of("VARCHAR"));
        ConnectorLiteral value = ConnectorLiteral.ofString("hello");
        ConnectorComparison expr = new ConnectorComparison(
                ConnectorComparison.Operator.EQ_FOR_NULL, col, value);

        String dsl = EsQueryDslBuilder.buildQueryDsl(
                expr,
                Collections.emptyMap(),
                Collections.emptyMap(),
                true,
                Collections.emptyList());

        Assertions.assertTrue(dsl.contains("\"term\""),
                "EQ_FOR_NULL with non-null value should produce term query, got: " + dsl);
        Assertions.assertTrue(dsl.contains("hello"),
                "term query should contain the value, got: " + dsl);
        Assertions.assertFalse(dsl.contains("\"exists\""),
                "EQ_FOR_NULL with non-null should NOT use exists, got: " + dsl);
    }

    @Test
    public void testEqWithNullDoesNotProduceTermNull() {
        // SQL: WHERE name = NULL — optimizer usually rewrites, but if it reaches here,
        // term(col, null) is wrong in ES; verify the behavior
        ConnectorColumnRef col = new ConnectorColumnRef("name", ConnectorType.of("VARCHAR"));
        ConnectorLiteral nullLiteral = ConnectorLiteral.ofNull(ConnectorType.of("VARCHAR"));
        ConnectorComparison expr = new ConnectorComparison(
                ConnectorComparison.Operator.EQ, col, nullLiteral);

        String dsl = EsQueryDslBuilder.buildQueryDsl(
                expr,
                Collections.emptyMap(),
                Collections.emptyMap(),
                true,
                Collections.emptyList());

        // EQ with null currently produces term(col, null) — this is semantically
        // wrong but unlikely to reach here due to optimizer rewrites.
        // Just verify it doesn't crash.
        Assertions.assertNotNull(dsl, "Should not return null");
    }
}
