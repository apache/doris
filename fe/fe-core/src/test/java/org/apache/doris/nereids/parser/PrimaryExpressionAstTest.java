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

package org.apache.doris.nereids.parser;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.DereferenceExpression;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArraySlice;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ConvertTo;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

class PrimaryExpressionAstTest extends ParserTestBase {
    private final NereidsParser parser = new NereidsParser();

    @ParameterizedTest(name = "{0}")
    @MethodSource("representativeExpressions")
    void preservesRepresentativeExpressionRoots(
            String description, String sql, Class<? extends Expression> expectedClass) {
        Expression expression = parser.parseExpression(sql);
        Assertions.assertInstanceOf(expectedClass, expression);
    }

    private static Stream<Arguments> representativeExpressions() {
        return Stream.of(
                Arguments.of("searched case", "CASE WHEN a THEN 1 ELSE 2 END", CaseWhen.class),
                Arguments.of("simple case", "CASE a WHEN 1 THEN 2 ELSE 3 END", CaseWhen.class),
                Arguments.of("convert charset", "CONVERT(IF(a, b, c) USING utf8)", ConvertTo.class),
                Arguments.of("convert type", "CONVERT(IF(a, b, c), BIGINT)", Cast.class),
                Arguments.of("generic convert function", "CONVERT(a)", UnboundFunction.class),
                Arguments.of("keyword column", "CAST", UnboundSlot.class),
                Arguments.of("function call", "db.fn(a)", UnboundFunction.class),
                Arguments.of("element at", "a[1]", ElementAt.class),
                Arguments.of("array slice", "a[1:2]", ArraySlice.class),
                Arguments.of("slot dereference", "a.b.c", UnboundSlot.class),
                Arguments.of("expression dereference", "fn(a).field", DereferenceExpression.class));
    }

    @Test
    void preservesPostfixChainOrder() {
        Expression expression = parser.parseExpression("fn(a)[1:2][3].field COLLATE utf8_general_ci");

        Assertions.assertInstanceOf(DereferenceExpression.class, expression);
        Assertions.assertInstanceOf(ElementAt.class, expression.child(0));
        Assertions.assertInstanceOf(ArraySlice.class, expression.child(0).child(0));
        Assertions.assertInstanceOf(UnboundFunction.class, expression.child(0).child(0).child(0));
    }

    @Test
    void preservesArithmeticComparisonAndBooleanPrecedence() {
        Expression expression = parser.parseExpression("NOT a = b + c * d OR e AND f");

        Assertions.assertInstanceOf(Or.class, expression);
        Assertions.assertInstanceOf(Not.class, expression.child(0));
        Assertions.assertInstanceOf(EqualTo.class, expression.child(0).child(0));
        Assertions.assertInstanceOf(Add.class, expression.child(0).child(0).child(1));
        Assertions.assertInstanceOf(Multiply.class, expression.child(0).child(0).child(1).child(1));
        Assertions.assertInstanceOf(And.class, expression.child(1));
    }

    @Test
    void preservesLeftAssociativeArithmetic() {
        Expression expression = parser.parseExpression("a - b - c");

        Assertions.assertInstanceOf(Subtract.class, expression);
        Assertions.assertInstanceOf(Subtract.class, expression.child(0));
    }

    @Test
    void preservesSimpleCasePlaceholderOrder() {
        List<Integer> placeholderIds = parser.parseMultiple("SELECT CASE ? WHEN ? THEN ? ELSE ? END").get(0).second
                .getPlaceholders().stream()
                .map(placeholder -> placeholder.getPlaceholderId().asInt())
                .collect(ImmutableList.toImmutableList());

        Assertions.assertEquals(List.of(0, 1, 2, 3), placeholderIds);
    }

    @Test
    void preservesCreateViewDereferenceSourceInterval() {
        LogicalPlan plan = parser.parseForCreateView("SELECT db.tbl.col FROM t");
        UnboundSlot slot = plan.<LogicalPlan>collectToList(ignored -> true).stream()
                .flatMap(node -> node.getExpressions().stream())
                .flatMap(expression -> expression.<UnboundSlot>collectToList(
                        UnboundSlot.class::isInstance).stream())
                .filter(unboundSlot -> unboundSlot.getNameParts().equals(List.of("db", "tbl", "col")))
                .findFirst()
                .orElseThrow();

        Assertions.assertEquals(Pair.of(7, 16), slot.getIndexInSqlString().orElseThrow());
    }
}
