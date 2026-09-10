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

import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.parser.ParserTestBase;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.Expression.SqlRenderMode;
import org.apache.doris.nereids.trees.expressions.WindowFrame.FrameBoundary;
import org.apache.doris.nereids.trees.expressions.WindowFrame.FrameUnitsType;
import org.apache.doris.nereids.trees.expressions.functions.agg.GroupConcat;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinctGroupConcat;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayMap;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Concat;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lambda;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Pi;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class DefaultSqlRenderingTest extends ParserTestBase {
    private final SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
    private final SlotReference b = new SlotReference("b", IntegerType.INSTANCE);

    @Test
    void testAliasEscapesBackticks() {
        Alias alias = new Alias(new IntegerLiteral(1), "a`b");
        assertSql(alias, "1 AS `a``b`");
        new NereidsParser().parseSingle("SELECT " + alias.toSql());
    }

    @Test
    void testCompoundPredicatePreservesGrouping() {
        Expression predicate = new And(new EqualTo(a, new IntegerLiteral(1)),
                new Or(new EqualTo(b, new IntegerLiteral(2)), new EqualTo(b, new IntegerLiteral(3))));
        assertSql(predicate, "((a = 1) AND ((b = 2) OR (b = 3)))");
        Expression parsed = new NereidsParser().parseExpression(predicate.toSql());
        Assertions.assertInstanceOf(And.class, parsed);
        Assertions.assertInstanceOf(Or.class, parsed.child(1));
    }

    @Test
    void testAllOrderKeyDirectionsAndNullPlacements() {
        for (boolean ascending : new boolean[] {true, false}) {
            for (boolean nullFirst : new boolean[] {true, false}) {
                OrderKey key = new OrderKey(a, ascending, nullFirst);
                String expected = "a" + (ascending ? " ASC" : " DESC")
                        + (nullFirst ? " NULLS FIRST" : " NULLS LAST");
                Assertions.assertEquals(expected, key.toSql());
                Assertions.assertEquals(expected, key.toSql(SqlRenderMode.DEFAULT));
                assertSql(new OrderExpression(key), expected);
                new NereidsParser().parseSingle("SELECT a FROM t ORDER BY " + key.toSql());
            }
        }
    }

    @Test
    void testWindowFrameBoundariesUseSql() {
        FrameBoundary preceding = FrameBoundary.newPrecedingBoundary();
        FrameBoundary current = FrameBoundary.newCurrentRowBoundary();
        assertSql(new WindowFrame(FrameUnitsType.ROWS, preceding, null), "ROWS UNBOUNDED PRECEDING");
        WindowFrame frame = new WindowFrame(FrameUnitsType.ROWS, preceding, current);
        assertSql(frame, "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");
        new NereidsParser().parseExpression("sum(a) OVER (ORDER BY b " + frame.toSql() + ")");
        FrameBoundary offset = FrameBoundary.newFollowingBoundary(new Add(new IntegerLiteral(1), new IntegerLiteral(2)));
        Assertions.assertEquals("(1 + 2) FOLLOWING", offset.toSql());
        assertSql(new WindowFrame(FrameUnitsType.RANGE, current, FrameBoundary.newFollowingBoundary()),
                "RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING");
    }

    @Test
    void testGroupConcatArgumentsAndOrderKeys() {
        assertSql(new GroupConcat(a), "group_concat(a)");
        assertSql(new GroupConcat(a, new StringLiteral(",")), "group_concat(a, ',')");
        OrderExpression first = new OrderExpression(new OrderKey(a, true, false));
        OrderExpression second = new OrderExpression(new OrderKey(b, false, true));
        GroupConcat ordered = new GroupConcat(true, a, new StringLiteral(","), first, second);
        assertSql(ordered, "group_concat(DISTINCT a, ',' ORDER BY a ASC NULLS LAST, b DESC NULLS FIRST)");
        new NereidsParser().parseExpression(ordered.toSql());
        GroupConcat oneArgument = new GroupConcat(a, first);
        assertSql(oneArgument, "group_concat(a ORDER BY a ASC NULLS LAST)");
        new NereidsParser().parseExpression(oneArgument.toSql());
        assertSql(new MultiDistinctGroupConcat(a, first, second),
                "multi_distinct_group_concat(a ORDER BY a ASC NULLS LAST, b DESC NULLS FIRST)");
    }

    @Test
    void testBoundFunctionOrdinaryArguments() {
        assertSql(new Pi(), "pi()");
        assertSql(new Concat(new StringLiteral("a"), new StringLiteral("b")), "concat('a', 'b')");
    }

    @Test
    void testBoundLambdaUsesVariablesAndInputArrays() {
        ArrayLiteral array = new ArrayLiteral(ImmutableList.of(new IntegerLiteral(1), new IntegerLiteral(2)));
        ArrayItemReference x = new ArrayItemReference("x", array);
        assertSql(x, "x");
        Assertions.assertEquals("x", x.toSql(SqlRenderMode.FOR_VIEW));
        Lambda single = new Lambda(ImmutableList.of("x"), new Add(x, new IntegerLiteral(1)), ImmutableList.of(x));
        assertSql(single, "x -> (x + 1), [1, 2]");
        ArrayMap map = new ArrayMap(single);
        assertSql(map, "array_map(x -> (x + 1), [1, 2])");
        Assertions.assertEquals(map.toSql(), map.toSql(SqlRenderMode.FOR_VIEW));
        new NereidsParser().parseExpression(map.toSql());
        ArrayItemReference y = new ArrayItemReference("y", array);
        Lambda multiple = new Lambda(ImmutableList.of("x", "y"), new Add(x, y), ImmutableList.of(x, y));
        assertSql(multiple, "(x, y) -> (x + y), [1, 2], [1, 2]");
        new NereidsParser().parseExpression(new ArrayMap(multiple).toSql());
    }

    private void assertSql(Expression expression, String expected) {
        Assertions.assertEquals(expected, expression.computeToSql());
        Assertions.assertEquals(expected, expression.computeToSql(SqlRenderMode.DEFAULT));
        Assertions.assertEquals(expected, expression.toSql());
        Assertions.assertEquals(expected, expression.toSql(SqlRenderMode.DEFAULT));
    }
}
