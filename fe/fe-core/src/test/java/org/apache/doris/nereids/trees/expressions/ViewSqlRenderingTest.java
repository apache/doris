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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.parser.ParserTestBase;
import org.apache.doris.nereids.trees.expressions.Expression.SqlRenderMode;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lambda;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IPv4Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.JsonLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.commands.info.BaseViewInfo;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SqlModeHelper;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.TreeMap;

class ViewSqlRenderingTest extends ParserTestBase {
    @Test
    void testQualifiedExpressionAndIndependentCache() {
        SlotReference slot = new SlotReference(new ExprId(1), "a`b", IntegerType.INSTANCE,
                true, ImmutableList.of("db", "t"));
        Alias expression = new Alias(new Add(slot, new IntegerLiteral(1)), "a`b");
        String original = expression.toSql();
        String viewSql = expression.toSql(SqlRenderMode.FOR_VIEW);
        Assertions.assertEquals("(`db`.`t`.`a``b` + 1) AS `a``b`", viewSql);
        Assertions.assertEquals(original, expression.toSql());
        new NereidsParser().parseSingle("SELECT " + viewSql + " FROM db.t");
        Alias fresh = new Alias(new Add(slot, new IntegerLiteral(1)), "a`b");
        Assertions.assertEquals(viewSql, fresh.toSql(SqlRenderMode.FOR_VIEW));
        Assertions.assertEquals(original, fresh.toSql());
    }

    @Test
    void testSubPathAndStringEscaping() {
        SlotReference slot = new SlotReference(new ExprId(2), "payload", IntegerType.INSTANCE,
                true, ImmutableList.of("t")).withSubPath(ImmutableList.of("user", "name"));
        Assertions.assertEquals("`t`.`payload`[\"user\"][\"name\"]", slot.toSql(SqlRenderMode.FOR_VIEW));
        StringLiteral literal = new StringLiteral("O'Reilly");
        Expression parsed = new NereidsParser().parseExpression(literal.toSql(SqlRenderMode.FOR_VIEW));
        Assertions.assertEquals(literal, parsed);
    }

    @Test
    void testConcurrentModes() throws Exception {
        SlotReference slot = new SlotReference(new ExprId(3), "a", IntegerType.INSTANCE,
                true, ImmutableList.of("t"));
        Expression expression = new Add(slot, new IntegerLiteral(1));
        java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newFixedThreadPool(2);
        try {
            java.util.concurrent.Future<String> diagnostic = executor.submit(() -> {
                return expression.toSql();
            });
            java.util.concurrent.Future<String> view = executor.submit(() -> expression.toSql(SqlRenderMode.FOR_VIEW));
            Assertions.assertEquals("(a + 1)", diagnostic.get());
            Assertions.assertEquals("(`t`.`a` + 1)", view.get());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testLambdaSyntax() {
        Lambda lambda = new Lambda(ImmutableList.of("x"), new UnboundSlot("x"));
        Assertions.assertEquals("x -> x", lambda.toSql(SqlRenderMode.FOR_VIEW));
        new NereidsParser().parseExpression("array_map(" + lambda.toSql(SqlRenderMode.FOR_VIEW) + ", [1, 2])");
    }

    @Test
    void testTypedLiterals() {
        for (Literal literal : ImmutableList.of(new DateV2Literal("2026-01-02"),
                new DoubleLiteral(1.25), new DoubleLiteral(Double.POSITIVE_INFINITY),
                new JsonLiteral("{\"x\":1}"), new IPv4Literal("127.0.0.1"))) {
            Expression parsed = new NereidsParser().parseExpression(literal.toSql(SqlRenderMode.FOR_VIEW));
            Assertions.assertInstanceOf(Cast.class, parsed);
            Assertions.assertEquals(literal.getDataType(), parsed.getDataType());
        }
        Assertions.assertEquals("'2026-01-02'", new DateV2Literal("2026-01-02").toSql());
    }

    @Test
    void testStringSqlModes() {
        long originalMode = ConnectContext.get().getSessionVariable().getSqlMode();
        try {
            StringLiteral literal = new StringLiteral("C:\\new\\file O'Reilly");
            for (long mode : new long[] {0, SqlModeHelper.MODE_NO_BACKSLASH_ESCAPES}) {
                ConnectContext.get().getSessionVariable().setSqlMode(mode);
                Assertions.assertEquals(literal,
                        new NereidsParser().parseExpression(literal.toSql(SqlRenderMode.FOR_VIEW)));
            }
        } finally {
            ConnectContext.get().getSessionVariable().setSqlMode(originalMode);
        }
    }

    @Test
    void testOutermostRewriteWins() {
        TreeMap<Pair<Integer, Integer>, String> ranges = new StatementContext().getIndexInSqlToString();
        ranges.put(Pair.of(7, 25), "99 AS `a`");
        ranges.put(Pair.of(17, 17), "ignored");
        ranges.put(Pair.of(24, 24), "ignored");
        ranges.put(Pair.of(7, 17), "ignored");
        ranges.put(Pair.of(17, 25), "ignored");
        ranges.put(Pair.of(32, 32), "`t`");
        Assertions.assertEquals("SELECT 99 AS `a` FROM `t`",
                BaseViewInfo.rewriteSql(ranges, "SELECT * REPLACE(a+1 AS a) FROM t"));
        Assertions.assertEquals(6, ranges.size());
    }

    @Test
    void testAdjacentAndInvalidRanges() {
        TreeMap<Pair<Integer, Integer>, String> ranges = new StatementContext().getIndexInSqlToString();
        ranges.put(Pair.of(0, 1), "A");
        ranges.put(Pair.of(2, 3), "B");
        Assertions.assertEquals("ABef", BaseViewInfo.rewriteSql(ranges, "abcdef"));
        ranges.put(Pair.of(1, 2), "crossing");
        Assertions.assertThrows(IllegalArgumentException.class, () -> BaseViewInfo.rewriteSql(ranges, "abcdef"));
        ranges.clear();
        ranges.put(Pair.of(0, 6), "invalid");
        Assertions.assertThrows(IllegalArgumentException.class, () -> BaseViewInfo.rewriteSql(ranges, "abcdef"));
    }
}
