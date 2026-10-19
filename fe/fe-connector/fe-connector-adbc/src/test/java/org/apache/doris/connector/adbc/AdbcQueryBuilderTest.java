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

package org.apache.doris.connector.adbc;

import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.handle.NamedColumnHandle;
import org.apache.doris.connector.spi.pushdown.ConnectorAnd;
import org.apache.doris.connector.spi.pushdown.ConnectorBetween;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.ConnectorFunctionCall;
import org.apache.doris.connector.spi.pushdown.ConnectorIn;
import org.apache.doris.connector.spi.pushdown.ConnectorIsNull;
import org.apache.doris.connector.spi.pushdown.ConnectorLike;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;
import org.apache.doris.connector.spi.pushdown.ConnectorNot;
import org.apache.doris.connector.spi.pushdown.ConnectorOr;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The statement a scan sends to its source.
 *
 * <p>Two of these assertions are about rows rather than text, and are the reason this class exists:
 *
 * <ul>
 *   <li>the select list is never {@code *} -- BE rejects any column the query did not ask for, so a
 *       widened projection does not waste bandwidth, it fails the scan;</li>
 *   <li>a row limit is emitted only when every predicate was, because BE applies the predicates again to
 *       whatever comes back, and a source that truncated first would leave too few rows to filter.</li>
 * </ul>
 */
class AdbcQueryBuilderTest {

    private static final AdbcDialect ANSI = AdbcDialectRegistry.defaultDialect();
    private static final AdbcTableHandle T1 =
            new AdbcTableHandle(new AdbcNamespace("main", ""), "t1");

    private static List<ConnectorColumnHandle> columns(String... names) {
        List<ConnectorColumnHandle> handles = new ArrayList<>(names.length);
        for (String name : names) {
            handles.add(new NamedColumnHandle(name));
        }
        return handles;
    }

    private static ConnectorColumnRef col(String name) {
        return new ConnectorColumnRef(name, ConnectorType.of("INT"));
    }

    private static ConnectorLiteral num(long value) {
        return ConnectorLiteral.ofLong(value);
    }

    private static String sql(List<ConnectorColumnHandle> cols, ConnectorExpression filter, long limit) {
        return AdbcQueryBuilder.build(ANSI, T1, cols, Optional.ofNullable(filter), limit).getSql();
    }

    // ---------- projection ----------

    @Test
    void selectsExactlyTheRequestedColumns() {
        Assertions.assertEquals("SELECT \"a\", \"b\" FROM \"main\".\"t1\"",
                sql(columns("a", "b"), null, -1));
    }

    @Test
    void neverSelectsStar() {
        // BE matches returned columns to query slots by name and errors on one it did not request, so a
        // star would fail the scan outright rather than merely over-read.
        Assertions.assertFalse(sql(columns("a"), null, -1).contains("*"));
    }

    @Test
    void selectsAConstantWhenNoColumnsAreRequested() {
        // An empty projection is COUNT(*) pushed down: the scan wants rows counted, no values. A constant
        // is one narrow column per row instead of the whole table width, and BE counts without
        // materializing when it asked for no columns.
        Assertions.assertEquals("SELECT 1 FROM \"main\".\"t1\"", sql(columns(), null, -1));
    }

    // ---------- predicates that are pushed ----------

    @Test
    void pushesComparisons() {
        Assertions.assertEquals("SELECT \"a\" FROM \"main\".\"t1\" WHERE (\"a\" > 10)",
                sql(columns("a"),
                        new ConnectorComparison(ConnectorComparison.Operator.GT, col("a"), num(10)), -1));
    }

    @Test
    void pushesNullTestsInAndOut() {
        Assertions.assertEquals("SELECT \"a\" FROM \"main\".\"t1\" WHERE (\"a\" IS NULL)",
                sql(columns("a"), new ConnectorIsNull(col("a"), false), -1));
        Assertions.assertEquals("SELECT \"a\" FROM \"main\".\"t1\" WHERE (\"a\" IS NOT NULL)",
                sql(columns("a"), new ConnectorIsNull(col("a"), true), -1));
    }

    @Test
    void pushesInLists() {
        Assertions.assertEquals("SELECT \"a\" FROM \"main\".\"t1\" WHERE (\"a\" IN (1, 2))",
                sql(columns("a"), new ConnectorIn(col("a"), List.of(num(1), num(2)), false), -1));
        Assertions.assertEquals("SELECT \"a\" FROM \"main\".\"t1\" WHERE (\"a\" NOT IN (1))",
                sql(columns("a"), new ConnectorIn(col("a"), List.of(num(1)), true), -1));
    }

    @Test
    void pushesBooleanConnectives() {
        ConnectorExpression either = new ConnectorOr(List.of(
                new ConnectorComparison(ConnectorComparison.Operator.EQ, col("a"), num(1)),
                new ConnectorComparison(ConnectorComparison.Operator.EQ, col("a"), num(2))));
        Assertions.assertEquals(
                "SELECT \"a\" FROM \"main\".\"t1\" WHERE ((\"a\" = 1) OR (\"a\" = 2))",
                sql(columns("a"), either, -1));
        Assertions.assertEquals(
                "SELECT \"a\" FROM \"main\".\"t1\" WHERE (NOT ((\"a\" = 1) OR (\"a\" = 2)))",
                sql(columns("a"), new ConnectorNot(either), -1));
    }

    @Test
    void splitsTopLevelConjunctsIntoSeparatePredicates() {
        ConnectorExpression both = new ConnectorAnd(List.of(
                new ConnectorComparison(ConnectorComparison.Operator.GT, col("a"), num(1)),
                new ConnectorIsNull(col("b"), true)));
        Assertions.assertEquals(
                "SELECT \"a\" FROM \"main\".\"t1\" WHERE (\"a\" > 1) AND (\"b\" IS NOT NULL)",
                sql(columns("a"), both, -1));
    }

    // ---------- predicates that are refused ----------

    @Test
    void keepsATranslatablePredicateAndDropsTheOneBesideIt() {
        // All-or-nothing is per conjunct, not per query: dropping one conjunct still leaves a superset of
        // the rows, and BE applies the dropped one itself.
        ConnectorExpression both = new ConnectorAnd(List.of(
                new ConnectorComparison(ConnectorComparison.Operator.GT, col("a"), num(1)),
                new ConnectorLike(ConnectorLike.Operator.LIKE, col("b"),
                        ConnectorLiteral.ofString("x%"))));
        Assertions.assertEquals("SELECT \"a\" FROM \"main\".\"t1\" WHERE (\"a\" > 1)",
                sql(columns("a"), both, -1));
    }

    @Test
    void refusesAConjunctWholeWhenAnyPartOfItIsUntranslatable() {
        // Half of a predicate is a DIFFERENT predicate, not a weaker one: emitting "a > 1" for
        // "a > 1 OR f(b)" would drop the rows the function would have matched.
        ConnectorExpression either = new ConnectorOr(List.of(
                new ConnectorComparison(ConnectorComparison.Operator.GT, col("a"), num(1)),
                new ConnectorFunctionCall("some_udf", ConnectorType.of("BOOLEAN"), List.of(col("b")))));
        Assertions.assertEquals("SELECT \"a\" FROM \"main\".\"t1\"", sql(columns("a"), either, -1));
    }

    @Test
    void refusesTheConstructsOutsideTheConservativeSet() {
        Assertions.assertEquals("SELECT \"a\" FROM \"main\".\"t1\"",
                sql(columns("a"), new ConnectorLike(ConnectorLike.Operator.LIKE, col("b"),
                        ConnectorLiteral.ofString("x%")), -1));
        Assertions.assertEquals("SELECT \"a\" FROM \"main\".\"t1\"",
                sql(columns("a"), new ConnectorBetween(col("a"), num(1), num(9)), -1));
        Assertions.assertEquals("SELECT \"a\" FROM \"main\".\"t1\"",
                sql(columns("a"), new ConnectorFunctionCall("abs", ConnectorType.of("INT"),
                        List.of(col("a"))), -1));
    }

    @Test
    void refusesNullSafeEquality() {
        // Standard SQL has no portable spelling for it, and every substitute differs from it on nulls --
        // which changes which rows match instead of failing.
        Assertions.assertEquals("SELECT \"a\" FROM \"main\".\"t1\"",
                sql(columns("a"), new ConnectorComparison(
                        ConnectorComparison.Operator.EQ_FOR_NULL, col("a"), num(1)), -1));
    }

    @Test
    void refusesAComparisonWhoseLiteralTheDialectCannotRender() {
        Assertions.assertEquals("SELECT \"a\" FROM \"main\".\"t1\"",
                sql(columns("a"), new ConnectorComparison(ConnectorComparison.Operator.EQ, col("a"),
                        ConnectorLiteral.ofNull(ConnectorType.of("INT"))), -1));
    }

    // ---------- the row limit ----------

    @Test
    void pushesTheLimitWhenEveryPredicateWentWithIt() {
        Assertions.assertEquals("SELECT \"a\" FROM \"main\".\"t1\" WHERE (\"a\" > 1) LIMIT 5",
                sql(columns("a"),
                        new ConnectorComparison(ConnectorComparison.Operator.GT, col("a"), num(1)), 5));
        Assertions.assertEquals("SELECT \"a\" FROM \"main\".\"t1\" LIMIT 5", sql(columns("a"), null, 5));
    }

    @Test
    void withholdsTheLimitWhenAPredicateStayedBehind() {
        // THE row-count bug this file exists to prevent: the source would return 5 rows, BE would then
        // apply the predicate it kept, and the query would answer with fewer than 5.
        String generated = sql(columns("a"), new ConnectorAnd(List.of(
                new ConnectorComparison(ConnectorComparison.Operator.GT, col("a"), num(1)),
                new ConnectorLike(ConnectorLike.Operator.LIKE, col("b"),
                        ConnectorLiteral.ofString("x%")))), 5);
        Assertions.assertEquals("SELECT \"a\" FROM \"main\".\"t1\" WHERE (\"a\" > 1)", generated);
        Assertions.assertFalse(AdbcQueryBuilder.build(ANSI, T1, columns("a"),
                Optional.of(new ConnectorLike(ConnectorLike.Operator.LIKE, col("b"),
                        ConnectorLiteral.ofString("x%"))), 5).isAllFiltersPushed());
    }

    @Test
    void emitsNoLimitClauseWhenThereIsNoLimit() {
        Assertions.assertFalse(sql(columns("a"), null, -1).contains("LIMIT"));
        Assertions.assertFalse(sql(columns("a"), null, 0).contains("LIMIT"));
    }

    @Test
    void withholdsTheLimitFromADialectThatHasNoLimitClause() {
        AdbcDialect noLimit = new BacktickDialect() {
            @Override
            public boolean supportsLimitClause() {
                return false;
            }
        };
        Assertions.assertFalse(
                AdbcQueryBuilder.build(noLimit, T1, columns("a"), Optional.empty(), 5)
                        .getSql().contains("LIMIT"));
    }

    // ---------- the extension point ----------

    @Test
    void speaksAnyRegisteredDialectWithoutTheBuilderKnowingIt() {
        // The invariant the dialect layer exists for: a dialect defined entirely outside this connector
        // changes every part of the statement, and the builder has no branch for it. If this ever needs a
        // change inside AdbcQueryBuilder, the layer has stopped doing its job.
        Assertions.assertEquals("SELECT `a` FROM `t1` WHERE (`a` > <10>) LIMIT 5",
                AdbcQueryBuilder.build(new BacktickDialect(), T1, columns("a"),
                        Optional.of(new ConnectorComparison(
                                ConnectorComparison.Operator.GT, col("a"), num(10))), 5).getSql());
    }

    /** Quotes, qualifies and spells literals differently from every shipped dialect. */
    private static class BacktickDialect implements AdbcDialect {

        @Override
        public String name() {
            return "backtick";
        }

        @Override
        public String quoteIdentifier(String identifier) {
            return "`" + identifier + "`";
        }

        @Override
        public String qualifiedTableName(AdbcTableHandle handle) {
            return quoteIdentifier(handle.getRemoteTable());
        }

        @Override
        public String renderLiteral(ConnectorLiteral value) {
            return "<" + value.getValue() + ">";
        }
    }
}
