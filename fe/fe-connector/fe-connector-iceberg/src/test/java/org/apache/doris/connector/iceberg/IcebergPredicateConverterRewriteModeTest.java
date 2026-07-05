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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.pushdown.ConnectorAnd;
import org.apache.doris.connector.api.pushdown.ConnectorBetween;
import org.apache.doris.connector.api.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.api.pushdown.ConnectorComparison;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.pushdown.ConnectorIn;
import org.apache.doris.connector.api.pushdown.ConnectorIsNull;
import org.apache.doris.connector.api.pushdown.ConnectorLiteral;
import org.apache.doris.connector.api.pushdown.ConnectorNot;
import org.apache.doris.connector.api.pushdown.ConnectorOr;

import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;

/**
 * REWRITE-mode (rewrite_data_files file scoping, P6.6-FIX-H9) tests for {@link IcebergPredicateConverter}.
 * REWRITE mode mirrors legacy {@code IcebergNereidsUtils.convertNereidsToIcebergExpression}: the broad node set
 * (cross-column {@code OR}, any-child {@code NOT}, {@code NE}, {@code IN}, {@code IS NULL}, {@code BETWEEN}) but
 * strictly all-or-nothing (precise or dropped, never widened). It differs from {@link Mode#CONFLICT}, which
 * narrows cross-column OR / NOT(comparison) / NE; and from {@link Mode#SCAN}, which has no IS NULL / BETWEEN node
 * case and degrades AND. The user-signed contract (2026-06-29「精确下推否则报错」): the forms the live (master)
 * code pushed must push again, while a partially-unrepresentable WHERE must collapse so the rewrite planner can
 * fail loud rather than silently widen the set of files compacted.
 */
public class IcebergPredicateConverterRewriteModeTest {

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "c_int", Types.IntegerType.get()),
            Types.NestedField.required(2, "c_long", Types.LongType.get()),
            Types.NestedField.required(3, "c_str", Types.StringType.get()),
            Types.NestedField.optional(4, "c_list", Types.ListType.ofOptional(5, Types.IntegerType.get())),
            Types.NestedField.optional(6, "c_date", Types.DateType.get()));

    private static IcebergPredicateConverter rewrite() {
        return new IcebergPredicateConverter(SCHEMA, ZoneOffset.UTC, IcebergPredicateConverter.Mode.REWRITE);
    }

    private static IcebergPredicateConverter scan() {
        return new IcebergPredicateConverter(SCHEMA, ZoneOffset.UTC);
    }

    private static ConnectorColumnRef col(String name) {
        return new ConnectorColumnRef(name, ConnectorType.of("UNKNOWN"));
    }

    private static ConnectorLiteral intLit(long v) {
        return new ConnectorLiteral(ConnectorType.of("INT"), v);
    }

    private static ConnectorLiteral strLit(String v) {
        return new ConnectorLiteral(ConnectorType.of("STRING"), v);
    }

    private static ConnectorComparison cmp(ConnectorComparison.Operator op, String c, ConnectorLiteral lit) {
        return new ConnectorComparison(op, col(c), lit);
    }

    private static String pushed(ConnectorExpression expr) {
        List<Expression> out = rewrite().convert(expr);
        Assertions.assertEquals(1, out.size(), "expected exactly one pushed rewrite expression");
        return out.get(0).toString();
    }

    private static void dropped(IcebergPredicateConverter conv, ConnectorExpression expr) {
        Assertions.assertTrue(conv.convert(expr).isEmpty(), "expected the predicate to be dropped");
    }

    // ---- the regression forms: cross-column OR / NOT(comparison) / NE (conflict mode drops these) ----

    @Test
    public void crossColumnOrPushed() {
        // The headline regression: `c_int = 1 OR c_str = 'x'` -- master pushed it, conflict mode rejects it
        // (different columns). REWRITE pushes the exact disjunction so file pruning honors the user's WHERE.
        ConnectorExpression crossColumnOr = new ConnectorOr(Arrays.asList(
                cmp(ConnectorComparison.Operator.EQ, "c_int", intLit(1)),
                cmp(ConnectorComparison.Operator.EQ, "c_str", strLit("x"))));
        Assertions.assertEquals(
                Expressions.or(Expressions.equal("c_int", 1), Expressions.equal("c_str", "x")).toString(),
                pushed(crossColumnOr));
    }

    @Test
    public void notComparisonPushed() {
        // `NOT(c_int > 5)` -- conflict mode allows NOT only over IS NULL; REWRITE allows NOT over any child.
        ConnectorExpression notCmp = new ConnectorNot(cmp(ConnectorComparison.Operator.GT, "c_int", intLit(5)));
        Assertions.assertEquals(Expressions.not(Expressions.greaterThan("c_int", 5)).toString(), pushed(notCmp));
    }

    @Test
    public void notEqualPushedViaBothForms() {
        // `!=` reaches the connector either as a NE comparison or (the parser form, LogicalPlanBuilder:3030) as
        // Not(EQ); both lower to not(equal). Conflict mode drops NE entirely.
        String expected = Expressions.not(Expressions.equal("c_int", 1)).toString();
        Assertions.assertEquals(expected, pushed(cmp(ConnectorComparison.Operator.NE, "c_int", intLit(1))));
        Assertions.assertEquals(expected,
                pushed(new ConnectorNot(cmp(ConnectorComparison.Operator.EQ, "c_int", intLit(1)))));
    }

    // ---- node-emitted IS NULL / BETWEEN / IN (scan mode has no node case for the first two) ----

    @Test
    public void isNullPushed() {
        Assertions.assertEquals(Expressions.isNull("c_int").toString(),
                pushed(new ConnectorIsNull(col("c_int"), false)));
    }

    @Test
    public void isNotNullViaNegatedNodePushed() {
        // A negated ConnectorIsNull (IS NOT NULL) -> not(isNull). Guards the isNegated arm of buildRewriteIsNull.
        Assertions.assertEquals(Expressions.not(Expressions.isNull("c_int")).toString(),
                pushed(new ConnectorIsNull(col("c_int"), true)));
    }

    @Test
    public void betweenPushed() {
        ConnectorExpression between = new ConnectorBetween(col("c_int"), intLit(1), intLit(10));
        Assertions.assertEquals(
                Expressions.and(Expressions.greaterThanOrEqual("c_int", 1),
                        Expressions.lessThanOrEqual("c_int", 10)).toString(),
                pushed(between));
    }

    @Test
    public void inPushed() {
        ConnectorExpression in = new ConnectorIn(col("c_int"), Arrays.asList(intLit(1), intLit(2)), false);
        Assertions.assertEquals(Expressions.in("c_int", 1, 2).toString(), pushed(in));
    }

    @Test
    public void betweenWithValidDateBoundsPushed() {
        // Both temporal bounds bind -> the full and(gte, lte) is pushed.
        ConnectorExpression between = new ConnectorBetween(col("c_date"), strLit("2020-01-01"), strLit("2020-12-31"));
        Assertions.assertEquals(
                Expressions.and(Expressions.greaterThanOrEqual("c_date", "2020-01-01"),
                        Expressions.lessThanOrEqual("c_date", "2020-12-31")).toString(),
                pushed(between));
    }

    @Test
    public void betweenWithOneMalformedBoundDroppedNotWidened() {
        // The silent-widen hole the fix closes. extractIcebergLiteral passes temporal STRING bounds through
        // unvalidated, so `c_date BETWEEN '2020-01-01' AND '2020-12-1'` builds and(gte('2020-01-01'),
        // lte('2020-12-1')) with both bounds non-null. The malformed upper ('2020-12-1', non-ISO) only fails at
        // BIND time. Without the REWRITE all-or-nothing gate in checkConversion, the AND would degrade to the
        // surviving gte alone -> a predicate WEAKER than the WHERE that passes the planner's count guard ->
        // silently rewrite every file with c_date >= '2020-01-01'. REWRITE must DROP the whole BETWEEN so the
        // planner fails loud (master hands the raw and to planFiles(), which throws on the bad bound).
        ConnectorExpression between = new ConnectorBetween(col("c_date"), strLit("2020-01-01"), strLit("2020-12-1"));
        dropped(rewrite(), between);
    }

    // ---- all-or-nothing: never silently widen (the R7 invariant the user kept) ----

    @Test
    public void nestedUnconvertibleArmCollapsesWholeOr() {
        // `c_str = 'x' OR (c_int = 1 AND c_int = 'bad')`: the inner `c_int = 'bad'` cannot bind (int column vs a
        // non-numeric string literal). REWRITE must DROP the whole expression (all-or-nothing) so the planner
        // fails loud; SCAN would degrade the inner AND to `c_int = 1` and WIDEN the OR -- the precise contrast
        // this test pins. If REWRITE's buildAnd silently dropped the bad arm (mutation), REWRITE would push too.
        ConnectorExpression badInt = cmp(ConnectorComparison.Operator.EQ, "c_int", strLit("bad"));
        ConnectorExpression nested = new ConnectorOr(Arrays.asList(
                cmp(ConnectorComparison.Operator.EQ, "c_str", strLit("x")),
                new ConnectorAnd(Arrays.asList(cmp(ConnectorComparison.Operator.EQ, "c_int", intLit(1)), badInt))));

        dropped(rewrite(), nested);
        Assertions.assertFalse(scan().convert(nested).isEmpty(),
                "scan mode degrades the inner AND and widens the OR -- the behavior REWRITE must NOT share");
    }

    @Test
    public void topLevelMultiConjunctAndFlattensToList() {
        // A top-level ConnectorAnd is flattened to one iceberg expression per conjunct (the planner applies each
        // as a separate scan.filter). Both convertible -> size 2; the planner's size-vs-count guard then accepts.
        ConnectorExpression and = new ConnectorAnd(Arrays.asList(
                cmp(ConnectorComparison.Operator.GE, "c_int", intLit(1)),
                cmp(ConnectorComparison.Operator.LE, "c_int", intLit(9))));
        List<Expression> out = rewrite().convert(and);
        Assertions.assertEquals(2, out.size());
        Assertions.assertEquals(Expressions.greaterThanOrEqual("c_int", 1).toString(), out.get(0).toString());
        Assertions.assertEquals(Expressions.lessThanOrEqual("c_int", 9).toString(), out.get(1).toString());
    }

    @Test
    public void unrepresentableLeafDropped() {
        // A column not in the schema and a struct/list column cannot be pushed; REWRITE drops them (the planner
        // turns a dropped top-level conjunct into a hard error).
        dropped(rewrite(), cmp(ConnectorComparison.Operator.EQ, "c_missing", intLit(1)));
        dropped(rewrite(), cmp(ConnectorComparison.Operator.EQ, "c_list", intLit(1)));
    }

    // ---- scan mode is untouched by the new REWRITE branch (regression guard) ----

    @Test
    public void scanModeStillHasNoIsNullOrBetweenNodeCase() {
        // The rewrite-side neutral converter emits IS NULL / BETWEEN nodes directly; scan mode (fed pre-lowered
        // comparisons) has no case for them and drops them. This proves the REWRITE additions did not leak into
        // scan dispatch (mode == REWRITE gate).
        dropped(scan(), new ConnectorIsNull(col("c_int"), false));
        dropped(scan(), new ConnectorBetween(col("c_int"), intLit(1), intLit(10)));
    }
}
