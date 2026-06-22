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
import org.apache.doris.connector.api.pushdown.ConnectorLike;
import org.apache.doris.connector.api.pushdown.ConnectorLiteral;
import org.apache.doris.connector.api.pushdown.ConnectorNot;
import org.apache.doris.connector.api.pushdown.ConnectorOr;

import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Parity tests for {@link IcebergPredicateConverter}, the self-contained port of fe-core
 * {@code IcebergUtils.convertToIcebergExpr}. The primary oracle is the 9-column x 13-literal pushability
 * grid copied verbatim from fe-core {@code IcebergPredicateTest} (same schema, same literals, same expected
 * grid) — translated to the {@link ConnectorExpression} input the SPI delivers. If a (column, literal) pair
 * is pushable in legacy it must be pushable here, and vice versa. No Mockito; fail-loud assertions.
 */
public class IcebergPredicateConverterTest {

    // Same schema (ids/types) as fe-core IcebergPredicateTest.
    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "c_int", Types.IntegerType.get()),
            Types.NestedField.required(2, "c_long", Types.LongType.get()),
            Types.NestedField.required(3, "c_bool", Types.BooleanType.get()),
            Types.NestedField.required(4, "c_float", Types.FloatType.get()),
            Types.NestedField.required(5, "c_double", Types.DoubleType.get()),
            Types.NestedField.required(6, "c_dec", Types.DecimalType.of(20, 10)),
            Types.NestedField.required(7, "c_date", Types.DateType.get()),
            Types.NestedField.required(8, "c_ts", Types.TimestampType.withoutZone()),
            Types.NestedField.required(10, "c_str", Types.StringType.get()));

    private static final String[] COLS = {
            "c_int", "c_long", "c_bool", "c_float", "c_double", "c_dec", "c_date", "c_ts", "c_str"};

    // The 13 literals, carrying the same Java value + Doris source type ExprToConnectorExpressionConverter emits.
    private static List<ConnectorLiteral> literals() {
        return Arrays.asList(
                new ConnectorLiteral(ConnectorType.of("BOOLEAN"), Boolean.TRUE),
                new ConnectorLiteral(ConnectorType.of("DATEV2"), LocalDate.of(2023, 1, 2)),
                new ConnectorLiteral(ConnectorType.of("DATETIMEV2"),
                        LocalDateTime.of(2024, 1, 2, 12, 34, 56, 123456000)),
                new ConnectorLiteral(ConnectorType.of("DECIMALV3", 3, 2), new BigDecimal("1.23")),
                new ConnectorLiteral(ConnectorType.of("FLOAT"), 1.23d),
                new ConnectorLiteral(ConnectorType.of("DOUBLE"), 3.456d),
                new ConnectorLiteral(ConnectorType.of("TINYINT"), 1L),
                new ConnectorLiteral(ConnectorType.of("SMALLINT"), 1L),
                new ConnectorLiteral(ConnectorType.of("INT"), 1L),
                new ConnectorLiteral(ConnectorType.of("BIGINT"), 1L),
                new ConnectorLiteral(ConnectorType.of("VARCHAR"), "abc"),
                new ConnectorLiteral(ConnectorType.of("VARCHAR"), "2023-01-02"),
                new ConnectorLiteral(ConnectorType.of("VARCHAR"), "2023-01-02 01:02:03.456789"));
    }

    // Verbatim grid from fe-core IcebergPredicateTest (true == pushable). Rows follow COLS order.
    private static final boolean[][] EXPECTS = {
            {false, false, false, false, false, false, true, true, true, true, false, false, false},   // c_int
            {false, false, false, false, false, false, true, true, true, true, false, false, false},   // c_long
            {true, false, false, false, false, false, false, false, false, false, false, false, false}, // c_bool
            {false, false, false, false, true, false, true, true, true, true, false, false, false},     // c_float
            {false, false, false, true, true, true, true, true, true, true, false, false, false},       // c_double
            {false, false, false, true, true, true, true, true, true, true, false, false, false},       // c_dec
            {false, true, false, false, false, false, true, true, true, true, false, true, false},      // c_date
            {false, true, true, false, false, false, false, false, false, true, false, false, false},   // c_ts
            {true, true, true, true, false, false, false, false, false, false, true, true, true}        // c_str
    };

    private static IcebergPredicateConverter converter() {
        // Session zone UTC: the grid's only timestamp column is withoutZone (shouldAdjustToUTC == false),
        // so the zone is not consulted for the grid; UTC keeps the test deterministic.
        return new IcebergPredicateConverter(SCHEMA, ZoneOffset.UTC);
    }

    private static ConnectorColumnRef col(String name) {
        return new ConnectorColumnRef(name, ConnectorType.of("UNKNOWN"));
    }

    private static ConnectorComparison eq(String colName, ConnectorLiteral lit) {
        return new ConnectorComparison(ConnectorComparison.Operator.EQ, col(colName), lit);
    }

    /**
     * The EQ pushability grid is the canonical parity oracle: each (column, literal) pair must push iff legacy
     * pushed it. This encodes WHY pushdown matters — a divergent cell means iceberg files would be pruned
     * differently than the legacy IcebergScanNode, breaking partition/row-count parity. MUTATION: any
     * single-cell change in extractIcebergLiteral / checkConversion flips a cell and reddens this.
     */
    @Test
    public void binaryEqGridMatchesLegacy() {
        List<ConnectorLiteral> lits = literals();
        for (int i = 0; i < COLS.length; i++) {
            for (int j = 0; j < lits.size(); j++) {
                boolean pushed = !converter().convert(eq(COLS[i], lits.get(j))).isEmpty();
                Assertions.assertEquals(EXPECTS[i][j], pushed,
                        "EQ grid mismatch at column " + COLS[i] + " literal#" + j + " (" + lits.get(j) + ")");
            }
        }
    }

    /** IN and NOT-IN use the same per-element pushability matrix as EQ (legacy parity). */
    @Test
    public void inAndNotInGridMatchLegacy() {
        List<ConnectorLiteral> lits = literals();
        for (int i = 0; i < COLS.length; i++) {
            for (int j = 0; j < lits.size(); j++) {
                ConnectorIn in = new ConnectorIn(col(COLS[i]),
                        Collections.singletonList(lits.get(j)), false);
                ConnectorIn notIn = new ConnectorIn(col(COLS[i]),
                        Collections.singletonList(lits.get(j)), true);
                String where = "column " + COLS[i] + " literal#" + j;
                Assertions.assertEquals(EXPECTS[i][j], !converter().convert(in).isEmpty(), "IN " + where);
                Assertions.assertEquals(EXPECTS[i][j], !converter().convert(notIn).isEmpty(), "NOT IN " + where);
            }
        }
    }

    /** A single unconvertible IN element drops the whole IN (legacy parity: all-or-nothing on the value list). */
    @Test
    public void inWithOneBadElementDropsWholeIn() {
        ConnectorIn in = new ConnectorIn(col("c_int"),
                Arrays.asList(new ConnectorLiteral(ConnectorType.of("INT"), 1L),
                        new ConnectorLiteral(ConnectorType.of("VARCHAR"), "abc")), // "abc" -> INTEGER fails
                false);
        // MUTATION: collecting only the convertible elements (instead of dropping the whole IN) -> red.
        Assertions.assertTrue(converter().convert(in).isEmpty());
    }

    /** A bare boolean literal maps to alwaysTrue / alwaysFalse (legacy BoolLiteral path). */
    @Test
    public void boolLiteralMapsToAlwaysTrueFalse() {
        List<Expression> t = converter().convert(new ConnectorLiteral(ConnectorType.of("BOOLEAN"), Boolean.TRUE));
        List<Expression> f = converter().convert(new ConnectorLiteral(ConnectorType.of("BOOLEAN"), Boolean.FALSE));
        Assertions.assertEquals(Expression.Operation.TRUE, t.get(0).op());
        Assertions.assertEquals(Expression.Operation.FALSE, f.get(0).op());
    }

    /** col {@code <=>} NULL becomes IS NULL; every other null-valued comparison is dropped (legacy parity). */
    @Test
    public void eqForNullMapsToIsNull() {
        ConnectorComparison cmp = new ConnectorComparison(ConnectorComparison.Operator.EQ_FOR_NULL,
                col("c_int"), ConnectorLiteral.ofNull(ConnectorType.of("INT")));
        List<Expression> out = converter().convert(cmp);
        Assertions.assertEquals(Expression.Operation.IS_NULL, out.get(0).op());
        // A plain EQ against NULL is NOT pushable.
        Assertions.assertTrue(converter().convert(new ConnectorComparison(ConnectorComparison.Operator.EQ,
                col("c_int"), ConnectorLiteral.ofNull(ConnectorType.of("INT")))).isEmpty());
    }

    /**
     * Top-level AND flattens into one filter per pushable conjunct; an unconvertible conjunct is dropped while
     * the pushable ones survive (mirrors legacy createTableScan's per-conjunct scan.filter loop + AND keep-arm).
     */
    @Test
    public void topLevelAndDropsUnpushableConjunctKeepsRest() {
        ConnectorComparison valid = eq("c_int", new ConnectorLiteral(ConnectorType.of("INT"), 1L));
        ConnectorComparison invalid = eq("c_int", new ConnectorLiteral(ConnectorType.of("VARCHAR"), "abc"));
        List<Expression> out = converter().convert(new ConnectorAnd(Arrays.asList(valid, invalid)));
        Assertions.assertEquals(1, out.size());
        Assertions.assertEquals(Expression.Operation.EQ, out.get(0).op());
    }

    /**
     * The load-bearing nested case: {@code (valid AND invalid) OR valid2} must degrade to {@code valid OR
     * valid2} — the unbindable leaf is dropped BEFORE the OR is built, so the resulting OR binds cleanly. If
     * convertSingle didn't fold checkConversion into the recursion, the OR would carry an unbindable predicate
     * (a planning-time bind crash). MUTATION: build the OR from un-bind-checked children -> the assertion that
     * the result binds without throwing reddens.
     */
    @Test
    public void nestedAndInsideOrDegradesUnbindableLeaf() {
        ConnectorComparison valid = eq("c_int", new ConnectorLiteral(ConnectorType.of("INT"), 1L));
        ConnectorComparison invalid = eq("c_int", new ConnectorLiteral(ConnectorType.of("VARCHAR"), "abc"));
        ConnectorComparison valid2 = eq("c_long", new ConnectorLiteral(ConnectorType.of("BIGINT"), 2L));
        ConnectorExpression expr = new ConnectorOr(Arrays.asList(
                new ConnectorAnd(Arrays.asList(valid, invalid)), valid2));
        List<Expression> out = converter().convert(expr);
        Assertions.assertEquals(1, out.size());
        Assertions.assertEquals(Expression.Operation.OR, out.get(0).op());
        // Proves no unbindable predicate leaked into the OR: binding the whole tree must not throw.
        Assertions.assertDoesNotThrow(() ->
                org.apache.iceberg.expressions.Binder.bind(SCHEMA.asStruct(), out.get(0), true));
    }

    /** OR is all-or-nothing: one unpushable disjunct collapses the entire OR (dropping an arm widens results). */
    @Test
    public void orWithUnpushableDisjunctIsDropped() {
        ConnectorComparison valid = eq("c_int", new ConnectorLiteral(ConnectorType.of("INT"), 1L));
        ConnectorComparison invalid = eq("c_int", new ConnectorLiteral(ConnectorType.of("VARCHAR"), "abc"));
        Assertions.assertTrue(converter().convert(new ConnectorOr(Arrays.asList(valid, invalid))).isEmpty());
    }

    /** NOT is pushed iff its child is pushable. */
    @Test
    public void notIsPushedIffChildPushable() {
        ConnectorComparison valid = eq("c_int", new ConnectorLiteral(ConnectorType.of("INT"), 1L));
        ConnectorComparison invalid = eq("c_int", new ConnectorLiteral(ConnectorType.of("VARCHAR"), "abc"));
        Assertions.assertEquals(Expression.Operation.NOT,
                converter().convert(new ConnectorNot(valid)).get(0).op());
        Assertions.assertTrue(converter().convert(new ConnectorNot(invalid)).isEmpty());
    }

    /** Reversed `literal OP col` and col-col comparisons are not pushed (column-op-literal only). */
    @Test
    public void reversedAndColColComparisonsDropped() {
        ConnectorLiteral lit = new ConnectorLiteral(ConnectorType.of("INT"), 1L);
        Assertions.assertTrue(converter().convert(
                new ConnectorComparison(ConnectorComparison.Operator.EQ, lit, col("c_int"))).isEmpty());
        Assertions.assertTrue(converter().convert(
                new ConnectorComparison(ConnectorComparison.Operator.EQ, col("c_int"), col("c_long"))).isEmpty());
    }

    /** Column resolution is case-insensitive and rewrites to the canonical schema-cased name. */
    @Test
    public void columnResolutionIsCaseInsensitive() {
        List<Expression> out = converter().convert(
                eq("C_INT", new ConnectorLiteral(ConnectorType.of("INT"), 1L)));
        Assertions.assertEquals(1, out.size());
        // The bound/unbound predicate must reference the canonical "c_int" (not "C_INT").
        Assertions.assertTrue(out.get(0).toString().contains("c_int"), out.get(0).toString());
    }

    /** v3 row-lineage metadata columns are never pushed (mirror getPushdownField block). */
    @Test
    public void metadataColumnsBlocked() {
        Assertions.assertTrue(converter().convert(
                eq("_row_id", new ConnectorLiteral(ConnectorType.of("BIGINT"), 1L))).isEmpty());
        Assertions.assertTrue(converter().convert(
                eq("_last_updated_sequence_number",
                        new ConnectorLiteral(ConnectorType.of("BIGINT"), 1L))).isEmpty());
    }

    /** A predicate on a column absent from the schema is dropped. */
    @Test
    public void unknownColumnDropped() {
        Assertions.assertTrue(converter().convert(
                eq("no_such_col", new ConnectorLiteral(ConnectorType.of("INT"), 1L))).isEmpty());
    }

    /**
     * Node types legacy convertToIcebergExpr has no case for are dropped (IS NULL via ConnectorIsNull, LIKE,
     * BETWEEN) — BE residual-filters them. This is a deliberate divergence from paimon (which pushes IS NULL /
     * startsWith); see the design's deviations. MUTATION: adding a ConnectorIsNull/Like case -> red.
     */
    @Test
    public void unsupportedNodeTypesDropped() {
        Assertions.assertTrue(converter().convert(new ConnectorIsNull(col("c_int"), false)).isEmpty());
        Assertions.assertTrue(converter().convert(new ConnectorIsNull(col("c_int"), true)).isEmpty());
        Assertions.assertTrue(converter().convert(new ConnectorLike(ConnectorLike.Operator.LIKE,
                col("c_str"), new ConnectorLiteral(ConnectorType.of("VARCHAR"), "ab%"))).isEmpty());
        Assertions.assertTrue(converter().convert(new ConnectorBetween(col("c_int"),
                new ConnectorLiteral(ConnectorType.of("INT"), 1L),
                new ConnectorLiteral(ConnectorType.of("INT"), 9L))).isEmpty());
    }

    /** null input yields no predicates (no NPE). */
    @Test
    public void nullInputYieldsEmpty() {
        Assertions.assertTrue(converter().convert(null).isEmpty());
    }

    /**
     * For a zone-adjusted (timestamptz) column the wall-clock literal is interpreted in the SESSION zone, so
     * the pushed epoch-micros depend on the zone (mirrors legacy getUnixTimestampWithMicroseconds(session tz)).
     * The grid oracle never exercised a withZone() column — that gap is exactly why the alias-resolution
     * regression was UT-invisible. MUTATION: hardcoding UTC for timestamptz -> the two micros match -> red.
     */
    @Test
    public void timestamptzLiteralUsesSessionZone() {
        Schema tz = new Schema(Types.NestedField.required(1, "ts", Types.TimestampType.withZone()));
        LocalDateTime dt = LocalDateTime.of(2024, 1, 2, 12, 0, 0);
        ConnectorComparison cmp = new ConnectorComparison(ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("ts", ConnectorType.of("UNKNOWN")),
                new ConnectorLiteral(ConnectorType.of("DATETIMEV2"), dt));
        long cstMicros = singleMicros(new IcebergPredicateConverter(tz, ZoneId.of("Asia/Shanghai")).convert(cmp));
        long utcMicros = singleMicros(new IcebergPredicateConverter(tz, ZoneOffset.UTC).convert(cmp));
        // +08:00 is 8h ahead, so its epoch instant for the same wall clock is 8h earlier than UTC's.
        Assertions.assertEquals(8L * 3600 * 1_000_000L, utcMicros - cstMicros);
    }

    private static long singleMicros(List<Expression> out) {
        Assertions.assertEquals(1, out.size());
        Object value = ((org.apache.iceberg.expressions.UnboundPredicate<?>) out.get(0)).literal().value();
        return ((Number) value).longValue();
    }
}
