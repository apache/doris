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

package org.apache.doris.datasource.connector.converter;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
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
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Between;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DecimalV3Type;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * Unit tests for {@link NereidsToConnectorExpressionConverter} (P6.3-T07b, O5-2 production half).
 *
 * <p>The converter mirrors the legacy iceberg conflict matrix
 * ({@code IcebergNereidsUtils.convertNereidsToIcebergExpression}): And/Or/Not, the 5 comparisons,
 * IN, IS NULL, BETWEEN. Forms the legacy conflict path drops (Cast-wrapped column, NullSafeEqual,
 * col-col) yield {@code null} — a safe over-approximation that never narrows the conflict filter
 * (no missed conflict). Literal encoding routes through the analyzed-plan-identical
 * {@code Expr -> ConnectorExpression} path so type tokens stay byte-identical with the scan side.</p>
 */
public class NereidsToConnectorExpressionConverterTest {

    private static SlotReference slot(String name, ScalarType type) {
        Column column = new Column(name, type);
        return SlotReference.fromColumn(
                StatementScopeIdGenerator.newExprId(), Mockito.mock(TableIf.class), column, ImmutableList.of());
    }

    private static ConnectorLiteral rightLiteralOf(ConnectorExpression cmp) {
        Assertions.assertInstanceOf(ConnectorComparison.class, cmp);
        ConnectorExpression right = ((ConnectorComparison) cmp).getRight();
        Assertions.assertInstanceOf(ConnectorLiteral.class, right);
        return (ConnectorLiteral) right;
    }

    private static String colNameOf(ConnectorExpression cmp) {
        ConnectorExpression left = ((ConnectorComparison) cmp).getLeft();
        Assertions.assertInstanceOf(ConnectorColumnRef.class, left);
        return ((ConnectorColumnRef) left).getColumnName();
    }

    // ---- C2: integer literal type tokens must be the UPPERCASE primitive name (int32 != int64) ----

    @Test
    public void intLiteralUsesUppercaseIntToken() {
        ConnectorExpression e = NereidsToConnectorExpressionConverter.convert(
                new EqualTo(slot("a", ScalarType.INT), new IntegerLiteral(1)));
        ConnectorLiteral lit = rightLiteralOf(e);
        // The BE-facing type matrix in IcebergPredicateConverter.isInteger32 keys off this exact token;
        // a lowercase "integer" would mis-tag int32 as int64 and silently mis-convert. Pin it.
        Assertions.assertEquals("INT", lit.getType().getTypeName());
        Assertions.assertEquals(1L, lit.getValue());
        Assertions.assertEquals("a", colNameOf(e));
        Assertions.assertEquals(ConnectorComparison.Operator.EQ, ((ConnectorComparison) e).getOperator());
    }

    @Test
    public void integerFamilyTokensDistinguishWidth() {
        Assertions.assertEquals("TINYINT", rightLiteralOf(NereidsToConnectorExpressionConverter.convert(
                new EqualTo(slot("a", ScalarType.TINYINT), new TinyIntLiteral((byte) 1)))).getType().getTypeName());
        Assertions.assertEquals("SMALLINT", rightLiteralOf(NereidsToConnectorExpressionConverter.convert(
                new EqualTo(slot("a", ScalarType.SMALLINT), new SmallIntLiteral((short) 1)))).getType().getTypeName());
        Assertions.assertEquals("BIGINT", rightLiteralOf(NereidsToConnectorExpressionConverter.convert(
                new EqualTo(slot("a", ScalarType.BIGINT), new BigIntLiteral(1L)))).getType().getTypeName());
    }

    // ---- C1: DECIMAL literals carry precision/scale (the scan-side extractIcebergLiteral ignores it,
    //          but the type must still round-trip identically to the analyzed-plan path) ----

    @Test
    public void decimalLiteralCarriesPrecisionAndScale() {
        ConnectorLiteral lit = rightLiteralOf(NereidsToConnectorExpressionConverter.convert(
                new EqualTo(slot("d", ScalarType.createDecimalV3Type(10, 2)),
                        new DecimalV3Literal(DecimalV3Type.createDecimalV3Type(10, 2), new BigDecimal("1.23")))));
        Assertions.assertEquals(10, lit.getType().getPrecision());
        Assertions.assertEquals(2, lit.getType().getScale());
        Assertions.assertEquals(new BigDecimal("1.23"), lit.getValue());
    }

    @Test
    public void dateAndDatetimeLiteralsBecomeJavaTimeValues() {
        ConnectorLiteral date = rightLiteralOf(NereidsToConnectorExpressionConverter.convert(
                new EqualTo(slot("dt", ScalarType.DATEV2), new DateV2Literal(2023, 1, 2))));
        Assertions.assertEquals(LocalDate.of(2023, 1, 2), date.getValue());

        ConnectorLiteral ts = rightLiteralOf(NereidsToConnectorExpressionConverter.convert(
                new EqualTo(slot("ts", ScalarType.createDatetimeV2Type(0)),
                        new DateTimeV2Literal(2024, 1, 2, 12, 34, 56))));
        Assertions.assertEquals(LocalDateTime.of(2024, 1, 2, 12, 34, 56), ts.getValue());
    }

    // ---- node shape mapping ----

    @Test
    public void comparisonOperatorsMap() {
        Assertions.assertEquals(ConnectorComparison.Operator.GT, ((ConnectorComparison)
                NereidsToConnectorExpressionConverter.convert(
                        new GreaterThan(slot("a", ScalarType.INT), new IntegerLiteral(1)))).getOperator());
        Assertions.assertEquals(ConnectorComparison.Operator.GE, ((ConnectorComparison)
                NereidsToConnectorExpressionConverter.convert(
                        new GreaterThanEqual(slot("a", ScalarType.INT), new IntegerLiteral(1)))).getOperator());
        Assertions.assertEquals(ConnectorComparison.Operator.LT, ((ConnectorComparison)
                NereidsToConnectorExpressionConverter.convert(
                        new LessThan(slot("a", ScalarType.INT), new IntegerLiteral(1)))).getOperator());
        Assertions.assertEquals(ConnectorComparison.Operator.LE, ((ConnectorComparison)
                NereidsToConnectorExpressionConverter.convert(
                        new LessThanEqual(slot("a", ScalarType.INT), new IntegerLiteral(1)))).getOperator());
    }

    @Test
    public void reversedOperandsNormalizeColumnToLeft() {
        // `1 = a` must become a column-on-left comparison so the connector (which only pushes
        // column-op-literal) can convert it; legacy convertNereidsBinaryPredicate does the same swap.
        ConnectorExpression e = NereidsToConnectorExpressionConverter.convert(
                new EqualTo(new IntegerLiteral(1), slot("a", ScalarType.INT)));
        Assertions.assertEquals("a", colNameOf(e));
        Assertions.assertEquals(1L, rightLiteralOf(e).getValue());
    }

    @Test
    public void andFlattensToConnectorAnd() {
        ConnectorExpression e = NereidsToConnectorExpressionConverter.convert(new And(
                new EqualTo(slot("a", ScalarType.INT), new IntegerLiteral(1)),
                new EqualTo(slot("b", ScalarType.INT), new IntegerLiteral(2))));
        Assertions.assertInstanceOf(ConnectorAnd.class, e);
        Assertions.assertEquals(2, ((ConnectorAnd) e).getConjuncts().size());
    }

    @Test
    public void orMapsToConnectorOr() {
        ConnectorExpression e = NereidsToConnectorExpressionConverter.convert(new Or(
                new EqualTo(slot("a", ScalarType.INT), new IntegerLiteral(1)),
                new EqualTo(slot("a", ScalarType.INT), new IntegerLiteral(2))));
        Assertions.assertInstanceOf(ConnectorOr.class, e);
        Assertions.assertEquals(2, ((ConnectorOr) e).getDisjuncts().size());
    }

    @Test
    public void orWithUnconvertibleDisjunctDropsEntirelyToNull() {
        // O5-2-GAP-006: OR conversion is all-or-nothing — if ANY disjunct is unconvertible (here a NullSafeEqual,
        // which nullSafeEqualDropsToNull proves drops to null) the WHOLE OR drops to null. Pushing only the
        // convertible disjunct would NARROW the conflict filter (drop the unrepresentable alternative) -> a missed
        // conflict -> unsafe. orMapsToConnectorOr covers only the both-disjuncts-convertible case.
        Assertions.assertNull(NereidsToConnectorExpressionConverter.convert(new Or(
                new EqualTo(slot("a", ScalarType.INT), new IntegerLiteral(1)),
                new NullSafeEqual(slot("b", ScalarType.INT), new IntegerLiteral(2)))));
    }

    @Test
    public void isNullMapsToConnectorIsNullNotNegated() {
        ConnectorExpression e = NereidsToConnectorExpressionConverter.convert(
                new IsNull(slot("a", ScalarType.INT)));
        Assertions.assertInstanceOf(ConnectorIsNull.class, e);
        Assertions.assertFalse(((ConnectorIsNull) e).isNegated());
    }

    @Test
    public void notIsNullMapsToConnectorNotOfConnectorIsNull() {
        // Nereids represents IS NOT NULL as Not(IsNull); preserve that shape so the connector's
        // conflict-mode Not-only-IsNull rule lowers it to not(isNull) (legacy parity).
        ConnectorExpression e = NereidsToConnectorExpressionConverter.convert(
                new Not(new IsNull(slot("a", ScalarType.INT))));
        Assertions.assertInstanceOf(ConnectorNot.class, e);
        Assertions.assertInstanceOf(ConnectorIsNull.class, ((ConnectorNot) e).getOperand());
    }

    @Test
    public void inMapsToConnectorInNotNegated() {
        ConnectorExpression e = NereidsToConnectorExpressionConverter.convert(new InPredicate(
                slot("a", ScalarType.INT), ImmutableList.of(new IntegerLiteral(1), new IntegerLiteral(2))));
        Assertions.assertInstanceOf(ConnectorIn.class, e);
        ConnectorIn in = (ConnectorIn) e;
        Assertions.assertFalse(in.isNegated());
        Assertions.assertEquals(2, in.getInList().size());
        Assertions.assertEquals("a", ((ConnectorColumnRef) in.getValue()).getColumnName());
    }

    @Test
    public void betweenMapsToConnectorBetween() {
        ConnectorExpression e = NereidsToConnectorExpressionConverter.convert(new Between(
                slot("a", ScalarType.INT), new IntegerLiteral(1), new IntegerLiteral(9)));
        Assertions.assertInstanceOf(ConnectorBetween.class, e);
        ConnectorBetween bt = (ConnectorBetween) e;
        Assertions.assertEquals("a", ((ConnectorColumnRef) bt.getValue()).getColumnName());
        Assertions.assertEquals(1L, ((ConnectorLiteral) bt.getLower()).getValue());
        Assertions.assertEquals(9L, ((ConnectorLiteral) bt.getUpper()).getValue());
    }

    // ---- Option A faithfulness: forms legacy conflict path does not handle drop to null (safe) ----

    @Test
    public void castWrappedColumnDropsToNull() {
        // Legacy convertNereidsBinaryPredicate requires a bare Slot; a Cast-wrapped column is not
        // pushable. Unwrapping it would push MORE than legacy -> narrower conflict filter -> unsafe.
        Assertions.assertNull(NereidsToConnectorExpressionConverter.convert(
                new EqualTo(new Cast(slot("a", ScalarType.INT), BigIntType.INSTANCE), new BigIntLiteral(1L))));
    }

    @Test
    public void nullSafeEqualDropsToNull() {
        Assertions.assertNull(NereidsToConnectorExpressionConverter.convert(
                new NullSafeEqual(slot("a", ScalarType.INT), new IntegerLiteral(1))));
    }

    @Test
    public void columnToColumnComparisonDropsToNull() {
        Assertions.assertNull(NereidsToConnectorExpressionConverter.convert(
                new EqualTo(slot("a", ScalarType.INT), slot("b", ScalarType.INT))));
    }

    @Test
    public void booleanLiteralAloneDropsToNull() {
        Assertions.assertNull(NereidsToConnectorExpressionConverter.convert(BooleanLiteral.of(true)));
    }

    // ---- literal-encoding parity with the analyzed-plan-side ExprToConnectorExpressionConverter ----

    @Test
    public void literalEncodingMatchesExprConverter() {
        // The neutral ConnectorLiteral a comparison carries must be byte-identical to what the scan-side
        // ExprToConnectorExpressionConverter produces for the same literal, so the connector's shared
        // IcebergPredicateConverter type matrix behaves identically for both paths.
        IntegerLiteral nereidsLit = new IntegerLiteral(7);
        ConnectorLiteral viaNereids = rightLiteralOf(NereidsToConnectorExpressionConverter.convert(
                new EqualTo(slot("a", ScalarType.INT), nereidsLit)));
        ConnectorExpression viaExpr = ExprToConnectorExpressionConverter.convert(nereidsLit.toLegacyLiteral());
        Assertions.assertEquals(viaExpr, viaNereids);

        VarcharLiteral nereidsStr = new VarcharLiteral("abc");
        ConnectorLiteral strViaNereids = rightLiteralOf(NereidsToConnectorExpressionConverter.convert(
                new EqualTo(slot("s", ScalarType.createVarcharType(10)), nereidsStr)));
        ConnectorExpression strViaExpr = ExprToConnectorExpressionConverter.convert(nereidsStr.toLegacyLiteral());
        Assertions.assertEquals(strViaExpr, strViaNereids);
    }

    @Test
    public void nullInputReturnsNull() {
        Assertions.assertNull(NereidsToConnectorExpressionConverter.convert(null));
    }
}
