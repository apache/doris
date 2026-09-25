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

package org.apache.doris.nereids.trees.expressions.functions.agg;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Divide;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.combinator.CombineCombinator;
import org.apache.doris.nereids.trees.expressions.functions.combinator.StateCombinator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Pow;
import org.apache.doris.nereids.trees.expressions.literal.DecimalLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

public class PercentileReservoirParameterTest {
    @Test
    void testRejectNaNAndOutOfRangeLevels() {
        for (double level : new double[] {Double.NaN, Double.NEGATIVE_INFINITY,
                Double.POSITIVE_INFINITY, -0.1, 1.1}) {
            for (Expression expression : variants(new DoubleLiteral(level))) {
                assertRejected(expression, "level must be in [0, 1]");
            }
        }
    }

    @Test
    void testAcceptEndpointsAndInteriorLevels() {
        for (double level : new double[] {-0.0, 0.0, 0.5, 1.0}) {
            for (Expression expression : variants(new DoubleLiteral(level))) {
                assertAccepted(expression);
            }
        }
    }

    @Test
    void testFoldableConstantLevelIsRangeCheckedInBothPhases() {
        // 0.25 + 0.25 is a constant but not a literal before constant folding
        for (Expression expression : variants(new Add(new DoubleLiteral(0.25), new DoubleLiteral(0.25)))) {
            assertAccepted(expression);
        }
        for (Expression expression : variants(new Add(new DoubleLiteral(0.25), new DoubleLiteral(1.25)))) {
            assertRejected(expression, "level must be in [0, 1]");
        }
        for (Expression expression : variants(new Cast(new DecimalV3Literal(new BigDecimal("0.5")),
                DoubleType.INSTANCE))) {
            assertAccepted(expression);
        }
    }

    @Test
    void testRejectConstantLevelThatCannotBeFolded() {
        // pow has no FE constant folding implementation, so the level never becomes a literal
        for (Expression expression : variants(new Pow(new DoubleLiteral(0.5), new DoubleLiteral(1)))) {
            assertRejected(expression, "must be a constant");
        }
    }

    @Test
    void testRejectNonConstantLevel() {
        for (Expression expression : variants(SlotReference.of("level", DoubleType.INSTANCE))) {
            assertRejected(expression, "must be a constant");
        }
    }

    @Test
    void testNonDoubleLiteralLevelIsCastBeforeRangeCheck() {
        for (Expression expression : variants(new VarcharLiteral("0.5"))) {
            assertAccepted(expression);
        }
        for (Expression expression : variants(new VarcharLiteral("5"))) {
            assertRejected(expression, "level must be in [0, 1]");
        }
        for (Expression expression : variants(new NullLiteral(DoubleType.INSTANCE))) {
            assertAccepted(expression);
        }
    }

    @Test
    void testInvalidStringLevelFollowsImplicitCastMode() {
        // the level takes the same VARCHAR to DOUBLE cast as signature coercion, so the implicit
        // and the explicit form agree: NULL under non-strict cast, an error under strict cast
        List<Expression> levels = Arrays.asList(new VarcharLiteral(""), new VarcharLiteral("abc"),
                new Cast(new VarcharLiteral(""), DoubleType.INSTANCE));
        withStrictCast(false, () -> {
            for (Expression level : levels) {
                for (Expression expression : variants(level)) {
                    assertAccepted(expression);
                }
            }
        });
        withStrictCast(true, () -> {
            for (Expression level : levels) {
                for (Expression expression : variants(level)) {
                    assertRejected(expression, "can't cast to double in strict mode");
                }
            }
        });
    }

    @Test
    void testNanPayloadStringLevelIsRejected() {
        // BE parses a NaN payload as NaN, so the check must see NaN instead of a failed cast
        List<Expression> levels = Arrays.asList(new VarcharLiteral("nan(foo)"),
                new Cast(new VarcharLiteral(" -nan(ind) "), DoubleType.INSTANCE));
        for (boolean strictCast : new boolean[] {false, true}) {
            withStrictCast(strictCast, () -> {
                for (Expression level : levels) {
                    for (Expression expression : variants(level)) {
                        assertRejected(expression, "level must be in [0, 1], but got NaN");
                    }
                }
            });
        }
    }

    @Test
    void testDecimalV2DivisionLevelFoldsLikeBe() {
        DecimalV2Type type = DecimalV2Type.createDecimalV2Type(27, 9);
        // 0 / 2 is the valid level 0; FoldConstantTest pins that it folds to 0 rather than NULL
        for (Expression expression : variants(new Divide(
                new DecimalLiteral(type, BigDecimal.ZERO), new DecimalLiteral(type, new BigDecimal("2"))))) {
            assertAccepted(expression);
        }
        for (Expression expression : variants(new Divide(
                new DecimalLiteral(type, new BigDecimal("3")), new DecimalLiteral(type, new BigDecimal("2"))))) {
            assertRejected(expression, "level must be in [0, 1], but got 1.5");
        }
        for (Expression expression : variants(new Divide(
                new DecimalLiteral(type, BigDecimal.ONE), new DecimalLiteral(type, BigDecimal.ZERO)))) {
            assertAccepted(expression);
        }
        // a recurring or an excess-scale quotient is rounded to scale 9 like BE, not rejected
        for (Expression expression : variants(new Divide(
                new DecimalLiteral(type, BigDecimal.ONE), new DecimalLiteral(type, new BigDecimal("3"))))) {
            assertAccepted(expression);
        }
        for (Expression expression : variants(new Divide(
                new DecimalLiteral(type, BigDecimal.ONE), new DecimalLiteral(type, new BigDecimal("1024"))))) {
            assertAccepted(expression);
        }
        for (Expression expression : variants(new Divide(
                new DecimalLiteral(type, new BigDecimal("4")), new DecimalLiteral(type, new BigDecimal("3"))))) {
            assertRejected(expression, "level must be in [0, 1], but got 1.333333333");
        }
    }

    @Test
    void testDecimalV3DivisionLevelFoldsLikeBe() {
        // type coercion shapes 2.0 / 3 as DECIMALV3(6, 5) / DECIMALV3(3, 0); BE truncates the quotient
        DecimalV3Type dividendType = DecimalV3Type.createDecimalV3Type(6, 5);
        DecimalV3Type divisorType = DecimalV3Type.createDecimalV3Type(3, 0);
        for (Expression expression : variants(new Divide(
                new DecimalV3Literal(dividendType, new BigDecimal("2.00000")),
                new DecimalV3Literal(divisorType, new BigDecimal("3"))))) {
            assertAccepted(expression);
        }
        for (Expression expression : variants(new Divide(
                new DecimalV3Literal(dividendType, new BigDecimal("1.00000")),
                new DecimalV3Literal(DecimalV3Type.createDecimalV3Type(4, 0), new BigDecimal("1024"))))) {
            assertAccepted(expression);
        }
        for (Expression expression : variants(new Divide(
                new DecimalV3Literal(dividendType, new BigDecimal("4.00000")),
                new DecimalV3Literal(divisorType, new BigDecimal("3"))))) {
            assertRejected(expression, "level must be in [0, 1], but got 1.33333");
        }
        for (Expression expression : variants(new Divide(
                new DecimalV3Literal(dividendType, new BigDecimal("1.00000")),
                new DecimalV3Literal(divisorType, BigDecimal.ZERO)))) {
            assertAccepted(expression);
        }
    }

    private void withStrictCast(boolean strictCast, Runnable check) {
        ConnectContext previousContext = ConnectContext.get();
        ConnectContext connectContext = new ConnectContext();
        connectContext.getSessionVariable().enableStrictCast = strictCast;
        connectContext.setThreadLocalInfo();
        try {
            check.run();
        } finally {
            ConnectContext.remove();
            if (previousContext != null) {
                previousContext.setThreadLocalInfo();
            }
        }
    }

    private void assertAccepted(Expression expression) {
        Assertions.assertDoesNotThrow(expression::checkLegalityBeforeTypeCoercion);
        Assertions.assertDoesNotThrow(expression::checkLegalityAfterRewrite);
    }

    private void assertRejected(Expression expression, String message) {
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                expression::checkLegalityBeforeTypeCoercion);
        Assertions.assertTrue(exception.getMessage().contains(message), exception.getMessage());
        exception = Assertions.assertThrows(AnalysisException.class, expression::checkLegalityAfterRewrite);
        Assertions.assertTrue(exception.getMessage().contains(message), exception.getMessage());
    }

    private List<Expression> variants(Expression level) {
        PercentileReservoir function = new PercentileReservoir(
                SlotReference.of("value", DoubleType.INSTANCE), level);
        return Arrays.asList(function, StateCombinator.create(function),
                new CombineCombinator(function.getArguments(), function));
    }
}
