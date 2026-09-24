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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.combinator.CombineCombinator;
import org.apache.doris.nereids.trees.expressions.functions.combinator.StateCombinator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Pow;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.DoubleType;

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
        for (Expression expression : variants(new VarcharLiteral("abc"))) {
            assertRejected(expression, "can't cast to double");
        }
        for (Expression expression : variants(new NullLiteral(DoubleType.INSTANCE))) {
            assertAccepted(expression);
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
