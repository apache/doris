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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.combinator.CombineCombinator;
import org.apache.doris.nereids.trees.expressions.functions.combinator.StateCombinator;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.types.DoubleType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class PercentileReservoirParameterTest {
    @Test
    void testRejectNaNAndOutOfRangeLevels() {
        for (double level : new double[] {Double.NaN, Double.NEGATIVE_INFINITY,
                Double.POSITIVE_INFINITY, -0.1, 1.1}) {
            for (Expression expression : variants(level)) {
                AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                        expression::checkLegalityBeforeTypeCoercion);
                Assertions.assertTrue(exception.getMessage().contains("level must be in [0, 1]"));
            }
        }
    }

    @Test
    void testAcceptEndpointsAndInteriorLevels() {
        for (double level : new double[] {-0.0, 0.0, 0.5, 1.0}) {
            for (Expression expression : variants(level)) {
                Assertions.assertDoesNotThrow(expression::checkLegalityBeforeTypeCoercion);
            }
        }
    }

    private List<Expression> variants(double level) {
        PercentileReservoir function = new PercentileReservoir(
                SlotReference.of("value", DoubleType.INSTANCE), new DoubleLiteral(level));
        return Arrays.asList(function, StateCombinator.create(function),
                new CombineCombinator(function.getArguments(), function));
    }
}
