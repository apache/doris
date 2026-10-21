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
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.StringType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class WindowFunnelParameterTest {
    @Test
    void testRejectWindowColumn() {
        for (AggregateFunction function : functions(SlotReference.of("window", BigIntType.INSTANCE),
                new VarcharLiteral("default"))) {
            assertRejected(function, "window");
        }
    }

    @Test
    void testRejectModeColumn() {
        for (AggregateFunction function : functions(new BigIntLiteral(10),
                SlotReference.of("mode", StringType.INSTANCE))) {
            assertRejected(function, "mode");
        }
    }

    @Test
    void testAcceptConstantsWithEventColumns() {
        for (AggregateFunction function : functions(new BigIntLiteral(10), new VarcharLiteral("default"))) {
            for (Expression expression : variants(function)) {
                Assertions.assertDoesNotThrow(expression::checkLegalityBeforeTypeCoercion);
            }
        }
    }

    private void assertRejected(AggregateFunction function, String parameter) {
        for (Expression expression : variants(function)) {
            AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                    expression::checkLegalityBeforeTypeCoercion);
            Assertions.assertEquals("The " + parameter + " parameter of " + function.getName()
                    + " must be a constant", exception.getMessage());
        }
    }

    private List<Expression> variants(AggregateFunction function) {
        return Arrays.asList(function, StateCombinator.create(function),
                new CombineCombinator(function.getArguments(), function));
    }

    private List<AggregateFunction> functions(Expression window, Expression mode) {
        Expression timestamp = SlotReference.of("ts", DateTimeV2Type.SYSTEM_DEFAULT);
        Expression event = SlotReference.of("event", BooleanType.INSTANCE);
        return Arrays.asList(new WindowFunnel(window, mode, timestamp, event),
                new WindowFunnelV2(window, mode, timestamp, event));
    }
}
