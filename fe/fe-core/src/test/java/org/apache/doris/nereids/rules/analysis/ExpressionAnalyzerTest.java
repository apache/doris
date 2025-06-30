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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.analyzer.Scope;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ExpressionAnalyzerTest {

    @Test
    void testPreProcessUnboundFunctionForThreeArgsDataTimeFunction() {
        ExpressionAnalyzer analyzer = new ExpressionAnalyzer(null, new Scope(ImmutableList.of()),
                null, true, true);
        UnboundSlot unboundSlot = new UnboundSlot("YEAR");
        Expression arg1 = new TinyIntLiteral((byte) 1);
        Expression arg2 = new DateTimeV2Literal("2020-01-01");
        for (String functionName : DatetimeFunctionBinder.SUPPORT_FUNCTION_NAMES) {
            UnboundFunction unboundFunction = new UnboundFunction(functionName,
                    ImmutableList.of(unboundSlot, arg1, arg2));
            if (DatetimeFunctionBinder.isDatetimeArithmeticFunction(functionName)) {
                UnboundFunction ret = analyzer.preProcessUnboundFunction(unboundFunction, null);
                Assertions.assertInstanceOf(SlotReference.class, ret.getArgument(0));
                Assertions.assertEquals(arg1, ret.getArgument(1));
                Assertions.assertEquals(arg2, ret.getArgument(2));
            } else {
                Assertions.assertThrowsExactly(AnalysisException.class,
                        () -> analyzer.preProcessUnboundFunction(unboundFunction, null),
                        " Unknown column 'YEAR' in 'table list");
            }

        }
        UnboundFunction unboundFunction = new UnboundFunction("other_function",
                ImmutableList.of(unboundSlot, arg1, arg2));
        Assertions.assertThrowsExactly(AnalysisException.class,
                () -> analyzer.preProcessUnboundFunction(unboundFunction, null),
                " Unknown column 'YEAR' in 'table list");
    }
}
