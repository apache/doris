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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.analysis.ExpressionAnalyzer;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteTestHelper;
import org.apache.doris.nereids.rules.expression.ExpressionRuleExecutor;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnFE;
import org.apache.doris.nereids.rules.expression.rules.SimplifyConditionalFunction;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.TimeExtract;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TimeStampNsType;
import org.apache.doris.nereids.types.TimeV2Type;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.function.Function;

class CompositeTimeExtractTest extends ExpressionRewriteTestHelper {
    private static final List<Function<Expression, ScalarFunction>> FUNCTIONS = ImmutableList.of(
            HourMinute::new, HourSecond::new, MinuteSecond::new, SecondMicrosecond::new);

    @Test
    void testTimePrecisionAndExistingTemporalSignatures() {
        for (Function<Expression, ScalarFunction> factory : FUNCTIONS) {
            for (int scale = 0; scale <= 6; scale++) {
                TimeV2Type timeType = TimeV2Type.of(scale);
                ScalarFunction function = factory.apply(SlotReference.of("t", timeType));
                Assertions.assertEquals(timeType, function.getSignature().getArgType(0));
                Expression rewritten = ((TimeExtract) function).rewriteWhenAnalyze();
                Assertions.assertInstanceOf(TimeFormat.class, rewritten);
                Assertions.assertEquals(timeType, rewritten.child(0).getDataType());
            }
            for (DataType type : ImmutableList.of(DateTimeV2Type.SYSTEM_DEFAULT,
                    DateTimeV2Type.MAX, TimeStampNsType.INSTANCE)) {
                ScalarFunction function = factory.apply(SlotReference.of("dt", type));
                Assertions.assertEquals(type, function.getSignature().getArgType(0));
                Assertions.assertSame(function, ((TimeExtract) function).rewriteWhenAnalyze());
            }
        }
    }

    @Test
    void testTimeLiteralsSelectTimeInsteadOfDateTime() {
        for (Function<Expression, ScalarFunction> factory : FUNCTIONS) {
            for (String value : ImmutableList.of("12:34:56", "12:34:56.789123",
                    "00:00:00.000001", "-00:00:01.5", "123:34:56.123456")) {
                ScalarFunction function = factory.apply(new VarcharLiteral(value));
                Assertions.assertTrue(function.getSignature().getArgType(0) instanceof TimeV2Type, value);
            }
            ScalarFunction dateTime = factory.apply(new VarcharLiteral("2024-01-02 12:34:56.789123"));
            Assertions.assertEquals(DateTimeV2Type.MAX, dateTime.getSignature().getArgType(0));
        }
    }

    @Test
    void testDirectAndExtractSyntaxUseTheSameTimeFormatter() {
        NereidsParser parser = new NereidsParser();
        for (String name : ImmutableList.of("hour_minute", "hour_second", "minute_second", "second_microsecond")) {
            for (String argument : ImmutableList.of("'12:34:56.789123'",
                    "cast('12:34:56.789123' as time(6))")) {
                Expression direct = ExpressionAnalyzer.analyzeFunction(null, null,
                        parser.parseExpression(name + "(" + argument + ")"));
                Expression extract = ExpressionAnalyzer.analyzeFunction(null, null,
                        parser.parseExpression("extract(" + name + " from " + argument + ")"));
                Assertions.assertInstanceOf(TimeFormat.class, direct);
                Assertions.assertEquals(direct, extract);
                Assertions.assertEquals(TimeV2Type.MAX, direct.child(0).getDataType());
                Assertions.assertTrue(direct.checkInputDataTypes().success());
            }
        }
    }

    @Test
    void testOtherInputsKeepOriginalBinding() {
        for (Function<Expression, ScalarFunction> factory : FUNCTIONS) {
            ScalarFunction integer = factory.apply(SlotReference.of("i", IntegerType.INSTANCE));
            Assertions.assertTrue(integer.getSignature().getArgType(0) instanceof DateTimeV2Type);
            ScalarFunction array = factory.apply(SlotReference.of("a", ArrayType.of(IntegerType.INSTANCE)));
            Assertions.assertThrows(AnalysisException.class, array::getSignature);
            ScalarFunction volatileString = factory.apply(new Uuid());
            Assertions.assertTrue(volatileString.getSignature().getArgType(0) instanceof DateTimeV2Type);
        }
    }

    @Test
    void testStringColumnsPreserveDateTimeParsingAndAcceptTime() {
        for (Function<Expression, ScalarFunction> factory : FUNCTIONS) {
            for (DataType type : ImmutableList.of(VarcharType.SYSTEM_DEFAULT, StringType.INSTANCE)) {
                ScalarFunction function = factory.apply(SlotReference.of("s", type));
                function = (ScalarFunction) TypeCoercionUtils.processBoundFunction(function);
                Expression rewritten = ((TimeExtract) function).rewriteWhenAnalyze();
                Assertions.assertInstanceOf(TimeFormat.class, rewritten);
                Assertions.assertInstanceOf(Coalesce.class, rewritten.child(0));
                Assertions.assertEquals(TimeV2Type.MAX, rewritten.child(0).getDataType());
                Assertions.assertTrue(rewritten.nullable());
                Assertions.assertTrue(rewritten.checkInputDataTypes().success());
            }
        }
    }

    @Test
    void testStringFallbackFoldsInStrictAndNonStrictModes() {
        NereidsParser parser = new NereidsParser();
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(FoldConstantRuleOnFE.VISITOR_INSTANCE),
                bottomUp(SimplifyConditionalFunction.INSTANCE),
                bottomUp(FoldConstantRuleOnFE.VISITOR_INSTANCE)));
        List<String> names = ImmutableList.of("hour_minute", "hour_second", "minute_second", "second_microsecond");
        List<String> expected = ImmutableList.of("12:34", "12:34:56", "34:56", "56.789123");
        try (MockedStatic<SessionVariable> mockedSessionVariable = Mockito.mockStatic(SessionVariable.class)) {
            for (boolean strictCast : new boolean[] {false, true}) {
                mockedSessionVariable.when(SessionVariable::enableStrictCast).thenReturn(strictCast);
                for (int i = 0; i < names.size(); i++) {
                    for (String value : ImmutableList.of("12:34:56.789123", "2024-01-02 12:34:56.789123")) {
                        Expression analyzed = ExpressionAnalyzer.analyzeFunction(null, null,
                                parser.parseExpression(names.get(i) + "(concat('" + value + "', ''))"));
                        Assertions.assertEquals(new VarcharLiteral(expected.get(i)),
                                executor.rewrite(analyzed, context));
                    }
                }
            }
        }
    }
}
