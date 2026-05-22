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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.nereids.rules.expression.ExpressionNormalization;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteTestHelper;
import org.apache.doris.nereids.rules.expression.ExpressionRuleExecutor;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Sequence;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SequenceDayUnit;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SequenceHourUnit;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SequenceMinuteUnit;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SequenceMonthUnit;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SequenceQuarterUnit;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SequenceSecondUnit;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SequenceWeekUnit;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SequenceYearUnit;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.IntegerType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SequenceFunctionRewriteTest extends ExpressionRewriteTestHelper {

    @Test
    public void testSequenceFunctionRewrite() {
        executor = new ExpressionRuleExecutor(ExpressionNormalization.NORMALIZE_REWRITE_RULES);
        Assertions.assertEquals(9, new SequenceFunctionRewrite().buildRules().size());
        // Test integer sequence with 1 argument
        assertRewriteAfterConvertSequenceFunction(new Sequence(new IntegerLiteral(5)),
                "array_range(6)", ArrayType.of(IntegerType.INSTANCE));
        // Test integer sequence with 2 arguments
        assertRewriteAfterConvertSequenceFunction(new Sequence(new IntegerLiteral(1), new IntegerLiteral(5)),
                "array_range(1, 6)", ArrayType.of(IntegerType.INSTANCE));
        // Test integer sequence with 3 arguments
        assertRewriteAfterConvertSequenceFunction(new Sequence(new IntegerLiteral(1),
                        new IntegerLiteral(5), new IntegerLiteral(1)),
                "array_range(1, 6, 1)", ArrayType.of(IntegerType.INSTANCE));
        // Test datetime sequence - year unit
        assertRewriteAfterConvertSequenceFunction(new SequenceYearUnit(
                        new DateTimeV2Literal("2024-05-15 00:00:00"),
                        new DateTimeV2Literal("2024-05-17 00:00:00"), new IntegerLiteral(1)),
                "array_range_year_unit('2024-05-15 00:00:00', '2025-05-17 00:00:00', 1)",
                ArrayType.of(DateTimeV2Type.SYSTEM_DEFAULT));
        // Test datetime sequence - quarter unit
        assertRewriteAfterConvertSequenceFunction(new SequenceQuarterUnit(
                        new DateTimeV2Literal("2024-05-15 00:00:00"),
                        new DateTimeV2Literal("2024-05-17 00:00:00"), new IntegerLiteral(1)),
                "array_range_quarter_unit('2024-05-15 00:00:00', '2024-08-17 00:00:00', 1)",
                ArrayType.of(DateTimeV2Type.SYSTEM_DEFAULT));
        // Test datetime sequence - month unit
        assertRewriteAfterConvertSequenceFunction(new SequenceMonthUnit(
                        new DateTimeV2Literal("2024-05-15 00:00:00"),
                        new DateTimeV2Literal("2024-05-17 00:00:00"), new IntegerLiteral(1)),
                "array_range_month_unit('2024-05-15 00:00:00', '2024-06-17 00:00:00', 1)",
                ArrayType.of(DateTimeV2Type.SYSTEM_DEFAULT));
        // Test datetime sequence - week unit
        assertRewriteAfterConvertSequenceFunction(new SequenceWeekUnit(
                        new DateTimeV2Literal("2024-05-15 00:00:00"),
                        new DateTimeV2Literal("2024-05-17 00:00:00"), new IntegerLiteral(1)),
                "array_range_week_unit('2024-05-15 00:00:00', '2024-05-24 00:00:00', 1)",
                ArrayType.of(DateTimeV2Type.SYSTEM_DEFAULT));
        // Test datetime sequence - day unit
        assertRewriteAfterConvertSequenceFunction(new SequenceDayUnit(
                        new DateTimeV2Literal("2024-05-15 00:00:00"),
                        new DateTimeV2Literal("2024-05-17 00:00:00"), new IntegerLiteral(1)),
                "array_range_day_unit('2024-05-15 00:00:00', '2024-05-18 00:00:00', 1)",
                ArrayType.of(DateTimeV2Type.SYSTEM_DEFAULT));
        // Test datetime sequence - hour unit
        assertRewriteAfterConvertSequenceFunction(new SequenceHourUnit(
                        new DateTimeV2Literal("2024-05-15 00:00:00"),
                        new DateTimeV2Literal("2024-05-17 00:00:00"), new IntegerLiteral(1)),
                "array_range_hour_unit('2024-05-15 00:00:00', '2024-05-17 01:00:00', 1)",
                ArrayType.of(DateTimeV2Type.SYSTEM_DEFAULT));
        // Test datetime sequence - minute unit
        assertRewriteAfterConvertSequenceFunction(new SequenceMinuteUnit(
                        new DateTimeV2Literal("2024-05-15 00:00:00"),
                        new DateTimeV2Literal("2024-05-17 00:00:00"), new IntegerLiteral(1)),
                "array_range_minute_unit('2024-05-15 00:00:00', '2024-05-17 00:01:00', 1)",
                ArrayType.of(DateTimeV2Type.SYSTEM_DEFAULT));
        // Test datetime sequence - second unit
        assertRewriteAfterConvertSequenceFunction(new SequenceSecondUnit(
                        new DateTimeV2Literal("2024-05-15 00:00:00"),
                        new DateTimeV2Literal("2024-05-17 00:00:00"), new IntegerLiteral(1)),
                "array_range_second_unit('2024-05-15 00:00:00', '2024-05-17 00:00:01', 1)",
                ArrayType.of(DateTimeV2Type.SYSTEM_DEFAULT));
    }

    private void assertRewriteAfterConvertSequenceFunction(Expression needRewriteExpression, String expected,
            DataType expectedType) {
        Expression rewritten = executor.rewrite(needRewriteExpression, context);
        Expression expectedExpression = PARSER.parseExpression(expected);
        Assertions.assertEquals(expectedExpression.toSql(), rewritten.toSql());
        Assertions.assertEquals(expectedType, rewritten.getDataType());
    }

}
