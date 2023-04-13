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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.trees.expressions.functions.ExecutableFunctions;
import org.apache.doris.nereids.trees.expressions.functions.executable.DateTimeExtract;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ExecutableFunctionsTest {
    private static final DateLiteral[] dateLiterals = new DateLiteral[] {
            new DateLiteral(1999, 5, 15),
            new DateLiteral(1965, 7, 21),
            new DateLiteral(1325, 4, 12)
    };
    private static final DateTimeLiteral[] dateTimeLiterals = new DateTimeLiteral[] {
            new DateTimeLiteral(1999, 5, 15, 13, 45, 35),
            new DateTimeLiteral(1965, 7, 21, 10, 4, 24),
            new DateTimeLiteral(1325, 4, 12, 20, 47, 56)
    };
    private static final DateV2Literal[] dateV2Literals = new DateV2Literal[] {
            new DateV2Literal(1999, 5, 15),
            new DateV2Literal(1965, 7, 21),
            new DateV2Literal(1325, 4, 12)
    };
    private static final DateTimeV2Literal[] dateTimeV2Literals = new DateTimeV2Literal[] {
            new DateTimeV2Literal(1999, 5, 15, 13, 45, 35, 45),
            new DateTimeV2Literal(1965, 7, 21, 10, 4, 24, 464),
            new DateTimeV2Literal(1325, 4, 12, 20, 47, 56, 435)
    };
    private static final IntegerLiteral[] integerLiterals = new IntegerLiteral[] {
            new IntegerLiteral(1),
            new IntegerLiteral(3),
            new IntegerLiteral(5),
            new IntegerLiteral(15),
            new IntegerLiteral(30),
            new IntegerLiteral(55)
    };
    private static final VarcharLiteral[] dateTags = new VarcharLiteral[] {
            new VarcharLiteral("year"),
            new VarcharLiteral("month"),
            new VarcharLiteral("week"),
            new VarcharLiteral("day"),
            new VarcharLiteral("hour"),
            new VarcharLiteral("minute"),
            new VarcharLiteral("second")
    };

    private void testDateLiteralGroup(Object[] dateLikeLiteral, Class<? extends DateLiteral> dateType,
            String funcName, Object[][] answers) throws Exception {
        int answerIdx = 0;
        for (IntegerLiteral literal : integerLiterals) {
            for (int i = 0; i < dateLikeLiteral.length; ++i) {
                for (int j = 0; j < dateLikeLiteral.length; ++j) {
                    if (i != j) {
                        Object o1 = dateLikeLiteral[i];
                        Object o2 = dateLikeLiteral[j];
                        Class<ExecutableFunctions> cls = ExecutableFunctions.class;
                        Assertions.assertEquals(cls.getMethod(funcName, o1.getClass())
                                .invoke(null, o1), answers[answerIdx][0]);
                        Assertions.assertEquals(cls.getMethod(funcName, o1.getClass(), literal.getClass())
                                .invoke(null, o1, literal), answers[answerIdx][1]);
                        Assertions.assertEquals(cls.getMethod(funcName, o1.getClass(), o2.getClass())
                                .invoke(null, o1, o2), answers[answerIdx][2]);
                        Assertions.assertEquals(cls.getMethod(funcName, o1.getClass(), literal.getClass(), o2.getClass())
                                .invoke(null, o1, literal, o2), answers[answerIdx][3]);
                        answerIdx++;
                    }
                }
            }
        }
    }

    private void testDateGroupFunction(Object[] dateLikeLiteral, Class<? extends DateLiteral> dateType,
            Object[][][] answers) throws Exception {
        int answerIdx = 0;
        testDateLiteralGroup(dateLikeLiteral, dateType, "year_ceil", answers[answerIdx++]);
        testDateLiteralGroup(dateLikeLiteral, dateType, "month_ceil", answers[answerIdx++]);
        testDateLiteralGroup(dateLikeLiteral, dateType, "day_ceil", answers[answerIdx++]);
        testDateLiteralGroup(dateLikeLiteral, dateType, "hour_ceil", answers[answerIdx++]);
        testDateLiteralGroup(dateLikeLiteral, dateType, "minute_ceil", answers[answerIdx++]);
        testDateLiteralGroup(dateLikeLiteral, dateType, "second_ceil", answers[answerIdx++]);
        testDateLiteralGroup(dateLikeLiteral, dateType, "year_floor", answers[answerIdx++]);
        testDateLiteralGroup(dateLikeLiteral, dateType, "month_floor", answers[answerIdx++]);
        testDateLiteralGroup(dateLikeLiteral, dateType, "day_floor", answers[answerIdx++]);
        testDateLiteralGroup(dateLikeLiteral, dateType, "hour_floor", answers[answerIdx++]);
        testDateLiteralGroup(dateLikeLiteral, dateType, "minute_floor", answers[answerIdx++]);
        testDateLiteralGroup(dateLikeLiteral, dateType, "second_floor", answers[answerIdx]);
    }

    @Test
    public void testDateCeilAndFloor() throws Exception {
        testDateGroupFunction(dateTimeLiterals, DateTimeLiteral.class, null);
        testDateGroupFunction(dateV2Literals, DateV2Literal.class, null);
        testDateGroupFunction(dateTimeV2Literals, DateTimeV2Literal.class, null);
    }

    @Test void testWeekOfYear() {
        DateLiteral testDate = new DateLiteral(2020, 1, 1);
        int[] answer = new int[] {0, 1, 52, 1, 1, 0, 1, 52};
        for (int i = 0; i < 8; ++i) {
            Assertions.assertEquals(DateTimeExtract.week(testDate).getValue(), answer[i]);
        }
    }
}
