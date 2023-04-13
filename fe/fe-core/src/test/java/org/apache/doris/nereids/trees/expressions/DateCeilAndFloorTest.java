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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class DateCeilAndFloorTest {


    private void testDateLiteralGroup(Object[] dateLikeLiteral, Class<? extends DateLiteral> dateType,
            String funcName, Object[][] answers) throws Exception {
        int answerIdx = 0;
        for (IntegerLiteral literal : DateUtils.INTEGER_LITERALS) {
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

    @Disabled
    @Test
    public void testDateCeilAndFloor() throws Exception {
        testDateGroupFunction(DateUtils.DATETIME_LITERALS, DateTimeLiteral.class, null);
        testDateGroupFunction(DateUtils.DATEV2_LITERALS, DateV2Literal.class, null);
        testDateGroupFunction(DateUtils.DATETIMEV2_LITERALS, DateTimeV2Literal.class, null);
    }

    @Disabled
    @Test
    public void testWeekOfYear() {
        DateLiteral testDate = new DateLiteral(2020, 1, 1);
        int[] answer = new int[] {0, 1, 52, 1, 1, 0, 1, 52};
        for (int i = 0; i < 8; ++i) {
            Assertions.assertEquals(DateTimeExtract.week(testDate, new IntegerLiteral(i)).getValue(), answer[i]);
        }
    }
}
