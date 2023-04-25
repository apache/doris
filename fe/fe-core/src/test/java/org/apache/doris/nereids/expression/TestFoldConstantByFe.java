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

package org.apache.doris.nereids.expression;

import org.apache.doris.nereids.trees.expressions.functions.executable.DateTimeArithmetic;
import org.apache.doris.nereids.trees.expressions.functions.executable.DateTimeExtractAndTransform;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestFoldConstantByFe {
    private final DateLiteral[] dateForTest = new DateLiteral[] {
            new DateLiteral("2021-04-12"), new DateLiteral("1969-12-31"),
            new DateLiteral("1356-12-12"), new DateLiteral("0001-01-01"),
            new DateLiteral("9998-12-31")
    };
    private final DateTimeLiteral[] dateTimeForTest = new DateTimeLiteral[] {
            new DateTimeLiteral("2021-04-12 12:54:53"), new DateTimeLiteral("1969-12-31 23:59:59"),
            new DateTimeLiteral("1356-12-12 12:56:12"), new DateTimeLiteral("0001-01-01 00:00:01"),
            new DateTimeLiteral("9998-12-31 00:00:59")
    };
    private final DateV2Literal[] dateV2ForTest = new DateV2Literal[] {
            new DateV2Literal("2021-04-12"), new DateV2Literal("1969-12-31"),
            new DateV2Literal("1356-12-12"), new DateV2Literal("0001-01-01"),
            new DateV2Literal("9998-12-31")
    };
    private final DateTimeV2Literal[] dateTimeV2ForTest = new DateTimeV2Literal[] {
            new DateTimeV2Literal("2021-04-12 12:54:53"), new DateTimeV2Literal("1969-12-31 23:59:59"),
            new DateTimeV2Literal("1356-12-12 12:56:12"), new DateTimeV2Literal("0001-01-01 00:00:01"),
            new DateTimeV2Literal("9998-12-31 00:00:59")
    };
    private final IntegerLiteral[] intForTest = new IntegerLiteral[] {
            new IntegerLiteral(1), new IntegerLiteral(10),
            new IntegerLiteral(25), new IntegerLiteral(50),
            new IntegerLiteral(1024)
    };

    @Test
    public void testDateTimeArithmeticFunctions() {
        Object[] answer = {

        };
        int answerIdx = 0;
        for (DateLiteral dateLiteral : dateForTest) {
            for (IntegerLiteral integerLiteral : intForTest) {
                Assertions.assertEquals(DateTimeArithmetic.dateAdd(dateLiteral, integerLiteral), answer[answerIdx++]);
                Assertions.assertEquals(DateTimeArithmetic.dateSub(dateLiteral, integerLiteral), answer[answerIdx++]);
                Assertions.assertEquals(DateTimeArithmetic.yearsAdd(dateLiteral, integerLiteral), answer[answerIdx++]);
                Assertions.assertEquals(DateTimeArithmetic.yearsSub(dateLiteral, integerLiteral), answer[answerIdx++]);
                Assertions.assertEquals(DateTimeArithmetic.monthsAdd(dateLiteral, integerLiteral), answer[answerIdx++]);
                Assertions.assertEquals(DateTimeArithmetic.monthsSub(dateLiteral, integerLiteral), answer[answerIdx++]);
                Assertions.assertEquals(DateTimeArithmetic.daysAdd(dateLiteral, integerLiteral), answer[answerIdx++]);
                Assertions.assertEquals(DateTimeArithmetic.daysSub(dateLiteral, integerLiteral), answer[answerIdx++]);
            }
        }

        for (DateTimeLiteral dateLiteral : dateTimeForTest) {
            for (IntegerLiteral integerLiteral : intForTest) {
                Assertions.assertEquals(DateTimeArithmetic.dateAdd(dateLiteral, integerLiteral), answer[answerIdx++]);
                Assertions.assertEquals(DateTimeArithmetic.dateSub(dateLiteral, integerLiteral), answer[answerIdx++]);
                Assertions.assertEquals(DateTimeArithmetic.yearsAdd(dateLiteral, integerLiteral), answer[answerIdx++]);
                Assertions.assertEquals(DateTimeArithmetic.yearsSub(dateLiteral, integerLiteral), answer[answerIdx++]);
                Assertions.assertEquals(DateTimeArithmetic.monthsAdd(dateLiteral, integerLiteral), answer[answerIdx++]);
                Assertions.assertEquals(DateTimeArithmetic.monthsSub(dateLiteral, integerLiteral), answer[answerIdx++]);
                Assertions.assertEquals(DateTimeArithmetic.daysAdd(dateLiteral, integerLiteral), answer[answerIdx++]);
                Assertions.assertEquals(DateTimeArithmetic.daysSub(dateLiteral, integerLiteral), answer[answerIdx++]);
                Assertions.assertEquals(DateTimeArithmetic.hoursAdd(dateLiteral, integerLiteral), answer[answerIdx++]);
                Assertions.assertEquals(DateTimeArithmetic.hoursSub(dateLiteral, integerLiteral), answer[answerIdx++]);
                Assertions.assertEquals(DateTimeArithmetic.minutesAdd(dateLiteral, integerLiteral),
                        answer[answerIdx++]);
                Assertions.assertEquals(DateTimeArithmetic.minutesSub(dateLiteral, integerLiteral),
                        answer[answerIdx++]);
                Assertions.assertEquals(DateTimeArithmetic.secondsAdd(dateLiteral, integerLiteral),
                        answer[answerIdx++]);
                Assertions.assertEquals(DateTimeArithmetic.secondsSub(dateLiteral, integerLiteral),
                        answer[answerIdx++]);
            }
        }

        for (DateV2Literal dateLiteral : dateV2ForTest) {
            for (IntegerLiteral integerLiteral : intForTest) {
                Assertions.assertEquals(DateTimeArithmetic.dateAdd(dateLiteral, integerLiteral), answer[answerIdx++]);
                Assertions.assertEquals(DateTimeArithmetic.dateSub(dateLiteral, integerLiteral), answer[answerIdx++]);
                Assertions.assertEquals(DateTimeArithmetic.yearsAdd(dateLiteral, integerLiteral), answer[answerIdx++]);
                Assertions.assertEquals(DateTimeArithmetic.yearsSub(dateLiteral, integerLiteral), answer[answerIdx++]);
                Assertions.assertEquals(DateTimeArithmetic.monthsAdd(dateLiteral, integerLiteral), answer[answerIdx++]);
                Assertions.assertEquals(DateTimeArithmetic.monthsSub(dateLiteral, integerLiteral), answer[answerIdx++]);
                Assertions.assertEquals(DateTimeArithmetic.daysAdd(dateLiteral, integerLiteral), answer[answerIdx++]);
                Assertions.assertEquals(DateTimeArithmetic.daysSub(dateLiteral, integerLiteral), answer[answerIdx++]);
            }
        }

        for (DateTimeV2Literal dateLiteral : dateTimeV2ForTest) {
            for (IntegerLiteral integerLiteral : intForTest) {
                Assertions.assertEquals(DateTimeArithmetic.dateAdd(dateLiteral, integerLiteral), answer[answerIdx++]);
                Assertions.assertEquals(DateTimeArithmetic.dateSub(dateLiteral, integerLiteral), answer[answerIdx++]);
                Assertions.assertEquals(DateTimeArithmetic.yearsAdd(dateLiteral, integerLiteral), answer[answerIdx++]);
                Assertions.assertEquals(DateTimeArithmetic.yearsSub(dateLiteral, integerLiteral), answer[answerIdx++]);
                Assertions.assertEquals(DateTimeArithmetic.monthsAdd(dateLiteral, integerLiteral), answer[answerIdx++]);
                Assertions.assertEquals(DateTimeArithmetic.monthsSub(dateLiteral, integerLiteral), answer[answerIdx++]);
                Assertions.assertEquals(DateTimeArithmetic.daysAdd(dateLiteral, integerLiteral), answer[answerIdx++]);
                Assertions.assertEquals(DateTimeArithmetic.daysSub(dateLiteral, integerLiteral), answer[answerIdx++]);
                Assertions.assertEquals(DateTimeArithmetic.hoursAdd(dateLiteral, integerLiteral), answer[answerIdx++]);
                Assertions.assertEquals(DateTimeArithmetic.hoursSub(dateLiteral, integerLiteral), answer[answerIdx++]);
                Assertions.assertEquals(DateTimeArithmetic.minutesAdd(dateLiteral, integerLiteral),
                        answer[answerIdx++]);
                Assertions.assertEquals(DateTimeArithmetic.minutesSub(dateLiteral, integerLiteral),
                        answer[answerIdx++]);
                Assertions.assertEquals(DateTimeArithmetic.secondsAdd(dateLiteral, integerLiteral),
                        answer[answerIdx++]);
                Assertions.assertEquals(DateTimeArithmetic.secondsSub(dateLiteral, integerLiteral),
                        answer[answerIdx++]);
                Assertions.assertEquals(DateTimeArithmetic.microSecondsAdd(dateLiteral, integerLiteral),
                        answer[answerIdx++]);
            }
        }

        for (DateTimeLiteral dateTimeLiteral : dateTimeForTest) {
            for (DateTimeLiteral dateTimeLiteral1 : dateTimeForTest) {
                Assertions.assertEquals(DateTimeArithmetic.dateDiff(dateTimeLiteral, dateTimeLiteral1),
                        answer[answerIdx]);
            }
        }
        for (DateV2Literal dateTimeLiteral : dateV2ForTest) {
            for (DateV2Literal dateTimeLiteral1 : dateV2ForTest) {
                Assertions.assertEquals(DateTimeArithmetic.dateDiff(dateTimeLiteral, dateTimeLiteral1),
                        answer[answerIdx]);
            }
        }
        for (DateV2Literal dateTimeLiteral : dateV2ForTest) {
            for (DateTimeV2Literal dateTimeLiteral1 : dateTimeV2ForTest) {
                Assertions.assertEquals(DateTimeArithmetic.dateDiff(dateTimeLiteral, dateTimeLiteral1),
                        answer[answerIdx]);
            }
        }
        for (DateTimeV2Literal dateTimeLiteral : dateTimeV2ForTest) {
            for (DateV2Literal dateTimeLiteral1 : dateV2ForTest) {
                Assertions.assertEquals(DateTimeArithmetic.dateDiff(dateTimeLiteral, dateTimeLiteral1),
                        answer[answerIdx]);
            }
        }
        for (DateTimeV2Literal dateTimeLiteral : dateTimeV2ForTest) {
            for (DateTimeV2Literal dateTimeLiteral1 : dateTimeV2ForTest) {
                Assertions.assertEquals(DateTimeArithmetic.dateDiff(dateTimeLiteral, dateTimeLiteral1),
                        answer[answerIdx]);
            }
        }
    }

    @Test
    public void testDateTimeExtractAndTransform() {
        VarcharLiteral format = new VarcharLiteral("%Y-%m-%d");
        VarcharLiteral[] truncTags = new VarcharLiteral[] {
                new VarcharLiteral("second"), new VarcharLiteral("minute"),
                new VarcharLiteral("hour"), new VarcharLiteral("day"),
                new VarcharLiteral("month"), new VarcharLiteral("year")
        };

        Object[] answer = {};
        int answerIdx = 0;
        for (DateTimeLiteral literal : dateTimeForTest) {
            Assertions.assertEquals(DateTimeExtractAndTransform.date(literal), answer[answerIdx++]);
        }
        for (DateTimeV2Literal literal : dateTimeV2ForTest) {
            Assertions.assertEquals(DateTimeExtractAndTransform.date(literal), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.dateV2(literal), answer[answerIdx++]);
        }
        for (DateLiteral literal : dateForTest) {
            Assertions.assertEquals(DateTimeExtractAndTransform.year(literal), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.quarter(literal), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.month(literal), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.dayOfWeek(literal), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.dayOfMonth(literal), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.dayOfYear(literal), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.day(literal), answer[answerIdx++]);

            Assertions.assertEquals(DateTimeExtractAndTransform.dateFormat(literal, format), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.toMonday(literal), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.lastDay(literal), answer[answerIdx++]);
        }
        for (DateTimeLiteral literal : dateTimeForTest) {
            Assertions.assertEquals(DateTimeExtractAndTransform.year(literal), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.quarter(literal), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.month(literal), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.dayOfWeek(literal), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.dayOfMonth(literal), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.dayOfYear(literal), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.day(literal), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.hour(literal), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.minute(literal), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.second(literal), answer[answerIdx++]);

            Assertions.assertEquals(DateTimeExtractAndTransform.dateFormat(literal, format), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.toMonday(literal), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.lastDay(literal), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.toDate(literal), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.toDays(literal), answer[answerIdx++]);
            for (VarcharLiteral literal1 : truncTags) {
                Assertions.assertEquals(DateTimeExtractAndTransform.dateTrunc(literal, literal1), answer[answerIdx++]);
            }
        }
        for (DateV2Literal literal : dateV2ForTest) {
            Assertions.assertEquals(DateTimeExtractAndTransform.year(literal), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.quarter(literal), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.month(literal), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.dayOfWeek(literal), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.dayOfMonth(literal), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.dayOfYear(literal), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.day(literal), answer[answerIdx++]);

            Assertions.assertEquals(DateTimeExtractAndTransform.dateFormat(literal, format), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.toMonday(literal), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.lastDay(literal), answer[answerIdx++]);
        }
        for (DateTimeV2Literal literal : dateTimeV2ForTest) {
            Assertions.assertEquals(DateTimeExtractAndTransform.year(literal), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.quarter(literal), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.month(literal), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.dayOfWeek(literal), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.dayOfMonth(literal), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.dayOfYear(literal), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.day(literal), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.hour(literal), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.minute(literal), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.second(literal), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.hour(literal), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.minute(literal), answer[answerIdx++]);

            Assertions.assertEquals(DateTimeExtractAndTransform.dateFormat(literal, format), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.toMonday(literal), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.lastDay(literal), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.toDate(literal), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.toDays(literal), answer[answerIdx++]);
            Assertions.assertEquals(DateTimeExtractAndTransform.toDate(literal), answer[answerIdx++]);
            for (VarcharLiteral literal1 : truncTags) {
                Assertions.assertEquals(DateTimeExtractAndTransform.dateTrunc(literal, literal1), answer[answerIdx++]);
            }
        }

        // test makedate and strToDate
        IntegerLiteral[] years = new IntegerLiteral[] {
                new IntegerLiteral(1945), new IntegerLiteral(1968),
                new IntegerLiteral(9999), new IntegerLiteral(0),
                new IntegerLiteral(1234)
        };
        IntegerLiteral[] days = new IntegerLiteral[] {
                new IntegerLiteral(45), new IntegerLiteral(168),
                new IntegerLiteral(99), new IntegerLiteral(1),
                new IntegerLiteral(234)
        };
        for (IntegerLiteral literal : Stream.concat(Arrays.stream(intForTest), Arrays.stream(years))
                .collect(Collectors.toList())) {
            for (IntegerLiteral literal1 : Stream.concat(Arrays.stream(intForTest), Arrays.stream(days))
                    .collect(Collectors.toList())) {
                Assertions.assertEquals(DateTimeExtractAndTransform.makeDate(literal, literal1), answer[answerIdx++]);
            }
        }
    }
}
