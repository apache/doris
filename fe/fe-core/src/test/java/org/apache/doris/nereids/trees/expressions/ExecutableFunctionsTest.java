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

    @Test
    public void testGetDateFloor() {
        Object[][] answers = new Object[][] {
        };
        int answerIdx = 0;
        for (IntegerLiteral literal : integerLiterals) {
            for (int i = 0; i < dateTimeLiterals.length; ++i) {
                for (int j = 0; j < dateTimeLiterals.length; ++j) {
                    if (j != k) {
                        Assertions.assertEquals(ExecutableFunctions.yearCeil(dateTimeLiterals[i]),
                                answers[answerIdx][0]);
                        Assertions.assertEquals(ExecutableFunctions.yearCeil(dateTimeLiterals[i], literal),
                                answers[answerIdx][1]);
                        Assertions.assertEquals(ExecutableFunctions.yearCeil(dateTimeLiterals[i], dateTimeLiterals[j]),
                                answers[answerIdx][2]);
                        Assertions.assertEquals(ExecutableFunctions.yearCeil(dateTimeLiterals[i], literal, dateTimeLiterals[j]),
                                answers[answerIdx][2]);
                    }
                }
            }
        }
    }
}
